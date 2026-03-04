from __future__ import annotations

import base64
import json
import logging
from typing import Any

from fastapi import APIRouter, Body, Depends, File, Form, Query, Request, UploadFile, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import db_session
from app.core.errors import ConflictError, NotFoundError, UnprocessableError
from app.models.entities import (
    Artifact,
    Job,
    JobEvent,
    ExampleProfile,
    Pipeline,
    PipelineIndexingConfig,
    PipelineInput,
    PipelineSegmentationStage,
    Project,
    Retriever,
    RetrieverSession,
    RetrievalResult,
    RetrievalResultItem,
)
from app.schemas.artifacts import ArtifactLineageOut, ArtifactOut, IndexListItem
from app.schemas.capabilities import CapabilityMatrixOut, ExampleCapabilityMatrixOut
from app.schemas.common import APIMessage, PaginatedResponse
from app.schemas.jobs import JobDetailOut, JobEventOut, JobOut, ReindexRequest
from app.schemas.pipelines import PipelineCopyRequest, PipelineCreate, PipelineOut
from app.schemas.projects import ProjectCreate, ProjectOut, ProjectPatch, ProjectSummary
from app.schemas.retrievers import (
    RetrievalResultOut,
    RetrieverCreate,
    RetrieverOut,
    RetrieverQueryRequest,
    RetrieverSessionOut,
    ScoredSegment,
)
from app.services.artifacts import artifact_versions, build_lineage_backward, build_lineage_forward
from app.services.capabilities import capability_snapshot_to_response, get_or_create_capability_snapshot
from app.services.example_profiles import get_example_capability_matrix
from app.services.jobs import (
    assert_project_active,
    create_job,
    create_retriever,
    init_session,
    query_retriever,
    release_session,
    seed_example_profiles,
    transition_job,
)
from app.services.pipeline_validator import validate_pipeline
from app.workers.tasks import run_mineru_job_task, run_pipeline_job_task, run_reindex_job_task

router = APIRouter()
logger = logging.getLogger(__name__)


def _paginate_query(q, *, offset: int, limit: int):
    return q.offset(offset).limit(limit)


def _as_paginated(items: list[Any], total: int, offset: int, limit: int) -> dict[str, Any]:
    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "items": items,
    }


def _project_summary(db: Session, project_id: str) -> ProjectSummary:
    pipelines = db.execute(
        select(func.count(Pipeline.id)).where(Pipeline.project_id == project_id, Pipeline.deleted.is_(False))
    ).scalar_one()
    active_jobs = db.execute(
        select(func.count(Job.id)).where(
            Job.project_id == project_id,
            Job.status.in_(["queued", "running"]),
        )
    ).scalar_one()
    artifacts = db.execute(select(func.count(Artifact.id)).where(Artifact.project_id == project_id)).scalar_one()
    retrievers = db.execute(select(func.count(Retriever.id)).where(Retriever.project_id == project_id)).scalar_one()
    return ProjectSummary(pipelines=pipelines, active_jobs=active_jobs, artifacts=artifacts, retrievers=retrievers)


@router.post("/projects", response_model=ProjectOut, status_code=status.HTTP_201_CREATED)
def create_project(payload: ProjectCreate, db: Session = Depends(db_session)):
    row = Project(
        name=payload.name,
        description=payload.description,
        status="active",
        graph_store_config=payload.graph_store_config.model_dump() if payload.graph_store_config else None,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    out = ProjectOut.model_validate(row)
    out.summary = _project_summary(db, row.id)
    return out


@router.get("/projects", response_model=PaginatedResponse[ProjectOut])
def list_projects(
    status_filter: str | None = Query(default=None, alias="status"),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(db_session),
):
    stmt = select(Project)
    if status_filter:
        stmt = stmt.where(Project.status == status_filter)
    total = db.execute(select(func.count()).select_from(stmt.subquery())).scalar_one()
    rows = db.execute(_paginate_query(stmt.order_by(Project.created_at.desc()), offset=offset, limit=limit)).scalars().all()
    items = []
    for row in rows:
        out = ProjectOut.model_validate(row)
        out.summary = _project_summary(db, row.id)
        items.append(out)
    return _as_paginated(items, total, offset, limit)


@router.get("/projects/{pid}", response_model=ProjectOut)
def get_project(pid: str, db: Session = Depends(db_session)):
    row = db.get(Project, pid)
    if row is None:
        raise NotFoundError("Project not found")
    out = ProjectOut.model_validate(row)
    out.summary = _project_summary(db, row.id)
    return out


@router.patch("/projects/{pid}", response_model=ProjectOut)
def patch_project(pid: str, payload: ProjectPatch, db: Session = Depends(db_session)):
    row = db.get(Project, pid)
    if row is None:
        raise NotFoundError("Project not found")
    if row.status != "active":
        raise ConflictError("Archived project is read-only")

    data = payload.model_dump(exclude_unset=True)
    if "name" in data:
        row.name = data["name"]
    if "description" in data:
        row.description = data["description"]
    if "graph_store_config" in data:
        gsc = data["graph_store_config"]
        row.graph_store_config = gsc.model_dump() if gsc else None
    db.add(row)
    db.commit()
    db.refresh(row)
    out = ProjectOut.model_validate(row)
    out.summary = _project_summary(db, row.id)
    return out


@router.post("/projects/{pid}/archive", response_model=APIMessage)
def archive_project(pid: str, db: Session = Depends(db_session)):
    row = db.get(Project, pid)
    if row is None:
        raise NotFoundError("Project not found")
    if row.status == "archived":
        return APIMessage(message="Project already archived")

    row.status = "archived"
    db.add(row)
    # Cancel active jobs.
    active_jobs = db.execute(
        select(Job).where(Job.project_id == pid, Job.status.in_(["queued", "running"]))
    ).scalars().all()
    for job in active_jobs:
        job.canceled = True
        transition_job(db, job, status="canceled", stage=job.stage, message="Canceled by project archive")
    db.commit()
    return APIMessage(message="Project archived")


@router.post("/projects/{pid}/pipelines", response_model=PipelineOut, status_code=status.HTTP_201_CREATED)
def create_pipeline(pid: str, payload: PipelineCreate, db: Session = Depends(db_session)):
    assert_project_active(db, pid)
    snapshot = get_or_create_capability_snapshot(db)
    shape = validate_pipeline(payload, snapshot.capability_matrix)

    pipeline = Pipeline(
        project_id=pid,
        name=payload.name,
        description=payload.description,
        shape=shape,
        immutable=True,
        deleted=False,
        definition=payload.model_dump(),
    )
    db.add(pipeline)
    db.flush()

    for ref in payload.inputs:
        db.add(
            PipelineInput(
                pipeline_id=pipeline.id,
                alias=ref.alias,
                source_pipeline_id=ref.source_pipeline_id,
                source_stage_name=ref.source_stage_name,
                artifact_kind=ref.artifact_kind,
                pinned_version=ref.pinned_version,
            )
        )

    for stage in payload.segmentation_stages:
        db.add(
            PipelineSegmentationStage(
                pipeline_id=pipeline.id,
                stage_name=stage.stage_name,
                splitter_type=stage.splitter_type,
                params=stage.params,
                input_aliases=stage.input_aliases,
                position=stage.position,
            )
        )

    if payload.indexing is not None:
        db.add(
            PipelineIndexingConfig(
                pipeline_id=pipeline.id,
                index_type=payload.indexing.index_type,
                params=payload.indexing.params,
                collection_name=payload.indexing.collection_name,
                docstore_name=payload.indexing.docstore_name,
            )
        )

    db.commit()
    db.refresh(pipeline)
    return PipelineOut.model_validate(pipeline)


@router.get("/projects/{pid}/pipelines", response_model=PaginatedResponse[PipelineOut])
def list_pipelines(
    pid: str,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(db_session),
):
    stmt = select(Pipeline).where(Pipeline.project_id == pid, Pipeline.deleted.is_(False)).order_by(Pipeline.created_at.desc())
    total = db.execute(select(func.count()).select_from(stmt.subquery())).scalar_one()
    rows = db.execute(_paginate_query(stmt, offset=offset, limit=limit)).scalars().all()
    items = [PipelineOut.model_validate(row) for row in rows]
    return _as_paginated(items, total, offset, limit)


@router.get("/projects/{pid}/pipelines/{plid}", response_model=PipelineOut)
def get_pipeline(pid: str, plid: str, db: Session = Depends(db_session)):
    row = db.get(Pipeline, plid)
    if row is None or row.project_id != pid or row.deleted:
        raise NotFoundError("Pipeline not found")
    return PipelineOut.model_validate(row)


@router.post("/projects/{pid}/pipelines/{plid}/copy", response_model=PipelineOut, status_code=status.HTTP_201_CREATED)
def copy_pipeline(pid: str, plid: str, payload: PipelineCopyRequest, db: Session = Depends(db_session)):
    assert_project_active(db, pid)
    source = db.get(Pipeline, plid)
    if source is None or source.project_id != pid or source.deleted:
        raise NotFoundError("Pipeline not found")

    copied = Pipeline(
        project_id=pid,
        name=payload.name,
        description=payload.description if payload.description is not None else source.description,
        shape=source.shape,
        immutable=True,
        deleted=False,
        definition=source.definition,
    )
    db.add(copied)
    db.flush()

    for row in db.execute(select(PipelineInput).where(PipelineInput.pipeline_id == source.id)).scalars():
        db.add(
            PipelineInput(
                pipeline_id=copied.id,
                alias=row.alias,
                source_pipeline_id=row.source_pipeline_id,
                source_stage_name=row.source_stage_name,
                artifact_kind=row.artifact_kind,
                pinned_version=row.pinned_version,
            )
        )

    for row in db.execute(
        select(PipelineSegmentationStage).where(PipelineSegmentationStage.pipeline_id == source.id)
    ).scalars():
        db.add(
            PipelineSegmentationStage(
                pipeline_id=copied.id,
                stage_name=row.stage_name,
                splitter_type=row.splitter_type,
                params=row.params,
                input_aliases=row.input_aliases,
                position=row.position,
            )
        )

    idx = db.execute(select(PipelineIndexingConfig).where(PipelineIndexingConfig.pipeline_id == source.id)).scalar_one_or_none()
    if idx:
        db.add(
            PipelineIndexingConfig(
                pipeline_id=copied.id,
                index_type=idx.index_type,
                params=idx.params,
                collection_name=idx.collection_name,
                docstore_name=idx.docstore_name,
            )
        )

    db.commit()
    db.refresh(copied)
    return PipelineOut.model_validate(copied)


@router.delete("/projects/{pid}/pipelines/{plid}", response_model=APIMessage)
def delete_pipeline(pid: str, plid: str, db: Session = Depends(db_session)):
    row = db.get(Pipeline, plid)
    if row is None or row.project_id != pid or row.deleted:
        raise NotFoundError("Pipeline not found")
    row.deleted = True
    db.add(row)

    active_jobs = db.execute(
        select(Job).where(Job.pipeline_id == plid, Job.status.in_(["queued", "running"]))
    ).scalars().all()
    for job in active_jobs:
        job.canceled = True
        transition_job(db, job, status="canceled", stage=job.stage, message="Canceled by pipeline delete")
    db.commit()
    return APIMessage(message="Pipeline deleted")


@router.post("/projects/{pid}/pipelines/{plid}/runs", status_code=status.HTTP_202_ACCEPTED)
async def submit_run(
    pid: str,
    plid: str,
    request: Request,
    db: Session = Depends(db_session),
    file: UploadFile | None = File(default=None),
    form_payload: str | None = Form(default=None),
):
    assert_project_active(db, pid)
    pipeline = db.get(Pipeline, plid)
    if pipeline is None or pipeline.project_id != pid or pipeline.deleted:
        raise NotFoundError("Pipeline not found")

    payload: dict[str, Any] = {}
    content_type = request.headers.get("content-type", "")
    if content_type.startswith("application/json"):
        try:
            parsed = await request.json()
            if isinstance(parsed, dict):
                payload.update(parsed)
        except Exception as exc:
            raise UnprocessableError("Invalid JSON payload", details={"error": str(exc)}) from exc
    if form_payload:
        try:
            form_dict = json.loads(form_payload)
            payload.update(form_dict)
        except json.JSONDecodeError as exc:
            raise UnprocessableError("Invalid form payload JSON", details={"error": str(exc)}) from exc
    if file is not None:
        content = await file.read()
        payload["file_name"] = file.filename or "upload.bin"
        payload["file_content_b64"] = base64.b64encode(content).decode("ascii")

    example_profile_id = None
    if payload.get("example_profile_id"):
        profile_row = db.execute(
            select(ExampleProfile).where(ExampleProfile.profile_id == payload["example_profile_id"])
        ).scalar_one_or_none()
        if profile_row:
            example_profile_id = profile_row.id

    job = create_job(
        db,
        project_id=pid,
        pipeline_id=plid,
        kind="run_pipeline",
        payload=payload,
        example_profile_id=example_profile_id,
    )

    loader_type = (pipeline.definition.get("loader") or {}).get("type")
    if loader_type == "MinerULoader":
        async_result = run_mineru_job_task.delay(job.id)
    else:
        async_result = run_pipeline_job_task.delay(job.id)
    job.celery_task_id = async_result.id
    db.add(job)
    db.commit()
    logger.info(
        "Run submitted: project_id=%s pipeline_id=%s job_id=%s celery_task_id=%s payload_keys=%s file_b64_len=%s",
        pid,
        plid,
        job.id,
        job.celery_task_id,
        sorted(payload.keys()),
        len(payload.get("file_content_b64", "")) if isinstance(payload.get("file_content_b64"), str) else 0,
    )
    return {"job_id": job.id}


@router.get("/projects/{pid}/pipelines/{plid}/runs", response_model=PaginatedResponse[JobOut])
def list_runs(
    pid: str,
    plid: str,
    status_filter: str | None = Query(default=None, alias="status"),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(db_session),
):
    stmt = select(Job).where(Job.project_id == pid, Job.pipeline_id == plid).order_by(Job.created_at.desc())
    if status_filter:
        stmt = stmt.where(Job.status == status_filter)
    total = db.execute(select(func.count()).select_from(stmt.subquery())).scalar_one()
    rows = db.execute(_paginate_query(stmt, offset=offset, limit=limit)).scalars().all()
    items = [JobOut.model_validate(row) for row in rows]
    return _as_paginated(items, total, offset, limit)


@router.get("/jobs/{job_id}", response_model=JobDetailOut)
def get_job(job_id: str, db: Session = Depends(db_session)):
    row = db.get(Job, job_id)
    if row is None:
        raise NotFoundError("Job not found")
    events = db.execute(select(JobEvent).where(JobEvent.job_id == row.id).order_by(JobEvent.created_at.asc())).scalars().all()
    payload_size = 0
    if isinstance(row.payload, dict):
        try:
            payload_size = len(json.dumps(row.payload, ensure_ascii=False))
        except Exception:
            payload_size = 0
    out = JobDetailOut.model_validate(row)
    out.events = [JobEventOut.model_validate(e) for e in events]
    logger.info(
        "Job details returned: job_id=%s status=%s stage=%s events=%s payload_bytes=%s",
        row.id,
        row.status,
        row.stage,
        len(events),
        payload_size,
    )
    return out


@router.post("/jobs/{job_id}/cancel", response_model=APIMessage)
def cancel_job(job_id: str, db: Session = Depends(db_session)):
    row = db.get(Job, job_id)
    if row is None:
        raise NotFoundError("Job not found")
    row.canceled = True
    db.add(row)
    if row.status in {"queued", "running"}:
        transition_job(db, row, status="canceled", stage=row.stage, message="Canceled by user")
    else:
        db.commit()
    return APIMessage(message="Job cancellation requested")


@router.get("/projects/{pid}/pipelines/{plid}/documents", response_model=PaginatedResponse[ArtifactOut])
def list_documents(
    pid: str,
    plid: str,
    version: int | None = Query(default=None, ge=1),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(db_session),
):
    stmt = select(Artifact).where(
        Artifact.project_id == pid,
        Artifact.pipeline_id == plid,
        Artifact.artifact_kind == "document",
    ).order_by(Artifact.created_at.desc())
    if version is not None:
        stmt = stmt.where(Artifact.version == version)
    total = db.execute(select(func.count()).select_from(stmt.subquery())).scalar_one()
    rows = db.execute(_paginate_query(stmt, offset=offset, limit=limit)).scalars().all()
    return _as_paginated([ArtifactOut.model_validate(r) for r in rows], total, offset, limit)


@router.get("/projects/{pid}/documents/{did}", response_model=ArtifactOut)
def get_document(pid: str, did: str, db: Session = Depends(db_session)):
    row = db.get(Artifact, did)
    if row is None or row.project_id != pid or row.artifact_kind != "document":
        raise NotFoundError("Document artifact not found")
    return ArtifactOut.model_validate(row)


@router.get("/projects/{pid}/pipelines/{plid}/segments/{stage}", response_model=PaginatedResponse[ArtifactOut])
def list_segments(
    pid: str,
    plid: str,
    stage: str,
    version: int | None = Query(default=None, ge=1),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(db_session),
):
    stmt = select(Artifact).where(
        Artifact.project_id == pid,
        Artifact.pipeline_id == plid,
        Artifact.artifact_kind == "segment",
        Artifact.stage_name == stage,
    ).order_by(Artifact.created_at.desc())
    if version is not None:
        stmt = stmt.where(Artifact.version == version)
    total = db.execute(select(func.count()).select_from(stmt.subquery())).scalar_one()
    rows = db.execute(_paginate_query(stmt, offset=offset, limit=limit)).scalars().all()
    return _as_paginated([ArtifactOut.model_validate(r) for r in rows], total, offset, limit)


@router.get("/projects/{pid}/segments/{sid}", response_model=ArtifactOut)
def get_segment(pid: str, sid: str, db: Session = Depends(db_session)):
    row = db.get(Artifact, sid)
    if row is None or row.project_id != pid or row.artifact_kind != "segment":
        raise NotFoundError("Segment artifact not found")
    return ArtifactOut.model_validate(row)


@router.get("/projects/{pid}/indices", response_model=PaginatedResponse[IndexListItem])
def list_indices(
    pid: str,
    pipeline_id: str | None = Query(default=None),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(db_session),
):
    stmt = select(Artifact).where(
        Artifact.project_id == pid,
        Artifact.artifact_kind == "index",
    ).order_by(Artifact.created_at.desc())
    if pipeline_id:
        stmt = stmt.where(Artifact.pipeline_id == pipeline_id)
    total = db.execute(select(func.count()).select_from(stmt.subquery())).scalar_one()
    rows = db.execute(_paginate_query(stmt, offset=offset, limit=limit)).scalars().all()
    items = [
        IndexListItem(
            artifact_id=r.id,
            pipeline_id=r.pipeline_id,
            stage_name=r.stage_name,
            artifact_key=r.artifact_key,
            version=r.version,
            metadata_json=r.metadata_json,
            created_at=r.created_at,
        )
        for r in rows
    ]
    return _as_paginated(items, total, offset, limit)


@router.get("/projects/{pid}/indices/{iid}", response_model=ArtifactOut)
def get_index(pid: str, iid: str, version: int | None = Query(default=None, ge=1), db: Session = Depends(db_session)):
    row = db.get(Artifact, iid)
    if row is None or row.project_id != pid or row.artifact_kind != "index":
        raise NotFoundError("Index artifact not found")
    if version is not None and row.version != version:
        row = db.execute(
            select(Artifact).where(
                Artifact.project_id == pid,
                Artifact.artifact_kind == "index",
                Artifact.artifact_key == row.artifact_key,
                Artifact.version == version,
            )
        ).scalar_one_or_none()
        if row is None:
            raise NotFoundError("Index artifact version not found")
    return ArtifactOut.model_validate(row)


@router.post("/projects/{pid}/reindex", status_code=status.HTTP_202_ACCEPTED)
def submit_reindex(pid: str, payload: ReindexRequest, db: Session = Depends(db_session)):
    assert_project_active(db, pid)
    job = create_job(
        db,
        project_id=pid,
        pipeline_id=None,
        kind="reindex",
        payload=payload.model_dump(),
    )
    async_result = run_reindex_job_task.delay(job.id)
    job.celery_task_id = async_result.id
    db.add(job)
    db.commit()
    return {"job_id": job.id}


@router.get("/projects/{pid}/artifacts/{aid}/lineage", response_model=ArtifactLineageOut)
def artifact_lineage(pid: str, aid: str, db: Session = Depends(db_session)):
    artifact = db.get(Artifact, aid)
    if artifact is None or artifact.project_id != pid:
        raise NotFoundError("Artifact not found")
    inputs = build_lineage_backward(db, aid)
    return ArtifactLineageOut(
        artifact=ArtifactOut.model_validate(artifact),
        inputs=[ArtifactOut.model_validate(x) for x in inputs],
        dependents=[],
    )


@router.get("/projects/{pid}/artifacts/{aid}/dependents", response_model=ArtifactLineageOut)
def artifact_dependents(pid: str, aid: str, db: Session = Depends(db_session)):
    artifact = db.get(Artifact, aid)
    if artifact is None or artifact.project_id != pid:
        raise NotFoundError("Artifact not found")
    dependents = build_lineage_forward(db, aid)
    return ArtifactLineageOut(
        artifact=ArtifactOut.model_validate(artifact),
        inputs=[],
        dependents=[ArtifactOut.model_validate(x) for x in dependents],
    )


@router.get("/projects/{pid}/artifacts/{aid}/lineage/versions", response_model=list[ArtifactOut])
def artifact_lineage_versions(pid: str, aid: str, db: Session = Depends(db_session)):
    artifact = db.get(Artifact, aid)
    if artifact is None or artifact.project_id != pid:
        raise NotFoundError("Artifact not found")
    rows = artifact_versions(db, artifact)
    return [ArtifactOut.model_validate(x) for x in rows]


@router.post("/projects/{pid}/retrievers", response_model=RetrieverOut, status_code=status.HTTP_201_CREATED)
def create_retriever_endpoint(pid: str, payload: RetrieverCreate, db: Session = Depends(db_session)):
    assert_project_active(db, pid)
    row = create_retriever(
        db,
        project_id=pid,
        index_artifact_id=payload.index_artifact_id,
        retriever_type=payload.retriever_type,
        params=payload.params,
    )
    return RetrieverOut.model_validate(row)


@router.get("/projects/{pid}/retrievers", response_model=PaginatedResponse[RetrieverOut])
def list_retrievers(
    pid: str,
    index_id: str | None = Query(default=None),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(db_session),
):
    stmt = select(Retriever).where(Retriever.project_id == pid).order_by(Retriever.created_at.desc())
    if index_id:
        stmt = stmt.where(Retriever.index_artifact_id == index_id)
    total = db.execute(select(func.count()).select_from(stmt.subquery())).scalar_one()
    rows = db.execute(_paginate_query(stmt, offset=offset, limit=limit)).scalars().all()
    return _as_paginated([RetrieverOut.model_validate(r) for r in rows], total, offset, limit)


@router.get("/projects/{pid}/retrievers/{rid}", response_model=RetrieverOut)
def get_retriever(pid: str, rid: str, db: Session = Depends(db_session)):
    row = db.get(Retriever, rid)
    if row is None or row.project_id != pid:
        raise NotFoundError("Retriever not found")
    return RetrieverOut.model_validate(row)


@router.delete("/projects/{pid}/retrievers/{rid}", response_model=APIMessage)
def delete_retriever(pid: str, rid: str, db: Session = Depends(db_session)):
    row = db.get(Retriever, rid)
    if row is None or row.project_id != pid:
        raise NotFoundError("Retriever not found")
    db.query(RetrieverSession).filter(RetrieverSession.retriever_id == rid).delete()
    db.delete(row)
    db.commit()
    return APIMessage(message="Retriever deleted")


@router.post("/projects/{pid}/retrievers/{rid}/init", response_model=RetrieverSessionOut)
def init_retriever_session(pid: str, rid: str, db: Session = Depends(db_session)):
    row = db.get(Retriever, rid)
    if row is None or row.project_id != pid:
        raise NotFoundError("Retriever not found")
    session = init_session(db, row)
    return RetrieverSessionOut.model_validate(session)


@router.post("/projects/{pid}/retrievers/{rid}/query")
def query_retriever_endpoint(
    pid: str,
    rid: str,
    payload: RetrieverQueryRequest,
    db: Session = Depends(db_session),
):
    row = db.get(Retriever, rid)
    if row is None or row.project_id != pid:
        raise NotFoundError("Retriever not found")
    result = query_retriever(
        db,
        project_id=pid,
        retriever=row,
        query_text=payload.query,
        top_k=payload.top_k,
        session_id=payload.session_id,
    )
    items = db.execute(
        select(RetrievalResultItem).where(RetrievalResultItem.retrieval_result_id == result.id).order_by(RetrievalResultItem.rank.asc())
    ).scalars().all()
    scored = [
        ScoredSegment(
            score=item.score,
            segment=item.segment_payload,
            segment_artifact_id=item.segment_artifact_id,
        )
        for item in items
    ]
    return {
        "retrieval_result_id": result.id,
        "items": [s.model_dump() for s in scored],
    }


@router.post("/projects/{pid}/retrievers/{rid}/release", response_model=APIMessage)
def release_retriever_endpoint(
    pid: str,
    rid: str,
    payload: dict[str, Any] | None = Body(default=None),
    db: Session = Depends(db_session),
):
    row = db.get(Retriever, rid)
    if row is None or row.project_id != pid:
        raise NotFoundError("Retriever not found")
    count = release_session(db, row, session_id=(payload or {}).get("session_id"))
    return APIMessage(message=f"Released {count} session(s)")


@router.get("/projects/{pid}/retrievers/{rid}/results", response_model=PaginatedResponse[RetrievalResultOut])
def list_retrieval_results(
    pid: str,
    rid: str,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(db_session),
):
    row = db.get(Retriever, rid)
    if row is None or row.project_id != pid:
        raise NotFoundError("Retriever not found")
    stmt = select(RetrievalResult).where(RetrievalResult.retriever_id == rid).order_by(RetrievalResult.created_at.desc())
    total = db.execute(select(func.count()).select_from(stmt.subquery())).scalar_one()
    rows = db.execute(_paginate_query(stmt, offset=offset, limit=limit)).scalars().all()
    items = []
    for result in rows:
        result_items = db.execute(
            select(RetrievalResultItem)
            .where(RetrievalResultItem.retrieval_result_id == result.id)
            .order_by(RetrievalResultItem.rank.asc())
        ).scalars().all()
        out = RetrievalResultOut.model_validate(result)
        out.items = [
            ScoredSegment(
                score=item.score,
                segment=item.segment_payload,
                segment_artifact_id=item.segment_artifact_id,
            )
            for item in result_items
        ]
        items.append(out)
    return _as_paginated(items, total, offset, limit)


@router.get("/projects/{pid}/retrieval-results/{rlid}", response_model=RetrievalResultOut)
def get_retrieval_result(pid: str, rlid: str, db: Session = Depends(db_session)):
    row = db.get(RetrievalResult, rlid)
    if row is None or row.project_id != pid:
        raise NotFoundError("Retrieval result not found")
    items = db.execute(
        select(RetrievalResultItem).where(RetrievalResultItem.retrieval_result_id == row.id).order_by(RetrievalResultItem.rank.asc())
    ).scalars().all()
    out = RetrievalResultOut.model_validate(row)
    out.items = [
        ScoredSegment(score=item.score, segment=item.segment_payload, segment_artifact_id=item.segment_artifact_id)
        for item in items
    ]
    return out


@router.get("/capabilities", response_model=CapabilityMatrixOut)
def get_capabilities(db: Session = Depends(db_session)):
    snapshot = get_or_create_capability_snapshot(db)
    return capability_snapshot_to_response(snapshot)


@router.get("/capabilities/examples", response_model=ExampleCapabilityMatrixOut)
def get_examples_capabilities(db: Session = Depends(db_session)):
    seed_example_profiles(db)
    return get_example_capability_matrix(db)
