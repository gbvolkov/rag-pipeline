from __future__ import annotations

import json
import logging
import math
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Body, Depends, File, Form, Query, Request, UploadFile, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import db_session
from app.core.config import get_settings
from app.core.errors import ConflictError, NotFoundError, UnprocessableError
from app.models.entities import (
    Artifact,
    Job,
    JobEvent,
    Pipeline,
    PipelineIndexingConfig,
    PipelineInput,
    Project,
    Retriever,
    RetrieverSession,
    RetrievalResult,
    RetrievalResultItem,
)
from app.schemas.artifacts import ArtifactLineageOut, ArtifactOut, IndexListItem
from app.schemas.capabilities import CapabilityMatrixOut
from app.schemas.common import APIMessage, PaginatedResponse
from app.schemas.jobs import JobDetailOut, JobEventOut, JobOut, ReindexRequest
from app.schemas.pipelines import (
    PipelineCopyRequest,
    PipelineCreate,
    PipelineOut,
    PipelineValidationResultOut,
)
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
from app.services.capabilities import get_capabilities_response
from app.services.jobs import (
    assert_project_active,
    create_job,
    create_retriever,
    init_session,
    query_retriever,
    release_session,
    transition_job,
)
from app.services.pipeline_advisory_validator import validate_pipeline_advisory
from app.services.pipeline_validator import validate_indexing_params, validate_pipeline
from app.workers.tasks import run_mineru_job_task, run_pipeline_job_task, run_reindex_job_task

router = APIRouter()
logger = logging.getLogger(__name__)
_PRIMARY_SCORE_KEYS = (
    "score",
    "rerank_score",
    "similarity_score",
    "fuzzy_score",
    "max_similarity_score",
)


def _paginate_query(q, *, offset: int, limit: int):
    return q.offset(offset).limit(limit)


def _as_paginated(items: list[Any], total: int, offset: int, limit: int) -> dict[str, Any]:
    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "items": items,
    }


def _coerce_float(value: Any) -> float | None:
    try:
        converted = float(value)
    except Exception:
        return None
    if not math.isfinite(converted):
        return None
    return converted


def _extract_score_details(segment_payload: dict[str, Any]) -> dict[str, float]:
    details: dict[str, float] = {}

    raw_details = segment_payload.get("score_details")
    if isinstance(raw_details, dict):
        for key, value in raw_details.items():
            if not isinstance(key, str):
                continue
            converted = _coerce_float(value)
            if converted is not None:
                details[key] = converted

    metadata = segment_payload.get("metadata")
    if isinstance(metadata, dict):
        for key, value in metadata.items():
            if not isinstance(key, str) or key in details:
                continue
            if key != "score" and not key.endswith("_score"):
                continue
            converted = _coerce_float(value)
            if converted is not None:
                details[key] = converted

    return details


def _to_scored_segment(item: RetrievalResultItem) -> ScoredSegment:
    segment_payload = item.segment_payload if isinstance(item.segment_payload, dict) else {}
    score_details = _extract_score_details(segment_payload)
    score = _coerce_float(item.score)
    if score is None:
        for key in _PRIMARY_SCORE_KEYS:
            if key in score_details:
                score = score_details[key]
                break
    return ScoredSegment(
        score=score,
        similarity_score=score_details.get("similarity_score"),
        max_similarity_score=score_details.get("max_similarity_score"),
        fuzzy_score=score_details.get("fuzzy_score"),
        rerank_score=score_details.get("rerank_score"),
        score_details=score_details,
        segment=segment_payload,
        segment_artifact_id=item.segment_artifact_id,
    )


def _to_retrieval_result_out(result: RetrievalResult, result_items: list[RetrievalResultItem]) -> RetrievalResultOut:
    return RetrievalResultOut(
        id=result.id,
        project_id=result.project_id,
        retriever_id=result.retriever_id,
        query_text=result.query_text,
        top_k=result.top_k,
        created_at=result.created_at,
        items=[_to_scored_segment(item) for item in result_items],
    )


def _pipeline_out(row: Pipeline, *, validation_warnings=None) -> PipelineOut:
    out = PipelineOut.model_validate(row)
    if validation_warnings is not None:
        out.validation_warnings = list(validation_warnings)
    return out


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
    shape = validate_pipeline(payload)
    validation_warnings = validate_pipeline_advisory(payload)

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
    return _pipeline_out(pipeline, validation_warnings=validation_warnings)


@router.post("/projects/{pid}/pipelines/validate", response_model=PipelineValidationResultOut)
def validate_pipeline_endpoint(pid: str, payload: PipelineCreate, db: Session = Depends(db_session)):
    assert_project_active(db, pid)
    shape = validate_pipeline(payload)
    validation_warnings = validate_pipeline_advisory(payload)
    return PipelineValidationResultOut(shape=shape, validation_warnings=validation_warnings)


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
    items = [_pipeline_out(row) for row in rows]
    return _as_paginated(items, total, offset, limit)


@router.get("/projects/{pid}/pipelines/{plid}", response_model=PipelineOut)
def get_pipeline(pid: str, plid: str, db: Session = Depends(db_session)):
    row = db.get(Pipeline, plid)
    if row is None or row.project_id != pid or row.deleted:
        raise NotFoundError("Pipeline not found")
    return _pipeline_out(row)


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
    return _pipeline_out(copied)


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
    if "file_content_b64" in payload or "text" in payload:
        raise UnprocessableError(
            "Inline file_content_b64/text payloads are unsupported; upload a file or use runtime_input documents/segments",
        )
    if file is not None:
        content = await file.read()
        settings = get_settings()
        suffix = Path(file.filename or "upload.bin").suffix or ".bin"
        upload_dir = settings.local_blob_root / "_uploads"
        upload_dir.mkdir(parents=True, exist_ok=True)
        upload_path = upload_dir / f"{uuid4()}{suffix}"
        upload_path.write_bytes(content)
        payload["uploaded_file_path"] = str(upload_path)
        payload["file_name"] = file.filename or Path(payload["uploaded_file_path"]).name
    if "example_profile_id" in payload:
        raise UnprocessableError("example_profile_id is unsupported by the production API")

    job = create_job(
        db,
        project_id=pid,
        pipeline_id=plid,
        kind="run_pipeline",
        payload=payload,
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
        "Run submitted: project_id=%s pipeline_id=%s job_id=%s celery_task_id=%s payload_keys=%s uploaded_file=%s",
        pid,
        plid,
        job.id,
        job.celery_task_id,
        sorted(payload.keys()),
        payload.get("uploaded_file_path"),
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


@router.get("/projects/{pid}/pipelines/{plid}/graph-entities/{stage}", response_model=PaginatedResponse[ArtifactOut])
def list_graph_entities(
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
        Artifact.artifact_kind == "graph_entity",
        Artifact.stage_name == stage,
    ).order_by(Artifact.created_at.desc())
    if version is not None:
        stmt = stmt.where(Artifact.version == version)
    total = db.execute(select(func.count()).select_from(stmt.subquery())).scalar_one()
    rows = db.execute(_paginate_query(stmt, offset=offset, limit=limit)).scalars().all()
    return _as_paginated([ArtifactOut.model_validate(r) for r in rows], total, offset, limit)


@router.get("/projects/{pid}/graph-entities/{gid}", response_model=ArtifactOut)
def get_graph_entity(pid: str, gid: str, db: Session = Depends(db_session)):
    row = db.get(Artifact, gid)
    if row is None or row.project_id != pid or row.artifact_kind != "graph_entity":
        raise NotFoundError("Graph entity artifact not found")
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
            storage_backend=r.storage_backend,
            vector_collection_name=r.vector_collection_name,
            vector_persist_path=r.vector_persist_path,
            docstore_persist_path=r.docstore_persist_path,
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
    indexing = payload.indexing
    index_type = indexing.get("index_type")
    if not isinstance(index_type, str) or not index_type.strip():
        raise UnprocessableError("Reindex payload missing indexing.index_type")
    params = indexing.get("params", {})
    if params is None:
        params = {}
    if not isinstance(params, dict):
        raise UnprocessableError("Reindex payload indexing.params must be an object")
    validate_indexing_params(index_type, params, path=f"indexing.{index_type}.params")
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
        source_artifact_ids=payload.source_artifact_ids,
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
        session_id=payload.session_id,
    )
    items = db.execute(
        select(RetrievalResultItem).where(RetrievalResultItem.retrieval_result_id == result.id).order_by(RetrievalResultItem.rank.asc())
    ).scalars().all()
    scored = [_to_scored_segment(item) for item in items]
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
    session_id = payload.get("session_id") if payload is not None else None
    count = release_session(db, row, session_id=session_id)
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
        items.append(_to_retrieval_result_out(result, result_items))
    return _as_paginated(items, total, offset, limit)


@router.get("/projects/{pid}/retrieval-results/{rlid}", response_model=RetrievalResultOut)
def get_retrieval_result(pid: str, rlid: str, db: Session = Depends(db_session)):
    row = db.get(RetrievalResult, rlid)
    if row is None or row.project_id != pid:
        raise NotFoundError("Retrieval result not found")
    items = db.execute(
        select(RetrievalResultItem).where(RetrievalResultItem.retrieval_result_id == row.id).order_by(RetrievalResultItem.rank.asc())
    ).scalars().all()
    return _to_retrieval_result_out(row, items)


@router.get("/capabilities", response_model=CapabilityMatrixOut)
def get_capabilities():
    return get_capabilities_response()
