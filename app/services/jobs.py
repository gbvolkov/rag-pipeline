from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.errors import ConflictError, NotFoundError, ServiceUnavailableError, UnprocessableError
from app.models.entities import (
    Artifact,
    ArtifactInput,
    CapabilitySnapshot,
    ExampleProfile,
    Job,
    JobEvent,
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
from app.services.artifacts import create_artifact
from app.services.capabilities import get_or_create_capability_snapshot
from app.services.example_profiles import sync_profiles
from app.services.rag_adapter import (
    create_retriever_runtime,
    execute_retriever_query,
    build_index,
    init_retriever_session,
    release_retriever_session,
    run_loader,
    run_splitter,
)


STAGE_INPUT_RESOLUTION = "INPUT_RESOLUTION"
STAGE_LOADING = "LOADING"
STAGE_SEGMENTING = "SEGMENTING"
STAGE_INDEXING = "INDEXING"
logger = logging.getLogger(__name__)


@dataclass
class StageOutput:
    kind: str
    payload: list[dict[str, Any]]
    artifact_ids: list[str]


def _now() -> datetime:
    return datetime.now(tz=UTC)


def create_job(
    db: Session,
    *,
    project_id: str,
    pipeline_id: str | None,
    kind: str,
    payload: dict[str, Any] | None = None,
    example_profile_id: str | None = None,
) -> Job:
    snapshot = get_or_create_capability_snapshot(db)
    job = Job(
        project_id=project_id,
        pipeline_id=pipeline_id,
        kind=kind,
        status="queued",
        stage=None,
        payload=payload,
        capability_snapshot_id=snapshot.id,
        example_profile_snapshot_id=example_profile_id,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    add_job_event(db, job.id, status="queued", stage=None, message="Job queued")
    return job


def add_job_event(
    db: Session,
    job_id: str,
    *,
    status: str,
    stage: str | None,
    message: str | None = None,
    data: dict[str, Any] | None = None,
) -> None:
    db.add(JobEvent(job_id=job_id, status=status, stage=stage, message=message, data=data))
    db.commit()


def transition_job(
    db: Session,
    job: Job,
    *,
    status: str,
    stage: str | None = None,
    message: str | None = None,
    data: dict[str, Any] | None = None,
) -> None:
    job.status = status
    job.stage = stage
    if status == "running" and job.started_at is None:
        job.started_at = _now()
    if status in {"failed", "succeeded", "canceled"}:
        job.finished_at = _now()
    db.add(job)
    db.commit()
    logger.info(
        "Job transition: job_id=%s status=%s stage=%s message=%s",
        job.id,
        status,
        stage,
        message,
    )
    add_job_event(db, job.id, status=status, stage=stage, message=message, data=data)


def assert_project_active(db: Session, project_id: str) -> Project:
    project = db.get(Project, project_id)
    if project is None:
        raise NotFoundError("Project not found")
    if project.status != "active":
        raise ConflictError("Project is archived and read-only")
    return project


def _get_pipeline_or_404(db: Session, project_id: str, pipeline_id: str) -> Pipeline:
    pipeline = db.get(Pipeline, pipeline_id)
    if pipeline is None or pipeline.project_id != project_id or pipeline.deleted:
        raise NotFoundError("Pipeline not found")
    return pipeline


def _get_stage_rows(db: Session, pipeline_id: str) -> list[PipelineSegmentationStage]:
    return db.execute(
        select(PipelineSegmentationStage)
        .where(PipelineSegmentationStage.pipeline_id == pipeline_id)
        .order_by(PipelineSegmentationStage.position.asc())
    ).scalars().all()


def _get_input_rows(db: Session, pipeline_id: str) -> list[PipelineInput]:
    return db.execute(select(PipelineInput).where(PipelineInput.pipeline_id == pipeline_id)).scalars().all()


def _get_indexing_row(db: Session, pipeline_id: str) -> PipelineIndexingConfig | None:
    return db.execute(
        select(PipelineIndexingConfig).where(PipelineIndexingConfig.pipeline_id == pipeline_id)
    ).scalar_one_or_none()


def _load_source_artifacts_for_input(db: Session, project_id: str, ref: PipelineInput) -> list[Artifact]:
    if ref.pinned_version is not None:
        rows = db.execute(
            select(Artifact).where(
                Artifact.project_id == project_id,
                Artifact.pipeline_id == ref.source_pipeline_id,
                Artifact.stage_name == ref.source_stage_name,
                Artifact.artifact_kind == ref.artifact_kind,
                Artifact.version == ref.pinned_version,
            )
        ).scalars().all()
        return rows

    # Latest by artifact_key.
    subq = (
        select(
            Artifact.artifact_key,
            func.max(Artifact.version).label("max_version"),
        )
        .where(
            Artifact.project_id == project_id,
            Artifact.pipeline_id == ref.source_pipeline_id,
            Artifact.stage_name == ref.source_stage_name,
            Artifact.artifact_kind == ref.artifact_kind,
        )
        .group_by(Artifact.artifact_key)
        .subquery()
    )
    return db.execute(
        select(Artifact).join(
            subq,
            and_(
                Artifact.artifact_key == subq.c.artifact_key,
                Artifact.version == subq.c.max_version,
            ),
        )
    ).scalars().all()


def _artifact_to_document_payload(artifact: Artifact) -> dict[str, Any]:
    if artifact.content_json and "metadata" in artifact.content_json:
        metadata = artifact.content_json["metadata"] or {}
    else:
        metadata = artifact.metadata_json or {}
    return {"content": artifact.content_text or "", "metadata": metadata}


def _artifact_to_segment_payload(artifact: Artifact) -> dict[str, Any]:
    if artifact.content_json:
        data = dict(artifact.content_json)
        if "content" not in data and artifact.content_text:
            data["content"] = artifact.content_text
        return data
    return {
        "content": artifact.content_text or "",
        "metadata": artifact.metadata_json or {},
        "segment_id": artifact.metadata_json.get("segment_id") if artifact.metadata_json else None,
        "parent_id": artifact.metadata_json.get("parent_id") if artifact.metadata_json else None,
        "level": artifact.metadata_json.get("level", 0) if artifact.metadata_json else 0,
        "path": artifact.metadata_json.get("path", []) if artifact.metadata_json else [],
        "type": artifact.metadata_json.get("type", "text") if artifact.metadata_json else "text",
        "original_format": artifact.metadata_json.get("original_format", "text")
        if artifact.metadata_json
        else "text",
    }


def _persist_documents(
    db: Session,
    *,
    project_id: str,
    pipeline_id: str,
    job_id: str,
    stage_name: str,
    docs: list[dict[str, Any]],
    input_artifact_ids: list[str],
) -> StageOutput:
    artifact_ids: list[str] = []
    for idx, doc in enumerate(docs):
        artifact = create_artifact(
            db,
            project_id=project_id,
            pipeline_id=pipeline_id,
            job_id=job_id,
            artifact_kind="document",
            stage_name=stage_name,
            artifact_key=f"doc_{idx}",
            content_text=doc.get("content", ""),
            content_json={"metadata": doc.get("metadata") or {}},
            metadata_json=doc.get("metadata") or {},
            input_artifact_ids=input_artifact_ids,
        )
        artifact_ids.append(artifact.id)
    db.commit()
    return StageOutput(kind="document", payload=docs, artifact_ids=artifact_ids)


def _persist_segments(
    db: Session,
    *,
    project_id: str,
    pipeline_id: str,
    job_id: str,
    stage_name: str,
    segments: list[dict[str, Any]],
    input_artifact_ids: list[str],
) -> StageOutput:
    artifact_ids: list[str] = []
    for idx, seg in enumerate(segments):
        artifact = create_artifact(
            db,
            project_id=project_id,
            pipeline_id=pipeline_id,
            job_id=job_id,
            artifact_kind="segment",
            stage_name=stage_name,
            artifact_key=f"seg_{idx}",
            content_text=seg.get("content", ""),
            content_json=seg,
            metadata_json=seg.get("metadata") or {},
            input_artifact_ids=input_artifact_ids,
        )
        artifact_ids.append(artifact.id)
    db.commit()
    return StageOutput(kind="segment", payload=segments, artifact_ids=artifact_ids)


def _persist_index(
    db: Session,
    *,
    project_id: str,
    pipeline_id: str,
    job_id: str,
    stage_name: str,
    index_payload: dict[str, Any],
    input_artifact_ids: list[str],
) -> Artifact:
    artifact = create_artifact(
        db,
        project_id=project_id,
        pipeline_id=pipeline_id,
        job_id=job_id,
        artifact_kind="index",
        stage_name=stage_name,
        artifact_key=index_payload.get("collection_name", "index"),
        content_text=None,
        content_json=index_payload,
        metadata_json=index_payload,
        input_artifact_ids=input_artifact_ids,
    )
    db.commit()
    db.refresh(artifact)
    return artifact


def run_pipeline_job(db: Session, job_id: str) -> Job:
    job = db.get(Job, job_id)
    if job is None:
        raise NotFoundError("Job not found")
    logger.info(
        "Pipeline job execution started: job_id=%s project_id=%s pipeline_id=%s",
        job.id,
        job.project_id,
        job.pipeline_id,
    )
    if job.canceled:
        transition_job(db, job, status="canceled", stage=job.stage, message="Job canceled before execution")
        return job

    pipeline = _get_pipeline_or_404(db, job.project_id, job.pipeline_id or "")
    definition = pipeline.definition
    stage_rows = _get_stage_rows(db, pipeline.id)
    input_rows = _get_input_rows(db, pipeline.id)
    indexing_row = _get_indexing_row(db, pipeline.id)
    capabilities = (
        db.get(CapabilitySnapshot, job.capability_snapshot_id).capability_matrix
        if job.capability_snapshot_id
        else {}
    )

    outputs: dict[str, StageOutput] = {}
    artifacts_produced = {"document": 0, "segment": 0, "index": 0}

    try:
        transition_job(db, job, status="running", stage=STAGE_INPUT_RESOLUTION, message="Resolving inputs")

        for ref in input_rows:
            artifacts = _load_source_artifacts_for_input(db, job.project_id, ref)
            if not artifacts:
                raise UnprocessableError(
                    f"No source artifacts found for alias '{ref.alias}'",
                    details={
                        "source_pipeline_id": ref.source_pipeline_id,
                        "source_stage_name": ref.source_stage_name,
                        "artifact_kind": ref.artifact_kind,
                    },
                )
            if ref.artifact_kind == "document":
                payload = [_artifact_to_document_payload(a) for a in artifacts]
            elif ref.artifact_kind == "segment":
                payload = [_artifact_to_segment_payload(a) for a in artifacts]
            else:
                payload = [a.content_json or {} for a in artifacts]

            outputs[ref.alias] = StageOutput(
                kind=ref.artifact_kind,
                payload=payload,
                artifact_ids=[a.id for a in artifacts],
            )

        # LOADING stage
        loader_cfg = definition.get("loader")
        if loader_cfg:
            transition_job(db, job, status="running", stage=STAGE_LOADING, message="Loading documents")
            loader_type = loader_cfg["type"]
            logger.info(
                "Running loader: job_id=%s loader_type=%s",
                job.id,
                loader_type,
            )
            loader_caps = capabilities.get("loaders", {})
            if loader_type not in loader_caps:
                raise UnprocessableError(
                    f"Loader '{loader_type}' is unavailable in capability snapshot",
                    details={"available": sorted(loader_caps.keys())},
                )
            docs = run_loader(
                loader_type=loader_type,
                module_name=loader_caps[loader_type]["module"],
                params=loader_cfg.get("params", {}),
                run_payload=job.payload or {},
            )
            inherited_artifact_ids = [aid for out in outputs.values() for aid in out.artifact_ids]
            persisted = _persist_documents(
                db,
                project_id=job.project_id,
                pipeline_id=pipeline.id,
                job_id=job.id,
                stage_name=STAGE_LOADING,
                docs=docs,
                input_artifact_ids=inherited_artifact_ids,
            )
            outputs[STAGE_LOADING] = persisted
            artifacts_produced["document"] += len(docs)

        # SEGMENTING stages
        previous_alias = STAGE_LOADING if loader_cfg else None
        for stage_row in stage_rows:
            transition_job(
                db,
                job,
                status="running",
                stage=STAGE_SEGMENTING,
                message=f"Segmenting stage '{stage_row.stage_name}'",
            )
            logger.info(
                "Running splitter: job_id=%s stage_name=%s splitter_type=%s",
                job.id,
                stage_row.stage_name,
                stage_row.splitter_type,
            )
            input_aliases = stage_row.input_aliases or ([previous_alias] if previous_alias else [])
            stage_inputs = [outputs[alias] for alias in input_aliases if alias in outputs]
            if not stage_inputs:
                raise UnprocessableError(
                    f"Stage '{stage_row.stage_name}' has no resolvable inputs",
                    details={"input_aliases": input_aliases},
                )

            source_docs: list[dict[str, Any]] = []
            source_segments: list[dict[str, Any]] = []
            source_artifact_ids: list[str] = []
            for src in stage_inputs:
                source_artifact_ids.extend(src.artifact_ids)
                if src.kind == "document":
                    source_docs.extend(src.payload)
                elif src.kind == "segment":
                    source_segments.extend(src.payload)
                else:
                    source_segments.extend(src.payload)

            splitter_caps = capabilities.get("splitters", {})
            if stage_row.splitter_type not in splitter_caps:
                raise UnprocessableError(
                    f"Splitter '{stage_row.splitter_type}' is unavailable in capability snapshot",
                    details={"available": sorted(splitter_caps.keys())},
                )

            segments = run_splitter(
                splitter_type=stage_row.splitter_type,
                module_name=splitter_caps[stage_row.splitter_type]["module"],
                params=stage_row.params,
                source_documents=source_docs if source_docs else None,
                source_segments=source_segments if source_segments else None,
            )
            persisted = _persist_segments(
                db,
                project_id=job.project_id,
                pipeline_id=pipeline.id,
                job_id=job.id,
                stage_name=stage_row.stage_name,
                segments=segments,
                input_artifact_ids=source_artifact_ids,
            )
            outputs[stage_row.stage_name] = persisted
            previous_alias = stage_row.stage_name
            artifacts_produced["segment"] += len(segments)

        # INDEXING stage
        if indexing_row is not None:
            transition_job(db, job, status="running", stage=STAGE_INDEXING, message="Building index")
            logger.info(
                "Building index: job_id=%s index_type=%s collection_name=%s",
                job.id,
                indexing_row.index_type,
                indexing_row.collection_name,
            )
            if stage_rows:
                index_source = outputs[stage_rows[-1].stage_name]
            else:
                # Indexing-only with segment inputs.
                segment_sources = [out for out in outputs.values() if out.kind == "segment"]
                if segment_sources:
                    payload = [s for out in segment_sources for s in out.payload]
                    artifact_ids = [a for out in segment_sources for a in out.artifact_ids]
                    index_source = StageOutput(kind="segment", payload=payload, artifact_ids=artifact_ids)
                else:
                    # Fallback conversion from documents to single-segment entries.
                    document_sources = [out for out in outputs.values() if out.kind == "document"]
                    if not document_sources:
                        raise UnprocessableError("Indexing stage requires segment or document inputs")
                    payload = []
                    artifact_ids = []
                    for out in document_sources:
                        artifact_ids.extend(out.artifact_ids)
                        for doc in out.payload:
                            payload.append(
                                {
                                    "content": doc.get("content", ""),
                                    "metadata": doc.get("metadata") or {},
                                    "segment_id": None,
                                    "parent_id": None,
                                    "level": 0,
                                    "path": [],
                                    "type": "text",
                                    "original_format": "text",
                                }
                            )
                    index_source = StageOutput(kind="segment", payload=payload, artifact_ids=artifact_ids)

            index_payload = build_index(
                index_artifact_id=f"{job.id}:{pipeline.id}:{indexing_row.index_type}",
                index_type=indexing_row.index_type,
                params=indexing_row.params,
                raw_segments=index_source.payload,
            )
            index_artifact = _persist_index(
                db,
                project_id=job.project_id,
                pipeline_id=pipeline.id,
                job_id=job.id,
                stage_name=STAGE_INDEXING,
                index_payload=index_payload,
                input_artifact_ids=index_source.artifact_ids,
            )
            artifacts_produced["index"] += 1
            # Re-bind runtime index ID to persisted artifact ID.
            from app.services.rag_adapter import INDEX_RUNTIME_REGISTRY

            key = f"{job.id}:{pipeline.id}:{indexing_row.index_type}"
            if key in INDEX_RUNTIME_REGISTRY:
                INDEX_RUNTIME_REGISTRY[index_artifact.id] = INDEX_RUNTIME_REGISTRY.pop(key)

        job.result = {
            "artifacts_produced": artifacts_produced,
            "pipeline_shape": pipeline.shape,
        }
        transition_job(db, job, status="succeeded", stage=job.stage, message="Pipeline run completed")
        logger.info(
            "Pipeline job execution completed: job_id=%s artifacts=%s",
            job.id,
            artifacts_produced,
        )
        return job
    except Exception as exc:
        message = str(exc)
        if not message and hasattr(exc, "message"):
            message = str(getattr(exc, "message"))
        error_payload = {
            "message": message,
            "type": type(exc).__name__,
        }
        rag_type = getattr(exc, "rag_lib_exception_type", None)
        if rag_type:
            error_payload["rag_lib_exception_type"] = rag_type
        job.error = error_payload
        db.add(job)
        db.commit()
        transition_job(db, job, status="failed", stage=job.stage, message="Pipeline run failed", data=error_payload)
        logger.exception("Pipeline job execution failed: job_id=%s error_type=%s", job.id, type(exc).__name__)
        if isinstance(exc, (UnprocessableError, ConflictError, NotFoundError, ServiceUnavailableError)):
            raise
        raise ServiceUnavailableError(
            message="Pipeline execution failed",
            details=error_payload,
            rag_lib_exception_type=type(exc).__name__,
        ) from exc


def run_reindex_job(db: Session, job_id: str) -> Job:
    job = db.get(Job, job_id)
    if job is None:
        raise NotFoundError("Job not found")
    payload = job.payload or {}
    transition_job(db, job, status="running", stage=STAGE_INDEXING, message="Reindex started")

    source_refs = payload.get("source_segments", [])
    source_artifacts: list[Artifact] = []
    for ref in source_refs:
        rows = db.execute(
            select(Artifact).where(
                Artifact.project_id == job.project_id,
                Artifact.pipeline_id == ref["pipeline_id"],
                Artifact.stage_name == ref["stage_name"],
                Artifact.artifact_kind == "segment",
                Artifact.version == ref["version"],
            )
        ).scalars().all()
        source_artifacts.extend(rows)

    if not source_artifacts:
        raise UnprocessableError("No source segments resolved for reindex")

    raw_segments = [_artifact_to_segment_payload(a) for a in source_artifacts]
    index_cfg = payload.get("indexing", {})
    index_type = index_cfg.get("index_type")
    if not index_type:
        raise UnprocessableError("Reindex payload missing indexing.index_type")

    index_payload = build_index(
        index_artifact_id=f"reindex:{job.id}:{index_type}",
        index_type=index_type,
        params=index_cfg.get("params", {}),
        raw_segments=raw_segments,
    )
    artifact = _persist_index(
        db,
        project_id=job.project_id,
        pipeline_id=None,
        job_id=job.id,
        stage_name=STAGE_INDEXING,
        index_payload=index_payload,
        input_artifact_ids=[a.id for a in source_artifacts],
    )
    from app.services.rag_adapter import INDEX_RUNTIME_REGISTRY

    key = f"reindex:{job.id}:{index_type}"
    if key in INDEX_RUNTIME_REGISTRY:
        INDEX_RUNTIME_REGISTRY[artifact.id] = INDEX_RUNTIME_REGISTRY.pop(key)

    job.result = {"index_artifact_id": artifact.id}
    db.add(job)
    db.commit()
    transition_job(db, job, status="succeeded", stage=STAGE_INDEXING, message="Reindex completed")
    return job


def create_retriever(
    db: Session,
    *,
    project_id: str,
    index_artifact_id: str,
    retriever_type: str,
    params: dict[str, Any],
) -> Retriever:
    index_artifact = db.get(Artifact, index_artifact_id)
    if index_artifact is None or index_artifact.project_id != project_id or index_artifact.artifact_kind != "index":
        raise NotFoundError("Index artifact not found")

    row = Retriever(
        project_id=project_id,
        index_artifact_id=index_artifact_id,
        retriever_type=retriever_type,
        params=params,
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    require_session = retriever_type in {"create_bm25_retriever"}
    create_retriever_runtime(
        retriever_id=row.id,
        retriever_type=retriever_type,
        index_artifact_id=index_artifact_id,
        params=params,
        require_session=require_session,
    )
    return row


def init_session(db: Session, retriever: Retriever) -> RetrieverSession:
    settings = get_settings()
    active_count = db.execute(
        select(func.count(RetrieverSession.id)).where(
            RetrieverSession.retriever_id == retriever.id,
            RetrieverSession.status == "active",
        )
    ).scalar_one()
    if active_count >= settings.max_retriever_sessions_per_retriever:
        raise ConflictError("Retriever session cap reached")

    session = RetrieverSession(
        retriever_id=retriever.id,
        status="active",
        state={},
        expires_at=_now() + timedelta(seconds=settings.retriever_session_ttl_seconds),
    )
    db.add(session)
    db.commit()
    db.refresh(session)
    init_retriever_session(retriever.id, session.id, state={})
    return session


def release_session(db: Session, retriever: Retriever, session_id: str | None = None) -> int:
    query = select(RetrieverSession).where(
        RetrieverSession.retriever_id == retriever.id,
        RetrieverSession.status == "active",
    )
    if session_id:
        query = query.where(RetrieverSession.id == session_id)
    rows = db.execute(query).scalars().all()
    for row in rows:
        row.status = "released"
        db.add(row)
        release_retriever_session(row.id)
    db.commit()
    return len(rows)


def query_retriever(
    db: Session,
    *,
    project_id: str,
    retriever: Retriever,
    query_text: str,
    top_k: int,
    session_id: str | None,
) -> RetrievalResult:
    runtime_require_session = retriever.retriever_type in {"create_bm25_retriever"}
    if runtime_require_session:
        if not session_id:
            raise ConflictError("BM25 retriever requires session initialization")
        session = db.get(RetrieverSession, session_id)
        if session is None or session.retriever_id != retriever.id or session.status != "active":
            raise ConflictError("Active retriever session is required")
        if session.expires_at and session.expires_at < _now():
            session.status = "expired"
            db.add(session)
            db.commit()
            raise ConflictError("Retriever session expired")

    scored = execute_retriever_query(
        retriever_id=retriever.id,
        query=query_text,
        top_k=top_k,
    )

    result = RetrievalResult(
        project_id=project_id,
        retriever_id=retriever.id,
        query_text=query_text,
        top_k=top_k,
    )
    db.add(result)
    db.flush()

    for rank, item in enumerate(scored, start=1):
        segment = item["segment"]
        segment_id = segment.get("segment_id")
        artifact_id = None
        if segment_id:
            candidates = db.execute(
                select(Artifact).where(
                    Artifact.project_id == project_id,
                    Artifact.artifact_kind == "segment",
                )
            ).scalars().all()
            for row in candidates:
                metadata_segment_id = (row.metadata_json or {}).get("segment_id")
                payload_segment_id = (row.content_json or {}).get("segment_id")
                if str(metadata_segment_id or payload_segment_id) == str(segment_id):
                    artifact_id = row.id
                    break

        if not artifact_id:
            artifact = create_artifact(
                db,
                project_id=project_id,
                pipeline_id=None,
                job_id=None,
                artifact_kind="segment",
                stage_name="RETRIEVAL_RESULT",
                artifact_key=f"retrieved_{result.id}_{rank}",
                content_text=segment.get("content", ""),
                content_json=segment,
                metadata_json=segment.get("metadata") or {},
            )
            artifact_id = artifact.id

        db.add(
            RetrievalResultItem(
                retrieval_result_id=result.id,
                rank=rank,
                score=item.get("score"),
                segment_artifact_id=artifact_id,
                segment_payload=segment,
            )
        )
    db.commit()
    db.refresh(result)
    return result


def seed_example_profiles(db: Session) -> None:
    sync_profiles(db)
    db.commit()
