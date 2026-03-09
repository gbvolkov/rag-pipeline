from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.errors import APIError, ConflictError, NotFoundError, ServiceUnavailableError, UnprocessableError
from app.models.entities import (
    Artifact,
    ArtifactInput,
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
    new_id,
)
from app.services.artifacts import create_artifact
from app.services.pipeline_validator import validate_indexing_params
from app.services.rag_adapter import (
    INDEX_RUNTIME_REGISTRY,
    RETRIEVER_RUNTIME_REGISTRY,
    build_index,
    create_retriever_runtime,
    execute_retriever_query,
    init_retriever_session,
    release_retriever_session,
    restore_index_runtime,
    run_loader,
    run_processor,
    run_splitter,
)
from app.services.runtime_objects import RuntimeObjectError, validate_runtime_object_specs


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
    runtime_extras: dict[str, Any] | None = None
    diagnostics: dict[str, Any] | None = None


def _now() -> datetime:
    return datetime.now(tz=UTC)


def _error_message(exc: Exception) -> str:
    if isinstance(exc, APIError):
        return exc.message
    return str(exc)


def create_job(
    db: Session,
    *,
    project_id: str,
    pipeline_id: str | None,
    kind: str,
    payload: dict[str, Any] | None = None,
) -> Job:
    job = Job(
        project_id=project_id,
        pipeline_id=pipeline_id,
        kind=kind,
        status="queued",
        stage=None,
        payload=payload,
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


def _get_input_rows(db: Session, pipeline_id: str) -> list[PipelineInput]:
    return db.execute(select(PipelineInput).where(PipelineInput.pipeline_id == pipeline_id)).scalars().all()


def _get_indexing_row(db: Session, pipeline_id: str) -> PipelineIndexingConfig | None:
    return db.execute(
        select(PipelineIndexingConfig).where(PipelineIndexingConfig.pipeline_id == pipeline_id)
    ).scalar_one_or_none()


def _definition_stages(definition: dict[str, Any]) -> list[dict[str, Any]]:
    raw_stages = definition.get("stages", [])
    if not isinstance(raw_stages, list):
        return []
    stages = [stage for stage in raw_stages if isinstance(stage, dict)]
    return sorted(stages, key=lambda stage: int(stage.get("position", 0)))


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
    if not isinstance(artifact.content_json, dict):
        raise UnprocessableError(
            "Document artifact content_json must be an object",
            details={"artifact_id": artifact.id},
        )
    payload = dict(artifact.content_json)
    if "content" not in payload:
        raise UnprocessableError(
            "Document artifact payload is missing 'content'",
            details={"artifact_id": artifact.id},
        )
    if "metadata" not in payload:
        raise UnprocessableError(
            "Document artifact payload is missing 'metadata'",
            details={"artifact_id": artifact.id},
        )
    return payload


def _normalize_segment_payload(payload: dict[str, Any]) -> dict[str, Any]:
    data = dict(payload)
    metadata = data.get("metadata", {})
    if not isinstance(metadata, dict):
        raise UnprocessableError("Segment payload field 'metadata' must be an object")
    metadata = dict(metadata)

    segment_id = data.get("segment_id", metadata.get("segment_id"))
    parent_id = data.get("parent_id", metadata.get("parent_id"))
    if segment_id is not None:
        segment_id = str(segment_id)
        data["segment_id"] = segment_id
        metadata.setdefault("segment_id", segment_id)
    if parent_id is not None:
        parent_id = str(parent_id)
        data["parent_id"] = parent_id
        metadata.setdefault("parent_id", parent_id)
    data["metadata"] = metadata
    return data


def _artifact_to_segment_payload(artifact: Artifact) -> dict[str, Any]:
    if not isinstance(artifact.content_json, dict):
        raise UnprocessableError(
            "Segment artifact content_json must be an object",
            details={"artifact_id": artifact.id},
        )
    data = dict(artifact.content_json)
    if "content" not in data:
        raise UnprocessableError(
            "Segment artifact payload is missing 'content'",
            details={"artifact_id": artifact.id},
        )
    if "metadata" not in data:
        raise UnprocessableError(
            "Segment artifact payload is missing 'metadata'",
            details={"artifact_id": artifact.id},
        )
    return _normalize_segment_payload(data)


def _artifact_to_graph_entity_payload(artifact: Artifact) -> dict[str, Any]:
    return _artifact_to_segment_payload(artifact)


def _is_dual_storage_index(index_type: str, params: dict[str, Any] | None) -> bool:
    _ = index_type
    cfg = params or {}
    return bool(cfg.get("dual_storage"))


def _segment_payload_id(segment: dict[str, Any]) -> str | None:
    seg_id = segment.get("segment_id")
    if seg_id is None:
        metadata = segment.get("metadata")
        if isinstance(metadata, dict):
            seg_id = metadata.get("segment_id")
    if seg_id is None:
        return None
    return str(seg_id)


def _ordered_parent_ids(raw_segments: list[dict[str, Any]]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for segment in raw_segments:
        parent_id = segment.get("parent_id")
        if parent_id is None:
            metadata = segment.get("metadata")
            if isinstance(metadata, dict):
                parent_id = metadata.get("parent_id")
        if parent_id is None:
            continue
        key = str(parent_id)
        if key in seen:
            continue
        seen.add(key)
        ordered.append(key)
    return ordered


def _partition_dual_storage_segments(
    payloads: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    parent_ids = _ordered_parent_ids(payloads)
    if not parent_ids:
        return list(payloads), []

    parent_id_set = set(parent_ids)
    child_segments: list[dict[str, Any]] = []
    parent_by_id: dict[str, dict[str, Any]] = {}
    for payload in payloads:
        seg_id = _segment_payload_id(payload)
        if seg_id is not None and seg_id in parent_id_set and seg_id not in parent_by_id:
            parent_by_id[seg_id] = payload
            continue
        child_segments.append(payload)

    if not child_segments:
        # If partitioning cannot separate parent docs from children, keep original
        # payloads as children and resolve parents from DB.
        return list(payloads), []

    parent_segments = [parent_by_id[parent_id] for parent_id in parent_ids if parent_id in parent_by_id]
    return child_segments, parent_segments


def _resolve_parent_segments_from_stage_outputs(
    raw_segments: list[dict[str, Any]],
    segment_outputs: list[StageOutput],
) -> tuple[list[dict[str, Any]], list[str]]:
    parent_ids = _ordered_parent_ids(raw_segments)
    if not parent_ids:
        return [], []

    payload_by_id: dict[str, dict[str, Any]] = {}
    artifact_by_id: dict[str, str] = {}
    for output in segment_outputs:
        for idx, payload in enumerate(output.payload):
            seg_id = _segment_payload_id(payload)
            if seg_id is None or seg_id not in parent_ids or seg_id in payload_by_id:
                continue
            payload_by_id[seg_id] = payload
            if idx < len(output.artifact_ids):
                artifact_by_id[seg_id] = output.artifact_ids[idx]

    parent_segments: list[dict[str, Any]] = []
    parent_artifact_ids: list[str] = []
    for parent_id in parent_ids:
        payload = payload_by_id.get(parent_id)
        if payload is None:
            continue
        parent_segments.append(payload)
        artifact_id = artifact_by_id.get(parent_id)
        if artifact_id is not None:
            parent_artifact_ids.append(artifact_id)

    return parent_segments, parent_artifact_ids


def _resolve_parent_segments_from_db(
    db: Session,
    *,
    project_id: str,
    raw_segments: list[dict[str, Any]],
    pipeline_id: str | None = None,
) -> list[dict[str, Any]]:
    parent_ids = _ordered_parent_ids(raw_segments)
    if not parent_ids:
        return []

    query = select(Artifact).where(
        Artifact.project_id == project_id,
        Artifact.artifact_kind == "segment",
    )
    if pipeline_id is not None:
        query = query.where(Artifact.pipeline_id == pipeline_id)
    rows = db.execute(query).scalars().all()

    payload_by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        payload = _artifact_to_segment_payload(row)
        seg_id = _segment_payload_id(payload)
        if seg_id is None or seg_id not in parent_ids or seg_id in payload_by_id:
            continue
        payload_by_id[seg_id] = payload

    return [payload_by_id[parent_id] for parent_id in parent_ids if parent_id in payload_by_id]


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _cleanup_uploaded_file(run_payload: dict[str, Any]) -> None:
    uploaded_file_path = run_payload.get("uploaded_file_path")
    if not isinstance(uploaded_file_path, str) or not uploaded_file_path.strip():
        return
    try:
        Path(uploaded_file_path).unlink(missing_ok=True)
    except Exception:
        logger.warning("Failed to clean up uploaded file path: %s", uploaded_file_path, exc_info=True)


def _index_storage_columns(index_payload: dict[str, Any]) -> dict[str, str | None]:
    storage = index_payload.get("storage")
    if not isinstance(storage, dict):
        return {
            "storage_backend": None,
            "vector_collection_name": None,
            "vector_persist_path": None,
            "docstore_persist_path": None,
        }

    vector_store = storage.get("vector_store")
    vector_store = vector_store if isinstance(vector_store, dict) else {}
    doc_store = storage.get("doc_store")
    doc_store = doc_store if isinstance(doc_store, dict) else {}
    return {
        "storage_backend": storage.get("backend") if isinstance(storage.get("backend"), str) else None,
        "vector_collection_name": (
            vector_store.get("collection_name") if isinstance(vector_store.get("collection_name"), str) else None
        ),
        "vector_persist_path": (
            vector_store.get("persist_path") if isinstance(vector_store.get("persist_path"), str) else None
        ),
        "docstore_persist_path": doc_store.get("file_path") if isinstance(doc_store.get("file_path"), str) else None,
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
        payload = dict(doc)
        artifact = create_artifact(
            db,
            project_id=project_id,
            pipeline_id=pipeline_id,
            job_id=job_id,
            artifact_kind="document",
            stage_name=stage_name,
            artifact_key=f"doc_{idx}",
            content_text=payload.get("content", ""),
            content_json=payload,
            metadata_json=payload.get("metadata") or {},
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
    normalized_segments = [_normalize_segment_payload(seg) for seg in segments]
    for idx, seg in enumerate(normalized_segments):
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
    return StageOutput(kind="segment", payload=normalized_segments, artifact_ids=artifact_ids)


def _persist_graph_entities(
    db: Session,
    *,
    project_id: str,
    pipeline_id: str,
    job_id: str,
    stage_name: str,
    graph_entities: list[dict[str, Any]],
    input_artifact_ids: list[str],
) -> StageOutput:
    artifact_ids: list[str] = []
    normalized_entities = [_normalize_segment_payload(entity) for entity in graph_entities]
    for idx, entity in enumerate(normalized_entities):
        artifact = create_artifact(
            db,
            project_id=project_id,
            pipeline_id=pipeline_id,
            job_id=job_id,
            artifact_kind="graph_entity",
            stage_name=stage_name,
            artifact_key=f"graph_entity_{idx}",
            content_text=entity.get("content", ""),
            content_json=entity,
            metadata_json=entity.get("metadata") or {},
            input_artifact_ids=input_artifact_ids,
        )
        artifact_ids.append(artifact.id)
    db.commit()
    return StageOutput(kind="graph_entity", payload=normalized_entities, artifact_ids=artifact_ids)


def _persist_index(
    db: Session,
    *,
    artifact_id: str,
    project_id: str,
    pipeline_id: str | None,
    job_id: str | None,
    stage_name: str,
    index_payload: dict[str, Any],
    input_artifact_ids: list[str],
) -> Artifact:
    storage_columns = _index_storage_columns(index_payload)
    artifact = create_artifact(
        db,
        artifact_id=artifact_id,
        project_id=project_id,
        pipeline_id=pipeline_id,
        job_id=job_id,
        artifact_kind="index",
        stage_name=stage_name,
        artifact_key=index_payload.get("logical_collection_name") or "index",
        content_text=None,
        content_json=index_payload,
        metadata_json=index_payload,
        storage_backend=storage_columns["storage_backend"],
        vector_collection_name=storage_columns["vector_collection_name"],
        vector_persist_path=storage_columns["vector_persist_path"],
        docstore_persist_path=storage_columns["docstore_persist_path"],
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
    input_rows = _get_input_rows(db, pipeline.id)
    indexing_row = _get_indexing_row(db, pipeline.id)
    stage_defs = _definition_stages(definition)
    if job.payload is not None and not isinstance(job.payload, dict):
        raise UnprocessableError("Job payload must be an object when present")
    run_payload = job.payload or {}

    outputs: dict[str, StageOutput] = {}
    artifacts_produced = {"document": 0, "segment": 0, "graph_entity": 0, "index": 0}
    stage_diagnostics: dict[str, Any] = {}

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
                source_payload = [_artifact_to_document_payload(a) for a in artifacts]
            elif ref.artifact_kind == "segment":
                source_payload = [_artifact_to_segment_payload(a) for a in artifacts]
            else:
                source_payload = []
                for artifact in artifacts:
                    if not isinstance(artifact.content_json, dict):
                        raise UnprocessableError(
                            "Index artifact content_json must be an object",
                            details={"artifact_id": artifact.id},
                        )
                    source_payload.append(dict(artifact.content_json))

            outputs[ref.alias] = StageOutput(
                kind=ref.artifact_kind,
                payload=source_payload,
                artifact_ids=[a.id for a in artifacts],
                runtime_extras={},
                diagnostics={},
            )

        runtime_input_cfg = definition.get("runtime_input")
        if isinstance(runtime_input_cfg, dict):
            artifact_kind = runtime_input_cfg.get("artifact_kind")
            alias = runtime_input_cfg.get("alias") or "RUNTIME_INPUT"
            if artifact_kind not in {"document", "segment"}:
                raise UnprocessableError("runtime_input.artifact_kind must be 'document' or 'segment'")
            payload_key = "documents" if artifact_kind == "document" else "segments"
            runtime_payload = run_payload.get(payload_key)
            if not isinstance(runtime_payload, list):
                raise UnprocessableError(
                    f"Run payload must include '{payload_key}' as a list for runtime_input pipelines",
                )
            if not runtime_payload:
                raise UnprocessableError(
                    f"Run payload '{payload_key}' must be a non-empty list for runtime_input pipelines",
                )
            if artifact_kind == "document":
                persisted = _persist_documents(
                    db,
                    project_id=job.project_id,
                    pipeline_id=pipeline.id,
                    job_id=job.id,
                    stage_name=alias,
                    docs=runtime_payload,
                    input_artifact_ids=[],
                )
                artifacts_produced["document"] += len(runtime_payload)
            else:
                persisted = _persist_segments(
                    db,
                    project_id=job.project_id,
                    pipeline_id=pipeline.id,
                    job_id=job.id,
                    stage_name=alias,
                    segments=runtime_payload,
                    input_artifact_ids=[],
                )
                artifacts_produced["segment"] += len(runtime_payload)
            outputs[alias] = StageOutput(
                kind=persisted.kind,
                payload=persisted.payload,
                artifact_ids=persisted.artifact_ids,
                runtime_extras={},
                diagnostics={},
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
            loader_result = run_loader(
                loader_type=loader_type,
                params=loader_cfg.get("params", {}),
                run_payload=run_payload,
            )
            if not loader_result["payload"]:
                raise ServiceUnavailableError(
                    message=f"Loader '{loader_type}' returned no documents",
                    details={"loader_type": loader_type},
                    rag_lib_exception_type="SilentEmptyResult",
                )
            inherited_artifact_ids = [aid for out in outputs.values() for aid in out.artifact_ids]
            persisted = _persist_documents(
                db,
                project_id=job.project_id,
                pipeline_id=pipeline.id,
                job_id=job.id,
                stage_name=STAGE_LOADING,
                docs=loader_result["payload"],
                input_artifact_ids=inherited_artifact_ids,
            )
            outputs[STAGE_LOADING] = StageOutput(
                kind=persisted.kind,
                payload=persisted.payload,
                artifact_ids=persisted.artifact_ids,
                runtime_extras=loader_result.get("runtime_extras") or {},
                diagnostics=loader_result.get("diagnostics") or {},
            )
            if loader_result.get("diagnostics"):
                stage_diagnostics[STAGE_LOADING] = loader_result["diagnostics"]
            artifacts_produced["document"] += len(loader_result["payload"])

        previous_alias = None
        if loader_cfg:
            previous_alias = STAGE_LOADING
        elif isinstance(runtime_input_cfg, dict):
            previous_alias = runtime_input_cfg.get("alias") or "RUNTIME_INPUT"

        for stage_def in stage_defs:
            stage_name = str(stage_def.get("stage_name"))
            stage_kind = str(stage_def.get("stage_kind"))
            component_type = str(stage_def.get("component_type"))
            transition_job(
                db,
                job,
                status="running",
                stage=STAGE_SEGMENTING,
                message=f"Running stage '{stage_name}'",
            )
            logger.info(
                "Running pipeline stage: job_id=%s stage_name=%s stage_kind=%s component_type=%s",
                job.id,
                stage_name,
                stage_kind,
                component_type,
            )
            raw_input_aliases = stage_def.get("input_aliases")
            input_aliases = raw_input_aliases if isinstance(raw_input_aliases, list) else []
            if not input_aliases and previous_alias:
                input_aliases = [previous_alias]
            stage_inputs = [outputs[alias] for alias in input_aliases if alias in outputs]
            if not stage_inputs:
                raise UnprocessableError(
                    f"Stage '{stage_name}' has no resolvable inputs",
                    details={"input_aliases": input_aliases},
                )

            source_docs: list[dict[str, Any]] = []
            source_segments: list[dict[str, Any]] = []
            source_artifact_ids: list[str] = []
            runtime_context: dict[str, Any] = {}
            for src in stage_inputs:
                source_artifact_ids.extend(src.artifact_ids)
                if src.runtime_extras:
                    runtime_context.update(src.runtime_extras)
                if src.kind == "document":
                    source_docs.extend(src.payload)
                elif src.kind == "segment":
                    source_segments.extend(src.payload)
                else:
                    raise UnprocessableError(
                        f"Unsupported stage input kind '{src.kind}' for stage '{stage_name}'",
                    )
            if source_docs and source_segments:
                raise UnprocessableError(
                    f"Stage '{stage_name}' mixes document and segment inputs; rag-lib stages require one input type",
                )

            params = stage_def.get("params")
            if params is None:
                params = {}
            if not isinstance(params, dict):
                raise UnprocessableError(f"Stage '{stage_name}' params must be an object")

            if stage_kind == "splitter":
                stage_result = run_splitter(
                    splitter_type=component_type,
                    params=params,
                    source_documents=source_docs if source_docs else None,
                    source_segments=source_segments if source_segments else None,
                    runtime_context=runtime_context,
                )
            elif stage_kind == "processor":
                stage_result = run_processor(
                    processor_type=component_type,
                    params=params,
                    source_documents=source_docs if source_docs else None,
                    source_segments=source_segments if source_segments else None,
                    runtime_context=runtime_context,
                )
            else:
                raise UnprocessableError(f"Unsupported stage_kind '{stage_kind}' in stage '{stage_name}'")

            if stage_result["kind"] == "document":
                persisted = _persist_documents(
                    db,
                    project_id=job.project_id,
                    pipeline_id=pipeline.id,
                    job_id=job.id,
                    stage_name=stage_name,
                    docs=stage_result["payload"],
                    input_artifact_ids=source_artifact_ids,
                )
                artifacts_produced["document"] += len(stage_result["payload"])
            elif stage_result["kind"] == "segment":
                persisted = _persist_segments(
                    db,
                    project_id=job.project_id,
                    pipeline_id=pipeline.id,
                    job_id=job.id,
                    stage_name=stage_name,
                    segments=stage_result["payload"],
                    input_artifact_ids=source_artifact_ids,
                )
                artifacts_produced["segment"] += len(stage_result["payload"])
            elif stage_result["kind"] == "none":
                persisted_artifacts = stage_result.get("persisted_artifacts")
                if persisted_artifacts is None:
                    persisted_artifacts = []
                if not isinstance(persisted_artifacts, list):
                    raise UnprocessableError(
                        f"Stage '{stage_name}' returned invalid persisted_artifacts payload",
                        details={"stage_name": stage_name, "stage_kind": stage_kind},
                    )
                for persisted_artifact in persisted_artifacts:
                    if not isinstance(persisted_artifact, dict):
                        raise UnprocessableError(
                            f"Stage '{stage_name}' returned invalid persisted artifact entry",
                            details={"stage_name": stage_name, "stage_kind": stage_kind},
                        )
                    artifact_kind = persisted_artifact.get("artifact_kind")
                    artifact_payload = persisted_artifact.get("payload")
                    if not isinstance(artifact_payload, list):
                        raise UnprocessableError(
                            f"Stage '{stage_name}' returned invalid persisted payload for '{artifact_kind}'",
                            details={"stage_name": stage_name, "stage_kind": stage_kind},
                        )
                    if not artifact_payload:
                        continue
                    if artifact_kind == "graph_entity":
                        _persist_graph_entities(
                            db,
                            project_id=job.project_id,
                            pipeline_id=pipeline.id,
                            job_id=job.id,
                            stage_name=stage_name,
                            graph_entities=artifact_payload,
                            input_artifact_ids=source_artifact_ids,
                        )
                        artifacts_produced["graph_entity"] += len(artifact_payload)
                        continue
                    raise UnprocessableError(
                        f"Stage '{stage_name}' returned unsupported persisted artifact kind '{artifact_kind}'",
                        details={"stage_name": stage_name, "stage_kind": stage_kind},
                    )
                persisted = StageOutput(
                    kind="none",
                    payload=[],
                    artifact_ids=[],
                    runtime_extras=stage_result.get("runtime_extras") or {},
                    diagnostics=stage_result.get("diagnostics") or {},
                )
            else:
                raise UnprocessableError(
                    f"Stage '{stage_name}' returned unsupported artifact kind '{stage_result['kind']}'",
                )

            if stage_result["kind"] in {"document", "segment"} and not stage_result["payload"]:
                raise ServiceUnavailableError(
                    message=f"Pipeline stage '{stage_name}' returned an empty {stage_result['kind']} payload",
                    details={
                        "stage_name": stage_name,
                        "stage_kind": stage_kind,
                        "component_type": component_type,
                    },
                    rag_lib_exception_type="SilentEmptyResult",
                )

            outputs[stage_name] = StageOutput(
                kind=persisted.kind,
                payload=persisted.payload,
                artifact_ids=persisted.artifact_ids,
                runtime_extras=stage_result.get("runtime_extras") or {},
                diagnostics=stage_result.get("diagnostics") or {},
            )
            if stage_result.get("diagnostics"):
                stage_diagnostics[stage_name] = stage_result["diagnostics"]
            previous_alias = stage_name

        # INDEXING stage
        if indexing_row is not None:
            transition_job(db, job, status="running", stage=STAGE_INDEXING, message="Building index")
            logger.info(
                "Building index: job_id=%s index_type=%s collection_name=%s",
                job.id,
                indexing_row.index_type,
                indexing_row.collection_name,
            )
            segment_sources = [out for out in outputs.values() if out.kind == "segment"]
            if not segment_sources:
                raise UnprocessableError("Indexing stage requires segment inputs")

            index_source = None
            for stage_def in reversed(stage_defs):
                stage_name = str(stage_def.get("stage_name"))
                candidate = outputs.get(stage_name)
                if candidate is not None and candidate.kind == "segment":
                    index_source = candidate
                    break
            if index_source is None:
                payload_items = [s for out in segment_sources for s in out.payload]
                artifact_ids = [a for out in segment_sources for a in out.artifact_ids]
                runtime_extras: dict[str, Any] = {}
                for out in segment_sources:
                    if out.runtime_extras:
                        runtime_extras.update(out.runtime_extras)
                index_source = StageOutput(
                    kind="segment",
                    payload=payload_items,
                    artifact_ids=artifact_ids,
                    runtime_extras=runtime_extras,
                    diagnostics={},
                )
            if not index_source.payload:
                raise ServiceUnavailableError(
                    message="Indexing received an empty segment payload",
                    details={"pipeline_id": pipeline.id, "index_type": indexing_row.index_type},
                    rag_lib_exception_type="SilentEmptyResult",
                )

            if indexing_row.params is None:
                index_params: dict[str, Any] = {}
            elif isinstance(indexing_row.params, dict):
                index_params = dict(indexing_row.params)
            else:
                raise UnprocessableError(
                    "Pipeline indexing params must be an object",
                    details={"pipeline_id": pipeline.id, "index_type": indexing_row.index_type},
                )
            validate_indexing_params(
                indexing_row.index_type,
                index_params,
                path=f"indexing.{indexing_row.index_type}.params",
            )
            parent_segments: list[dict[str, Any]] = []
            parent_artifact_ids: list[str] = []
            if _is_dual_storage_index(indexing_row.index_type, index_params):
                segment_outputs = [out for out in outputs.values() if out.kind == "segment"]
                parent_segments, parent_artifact_ids = _resolve_parent_segments_from_stage_outputs(
                    index_source.payload,
                    segment_outputs,
                )
                if not parent_segments:
                    parent_segments = _resolve_parent_segments_from_db(
                        db,
                        project_id=job.project_id,
                        raw_segments=index_source.payload,
                        pipeline_id=pipeline.id,
                    )

            index_artifact_id = new_id()
            index_payload = build_index(
                index_artifact_id=index_artifact_id,
                index_type=indexing_row.index_type,
                params=index_params,
                raw_segments=index_source.payload,
                raw_parent_segments=parent_segments if parent_segments else None,
                runtime_extras=index_source.runtime_extras,
                logical_collection_name=indexing_row.collection_name,
                logical_docstore_name=indexing_row.docstore_name,
            )
            index_input_artifact_ids = _dedupe_preserve_order(
                list(index_source.artifact_ids) + list(parent_artifact_ids)
            )
            index_artifact = _persist_index(
                db,
                artifact_id=index_artifact_id,
                project_id=job.project_id,
                pipeline_id=pipeline.id,
                job_id=job.id,
                stage_name=STAGE_INDEXING,
                index_payload=index_payload,
                input_artifact_ids=index_input_artifact_ids,
            )
            artifacts_produced["index"] += 1

        job.result = {
            "artifacts_produced": artifacts_produced,
            "pipeline_shape": pipeline.shape,
            "stage_diagnostics": stage_diagnostics,
        }
        transition_job(db, job, status="succeeded", stage=job.stage, message="Pipeline run completed")
        logger.info(
            "Pipeline job execution completed: job_id=%s artifacts=%s",
            job.id,
            artifacts_produced,
        )
        return job
    except Exception as exc:
        error_payload = {
            "message": _error_message(exc),
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
    finally:
        _cleanup_uploaded_file(run_payload)


def run_reindex_job(db: Session, job_id: str) -> Job:
    job = db.get(Job, job_id)
    if job is None:
        raise NotFoundError("Job not found")
    if not isinstance(job.payload, dict):
        raise UnprocessableError("Reindex job payload must be an object")
    payload = job.payload
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

    index_params = index_cfg.get("params", {})
    if index_params is None:
        index_params = {}
    if not isinstance(index_params, dict):
        raise UnprocessableError("Reindex payload indexing.params must be an object")
    validate_indexing_params(
        index_type,
        index_params,
        path=f"indexing.{index_type}.params",
    )
    parent_segments: list[dict[str, Any]] = []
    if _is_dual_storage_index(index_type, index_params):
        source_pipeline_ids = {a.pipeline_id for a in source_artifacts if a.pipeline_id}
        pipeline_scope = next(iter(source_pipeline_ids)) if len(source_pipeline_ids) == 1 else None
        parent_segments = _resolve_parent_segments_from_db(
            db,
            project_id=job.project_id,
            raw_segments=raw_segments,
            pipeline_id=pipeline_scope,
        )

    index_artifact_id = new_id()
    index_payload = build_index(
        index_artifact_id=index_artifact_id,
        index_type=index_type,
        params=index_params,
        raw_segments=raw_segments,
        raw_parent_segments=parent_segments if parent_segments else None,
        logical_collection_name=index_cfg.get("collection_name"),
        logical_docstore_name=index_cfg.get("docstore_name"),
    )
    artifact = _persist_index(
        db,
        artifact_id=index_artifact_id,
        project_id=job.project_id,
        pipeline_id=None,
        job_id=job.id,
        stage_name=STAGE_INDEXING,
        index_payload=index_payload,
        input_artifact_ids=[a.id for a in source_artifacts],
    )

    job.result = {"index_artifact_id": artifact.id}
    db.add(job)
    db.commit()
    transition_job(db, job, status="succeeded", stage=STAGE_INDEXING, message="Reindex completed")
    return job


def _resolve_retriever_source_payloads(
    db: Session,
    *,
    project_id: str,
    source_artifact_ids: list[str],
) -> tuple[str, list[dict[str, Any]]]:
    if not source_artifact_ids:
        raise UnprocessableError("Retriever source_artifact_ids cannot be empty")

    rows = db.execute(select(Artifact).where(Artifact.id.in_(source_artifact_ids))).scalars().all()
    row_by_id = {row.id: row for row in rows}

    ordered_rows: list[Artifact] = []
    for artifact_id in source_artifact_ids:
        row = row_by_id.get(artifact_id)
        if row is None or row.project_id != project_id:
            raise NotFoundError("Retriever source artifact not found")
        ordered_rows.append(row)

    kinds = {row.artifact_kind for row in ordered_rows}
    if kinds - {"document", "segment", "graph_entity"}:
        raise UnprocessableError("Retriever source artifacts must be document, segment, or graph_entity artifacts only")
    if len(kinds) != 1:
        raise UnprocessableError("Retriever source artifacts must be homogeneous")

    artifact_kind = next(iter(kinds))
    if artifact_kind == "document":
        return artifact_kind, [_artifact_to_document_payload(row) for row in ordered_rows]
    if artifact_kind == "graph_entity":
        return artifact_kind, [_artifact_to_graph_entity_payload(row) for row in ordered_rows]
    return artifact_kind, [_artifact_to_segment_payload(row) for row in ordered_rows]


def _relevant_retriever_source_artifact_ids(db: Session, retriever: Retriever) -> list[str]:
    if retriever.source_artifact_ids:
        return list(retriever.source_artifact_ids)

    if retriever.index_artifact_id is None:
        return []

    rows = db.execute(
        select(ArtifactInput.input_artifact_id)
        .where(ArtifactInput.artifact_id == retriever.index_artifact_id)
        .order_by(ArtifactInput.created_at.asc())
    ).all()
    return [str(row[0]) for row in rows if row and row[0] is not None]


def _load_index_input_payloads(db: Session, index_artifact: Artifact) -> list[dict[str, Any]]:
    rows = db.execute(
        select(Artifact)
        .join(ArtifactInput, ArtifactInput.input_artifact_id == Artifact.id)
        .where(ArtifactInput.artifact_id == index_artifact.id)
        .order_by(ArtifactInput.created_at.asc())
    ).scalars().all()

    payloads: list[dict[str, Any]] = []
    for row in rows:
        if row.project_id != index_artifact.project_id or row.artifact_kind != "segment":
            continue
        payloads.append(_artifact_to_segment_payload(row))
    return payloads


def _resolve_index_runtime_params(
    db: Session,
    index_artifact: Artifact,
) -> tuple[str, dict[str, Any], dict[str, Any], str | None, str | None]:
    _ = db
    payload = index_artifact.content_json if isinstance(index_artifact.content_json, dict) else {}
    index_type = payload.get("index_type")
    params = payload.get("params") if isinstance(payload.get("params"), dict) else None
    storage = payload.get("storage") if isinstance(payload.get("storage"), dict) else None
    logical_collection_name = (
        payload.get("logical_collection_name") if isinstance(payload.get("logical_collection_name"), str) else None
    )
    logical_docstore_name = (
        payload.get("logical_docstore_name") if isinstance(payload.get("logical_docstore_name"), str) else None
    )

    if not isinstance(index_type, str) or not index_type.strip():
        raise UnprocessableError(
            "Index runtime cannot be rehydrated because the index type is unavailable",
            details={"index_artifact_id": index_artifact.id},
        )

    if params is None:
        raise UnprocessableError(
            "Index runtime cannot be rehydrated because indexing params are unavailable",
            details={"index_artifact_id": index_artifact.id},
        )

    if storage is None:
        raise UnprocessableError(
            "Index runtime cannot be rehydrated because the index artifact has no persisted storage descriptor",
            details={
                "index_artifact_id": index_artifact.id,
                "required_action": "recreate the index after resetting the database and Docker volumes",
            },
        )

    return index_type, dict(params), storage, logical_collection_name, logical_docstore_name


def _ensure_index_runtime(db: Session, index_artifact: Artifact) -> None:
    if index_artifact.id in INDEX_RUNTIME_REGISTRY:
        return

    index_type, params, storage, logical_collection_name, logical_docstore_name = _resolve_index_runtime_params(
        db,
        index_artifact,
    )
    payloads = _load_index_input_payloads(db, index_artifact)
    if not payloads:
        raise ServiceUnavailableError(
            message="Index runtime cannot be restored because no source segments were recovered from artifact lineage",
            details={"index_artifact_id": index_artifact.id},
            rag_lib_exception_type="SilentEmptyResult",
        )
    raw_segments = payloads
    raw_parent_segments: list[dict[str, Any]] = []
    if _is_dual_storage_index(index_type, params):
        raw_segments, raw_parent_segments = _partition_dual_storage_segments(payloads)
    if not raw_segments:
        raise ServiceUnavailableError(
            message="Index runtime cannot be restored because no child segments were recovered from artifact lineage",
            details={"index_artifact_id": index_artifact.id, "index_type": index_type},
            rag_lib_exception_type="SilentEmptyResult",
        )

    restore_index_runtime(
        index_artifact_id=index_artifact.id,
        index_type=index_type,
        params=params,
        raw_segments=raw_segments,
        raw_parent_segments=raw_parent_segments or None,
        logical_collection_name=logical_collection_name,
        logical_docstore_name=logical_docstore_name,
        storage=storage,
    )


def _ensure_retriever_runtime(db: Session, retriever: Retriever) -> None:
    if retriever.id in RETRIEVER_RUNTIME_REGISTRY:
        return

    project = db.get(Project, retriever.project_id)
    if project is None:
        raise NotFoundError("Project not found")
    source_artifact_kind: str | None = None
    source_payloads: list[dict[str, Any]] | None = None
    if retriever.index_artifact_id is not None:
        index_artifact = db.get(Artifact, retriever.index_artifact_id)
        if index_artifact is None or index_artifact.project_id != retriever.project_id or index_artifact.artifact_kind != "index":
            raise NotFoundError("Index artifact not found")
        _ensure_index_runtime(db, index_artifact)
    elif retriever.source_artifact_ids:
        source_artifact_kind, source_payloads = _resolve_retriever_source_payloads(
            db,
            project_id=retriever.project_id,
            source_artifact_ids=list(retriever.source_artifact_ids),
        )
        if not source_payloads:
            raise ServiceUnavailableError(
                message="Retriever runtime cannot be created because no source payloads were resolved",
                details={"retriever_id": retriever.id},
                rag_lib_exception_type="SilentEmptyResult",
            )

    create_retriever_runtime(
        retriever_id=retriever.id,
        retriever_type=retriever.retriever_type,
        index_artifact_id=retriever.index_artifact_id,
        params=retriever.params,
        source_payloads=source_payloads,
        source_artifact_kind=source_artifact_kind,
        project_graph_store_config=project.graph_store_config,
    )


def _artifact_segment_id(row: Artifact) -> str | None:
    payload = row.content_json if isinstance(row.content_json, dict) else {}
    metadata = row.metadata_json if isinstance(row.metadata_json, dict) else {}
    value = payload.get("segment_id")
    if value is None:
        value = metadata.get("segment_id")
    if value is None:
        return None
    return str(value)


def _resolve_retriever_artifact_lookup(db: Session, project_id: str, retriever: Retriever) -> dict[str, str]:
    relevant_ids = _dedupe_preserve_order(_relevant_retriever_source_artifact_ids(db, retriever))
    if not relevant_ids:
        return {}

    rows = db.execute(
        select(Artifact).where(
            Artifact.project_id == project_id,
            Artifact.id.in_(relevant_ids),
        )
    ).scalars().all()
    row_by_id = {row.id: row for row in rows}

    lookup: dict[str, str] = {}
    for artifact_id in relevant_ids:
        row = row_by_id.get(artifact_id)
        if row is None or row.artifact_kind not in {"segment", "graph_entity"}:
            continue
        segment_id = _artifact_segment_id(row)
        if segment_id is None or segment_id in lookup:
            continue
        lookup[segment_id] = row.id
    return lookup


def create_retriever(
    db: Session,
    *,
    project_id: str,
    index_artifact_id: str | None,
    source_artifact_ids: list[str] | None,
    retriever_type: str,
    params: dict[str, Any],
) -> Retriever:
    if (index_artifact_id is None) == (not source_artifact_ids):
        raise UnprocessableError("Provide exactly one of index_artifact_id or source_artifact_ids")

    project = db.get(Project, project_id)
    if project is None:
        raise NotFoundError("Project not found")
    index_artifact: Artifact | None = None
    resolved_source_artifact_ids = list(source_artifact_ids or [])
    if index_artifact_id is not None:
        index_artifact = db.get(Artifact, index_artifact_id)
        if index_artifact is None or index_artifact.project_id != project_id or index_artifact.artifact_kind != "index":
            raise NotFoundError("Index artifact not found")
    else:
        _resolve_retriever_source_payloads(
            db,
            project_id=project_id,
            source_artifact_ids=resolved_source_artifact_ids,
        )

    try:
        validate_runtime_object_specs(params, path=f"retriever.{retriever_type}.params")
    except RuntimeObjectError as exc:
        raise UnprocessableError(str(exc)) from exc

    row = Retriever(
        project_id=project_id,
        index_artifact_id=index_artifact_id,
        source_artifact_ids=resolved_source_artifact_ids or None,
        retriever_type=retriever_type,
        params=params,
    )
    db.add(row)
    db.flush()
    try:
        if index_artifact is not None:
            _ensure_index_runtime(db, index_artifact)
        _ensure_retriever_runtime(db, row)
    except Exception:
        db.rollback()
        raise
    db.commit()
    db.refresh(row)
    return row


def init_session(db: Session, retriever: Retriever) -> RetrieverSession:
    _ensure_retriever_runtime(db, retriever)
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
    session_id: str | None,
) -> RetrievalResult:
    _ensure_retriever_runtime(db, retriever)
    scored = execute_retriever_query(
        retriever_id=retriever.id,
        query=query_text,
    )

    result = RetrievalResult(
        project_id=project_id,
        retriever_id=retriever.id,
        query_text=query_text,
        top_k=len(scored),
    )
    db.add(result)
    db.flush()

    artifact_lookup = _resolve_retriever_artifact_lookup(db, project_id, retriever)

    for rank, item in enumerate(scored, start=1):
        segment = item["segment"]
        segment_id = segment.get("segment_id")
        artifact_id = artifact_lookup.get(str(segment_id)) if segment_id is not None else None

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
