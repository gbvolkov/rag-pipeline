from __future__ import annotations

from collections import deque
from typing import Any

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from app.models.entities import Artifact, ArtifactInput, new_id


def next_artifact_version(
    db: Session,
    *,
    project_id: str,
    pipeline_id: str | None,
    artifact_kind: str,
    stage_name: str | None,
    artifact_key: str,
) -> int:
    current = db.execute(
        select(func.max(Artifact.version)).where(
            Artifact.project_id == project_id,
            Artifact.pipeline_id == pipeline_id,
            Artifact.artifact_kind == artifact_kind,
            Artifact.stage_name == stage_name,
            Artifact.artifact_key == artifact_key,
        )
    ).scalar_one()
    return (current or 0) + 1


def create_artifact(
    db: Session,
    *,
    artifact_id: str | None = None,
    project_id: str,
    pipeline_id: str | None,
    job_id: str | None,
    artifact_kind: str,
    stage_name: str | None,
    artifact_key: str,
    alias: str | None = None,
    content_text: str | None = None,
    content_json: dict[str, Any] | None = None,
    blob_uri: str | None = None,
    metadata_json: dict[str, Any] | None = None,
    storage_backend: str | None = None,
    vector_collection_name: str | None = None,
    vector_persist_path: str | None = None,
    docstore_persist_path: str | None = None,
    input_artifact_ids: list[str] | None = None,
) -> Artifact:
    version = next_artifact_version(
        db,
        project_id=project_id,
        pipeline_id=pipeline_id,
        artifact_kind=artifact_kind,
        stage_name=stage_name,
        artifact_key=artifact_key,
    )

    artifact = Artifact(
        id=artifact_id or new_id(),
        project_id=project_id,
        pipeline_id=pipeline_id,
        job_id=job_id,
        artifact_kind=artifact_kind,
        stage_name=stage_name,
        artifact_key=artifact_key,
        version=version,
        alias=alias,
        content_text=content_text,
        content_json=content_json,
        blob_uri=blob_uri,
        metadata_json=metadata_json,
        storage_backend=storage_backend,
        vector_collection_name=vector_collection_name,
        vector_persist_path=vector_persist_path,
        docstore_persist_path=docstore_persist_path,
    )
    db.add(artifact)
    db.flush()

    for src_id in input_artifact_ids or []:
        db.add(ArtifactInput(artifact_id=artifact.id, input_artifact_id=src_id))

    db.flush()
    return artifact


def query_project_artifacts(db: Session, project_id: str, kind: str | None = None) -> Select[tuple[Artifact]]:
    stmt = select(Artifact).where(Artifact.project_id == project_id)
    if kind is not None:
        stmt = stmt.where(Artifact.artifact_kind == kind)
    return stmt.order_by(Artifact.created_at.desc())


def build_lineage_backward(db: Session, artifact_id: str) -> list[Artifact]:
    seen: set[str] = set()
    out: list[Artifact] = []
    queue: deque[str] = deque([artifact_id])

    while queue:
        current = queue.popleft()
        if current in seen:
            continue
        seen.add(current)
        edge_rows = db.execute(select(ArtifactInput).where(ArtifactInput.artifact_id == current)).scalars().all()
        for edge in edge_rows:
            src = db.get(Artifact, edge.input_artifact_id)
            if src is not None:
                out.append(src)
                queue.append(src.id)
    return out


def build_lineage_forward(db: Session, artifact_id: str) -> list[Artifact]:
    seen: set[str] = set()
    out: list[Artifact] = []
    queue: deque[str] = deque([artifact_id])

    while queue:
        current = queue.popleft()
        if current in seen:
            continue
        seen.add(current)
        edge_rows = db.execute(select(ArtifactInput).where(ArtifactInput.input_artifact_id == current)).scalars().all()
        for edge in edge_rows:
            dst = db.get(Artifact, edge.artifact_id)
            if dst is not None:
                out.append(dst)
                queue.append(dst.id)
    return out


def artifact_versions(db: Session, artifact: Artifact) -> list[Artifact]:
    return db.execute(
        select(Artifact).where(
            Artifact.project_id == artifact.project_id,
            Artifact.pipeline_id == artifact.pipeline_id,
            Artifact.artifact_kind == artifact.artifact_kind,
            Artifact.stage_name == artifact.stage_name,
            Artifact.artifact_key == artifact.artifact_key,
        ).order_by(Artifact.version.desc())
    ).scalars().all()
