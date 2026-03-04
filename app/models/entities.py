from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utcnow() -> datetime:
    return datetime.now(tz=UTC)


def new_id() -> str:
    return str(uuid4())


def json_type() -> type[JSON]:
    return JSON


class Base(DeclarativeBase):
    pass


class Project(Base):
    __tablename__ = "projects"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="active", nullable=False)
    graph_store_config: Mapped[dict[str, Any] | None] = mapped_column(json_type(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)

    pipelines: Mapped[list["Pipeline"]] = relationship(back_populates="project")


class Pipeline(Base):
    __tablename__ = "pipelines"
    __table_args__ = (
        UniqueConstraint("project_id", "name", name="uq_pipeline_name_per_project"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    shape: Mapped[str] = mapped_column(String(64), nullable=False)
    immutable: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    deleted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    definition: Mapped[dict[str, Any]] = mapped_column(json_type(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    project: Mapped["Project"] = relationship(back_populates="pipelines")
    inputs: Mapped[list["PipelineInput"]] = relationship(back_populates="pipeline")
    segmentation_stages: Mapped[list["PipelineSegmentationStage"]] = relationship(back_populates="pipeline")
    indexing_config: Mapped["PipelineIndexingConfig | None"] = relationship(back_populates="pipeline", uselist=False)


class PipelineInput(Base):
    __tablename__ = "pipeline_inputs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    pipeline_id: Mapped[str] = mapped_column(ForeignKey("pipelines.id", ondelete="CASCADE"), index=True, nullable=False)
    alias: Mapped[str] = mapped_column(String(128), nullable=False)
    source_pipeline_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    source_stage_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    artifact_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    pinned_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    pipeline: Mapped["Pipeline"] = relationship(back_populates="inputs")


class PipelineSegmentationStage(Base):
    __tablename__ = "pipeline_segmentation_stages"
    __table_args__ = (
        UniqueConstraint("pipeline_id", "stage_name", name="uq_stage_name_per_pipeline"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    pipeline_id: Mapped[str] = mapped_column(ForeignKey("pipelines.id", ondelete="CASCADE"), index=True, nullable=False)
    stage_name: Mapped[str] = mapped_column(String(128), nullable=False)
    splitter_type: Mapped[str] = mapped_column(String(128), nullable=False)
    params: Mapped[dict[str, Any]] = mapped_column(json_type(), nullable=False)
    input_aliases: Mapped[list[str]] = mapped_column(json_type(), nullable=False)
    position: Mapped[int] = mapped_column(Integer, nullable=False)

    pipeline: Mapped["Pipeline"] = relationship(back_populates="segmentation_stages")


class PipelineIndexingConfig(Base):
    __tablename__ = "pipeline_indexing_config"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    pipeline_id: Mapped[str] = mapped_column(
        ForeignKey("pipelines.id", ondelete="CASCADE"), index=True, nullable=False, unique=True
    )
    index_type: Mapped[str] = mapped_column(String(128), nullable=False)
    params: Mapped[dict[str, Any]] = mapped_column(json_type(), nullable=False)
    collection_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    docstore_name: Mapped[str | None] = mapped_column(String(255), nullable=True)

    pipeline: Mapped["Pipeline"] = relationship(back_populates="indexing_config")


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True)
    pipeline_id: Mapped[str | None] = mapped_column(ForeignKey("pipelines.id", ondelete="SET NULL"), nullable=True, index=True)
    kind: Mapped[str] = mapped_column(String(64), nullable=False)  # run_pipeline|reindex|example_conformance
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued")
    stage: Mapped[str | None] = mapped_column(String(64), nullable=True)
    payload: Mapped[dict[str, Any] | None] = mapped_column(json_type(), nullable=True)
    result: Mapped[dict[str, Any] | None] = mapped_column(json_type(), nullable=True)
    error: Mapped[dict[str, Any] | None] = mapped_column(json_type(), nullable=True)
    canceled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    celery_task_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    capability_snapshot_id: Mapped[str | None] = mapped_column(
        ForeignKey("capabilities_snapshots.id", ondelete="SET NULL"), nullable=True
    )
    example_profile_snapshot_id: Mapped[str | None] = mapped_column(
        ForeignKey("example_profiles.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    events: Mapped[list["JobEvent"]] = relationship(back_populates="job")


class JobEvent(Base):
    __tablename__ = "job_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    stage: Mapped[str | None] = mapped_column(String(64), nullable=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    data: Mapped[dict[str, Any] | None] = mapped_column(json_type(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    job: Mapped["Job"] = relationship(back_populates="events")


class Artifact(Base):
    __tablename__ = "artifacts"
    __table_args__ = (
        UniqueConstraint(
            "project_id",
            "pipeline_id",
            "artifact_kind",
            "stage_name",
            "artifact_key",
            "version",
            name="uq_artifact_monotonic_scope",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True)
    pipeline_id: Mapped[str | None] = mapped_column(ForeignKey("pipelines.id", ondelete="SET NULL"), nullable=True, index=True)
    job_id: Mapped[str | None] = mapped_column(ForeignKey("jobs.id", ondelete="SET NULL"), nullable=True, index=True)
    artifact_kind: Mapped[str] = mapped_column(String(64), nullable=False)  # document|segment|index|retrieval_result
    stage_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    artifact_key: Mapped[str] = mapped_column(String(255), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    alias: Mapped[str | None] = mapped_column(String(128), nullable=True)
    content_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    content_json: Mapped[dict[str, Any] | None] = mapped_column(json_type(), nullable=True)
    blob_uri: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(json_type(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    inputs: Mapped[list["ArtifactInput"]] = relationship(
        back_populates="artifact", foreign_keys="ArtifactInput.artifact_id"
    )


class ArtifactInput(Base):
    __tablename__ = "artifact_inputs"
    __table_args__ = (
        UniqueConstraint("artifact_id", "input_artifact_id", name="uq_artifact_input_edge"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    artifact_id: Mapped[str] = mapped_column(ForeignKey("artifacts.id", ondelete="CASCADE"), nullable=False, index=True)
    input_artifact_id: Mapped[str] = mapped_column(
        ForeignKey("artifacts.id", ondelete="CASCADE"), nullable=False, index=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    artifact: Mapped["Artifact"] = relationship(back_populates="inputs", foreign_keys=[artifact_id])


class Retriever(Base):
    __tablename__ = "retrievers"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True)
    index_artifact_id: Mapped[str] = mapped_column(ForeignKey("artifacts.id", ondelete="CASCADE"), nullable=False, index=True)
    retriever_type: Mapped[str] = mapped_column(String(128), nullable=False)
    params: Mapped[dict[str, Any]] = mapped_column(json_type(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class RetrieverSession(Base):
    __tablename__ = "retriever_sessions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    retriever_id: Mapped[str] = mapped_column(ForeignKey("retrievers.id", ondelete="CASCADE"), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    state: Mapped[dict[str, Any] | None] = mapped_column(json_type(), nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)


class RetrievalResult(Base):
    __tablename__ = "retrieval_results"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True)
    retriever_id: Mapped[str] = mapped_column(ForeignKey("retrievers.id", ondelete="CASCADE"), nullable=False, index=True)
    query_text: Mapped[str] = mapped_column(Text, nullable=False)
    top_k: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    items: Mapped[list["RetrievalResultItem"]] = relationship(back_populates="result")


class RetrievalResultItem(Base):
    __tablename__ = "retrieval_result_items"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    retrieval_result_id: Mapped[str] = mapped_column(
        ForeignKey("retrieval_results.id", ondelete="CASCADE"), nullable=False, index=True
    )
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    score: Mapped[float | None] = mapped_column(nullable=True)
    segment_artifact_id: Mapped[str] = mapped_column(ForeignKey("artifacts.id", ondelete="CASCADE"), nullable=False, index=True)
    segment_payload: Mapped[dict[str, Any]] = mapped_column(json_type(), nullable=False)

    result: Mapped["RetrievalResult"] = relationship(back_populates="items")


class CapabilitySnapshot(Base):
    __tablename__ = "capabilities_snapshots"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    rag_lib_version: Mapped[str] = mapped_column(String(64), nullable=False)
    source_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    capability_matrix: Mapped[dict[str, Any]] = mapped_column(json_type(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class ExampleProfile(Base):
    __tablename__ = "example_profiles"
    __table_args__ = (
        UniqueConstraint("profile_id", name="uq_example_profile_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    profile_id: Mapped[str] = mapped_column(String(255), nullable=False)
    profile_version: Mapped[str] = mapped_column(String(32), nullable=False, default="v1")
    family: Mapped[str] = mapped_column(String(128), nullable=False)
    source_examples: Mapped[list[str]] = mapped_column(json_type(), nullable=False)
    spec: Mapped[dict[str, Any]] = mapped_column(json_type(), nullable=False)
    support_status: Mapped[str] = mapped_column(String(32), nullable=False, default="declared")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)


class ExampleConformanceRun(Base):
    __tablename__ = "example_conformance_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    profile_id: Mapped[str] = mapped_column(ForeignKey("example_profiles.id", ondelete="CASCADE"), nullable=False, index=True)
    job_id: Mapped[str | None] = mapped_column(ForeignKey("jobs.id", ondelete="SET NULL"), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    report: Mapped[dict[str, Any] | None] = mapped_column(json_type(), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
