from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ArtifactOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    pipeline_id: str | None
    job_id: str | None
    artifact_kind: str
    stage_name: str | None
    artifact_key: str
    version: int
    alias: str | None
    content_text: str | None
    content_json: dict[str, Any] | None
    blob_uri: str | None
    metadata_json: dict[str, Any] | None
    created_at: datetime


class ArtifactLineageOut(BaseModel):
    artifact: ArtifactOut
    inputs: list[ArtifactOut] = Field(default_factory=list)
    dependents: list[ArtifactOut] = Field(default_factory=list)


class IndexListItem(BaseModel):
    artifact_id: str
    pipeline_id: str | None
    stage_name: str | None
    artifact_key: str
    version: int
    metadata_json: dict[str, Any] | None
    created_at: datetime

