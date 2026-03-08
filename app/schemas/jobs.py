from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class RunSubmitRequest(BaseModel):
    url: str | None = None
    file_name: str | None = None
    webhook_url: str | None = None
    pinned_versions: dict[str, int] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ReindexRequest(BaseModel):
    source_segments: list[dict[str, Any]]
    indexing: dict[str, Any]


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    pipeline_id: str | None
    kind: str
    status: str
    stage: str | None
    payload: dict[str, Any] | None
    result: dict[str, Any] | None
    error: dict[str, Any] | None
    canceled: bool
    celery_task_id: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class JobEventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    job_id: str
    status: str
    stage: str | None
    message: str | None
    data: dict[str, Any] | None
    created_at: datetime


class JobDetailOut(JobOut):
    events: list[JobEventOut] = Field(default_factory=list)
