from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class GraphStoreConfig(BaseModel):
    provider: str = Field(min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)


class ProjectCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    description: str | None = None
    graph_store_config: GraphStoreConfig | None = None


class ProjectPatch(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=255)
    description: str | None = None
    graph_store_config: GraphStoreConfig | None = None


class ProjectSummary(BaseModel):
    pipelines: int
    active_jobs: int
    artifacts: int
    retrievers: int


class ProjectOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    description: str | None
    status: str
    graph_store_config: dict[str, Any] | None
    created_at: datetime
    updated_at: datetime
    summary: ProjectSummary | None = None

