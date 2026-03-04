from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class RetrieverCreate(BaseModel):
    index_artifact_id: str = Field(min_length=1)
    retriever_type: str = Field(min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)


class RetrieverOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    index_artifact_id: str
    retriever_type: str
    params: dict[str, Any]
    created_at: datetime


class RetrieverSessionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    retriever_id: str
    status: str
    state: dict[str, Any] | None
    expires_at: datetime | None
    created_at: datetime
    updated_at: datetime


class RetrieverQueryRequest(BaseModel):
    query: str = Field(min_length=1)
    top_k: int = Field(default=5, ge=1, le=100)
    session_id: str | None = None


class ScoredSegment(BaseModel):
    score: float | None = None
    segment: dict[str, Any]
    segment_artifact_id: str


class RetrievalResultOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    retriever_id: str
    query_text: str
    top_k: int
    created_at: datetime
    items: list[ScoredSegment] = Field(default_factory=list)

