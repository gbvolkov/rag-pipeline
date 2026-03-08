from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RetrieverCreate(BaseModel):
    index_artifact_id: str | None = None
    source_artifact_ids: list[str] = Field(default_factory=list)
    retriever_type: str = Field(min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_source(self) -> "RetrieverCreate":
        has_index = self.index_artifact_id is not None
        has_source_artifacts = bool(self.source_artifact_ids)
        if has_index == has_source_artifacts:
            raise ValueError("Provide exactly one of index_artifact_id or source_artifact_ids")
        return self


class RetrieverOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    index_artifact_id: str | None
    source_artifact_ids: list[str] | None
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
    model_config = ConfigDict(extra="forbid")

    query: str = Field(min_length=1)
    session_id: str | None = None


class ScoredSegment(BaseModel):
    score: float | None = None
    similarity_score: float | None = None
    max_similarity_score: float | None = None
    fuzzy_score: float | None = None
    rerank_score: float | None = None
    score_details: dict[str, float] = Field(default_factory=dict)
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
