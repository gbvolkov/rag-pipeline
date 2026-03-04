from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class PluginRef(BaseModel):
    plugin_ref: str = Field(min_length=1)


JSONValue = str | int | float | bool | None | dict[str, Any] | list[Any]


class LoaderConfig(BaseModel):
    type: str = Field(min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)


class PipelineInputRef(BaseModel):
    alias: str = Field(min_length=1)
    source_pipeline_id: str
    source_stage_name: str
    artifact_kind: Literal["document", "segment", "index"]
    pinned_version: int | None = Field(default=None, ge=1)


class SegmentationStageConfig(BaseModel):
    stage_name: str = Field(min_length=1)
    splitter_type: str = Field(min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)
    input_aliases: list[str] = Field(default_factory=list)
    position: int = Field(ge=0)


class IndexingConfig(BaseModel):
    index_type: str = Field(min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)
    collection_name: str | None = None
    docstore_name: str | None = None


class PipelineCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    description: str | None = None
    loader: LoaderConfig | None = None
    inputs: list[PipelineInputRef] = Field(default_factory=list)
    segmentation_stages: list[SegmentationStageConfig] = Field(default_factory=list)
    indexing: IndexingConfig | None = None
    metadata: dict[str, JSONValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_shape(self) -> "PipelineCreate":
        if self.loader and self.inputs:
            raise ValueError("loader and inputs are mutually exclusive")
        if not self.loader and not self.inputs:
            raise ValueError("pipeline must define loader or inputs")
        return self


class PipelineCopyRequest(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    description: str | None = None


class PipelineOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    name: str
    description: str | None
    shape: str
    immutable: bool
    deleted: bool
    definition: dict[str, Any]
    created_at: datetime

