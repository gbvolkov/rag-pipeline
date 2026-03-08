from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


JSONValue = str | int | float | bool | None | dict[str, Any] | list[Any]


class LoaderConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str = Field(min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)


class RuntimeInputConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    alias: str = Field(default="RUNTIME_INPUT", min_length=1)
    artifact_kind: Literal["document", "segment"]


class PipelineInputRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    alias: str = Field(min_length=1)
    source_pipeline_id: str
    source_stage_name: str
    artifact_kind: Literal["document", "segment", "index"]
    pinned_version: int | None = Field(default=None, ge=1)


class PipelineStageConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stage_name: str = Field(min_length=1)
    stage_kind: Literal["splitter", "processor"]
    component_type: str = Field(min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)
    input_aliases: list[str] = Field(default_factory=list)
    position: int = Field(ge=0)


class IndexingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    index_type: str = Field(min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)
    collection_name: str | None = None
    docstore_name: str | None = None


class PipelineCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=255)
    description: str | None = None
    loader: LoaderConfig | None = None
    runtime_input: RuntimeInputConfig | None = None
    inputs: list[PipelineInputRef] = Field(default_factory=list)
    stages: list[PipelineStageConfig] = Field(default_factory=list)
    indexing: IndexingConfig | None = None
    metadata: dict[str, JSONValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_shape(self) -> "PipelineCreate":
        source_count = int(self.loader is not None) + int(self.runtime_input is not None) + int(bool(self.inputs))
        if source_count > 1:
            raise ValueError("loader, runtime_input, and inputs are mutually exclusive")
        if source_count == 0:
            raise ValueError("pipeline must define loader, runtime_input, or inputs")
        return self

    def ordered_stages(self) -> list[PipelineStageConfig]:
        return sorted(self.stages, key=lambda stage: stage.position)


class PipelineCopyRequest(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    description: str | None = None


class PipelineValidationWarning(BaseModel):
    code: str
    message: str
    path: str | None = None
    details: dict[str, Any] | None = None


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
    validation_warnings: list[PipelineValidationWarning] = Field(default_factory=list)


class PipelineValidationResultOut(BaseModel):
    shape: str
    validation_warnings: list[PipelineValidationWarning] = Field(default_factory=list)
