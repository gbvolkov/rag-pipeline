from __future__ import annotations

from datetime import datetime
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar("T")


class ErrorEnvelope(BaseModel):
    code: str
    message: str
    details: dict[str, Any] | None = None
    rag_lib_exception_type: str | None = None


class PaginatedResponse(BaseModel, Generic[T]):
    total: int
    offset: int
    limit: int
    items: list[T]


class APIMessage(BaseModel):
    message: str


class Timestamped(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    created_at: datetime


class IdMixin(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str = Field(min_length=1)

