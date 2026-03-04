from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class CapabilityMatrixOut(BaseModel):
    rag_lib_version: str
    source_hash: str
    generated_at: datetime
    matrix: dict[str, Any]


class ExampleCapabilityItem(BaseModel):
    example_path: str
    profile_id: str
    family: str
    support_status: str
    implemented: bool
    notes: str | None = None


class ExampleCapabilityMatrixOut(BaseModel):
    generated_at: datetime
    total_examples: int
    covered_examples: int
    items: list[ExampleCapabilityItem]

