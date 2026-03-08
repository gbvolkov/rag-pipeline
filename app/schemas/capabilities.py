from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class CapabilityMatrixOut(BaseModel):
    rag_lib_version: str
    source_hash: str
    generated_at: datetime
    matrix: dict[str, Any]
