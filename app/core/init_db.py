from __future__ import annotations

from app.core.database import engine
from app.models import Base


def create_all() -> None:
    Base.metadata.create_all(bind=engine)

