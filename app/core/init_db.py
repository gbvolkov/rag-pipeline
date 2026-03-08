from __future__ import annotations

from sqlalchemy import inspect

from app.core.database import engine
from app.models import Base


def _validate_existing_schema() -> None:
    with engine.begin() as conn:
        inspector = inspect(conn)
        tables = set(inspector.get_table_names())
        if "retrievers" not in tables:
            return

        columns = {column["name"]: column for column in inspector.get_columns("retrievers")}
        problems: list[str] = []
        if "source_artifact_ids" not in columns:
            problems.append("retrievers.source_artifact_ids column is missing")
        index_artifact_column = columns.get("index_artifact_id")
        if index_artifact_column is not None and not index_artifact_column.get("nullable", True):
            problems.append("retrievers.index_artifact_id must be nullable")

        if problems:
            raise RuntimeError(
                "Database schema is outdated; apply migrations before starting the API. "
                + "; ".join(problems)
            )


def create_all() -> None:
    Base.metadata.create_all(bind=engine)
    _validate_existing_schema()
