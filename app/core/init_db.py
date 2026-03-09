from __future__ import annotations

from sqlalchemy import inspect

from app.core.database import engine
from app.models import Base


def _validate_existing_schema() -> None:
    with engine.begin() as conn:
        inspector = inspect(conn)
        tables = set(inspector.get_table_names())
        if "retrievers" not in tables and "artifacts" not in tables:
            return

        problems: list[str] = []
        if "retrievers" in tables:
            retriever_columns = {column["name"]: column for column in inspector.get_columns("retrievers")}
            if "source_artifact_ids" not in retriever_columns:
                problems.append("retrievers.source_artifact_ids column is missing")
            index_artifact_column = retriever_columns.get("index_artifact_id")
            if index_artifact_column is not None and not index_artifact_column.get("nullable", True):
                problems.append("retrievers.index_artifact_id must be nullable")

        if "artifacts" in tables:
            artifact_columns = {column["name"]: column for column in inspector.get_columns("artifacts")}
            required_artifact_columns = {
                "storage_backend",
                "vector_collection_name",
                "vector_persist_path",
                "docstore_persist_path",
            }
            for column_name in sorted(required_artifact_columns):
                if column_name not in artifact_columns:
                    problems.append(f"artifacts.{column_name} column is missing")

        if problems:
            raise RuntimeError(
                "Database schema is outdated; recreate the database before starting the API. "
                + "; ".join(problems)
            )


def create_all() -> None:
    Base.metadata.create_all(bind=engine)
    _validate_existing_schema()
