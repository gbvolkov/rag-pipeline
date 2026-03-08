from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.core import init_db


class _FakeConnection:
    def __init__(self, *, dialect_name: str) -> None:
        self.dialect = SimpleNamespace(name=dialect_name)


class _FakeBegin:
    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection

    def __enter__(self) -> _FakeConnection:
        return self._connection

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeEngine:
    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection

    def begin(self) -> _FakeBegin:
        return _FakeBegin(self._connection)


class _FakeInspector:
    def __init__(self, *, tables: list[str], columns: list[dict[str, object]]) -> None:
        self._tables = tables
        self._columns = columns

    def get_table_names(self) -> list[str]:
        return list(self._tables)

    def get_columns(self, table_name: str) -> list[dict[str, object]]:
        assert table_name == "retrievers"
        return list(self._columns)


def test_validate_existing_schema_rejects_outdated_retrievers_table(monkeypatch):
    connection = _FakeConnection(dialect_name="postgresql")
    inspector = _FakeInspector(
        tables=["retrievers"],
        columns=[
            {"name": "id", "nullable": False},
            {"name": "project_id", "nullable": False},
            {"name": "index_artifact_id", "nullable": False},
        ],
    )

    monkeypatch.setattr(init_db, "engine", _FakeEngine(connection))
    monkeypatch.setattr(init_db, "inspect", lambda conn: inspector)

    with pytest.raises(RuntimeError, match="Database schema is outdated"):
        init_db._validate_existing_schema()


def test_validate_existing_schema_is_noop_when_retrievers_table_absent(monkeypatch):
    connection = _FakeConnection(dialect_name="postgresql")
    inspector = _FakeInspector(tables=["projects"], columns=[])

    monkeypatch.setattr(init_db, "engine", _FakeEngine(connection))
    monkeypatch.setattr(init_db, "inspect", lambda conn: inspector)

    init_db._validate_existing_schema()
