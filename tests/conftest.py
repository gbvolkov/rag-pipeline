from __future__ import annotations

import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]

os.environ.setdefault("DATABASE_URL", "sqlite:///./.rag_api_test.db")
os.environ.setdefault("CELERY_ALWAYS_EAGER", "true")

from app.core.database import engine
from app.models import Base
from app.main import create_app


@pytest.fixture(autouse=True)
def _reset_db() -> None:
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture()
def client() -> TestClient:
    app = create_app()
    with TestClient(app) as c:
        yield c
