from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "rag-api"
    app_version: str = "0.3.0"
    api_prefix: str = "/api/v1"

    database_url: str = Field(
        default="postgresql+psycopg://postgres:postgres@localhost:5432/rag_api",
        alias="DATABASE_URL",
    )

    redis_url: str = Field(default="redis://localhost:6379/0", alias="REDIS_URL")
    celery_always_eager: bool = Field(default=True, alias="CELERY_ALWAYS_EAGER")
    celery_queue_default: str = "rag-jobs"
    celery_queue_mineru: str = "rag-mineru"

    max_active_jobs_per_project: int = 4
    max_playwright_jobs: int = 2
    max_retriever_sessions_per_retriever: int = 8
    retriever_session_ttl_seconds: int = 3600

    local_blob_root: Path = Path("./data/blobstore")
    blob_backend: str = "filesystem"  # filesystem|minio
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_secure: bool = False
    minio_bucket: str = "rag-artifacts"

    rag_lib_source_dir: Path = Path("C:/Projects/rag-lib/src/rag_lib")
    rag_lib_examples_dir: Path = Path("C:/Projects/rag-lib/examples")

    plugin_registry_file: Path = Path("./config/plugins.json")


@lru_cache(1)
def get_settings() -> Settings:
    return Settings()

