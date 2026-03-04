from __future__ import annotations

from celery import Celery

from app.core.config import get_settings

settings = get_settings()

celery_app = Celery(
    "rag_api",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=["app.workers.tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    task_track_started=True,
    task_always_eager=settings.celery_always_eager,
    task_default_queue=settings.celery_queue_default,
    task_routes={
        "app.workers.tasks.run_pipeline_job": {"queue": settings.celery_queue_default},
        "app.workers.tasks.run_reindex_job": {"queue": settings.celery_queue_default},
        "app.workers.tasks.run_mineru_job": {"queue": settings.celery_queue_mineru},
    },
)

# Ensure task decorators are registered when this module is imported by the worker.
from app.workers import tasks as _tasks  # noqa: F401
