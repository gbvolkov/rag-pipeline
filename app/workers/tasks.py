from __future__ import annotations

import logging

from app.core.celery_app import celery_app
from app.core.database import SessionLocal
from app.services.jobs import run_pipeline_job, run_reindex_job

logger = logging.getLogger(__name__)


@celery_app.task(name="app.workers.tasks.run_pipeline_job")
def run_pipeline_job_task(job_id: str) -> str:
    logger.info("Worker received pipeline job: job_id=%s", job_id)
    db = SessionLocal()
    try:
        run_pipeline_job(db, job_id)
        logger.info("Worker finished pipeline job successfully: job_id=%s", job_id)
        return job_id
    except Exception:
        logger.exception("Worker failed pipeline job: job_id=%s", job_id)
        raise
    finally:
        db.close()


@celery_app.task(name="app.workers.tasks.run_reindex_job")
def run_reindex_job_task(job_id: str) -> str:
    logger.info("Worker received reindex job: job_id=%s", job_id)
    db = SessionLocal()
    try:
        run_reindex_job(db, job_id)
        logger.info("Worker finished reindex job successfully: job_id=%s", job_id)
        return job_id
    except Exception:
        logger.exception("Worker failed reindex job: job_id=%s", job_id)
        raise
    finally:
        db.close()


@celery_app.task(name="app.workers.tasks.run_mineru_job")
def run_mineru_job_task(job_id: str) -> str:
    # MinerU jobs follow the same orchestration today; dedicated queue is reserved.
    logger.info("Worker received MinerU job: job_id=%s", job_id)
    db = SessionLocal()
    try:
        run_pipeline_job(db, job_id)
        logger.info("Worker finished MinerU job successfully: job_id=%s", job_id)
        return job_id
    except Exception:
        logger.exception("Worker failed MinerU job: job_id=%s", job_id)
        raise
    finally:
        db.close()
