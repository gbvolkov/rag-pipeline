from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import router
from app.core.config import get_settings
from app.core.errors import install_exception_handlers
from app.core.init_db import create_all
from app.core.database import SessionLocal
from app.services.example_profiles import write_catalog_file
from app.services.jobs import seed_example_profiles


def create_app() -> FastAPI:
    settings = get_settings()

    @asynccontextmanager
    async def _lifespan(_: FastAPI):
        create_all()
        db = SessionLocal()
        try:
            seed_example_profiles(db)
        finally:
            db.close()
        write_catalog_file()
        yield

    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        openapi_url=f"{settings.api_prefix}/openapi.json",
        docs_url=f"{settings.api_prefix}/docs",
        redoc_url=f"{settings.api_prefix}/redoc",
        lifespan=_lifespan,
    )
    install_exception_handlers(app)
    app.include_router(router, prefix=settings.api_prefix)

    return app


app = create_app()
