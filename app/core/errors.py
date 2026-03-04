from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError


@dataclass
class APIError(Exception):
    status_code: int
    code: str
    message: str
    details: dict[str, Any] | None = None
    rag_lib_exception_type: str | None = None


class NotFoundError(APIError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(404, "not_found", message, details)


class ConflictError(APIError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(409, "conflict", message, details)


class UnprocessableError(APIError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(422, "unprocessable", message, details)


class ServiceUnavailableError(APIError):
    def __init__(
        self,
        message: str,
        details: dict[str, Any] | None = None,
        rag_lib_exception_type: str | None = None,
    ):
        super().__init__(503, "service_unavailable", message, details, rag_lib_exception_type)


def _error_response(err: APIError) -> JSONResponse:
    return JSONResponse(
        status_code=err.status_code,
        content={
            "code": err.code,
            "message": err.message,
            "details": err.details,
            "rag_lib_exception_type": err.rag_lib_exception_type,
        },
    )


def install_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(APIError)
    async def _api_error_handler(_: Request, exc: APIError) -> JSONResponse:
        return _error_response(exc)

    @app.exception_handler(ValidationError)
    async def _validation_error_handler(_: Request, exc: ValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={"code": "unprocessable", "message": "Validation failed", "details": {"errors": exc.errors()}},
        )

    @app.exception_handler(IntegrityError)
    async def _integrity_error_handler(_: Request, exc: IntegrityError) -> JSONResponse:
        return JSONResponse(
            status_code=409,
            content={"code": "conflict", "message": "Constraint violation", "details": {"error": str(exc.orig)}},
        )

    @app.exception_handler(Exception)
    async def _fallback_error_handler(_: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={"code": "internal_error", "message": "Unhandled server error", "details": {"error": str(exc)}},
        )

