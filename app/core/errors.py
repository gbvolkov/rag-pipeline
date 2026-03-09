from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger(__name__)


@dataclass
class APIError(Exception):
    status_code: int
    code: str
    message: str
    details: dict[str, Any] | None = None
    rag_lib_exception_type: str | None = None

    def __post_init__(self) -> None:
        self.args = self._serialization_args()

    def _serialization_args(self) -> tuple[Any, ...]:
        if type(self) is APIError:
            return (
                self.status_code,
                self.code,
                self.message,
                self.details,
                self.rag_lib_exception_type,
            )
        if type(self) is ServiceUnavailableError:
            return (self.message, self.details, self.rag_lib_exception_type)
        return (self.message, self.details)

    def __str__(self) -> str:
        return self.message


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


def _normalized_validation_errors(exc: ValidationError | RequestValidationError) -> list[dict[str, Any]]:
    try:
        raw_errors = exc.errors(include_input=False)
    except TypeError:
        raw_errors = exc.errors()

    normalized: list[dict[str, Any]] = []
    for error in raw_errors:
        if not isinstance(error, dict):
            continue
        entry = dict(error)
        entry.pop("input", None)
        normalized.append(entry)
    return normalized


def install_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(APIError)
    async def _api_error_handler(request: Request, exc: APIError) -> JSONResponse:
        logger.warning(
            "APIError: method=%s path=%s query=%s status=%s code=%s message=%s details=%s rag_lib_exception_type=%s",
            request.method,
            request.url.path,
            request.url.query or "",
            exc.status_code,
            exc.code,
            exc.message,
            exc.details,
            exc.rag_lib_exception_type,
        )
        return _error_response(exc)

    @app.exception_handler(ValidationError)
    async def _validation_error_handler(request: Request, exc: ValidationError) -> JSONResponse:
        errors = _normalized_validation_errors(exc)
        logger.warning(
            "ValidationError: method=%s path=%s query=%s errors=%s",
            request.method,
            request.url.path,
            request.url.query or "",
            errors,
        )
        return JSONResponse(
            status_code=422,
            content={"code": "unprocessable", "message": "Validation failed", "details": {"errors": errors}},
        )

    @app.exception_handler(RequestValidationError)
    async def _request_validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        errors = _normalized_validation_errors(exc)
        logger.warning(
            "RequestValidationError: method=%s path=%s query=%s errors=%s",
            request.method,
            request.url.path,
            request.url.query or "",
            errors,
        )
        return JSONResponse(
            status_code=422,
            content={"code": "unprocessable", "message": "Validation failed", "details": {"errors": errors}},
        )

    @app.exception_handler(IntegrityError)
    async def _integrity_error_handler(request: Request, exc: IntegrityError) -> JSONResponse:
        logger.warning(
            "IntegrityError: method=%s path=%s query=%s error=%s",
            request.method,
            request.url.path,
            request.url.query or "",
            str(exc.orig),
        )
        return JSONResponse(
            status_code=409,
            content={"code": "conflict", "message": "Constraint violation", "details": {"error": str(exc.orig)}},
        )

    @app.exception_handler(Exception)
    async def _unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception(
            "Unhandled exception: method=%s path=%s query=%s error_type=%s",
            request.method,
            request.url.path,
            request.url.query or "",
            type(exc).__name__,
        )
        return JSONResponse(
            status_code=500,
            content={"code": "internal_error", "message": "Unhandled server error", "details": {"error": str(exc)}},
        )
