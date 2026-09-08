"""One error shape for the whole API.

Every failure the application raises deliberately carries a stable code and an HTTP
status, so the envelope is rendered from exception handlers rather than assembled by hand
in each route. A client can switch on ``error.code``; a human gets ``error.message``; and
the request id ties a response to the log lines that produced it.
"""

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from dag_doctor.core.exceptions import DagDoctorError
from dag_doctor.core.logging import get_logger

logger = get_logger(__name__)


class ErrorBody(BaseModel):
    """The error itself."""

    code: str = Field(examples=["NOT_FOUND"])
    message: str = Field(examples=["No incident 0b7f..."])
    details: dict[str, object] = Field(default_factory=dict)
    request_id: str | None = None


class ErrorResponse(BaseModel):
    """The envelope every failed request returns."""

    error: ErrorBody


def _render(
    request: Request, status_code: int, code: str, message: str, details: dict[str, object]
) -> JSONResponse:
    """Build the envelope, attaching the request id the middleware assigned."""
    body = ErrorBody(
        code=code,
        message=message,
        details=details,
        request_id=getattr(request.state, "request_id", None),
    )
    return JSONResponse(
        status_code=status_code, content=jsonable_encoder(ErrorResponse(error=body))
    )


def install_error_handlers(app: FastAPI) -> None:
    """Register the handlers that render every failure the same way.

    Args:
        app: The application to install them on.
    """

    @app.exception_handler(DagDoctorError)
    async def _domain_error(request: Request, exc: Exception) -> JSONResponse:
        error = exc if isinstance(exc, DagDoctorError) else DagDoctorError(str(exc))
        # Client mistakes are not incidents. Logging a 404 at error level trains people
        # to ignore the error log, which is where the 500s live.
        log = logger.warning if error.http_status < 500 else logger.error
        log("api.error", code=error.code, status=error.http_status, message=error.message)
        return _render(request, error.http_status, error.code, error.message, error.details)

    @app.exception_handler(RequestValidationError)
    async def _validation_error(request: Request, exc: Exception) -> JSONResponse:
        errors = exc.errors() if isinstance(exc, RequestValidationError) else []
        return _render(
            request,
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "VALIDATION_ERROR",
            "The request did not match the expected shape",
            {"errors": jsonable_encoder(errors)},
        )

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
        # The message is deliberately generic. Whatever went wrong is in the log under
        # this request id; echoing it to a caller leaks internals for no benefit.
        logger.exception("api.unhandled", error=type(exc).__name__)
        return _render(
            request,
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
            "The request could not be completed",
            {},
        )


#: The responses every route can return, for the OpenAPI document.
COMMON_RESPONSES: dict[int | str, dict[str, object]] = {
    401: {"model": ErrorResponse, "description": "The API key is missing or wrong"},
    404: {"model": ErrorResponse, "description": "No such resource"},
    422: {"model": ErrorResponse, "description": "The request did not validate"},
    500: {"model": ErrorResponse, "description": "Something went wrong"},
}
