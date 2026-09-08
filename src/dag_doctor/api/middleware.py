"""Request-scoped logging context.

Every log line emitted while handling a request carries the same request id, and the id
comes back on the response. That is what lets someone paste an id from a failed call and
find every line the agent wrote about it.
"""

import time
from collections.abc import Awaitable, Callable
from uuid import uuid4

from fastapi import FastAPI, Request, Response

from dag_doctor.core.logging import get_logger, log_context

logger = get_logger(__name__)

REQUEST_ID_HEADER = "X-Request-ID"


def install_request_context(app: FastAPI) -> None:
    """Bind a request id onto every log line, and time each request.

    Args:
        app: The application to install the middleware on.
    """

    @app.middleware("http")
    async def _request_context(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        # An id supplied by a caller is honoured, so a trace can span a gateway and this
        # service rather than restarting at the door.
        request_id = request.headers.get(REQUEST_ID_HEADER) or str(uuid4())
        request.state.request_id = request_id
        started = time.perf_counter()

        with log_context(request_id=request_id, path=request.url.path, method=request.method):
            response = await call_next(request)

        duration_ms = int((time.perf_counter() - started) * 1000)
        response.headers[REQUEST_ID_HEADER] = request_id
        # /metrics is scraped every few seconds and would otherwise dominate the log.
        if request.url.path != "/metrics":
            logger.info(
                "api.request",
                request_id=request_id,
                path=request.url.path,
                method=request.method,
                status=response.status_code,
                duration_ms=duration_ms,
            )
        return response
