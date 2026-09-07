"""structlog configuration: JSON in production, human-readable in development.

Every line emitted during an investigation carries its ``incident_id``. That is bound once
into a context variable rather than threaded through call signatures, which is what makes
it possible to reconstruct a single investigation from a log stream carrying many.
"""

import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from uuid import UUID

import structlog

from dag_doctor.core.settings import AppEnv

_configured = False

# Third-party libraries that log every HTTP request or broker heartbeat at INFO, which
# buries the agent's own events under consumer chatter.
_NOISY_LIBRARIES = (
    "httpx",
    "httpcore",
    "urllib3",
    "aiokafka",
    "kafka",
    "sqlalchemy.engine",
)


def configure_logging(app_env: AppEnv = "dev", log_level: str = "INFO") -> None:
    """Configure structlog and the standard library root logger.

    Safe to call more than once; only the first call takes effect.

    Args:
        app_env: ``"prod"`` selects JSON output, ``"dev"`` selects the console renderer.
        log_level: Standard logging level name.
    """
    global _configured
    if _configured:
        return

    logging.basicConfig(format="%(message)s", stream=sys.stderr, level=log_level.upper())
    for name in _NOISY_LIBRARIES:
        logging.getLogger(name).setLevel(logging.WARNING)

    shared: list[structlog.typing.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]
    renderer: structlog.typing.Processor = (
        structlog.processors.JSONRenderer()
        if app_env == "prod"
        else structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())
    )

    structlog.configure(
        processors=[*shared, structlog.processors.format_exc_info, renderer],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    _configured = True


def reset_logging() -> None:
    """Forget that logging was configured, so a test can configure it again."""
    global _configured
    _configured = False


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a logger bound to a module name.

    Args:
        name: Usually ``__name__``.

    Returns:
        A structlog logger.
    """
    logger: structlog.stdlib.BoundLogger = structlog.stdlib.get_logger(name)
    return logger


@contextmanager
def log_context(**values: object) -> Iterator[None]:
    """Bind values onto every log line emitted inside the block.

    Args:
        **values: Fields to bind, such as ``node`` or ``tool``.

    Yields:
        Nothing; the binding is ambient.
    """
    tokens = structlog.contextvars.bind_contextvars(**values)
    try:
        yield
    finally:
        structlog.contextvars.reset_contextvars(**tokens)


@contextmanager
def incident_context(incident_id: UUID, **values: object) -> Iterator[None]:
    """Bind an incident onto every log line emitted inside the block.

    Args:
        incident_id: The investigation this work belongs to.
        **values: Any further fields to bind alongside it.

    Yields:
        Nothing; the binding is ambient.
    """
    with log_context(incident_id=str(incident_id), **values):
        yield
