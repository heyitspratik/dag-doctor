"""Liveness and readiness.

They answer different questions and are not interchangeable. Liveness asks whether the
process should be restarted; readiness asks whether it should be sent traffic. Wiring a
dependency check into liveness is how a brief database blip turns into a restart loop.
"""

import asyncio
from collections.abc import Awaitable, Callable

from fastapi import APIRouter, Request, Response, status
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dag_doctor.core.llm import check_llm_health
from dag_doctor.core.logging import get_logger
from dag_doctor.core.settings import Settings

logger = get_logger(__name__)

router = APIRouter(tags=["health"])

#: A readiness probe that hangs is worse than one that fails, because the orchestrator
#: learns nothing while it waits.
CHECK_TIMEOUT_S = 5.0

type Check = Callable[[], Awaitable[None]]


class Liveness(BaseModel):
    """The liveness answer."""

    status: str = "alive"


class Readiness(BaseModel):
    """The readiness answer, with each dependency's verdict."""

    ready: bool
    checks: dict[str, str]


@router.get("/health/live", summary="Is the process alive?")
async def live() -> Liveness:
    """Return without touching anything.

    Deliberately trivial. This answers whether the process is running, and nothing else.
    """
    return Liveness()


@router.get("/health/ready", summary="Should this process be sent traffic?")
async def ready(request: Request, response: Response) -> Readiness:
    """Check every dependency and report each one separately.

    Reporting per dependency rather than one boolean is what makes a failed probe
    actionable: "postgres unreachable" and "the model is not pulled" need different people.
    """
    checks: dict[str, str] = {}
    for name, check in _checks(request).items():
        try:
            await check()
        except Exception as exc:
            checks[name] = f"{type(exc).__name__}: {exc}"
        else:
            checks[name] = "ok"

    healthy = all(result == "ok" for result in checks.values())
    if not healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        logger.warning("api.not_ready", checks=checks)
    return Readiness(ready=healthy, checks=checks)


def _checks(request: Request) -> dict[str, Check]:
    """The readiness checks, which a test can replace wholesale."""
    override: dict[str, Check] | None = getattr(request.app.state, "readiness_checks", None)
    if override is not None:
        return override

    settings: Settings = request.app.state.settings
    factory: async_sessionmaker[AsyncSession] = request.app.state.session_factory

    async def database() -> None:
        async with factory() as session:
            await session.execute(text("SELECT 1"))

    async def broker() -> None:
        from dag_doctor.messaging.producer import _build_aiokafka_producer

        producer = _build_aiokafka_producer(settings.kafka)
        # Connecting and disconnecting is the honest check. Anything cheaper would only
        # confirm that the configuration parses, which is not what readiness means.
        try:
            await asyncio.wait_for(producer.start(), timeout=CHECK_TIMEOUT_S)
        finally:
            await producer.stop()

    async def provider() -> None:
        await check_llm_health(settings.llm, timeout_s=CHECK_TIMEOUT_S)

    return {"postgres": database, "redpanda": broker, "llm_provider": provider}
