"""The worker entrypoint: consume failures, persist incidents.

The handler is deliberately one unit of work per message. The database transaction commits
first and the Kafka offset second, so the only possible inconsistency is a message
processed twice, which the incident's unique constraint absorbs. The reverse order would
allow a failure to be acknowledged and then lost.

Running the investigation graph is not wired in yet; that arrives with the graph itself.
"""

import asyncio
import signal
from collections.abc import Awaitable, Callable

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dag_doctor.core.logging import configure_logging, get_logger, incident_context
from dag_doctor.core.models import FailureEvent
from dag_doctor.core.settings import Settings, get_settings
from dag_doctor.db.repositories import IncidentRepository
from dag_doctor.db.session import build_engine, build_session_factory, session_scope
from dag_doctor.messaging.consumer import FailureConsumer

logger = get_logger(__name__)


def build_handler(
    session_factory: async_sessionmaker[AsyncSession],
) -> Callable[[FailureEvent], Awaitable[None]]:
    """Build the handler that turns a failure event into a persisted incident.

    Args:
        session_factory: Where each message's unit of work gets its session.

    Returns:
        The handler to hand to the consumer.
    """

    async def handle(event: FailureEvent) -> None:
        async with session_scope(session_factory) as session:
            incident, created = await IncidentRepository(session).get_or_create(event)
            with incident_context(incident.id, dag_id=event.dag_id, task_id=event.task_id):
                logger.info("incident.recorded", created=created, status=incident.status.value)

    return handle


async def run_worker(settings: Settings | None = None) -> None:
    """Run the consumer until the process is asked to stop.

    Args:
        settings: Application settings; read from the environment when absent.
    """
    settings = settings or get_settings()
    configure_logging(settings.app_env, settings.log_level)

    engine = build_engine(settings.db)
    consumer = FailureConsumer(settings.kafka, build_handler(build_session_factory(engine)))
    _install_signal_handlers(consumer)

    try:
        async with consumer:
            await consumer.run()
    finally:
        await engine.dispose()


def _install_signal_handlers(consumer: FailureConsumer) -> None:
    """Ask the consumer to drain on SIGTERM rather than abandoning work in flight.

    This is what the Kubernetes preStop hook depends on: the pod stops accepting new
    records but finishes the incident it is holding.
    """
    loop = asyncio.get_running_loop()
    for received in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(received, consumer.request_stop)


def main() -> None:
    """Console entrypoint for the worker container."""
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
