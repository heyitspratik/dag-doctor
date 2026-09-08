"""The worker entrypoint: consume failures, investigate them, persist the result.

The handler is deliberately one unit of work per message. Everything is written before the
Kafka offset moves, so the only possible inconsistency is a message processed twice, which
the incident's unique constraint absorbs. The reverse order would allow a failure to be
acknowledged and then lost.

A failure that has already been diagnosed is not investigated again on redelivery: the
answer would be the same and the model would be paid for it twice.
"""

import asyncio
import signal
from collections.abc import Awaitable, Callable

from prometheus_client import start_http_server
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dag_doctor.core.logging import configure_logging, get_logger, incident_context
from dag_doctor.core.metrics import WORKER_UP
from dag_doctor.core.models import FailureEvent
from dag_doctor.core.settings import Settings, get_settings
from dag_doctor.db.repositories import IncidentRepository
from dag_doctor.db.session import build_engine, build_session_factory, session_scope
from dag_doctor.graph.model import LangChainCaller
from dag_doctor.graph.toolbox import Toolbox
from dag_doctor.messaging.consumer import FailureConsumer
from dag_doctor.messaging.producer import DiagnosisPublisher
from dag_doctor.tools.connections import ConnectionRegistry
from dag_doctor.tools.factory import build_tools
from dag_doctor.worker.investigator import Investigator

logger = get_logger(__name__)


def build_handler(
    session_factory: async_sessionmaker[AsyncSession],
) -> Callable[[FailureEvent], Awaitable[None]]:
    """Build a handler that records a failure as an incident and nothing more.

    Used where the graph is not wanted, such as an ingest-only deployment.

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


def build_investigating_handler(
    investigator: Investigator,
) -> Callable[[FailureEvent], Awaitable[None]]:
    """Build the handler the worker actually runs: record, then investigate.

    Anything the investigation raises propagates, so the consumer retries the message and
    then dead-letters it rather than committing past an incident it never diagnosed.

    Args:
        investigator: The service that runs the graph and persists its result.

    Returns:
        The handler to hand to the consumer.
    """

    async def handle(event: FailureEvent) -> None:
        await investigator.handle(event)

    return handle


async def run_worker(settings: Settings | None = None) -> None:
    """Run the consumer until the process is asked to stop.

    Args:
        settings: Application settings; read from the environment when absent.
    """
    settings = settings or get_settings()
    configure_logging(settings.app_env, settings.log_level)

    # The worker and the API are separate processes with separate registries, so the
    # worker serves its own scrape endpoint rather than trying to share one.
    start_http_server(settings.metrics_port)
    WORKER_UP.set(1)

    engine = build_engine(settings.db)
    session_factory = build_session_factory(engine)
    connections = ConnectionRegistry.from_settings(settings)
    toolbox = Toolbox(build_tools(settings, session_factory, connections))

    async with LangChainCaller(settings.llm) as caller:
        investigator = Investigator(
            settings,
            session_factory,
            caller,
            toolbox,
            DiagnosisPublisher(settings.kafka),
        )
        consumer = FailureConsumer(settings.kafka, build_investigating_handler(investigator))
        _install_signal_handlers(consumer)
        try:
            async with investigator, consumer:
                await consumer.run()
        finally:
            WORKER_UP.set(0)
            await connections.dispose()
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
