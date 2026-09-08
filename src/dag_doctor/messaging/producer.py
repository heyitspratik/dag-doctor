"""Publishing onto the event log.

The producer is deliberately thin and injectable. Every test in this repository drives it
through a fake, so no test needs a broker, and the Airflow callback gets a synchronous
wrapper because Airflow calls its callbacks from ordinary synchronous task code.
"""

import asyncio
from types import TracebackType
from typing import Protocol, Self, runtime_checkable

from dag_doctor.core.exceptions import MessagingError
from dag_doctor.core.logging import get_logger
from dag_doctor.core.models import FailureEvent
from dag_doctor.core.settings import KafkaSettings
from dag_doctor.messaging.schemas import DiagnosisCompletedMessage, TaskFailureMessage

logger = get_logger(__name__)


@runtime_checkable
class AsyncProducer(Protocol):
    """The slice of an aiokafka producer this package actually uses."""

    async def start(self) -> None:
        """Connect to the broker."""
        ...

    async def stop(self) -> None:
        """Flush and disconnect."""
        ...

    async def send_and_wait(self, topic: str, value: bytes, key: bytes | None = None) -> object:
        """Send one record and wait for the broker to acknowledge it."""
        ...


def _build_aiokafka_producer(settings: KafkaSettings) -> AsyncProducer:
    """Construct the real producer.

    ``acks="all"`` with idempotence on is the point of using a log at all: a failure event
    that the broker never durably accepted is an incident nobody will ever investigate.
    """
    from aiokafka import AIOKafkaProducer

    producer: AsyncProducer = AIOKafkaProducer(
        bootstrap_servers=settings.bootstrap_servers,
        acks="all",
        enable_idempotence=True,
    )
    return producer


class FailurePublisher:
    """Publishes task failure events onto the failure topic."""

    def __init__(
        self,
        settings: KafkaSettings,
        producer: AsyncProducer | None = None,
    ) -> None:
        """Initialise the publisher.

        Args:
            settings: Broker address and topic names.
            producer: Injected in tests; the real client is built lazily otherwise, so
                importing this module does not require a broker or the aiokafka package.
        """
        self._settings = settings
        self._producer = producer
        self._started = False

    async def __aenter__(self) -> Self:
        """Connect to the broker."""
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Flush and disconnect."""
        await self.stop()

    async def start(self) -> None:
        """Connect to the broker, building the client if one was not injected."""
        if self._started:
            return
        if self._producer is None:
            self._producer = _build_aiokafka_producer(self._settings)
        try:
            await self._producer.start()
        except Exception as exc:
            raise MessagingError(
                f"Could not reach the broker at {self._settings.bootstrap_servers}",
                details={"bootstrap_servers": self._settings.bootstrap_servers},
            ) from exc
        self._started = True

    async def stop(self) -> None:
        """Flush and disconnect, tolerating a publisher that never started."""
        if self._producer is not None and self._started:
            await self._producer.stop()
        self._started = False

    async def publish(self, event: FailureEvent) -> None:
        """Publish one failure event.

        Args:
            event: The failure to record on the topic.

        Raises:
            MessagingError: If the broker did not acknowledge the record.
        """
        if self._producer is None or not self._started:
            raise MessagingError("Publisher used before start()")

        message = TaskFailureMessage.from_event(event)
        try:
            await self._producer.send_and_wait(
                self._settings.topic_failures,
                value=message.to_bytes(),
                key=message.partition_key,
            )
        except Exception as exc:
            raise MessagingError(
                "The broker did not acknowledge the failure event",
                details={"topic": self._settings.topic_failures, "dag_id": event.dag_id},
            ) from exc
        logger.info(
            "failure.published",
            topic=self._settings.topic_failures,
            dag_id=event.dag_id,
            task_id=event.task_id,
            run_id=event.run_id,
            try_number=event.try_number,
        )


async def publish_failure_event(
    event: FailureEvent,
    settings: KafkaSettings,
    producer: AsyncProducer | None = None,
) -> None:
    """Publish a single event, opening and closing a connection around it.

    Args:
        event: The failure to record.
        settings: Broker address and topic names.
        producer: Injected in tests.
    """
    async with FailurePublisher(settings, producer) as publisher:
        await publisher.publish(event)


def publish_failure_event_blocking(
    event: FailureEvent,
    settings: KafkaSettings,
    producer: AsyncProducer | None = None,
) -> None:
    """Publish a single event from synchronous code.

    Airflow invokes ``on_failure_callback`` from ordinary synchronous task code, where
    there is no running event loop to schedule onto.

    Args:
        event: The failure to record.
        settings: Broker address and topic names.
        producer: Injected in tests.

    Raises:
        MessagingError: If called from inside a running event loop, where starting another
            would deadlock.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(publish_failure_event(event, settings, producer))
        return
    raise MessagingError("publish_failure_event_blocking called from a running event loop")


class DiagnosisPublisher:
    """Announces finished investigations on the diagnosis topic.

    Publishing failures here would be pointless, so it does not: a diagnosis that could
    not be announced is logged and the investigation still counts as done. The record of
    it is already in the database, which is the copy that matters.
    """

    def __init__(self, settings: KafkaSettings, producer: AsyncProducer | None = None) -> None:
        """Initialise the publisher.

        Args:
            settings: Broker address and topic names.
            producer: Injected in tests; the real client is built lazily otherwise.
        """
        self._settings = settings
        self._producer = producer
        self._started = False

    async def start(self) -> None:
        """Connect to the broker."""
        if self._started:
            return
        if self._producer is None:
            self._producer = _build_aiokafka_producer(self._settings)
        await self._producer.start()
        self._started = True

    async def stop(self) -> None:
        """Flush and disconnect, tolerating a publisher that never started."""
        if self._producer is not None and self._started:
            await self._producer.stop()
        self._started = False

    async def publish(self, message: DiagnosisCompletedMessage) -> bool:
        """Announce one finished investigation.

        Args:
            message: What to announce.

        Returns:
            Whether the broker accepted it. A false here is not a failed investigation.
        """
        if self._producer is None or not self._started:
            logger.warning("diagnosis.publisher_not_started", incident_id=message.incident_id)
            return False
        try:
            await self._producer.send_and_wait(
                self._settings.topic_diagnoses,
                value=message.to_bytes(),
                key=message.partition_key,
            )
        except Exception as exc:
            logger.error(
                "diagnosis.publish_failed",
                incident_id=message.incident_id,
                error=f"{type(exc).__name__}: {exc}",
            )
            return False
        logger.info(
            "diagnosis.published",
            incident_id=message.incident_id,
            category=message.root_cause_category.value,
            confidence=message.confidence,
        )
        return True
