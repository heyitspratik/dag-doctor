"""The failure topic consumer.

Three properties matter here, and all three are correctness rather than polish.

Offsets are committed manually, only after processing succeeded. A crash mid-diagnosis
therefore reprocesses the failure rather than silently dropping it, which is the whole
reason for putting a log between Airflow and the agent.

Processing is retried a bounded number of times within one delivery. Beyond that the
message is parked on the dead-letter topic and the offset moves on, because one poison
message must not starve every later incident on its partition.

Redelivery is safe because incidents are keyed on the failure's identifying tuple, so
reprocessing updates a row rather than opening a second investigation.
"""

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from types import TracebackType
from typing import Protocol, Self

from aiokafka.structs import TopicPartition

from dag_doctor.core.exceptions import DagDoctorError, InvalidFailureEventError, MessagingError
from dag_doctor.core.logging import get_logger, log_context
from dag_doctor.core.models import FailureEvent
from dag_doctor.core.settings import KafkaSettings
from dag_doctor.messaging.dlq import DeadLetterPublisher
from dag_doctor.messaging.schemas import TaskFailureMessage

logger = get_logger(__name__)

#: Base delay between retries within one delivery, multiplied by the attempt number.
RETRY_BACKOFF_S = 1.0

#: Called with a parsed failure. Anything it raises is retried, then dead-lettered.
type FailureHandler = Callable[[FailureEvent], Awaitable[None]]


class ConsumerRecordLike(Protocol):
    """The fields this package reads off a consumed record."""

    @property
    def topic(self) -> str:
        """The topic the record came from."""
        ...

    @property
    def partition(self) -> int:
        """The partition the record came from."""
        ...

    @property
    def offset(self) -> int:
        """The record's offset on its partition."""
        ...

    @property
    def value(self) -> bytes:
        """The raw record payload."""
        ...


class AsyncConsumer(Protocol):
    """The slice of an aiokafka consumer this package actually uses."""

    async def start(self) -> None:
        """Join the consumer group."""
        ...

    async def stop(self) -> None:
        """Leave the consumer group."""
        ...

    async def commit(self, offsets: Mapping[object, int]) -> None:
        """Commit the given offsets."""
        ...

    def __aiter__(self) -> "AsyncConsumer":
        """Iterate delivered records."""
        ...

    async def __anext__(self) -> ConsumerRecordLike:
        """Return the next delivered record."""
        ...


def _build_aiokafka_consumer(settings: KafkaSettings) -> AsyncConsumer:
    """Construct the real consumer.

    ``enable_auto_commit=False`` is the point. With auto-commit an offset advances on a
    timer, so a worker that dies mid-investigation loses the incident it was working on.
    """
    from aiokafka import AIOKafkaConsumer

    consumer: AsyncConsumer = AIOKafkaConsumer(
        settings.topic_failures,
        bootstrap_servers=settings.bootstrap_servers,
        group_id=settings.consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    return consumer


class FailureConsumer:
    """Consumes the failure topic and hands parsed events to a handler."""

    def __init__(
        self,
        settings: KafkaSettings,
        handler: FailureHandler,
        consumer: AsyncConsumer | None = None,
        dead_letters: DeadLetterPublisher | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Initialise the consumer.

        Args:
            settings: Broker address, group, and topic names.
            handler: Called with each parsed failure event.
            consumer: Injected in tests; the real client is built lazily otherwise.
            dead_letters: Where unprocessable messages are parked.
            sleep: Injected in tests so retry backoff costs no wall-clock time.
        """
        self._settings = settings
        self._handler = handler
        self._consumer = consumer
        self._dead_letters = dead_letters or DeadLetterPublisher(settings)
        self._sleep = sleep
        self._started = False
        self._stopping = False

    async def __aenter__(self) -> Self:
        """Join the consumer group."""
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Leave the consumer group."""
        await self.stop()

    async def start(self) -> None:
        """Connect the consumer and the dead-letter publisher."""
        if self._started:
            return
        if self._consumer is None:
            self._consumer = _build_aiokafka_consumer(self._settings)
        try:
            await self._consumer.start()
            await self._dead_letters.start()
        except Exception as exc:
            raise MessagingError(
                f"Could not reach the broker at {self._settings.bootstrap_servers}",
                details={"bootstrap_servers": self._settings.bootstrap_servers},
            ) from exc
        self._started = True

    async def stop(self) -> None:
        """Disconnect, tolerating a consumer that never started."""
        self._stopping = True
        if self._consumer is not None and self._started:
            await self._consumer.stop()
        await self._dead_letters.stop()
        self._started = False

    def request_stop(self) -> None:
        """Ask the run loop to finish the record in flight and then return.

        Called from a signal handler, so the worker drains rather than abandoning an
        investigation partway through.
        """
        self._stopping = True

    async def run(self) -> None:
        """Consume until asked to stop.

        Raises:
            MessagingError: If the consumer was not started.
        """
        if self._consumer is None or not self._started:
            raise MessagingError("Consumer used before start()")

        logger.info(
            "consumer.running",
            topic=self._settings.topic_failures,
            group=self._settings.consumer_group,
        )
        async for record in self._consumer:
            await self.process(record)
            if self._stopping:
                break
        logger.info("consumer.stopped")

    async def process(self, record: ConsumerRecordLike) -> None:
        """Process one record, committing its offset only once it is genuinely done.

        Args:
            record: The delivered record.
        """
        with log_context(topic=record.topic, partition=record.partition, offset=record.offset):
            try:
                message = TaskFailureMessage.from_bytes(record.value)
            except InvalidFailureEventError as exc:
                # Unparseable will not become parseable on a retry, so park it now
                # rather than burning the retry budget discovering that.
                if await self._park(record, reason="unparseable", error=exc.message, attempts=1):
                    await self._commit(record)
                return

            if await self._handle_with_retries(message.to_event(), record):
                await self._commit(record)

    async def _handle_with_retries(self, event: FailureEvent, record: ConsumerRecordLike) -> bool:
        """Run the handler, retrying transient failures within this delivery.

        Returns:
            Whether the offset may now be committed. ``False`` means the dead-letter write
            failed too, and the message must be redelivered rather than dropped.
        """
        attempts = self._settings.max_delivery_attempts
        for attempt in range(1, attempts + 1):
            try:
                await self._handler(event)
            except InvalidFailureEventError as exc:
                return await self._park(
                    record, reason="rejected", error=exc.message, attempts=attempt
                )
            except DagDoctorError as exc:
                if attempt == attempts:
                    return await self._park(
                        record, reason="handler_failed", error=exc.message, attempts=attempt
                    )
                logger.warning(
                    "message.retrying",
                    attempt=attempt,
                    of=attempts,
                    error=exc.message,
                    dag_id=event.dag_id,
                    task_id=event.task_id,
                )
                await self._sleep(RETRY_BACKOFF_S * attempt)
            else:
                return True
        return True

    async def _park(
        self, record: ConsumerRecordLike, *, reason: str, error: str, attempts: int
    ) -> bool:
        """Send a record to the dead-letter topic.

        Returns:
            Whether the offset may now be committed. A failed dead-letter write returns
            ``False`` so the offset stays put: committing anyway would drop a message that
            was never recorded anywhere, which is the one outcome the event log exists to
            prevent.
        """
        try:
            await self._dead_letters.park(
                record.value,
                reason=reason,
                error=error,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                attempts=attempts,
            )
        except MessagingError as exc:
            logger.error("message.dead_letter_failed", error=exc.message, reason=reason)
            return False
        return True

    async def _commit(self, record: ConsumerRecordLike) -> None:
        """Commit past one record.

        Kafka commits the offset of the *next* record to read, hence the increment.
        """
        if self._consumer is None:
            raise MessagingError("Consumer used before start()")
        partition = TopicPartition(record.topic, record.partition)
        await self._consumer.commit({partition: record.offset + 1})
