"""The dead-letter path.

A message that cannot be processed must not block its partition forever. Parking it here
and moving the offset on is the trade: one incident is deferred to a human instead of every
later incident on that partition being starved behind it.
"""

from dag_doctor.core.exceptions import MessagingError
from dag_doctor.core.logging import get_logger
from dag_doctor.core.metrics import MESSAGES_DEAD_LETTERED
from dag_doctor.core.settings import KafkaSettings
from dag_doctor.messaging.producer import AsyncProducer, _build_aiokafka_producer
from dag_doctor.messaging.schemas import DeadLetterMessage

logger = get_logger(__name__)


class DeadLetterPublisher:
    """Parks unprocessable messages on the dead-letter topic."""

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

    async def park(
        self,
        raw: bytes,
        *,
        reason: str,
        error: str,
        topic: str,
        partition: int,
        offset: int,
        attempts: int,
    ) -> None:
        """Park one message, with the context needed to understand and replay it.

        Args:
            raw: The exact bytes from the source topic.
            reason: A short machine-readable category, such as ``"unparseable"``.
            error: The human-readable failure description.
            topic: The topic the message came from.
            partition: The partition it came from.
            offset: Its offset on that partition.
            attempts: How many times processing was tried before giving up.

        Raises:
            MessagingError: If the dead-letter topic itself could not be written. The
                caller must not commit the offset in that case: dropping a message the
                dead-letter topic never accepted would lose it entirely.
        """
        if self._producer is None or not self._started:
            raise MessagingError("Dead-letter publisher used before start()")

        message = DeadLetterMessage.park(
            raw,
            reason=reason,
            error=error,
            topic=topic,
            partition=partition,
            offset=offset,
            attempts=attempts,
        )
        try:
            await self._producer.send_and_wait(
                self._settings.topic_dlq,
                value=message.to_bytes(),
                key=f"{topic}/{partition}/{offset}".encode(),
            )
        except Exception as exc:
            raise MessagingError(
                "Could not write to the dead-letter topic",
                details={"topic": self._settings.topic_dlq, "reason": reason},
            ) from exc

        MESSAGES_DEAD_LETTERED.labels(reason=reason).inc()
        logger.warning(
            "message.dead_lettered",
            reason=reason,
            error=error,
            source_topic=topic,
            source_partition=partition,
            source_offset=offset,
            delivery_attempts=attempts,
        )
