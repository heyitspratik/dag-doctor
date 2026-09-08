"""The wire format for the topics, kept separate from the domain models.

:class:`dag_doctor.core.models.FailureEvent` is what the investigation reasons about. What
travels over the topic is this envelope, which carries a schema version and tolerates
fields the current code does not know about. Keeping them apart means a change to the wire
format is a translation change here rather than a change rippling into the graph.

The same module is imported by the Airflow callback that produces the message and by the
worker that consumes it, so producer and consumer cannot drift apart.
"""

import base64
from datetime import UTC, datetime
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from dag_doctor.core.exceptions import InvalidFailureEventError
from dag_doctor.core.models import Diagnosis, FailureEvent, HaltReason, RootCauseCategory

#: Bumped when the payload changes shape incompatibly. Consumers refuse a version they do
#: not understand rather than silently misreading a field that has moved.
SCHEMA_VERSION = 1


class TaskFailureMessage(BaseModel):
    """One message on ``airflow.task.failed``.

    ``extra="ignore"`` is deliberate: a newer producer may add fields, and an older
    consumer should carry on rather than dead-letter a message it could have handled.
    """

    model_config = ConfigDict(extra="ignore")

    schema_version: int = SCHEMA_VERSION
    dag_id: str = Field(min_length=1)
    task_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    try_number: int = Field(ge=1)
    map_index: int = -1
    logical_date: datetime | None = None
    failed_at: datetime
    log_url: str | None = None
    exception_type: str | None = None
    exception_message: str | None = None

    @classmethod
    def from_event(cls, event: FailureEvent) -> Self:
        """Build a wire message from a domain event."""
        return cls(schema_version=SCHEMA_VERSION, **event.model_dump())

    def to_event(self) -> FailureEvent:
        """Build a domain event from a wire message."""
        fields = self.model_dump(exclude={"schema_version"})
        return FailureEvent(**fields)

    @property
    def partition_key(self) -> bytes:
        """Every attempt at one task instance lands on one partition, in order.

        ``try_number`` is deliberately absent: retries of the same task are the same
        story, and a consumer benefits from seeing them in the order they happened.
        """
        return f"{self.dag_id}/{self.task_id}/{self.run_id}/{self.map_index}".encode()

    def to_bytes(self) -> bytes:
        """Serialise to the JSON payload that goes on the topic."""
        return self.model_dump_json().encode()

    @classmethod
    def from_bytes(cls, raw: bytes) -> Self:
        """Parse a message off the topic.

        Args:
            raw: The raw message value.

        Returns:
            The parsed message.

        Raises:
            InvalidFailureEventError: If the payload is not valid JSON, does not match the
                schema, or carries a schema version this code does not understand. The
                consumer dead-letters these rather than retrying: a malformed payload will
                not become well-formed on the second attempt.
        """
        try:
            message = cls.model_validate_json(raw)
        except ValidationError as exc:
            raise InvalidFailureEventError(
                "Message on the failure topic does not match the failure event schema",
                details={"errors": exc.error_count(), "payload_bytes": len(raw)},
            ) from exc

        if message.schema_version != SCHEMA_VERSION:
            raise InvalidFailureEventError(
                f"Unsupported failure event schema version {message.schema_version}",
                details={"received": message.schema_version, "supported": SCHEMA_VERSION},
            )
        return message


class DeadLetterMessage(BaseModel):
    """One message that could not be processed, parked with the reason why.

    The original bytes are carried verbatim, base64 encoded, rather than the parsed
    fields: a message lands here precisely because parsing it did not work, and a
    lossy record of a poison message cannot be replayed once the bug is fixed.
    """

    schema_version: int = SCHEMA_VERSION
    reason: str
    error: str
    source_topic: str
    source_partition: int
    source_offset: int
    delivery_attempts: int
    parked_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    original_payload_b64: str

    @classmethod
    def park(
        cls,
        raw: bytes,
        *,
        reason: str,
        error: str,
        topic: str,
        partition: int,
        offset: int,
        attempts: int,
    ) -> Self:
        """Wrap a failed message with the context needed to understand and replay it."""
        return cls(
            reason=reason,
            error=error,
            source_topic=topic,
            source_partition=partition,
            source_offset=offset,
            delivery_attempts=attempts,
            original_payload_b64=base64.b64encode(raw).decode("ascii"),
        )

    @property
    def original_payload(self) -> bytes:
        """The exact bytes that were on the source topic."""
        return base64.b64decode(self.original_payload_b64)

    def to_bytes(self) -> bytes:
        """Serialise to the JSON payload that goes on the dead-letter topic."""
        return self.model_dump_json().encode()


class DiagnosisCompletedMessage(BaseModel):
    """One message on ``agent.diagnosis.completed``.

    Deliberately thin. This topic exists so that other consumers, alerting, metrics, a
    ticket opener, can react without touching the agent, and none of them need the whole
    evidence chain. Anything that does can follow the incident id to the API.
    """

    schema_version: int = SCHEMA_VERSION
    incident_id: str
    dag_id: str
    task_id: str
    run_id: str
    attempt: int = 1
    root_cause_category: RootCauseCategory
    confidence: float
    halt_reason: HaltReason
    conclusive: bool
    responsible_dag_id: str | None = None
    responsible_task_id: str | None = None
    model_used: str = ""
    completed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @classmethod
    def from_diagnosis(
        cls, diagnosis: Diagnosis, *, dag_id: str, task_id: str, run_id: str, attempt: int = 1
    ) -> Self:
        """Build the message announcing a finished investigation."""
        return cls(
            incident_id=str(diagnosis.incident_id),
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            attempt=attempt,
            root_cause_category=diagnosis.root_cause_category,
            confidence=diagnosis.confidence,
            halt_reason=diagnosis.halt_reason,
            conclusive=diagnosis.is_conclusive,
            responsible_dag_id=diagnosis.responsible_dag_id,
            responsible_task_id=diagnosis.responsible_task_id,
            model_used=diagnosis.model_used,
        )

    @property
    def partition_key(self) -> bytes:
        """Keyed by incident, so every attempt at one incident stays in order."""
        return self.incident_id.encode()

    def to_bytes(self) -> bytes:
        """Serialise to the JSON payload that goes on the topic."""
        return self.model_dump_json().encode()
