"""The ``on_failure_callback`` Airflow attaches to every task in the broken DAGs.

This module deliberately does not import Airflow. It reads the callback context as a
plain mapping and duck-types the task instance, which means the whole producer path is
unit-testable with a fake context and a fake broker, and the tests do not need Airflow
installed. Reading the context defensively is not only for testing: context keys have
moved between Airflow versions, and a callback that raises on a missing key would replace
a real, diagnosable task failure with a confusing one.
"""

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable

from dag_doctor.core.exceptions import DagDoctorError, InvalidFailureEventError
from dag_doctor.core.logging import get_logger
from dag_doctor.core.models import FailureEvent
from dag_doctor.core.settings import KafkaSettings
from dag_doctor.messaging.producer import AsyncProducer, publish_failure_event_blocking

logger = get_logger(__name__)

#: A traceback can run to megabytes. The message carries enough to fingerprint the
#: failure; the agent's first tool call fetches the full log when it needs it.
MAX_EXCEPTION_MESSAGE_CHARS = 2000


@runtime_checkable
class TaskInstanceLike(Protocol):
    """The attributes this callback reads off an Airflow task instance."""

    dag_id: str
    task_id: str
    run_id: str


def build_failure_event(context: Mapping[str, object]) -> FailureEvent:
    """Extract a failure event from an Airflow callback context.

    Args:
        context: The mapping Airflow hands to ``on_failure_callback``.

    Returns:
        The failure event to publish.

    Raises:
        InvalidFailureEventError: If the context carries no usable task instance.
    """
    task_instance = context.get("task_instance") or context.get("ti")
    if task_instance is None:
        raise InvalidFailureEventError(
            "Airflow callback context carried no task instance",
            details={"context_keys": sorted(context)},
        )

    dag_id = _attr_str(task_instance, "dag_id")
    task_id = _attr_str(task_instance, "task_id")
    run_id = _attr_str(task_instance, "run_id") or _str(context.get("run_id"))
    if not (dag_id and task_id and run_id):
        raise InvalidFailureEventError(
            "Airflow callback context did not identify a task instance",
            details={"dag_id": dag_id, "task_id": task_id, "run_id": run_id},
        )

    exception = context.get("exception")
    return FailureEvent(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        # Airflow has moved try_number semantics between versions, so treat anything
        # unreadable as the first attempt rather than dropping the event.
        try_number=max(1, _attr_int(task_instance, "try_number", default=1)),
        map_index=_attr_int(task_instance, "map_index", default=-1),
        logical_date=_datetime(context.get("logical_date"))
        or _datetime(context.get("execution_date")),
        failed_at=datetime.now(UTC),
        log_url=_attr_str(task_instance, "log_url") or None,
        exception_type=type(exception).__name__ if exception is not None else None,
        exception_message=_truncate(str(exception)) if exception is not None else None,
    )


def on_task_failure(
    context: Mapping[str, object],
    settings: KafkaSettings | None = None,
    producer: AsyncProducer | None = None,
) -> None:
    """Publish a task failure onto the event log.

    Every anticipated fault is caught and logged rather than raised. A callback that
    raises would bury a real, diagnosable task failure under a confusing one, and losing
    the notification is the lesser harm: it shows up as an Airflow failure with no
    incident, which is itself a diagnosable state.

    Args:
        context: The mapping Airflow hands to ``on_failure_callback``.
        settings: Broker address and topic names; read from the environment when absent.
        producer: Injected in tests.
    """
    try:
        event = build_failure_event(context)
    except DagDoctorError as exc:
        logger.error("failure.callback_context_unusable", error=exc.message, details=exc.details)
        return

    try:
        publish_failure_event_blocking(event, settings or KafkaSettings(), producer)
    except DagDoctorError as exc:
        # details is nested rather than splatted: it carries keys such as dag_id that
        # would collide with the explicit ones and raise out of the error handler itself.
        logger.error(
            "failure.publish_failed",
            error=exc.message,
            dag_id=event.dag_id,
            task_id=event.task_id,
            run_id=event.run_id,
            details=exc.details,
        )


def _str(value: object) -> str:
    """Render a context value as a string, treating absence as empty."""
    return value.strip() if isinstance(value, str) else ""


def _attr_str(obj: object, name: str) -> str:
    """Read a string attribute off a duck-typed Airflow object."""
    return _str(getattr(obj, name, None))


def _attr_int(obj: object, name: str, *, default: int) -> int:
    """Read an integer attribute off a duck-typed Airflow object."""
    value = getattr(obj, name, None)
    return value if isinstance(value, int) and not isinstance(value, bool) else default


def _datetime(value: object) -> datetime | None:
    """Accept a datetime as-is and ignore anything else."""
    return value if isinstance(value, datetime) else None


def _truncate(message: str) -> str:
    """Cap an exception message so one runaway traceback cannot outgrow the topic."""
    if len(message) <= MAX_EXCEPTION_MESSAGE_CHARS:
        return message
    return f"{message[:MAX_EXCEPTION_MESSAGE_CHARS]}... [truncated]"
