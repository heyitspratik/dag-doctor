from datetime import UTC, datetime

import pytest

from dag_doctor.core.exceptions import InvalidFailureEventError
from dag_doctor.messaging.airflow_callback import (
    MAX_EXCEPTION_MESSAGE_CHARS,
    build_failure_event,
    on_task_failure,
)
from dag_doctor.messaging.schemas import TaskFailureMessage

from .conftest import FakeProducer, FakeTaskInstance


def _context(**overrides) -> dict[str, object]:
    context: dict[str, object] = {
        "task_instance": FakeTaskInstance(),
        "logical_date": datetime(2026, 9, 7, 10, 0, tzinfo=UTC),
        "exception": None,
    }
    context.update(overrides)
    return context


def test_the_event_carries_what_identifies_the_failed_task():
    event = build_failure_event(_context())

    assert event.dag_id == "schema_drift_orders"
    assert event.task_id == "build_orders_by_customer"
    assert event.try_number == 1
    assert event.log_url is not None


def test_the_short_ti_alias_is_accepted_too():
    # Airflow puts the task instance under both keys, and which one is present has moved
    # between versions.
    event = build_failure_event({"ti": FakeTaskInstance()})

    assert event.dag_id == "schema_drift_orders"


def test_the_exception_is_fingerprinted_onto_the_event():
    exception = ValueError('column "customer_id" does not exist')

    event = build_failure_event(_context(exception=exception))

    assert event.exception_type == "ValueError"
    assert event.exception_message == 'column "customer_id" does not exist'


def test_a_runaway_traceback_cannot_outgrow_the_topic():
    event = build_failure_event(_context(exception=RuntimeError("x" * 50_000)))

    assert event.exception_message is not None
    assert len(event.exception_message) < MAX_EXCEPTION_MESSAGE_CHARS + 100
    assert event.exception_message.endswith("[truncated]")


def test_a_successful_task_leaves_no_exception_fingerprint():
    event = build_failure_event(_context(exception=None))

    assert event.exception_type is None
    assert event.exception_message is None


def test_an_unreadable_try_number_falls_back_to_the_first_attempt():
    # Airflow has moved try_number semantics between versions, and dropping the event
    # would be a worse outcome than recording attempt one.
    event = build_failure_event(_context(task_instance=FakeTaskInstance(try_number=None)))

    assert event.try_number == 1


def test_a_boolean_try_number_is_not_mistaken_for_an_integer():
    event = build_failure_event(_context(task_instance=FakeTaskInstance(try_number=True)))

    assert event.try_number == 1


def test_a_mapped_task_keeps_its_map_index():
    event = build_failure_event(_context(task_instance=FakeTaskInstance(map_index=3)))

    assert event.map_index == 3
    assert event.idempotency_key.endswith("/3")


def test_the_run_id_falls_back_to_the_context_when_the_task_instance_lacks_it():
    context = _context(task_instance=FakeTaskInstance(run_id=""), run_id="scheduled__2026-09-07")

    assert build_failure_event(context).run_id == "scheduled__2026-09-07"


def test_an_older_execution_date_key_is_accepted():
    context = _context(logical_date=None, execution_date=datetime(2026, 9, 6, tzinfo=UTC))

    assert build_failure_event(context).logical_date == datetime(2026, 9, 6, tzinfo=UTC)


@pytest.mark.parametrize(
    "context",
    [{}, {"task_instance": None}, {"exception": ValueError("boom")}],
)
def test_a_context_with_no_task_instance_is_refused(context):
    with pytest.raises(InvalidFailureEventError):
        build_failure_event(context)


def test_a_task_instance_missing_its_identifiers_is_refused():
    with pytest.raises(InvalidFailureEventError) as excinfo:
        build_failure_event(_context(task_instance=FakeTaskInstance(task_id="")))

    assert excinfo.value.details["task_id"] == ""


def test_the_callback_publishes_the_failure(kafka_settings, producer):
    on_task_failure(_context(exception=ValueError("boom")), kafka_settings, producer)

    topic, value, _key = producer.records[0]
    assert topic == "airflow.task.failed"
    assert TaskFailureMessage.from_bytes(value).exception_type == "ValueError"


def test_an_unusable_context_never_raises_into_airflow(kafka_settings, producer):
    # A callback that raises replaces a real, diagnosable task failure with a confusing
    # one. Losing the notification is the lesser harm, and it is visible as a failure
    # with no incident.
    on_task_failure({}, kafka_settings, producer)

    assert producer.records == []


def test_an_unreachable_broker_never_raises_into_airflow(kafka_settings):
    on_task_failure(_context(), kafka_settings, FakeProducer(fail_on_start=True))


def test_a_broker_that_never_acknowledges_never_raises_into_airflow(kafka_settings):
    on_task_failure(_context(), kafka_settings, FakeProducer(fail_on_send=True))
