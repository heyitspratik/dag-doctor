import json

import pytest

from dag_doctor.core.exceptions import InvalidFailureEventError
from dag_doctor.core.models import FailureEvent
from dag_doctor.messaging.schemas import SCHEMA_VERSION, TaskFailureMessage


def test_an_event_survives_the_round_trip_through_the_topic(failure_event):
    restored = TaskFailureMessage.from_bytes(
        TaskFailureMessage.from_event(failure_event).to_bytes()
    ).to_event()

    assert restored == failure_event


def test_the_message_carries_its_schema_version(failure_event):
    assert TaskFailureMessage.from_event(failure_event).schema_version == SCHEMA_VERSION


def test_every_attempt_at_one_task_shares_a_partition(failure_event):
    first = TaskFailureMessage.from_event(failure_event)
    retry = TaskFailureMessage.from_event(failure_event.model_copy(update={"try_number": 2}))

    assert first.partition_key == retry.partition_key


def test_different_task_instances_do_not_share_a_partition_key(failure_event):
    other = failure_event.model_copy(update={"task_id": "land_raw_orders"})

    assert (
        TaskFailureMessage.from_event(failure_event).partition_key
        != TaskFailureMessage.from_event(other).partition_key
    )


def test_a_field_the_consumer_does_not_know_about_is_ignored(failure_event):
    payload = TaskFailureMessage.from_event(failure_event).model_dump(mode="json")
    payload["queued_by_job_id"] = 41

    # A newer producer must not be able to dead-letter an older consumer.
    message = TaskFailureMessage.model_validate(payload)

    assert message.dag_id == "schema_drift_orders"


@pytest.mark.parametrize(
    "raw",
    [b"", b"not json at all", b"{}", b'{"dag_id": "x"}', b"[1, 2, 3]"],
)
def test_a_malformed_payload_is_rejected_rather_than_guessed_at(raw):
    with pytest.raises(InvalidFailureEventError):
        TaskFailureMessage.from_bytes(raw)


def test_an_unknown_schema_version_is_rejected(failure_event):
    payload = TaskFailureMessage.from_event(failure_event).model_dump(mode="json")
    payload["schema_version"] = SCHEMA_VERSION + 1

    with pytest.raises(InvalidFailureEventError) as excinfo:
        TaskFailureMessage.from_bytes(json.dumps(payload).encode())

    assert "schema version" in excinfo.value.message


def test_an_incomplete_payload_names_no_secrets_in_its_details():
    with pytest.raises(InvalidFailureEventError) as excinfo:
        TaskFailureMessage.from_bytes(b'{"dag_id": ""}')

    assert set(excinfo.value.details) == {"errors", "payload_bytes"}


def test_the_domain_model_and_the_wire_message_agree_on_their_fields(failure_event):
    # The two are kept separate on purpose, so a field added to one and forgotten on the
    # other would silently stop travelling. This is the test that notices.
    domain = set(FailureEvent.model_fields)
    wire = set(TaskFailureMessage.model_fields) - {"schema_version"}

    assert domain == wire
