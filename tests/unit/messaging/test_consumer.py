import pytest

from dag_doctor.core.exceptions import (
    InvalidFailureEventError,
    MessagingError,
    PersistenceError,
)
from dag_doctor.core.models import FailureEvent
from dag_doctor.messaging.consumer import FailureConsumer
from dag_doctor.messaging.dlq import DeadLetterPublisher
from dag_doctor.messaging.schemas import DeadLetterMessage, TaskFailureMessage

from .conftest import FakeProducer


class FakeRecord:
    def __init__(self, value: bytes, *, offset: int = 7, partition: int = 1) -> None:
        self.topic = "airflow.task.failed"
        self.partition = partition
        self.offset = offset
        self.value = value


class FakeConsumer:
    def __init__(self, records: list[FakeRecord] | None = None) -> None:
        self.records = records or []
        self.started = False
        self.stop_calls = 0
        self.commits: list[dict[object, int]] = []

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stop_calls += 1
        self.started = False

    async def commit(self, offsets) -> None:
        self.commits.append(dict(offsets))

    def __aiter__(self):
        self._iterator = iter(self.records)
        return self

    async def __anext__(self):
        try:
            return next(self._iterator)
        except StopIteration:
            raise StopAsyncIteration from None


def _record(event: FailureEvent, **kwargs) -> FakeRecord:
    return FakeRecord(TaskFailureMessage.from_event(event).to_bytes(), **kwargs)


@pytest.fixture
def dlq_producer() -> FakeProducer:
    return FakeProducer()


@pytest.fixture
def dead_letters(kafka_settings, dlq_producer) -> DeadLetterPublisher:
    return DeadLetterPublisher(kafka_settings, dlq_producer)


async def _consumer(kafka_settings, handler, records, dead_letters) -> FailureConsumer:
    async def no_sleep(_seconds: float) -> None:
        return None

    consumer = FailureConsumer(
        kafka_settings,
        handler,
        consumer=FakeConsumer(records),
        dead_letters=dead_letters,
        sleep=no_sleep,
    )
    await consumer.start()
    return consumer


async def test_a_handled_message_commits_past_itself(kafka_settings, failure_event, dead_letters):
    seen: list[FailureEvent] = []

    async def handler(event):
        seen.append(event)

    consumer = await _consumer(kafka_settings, handler, [_record(failure_event)], dead_letters)
    await consumer.run()

    assert seen == [failure_event]
    # Kafka commits the offset of the next record to read, so a record at 7 commits 8.
    assert list(consumer._consumer.commits[0].values()) == [8]


async def test_the_offset_is_not_committed_when_the_handler_keeps_failing(
    kafka_settings, failure_event, dead_letters, dlq_producer
):
    # The message is dead-lettered rather than retried forever, and only then does the
    # offset move, so one poison message cannot starve its partition.
    async def handler(_event):
        raise PersistenceError("the database is down")

    consumer = await _consumer(kafka_settings, handler, [_record(failure_event)], dead_letters)
    await consumer.run()

    assert len(dlq_producer.records) == 1
    assert len(consumer._consumer.commits) == 1


async def test_a_transient_failure_is_retried_within_one_delivery(
    kafka_settings, failure_event, dead_letters, dlq_producer
):
    attempts = {"n": 0}

    async def handler(_event):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise PersistenceError("connection reset")

    consumer = await _consumer(kafka_settings, handler, [_record(failure_event)], dead_letters)
    await consumer.run()

    assert attempts["n"] == 3
    assert dlq_producer.records == []
    assert len(consumer._consumer.commits) == 1


async def test_an_unparseable_message_is_parked_without_burning_the_retry_budget(
    kafka_settings, dead_letters, dlq_producer
):
    handled = {"n": 0}

    async def handler(_event):
        handled["n"] += 1

    consumer = await _consumer(
        kafka_settings, handler, [FakeRecord(b"not json at all")], dead_letters
    )
    await consumer.run()

    assert handled["n"] == 0
    parked = DeadLetterMessage.model_validate_json(dlq_producer.records[0][1])
    assert parked.reason == "unparseable"
    assert parked.delivery_attempts == 1
    assert len(consumer._consumer.commits) == 1


async def test_a_parked_message_keeps_its_original_bytes_for_replay(
    kafka_settings, dead_letters, dlq_producer
):
    async def handler(_event):
        return None

    consumer = await _consumer(
        kafka_settings, handler, [FakeRecord(b"\xff\xfe binary")], dead_letters
    )
    await consumer.run()

    parked = DeadLetterMessage.model_validate_json(dlq_producer.records[0][1])
    assert parked.original_payload == b"\xff\xfe binary"
    assert parked.source_offset == 7
    assert parked.source_partition == 1


async def test_a_message_the_handler_rejects_outright_is_not_retried(
    kafka_settings, failure_event, dead_letters, dlq_producer
):
    attempts = {"n": 0}

    async def handler(_event):
        attempts["n"] += 1
        raise InvalidFailureEventError("this failure names a DAG that does not exist")

    consumer = await _consumer(kafka_settings, handler, [_record(failure_event)], dead_letters)
    await consumer.run()

    assert attempts["n"] == 1
    assert DeadLetterMessage.model_validate_json(dlq_producer.records[0][1]).reason == "rejected"


async def test_the_offset_stays_put_when_the_dead_letter_write_also_fails(
    kafka_settings, failure_event
):
    # Committing here would drop a message that was never recorded anywhere, which is the
    # one outcome the event log exists to prevent. Leaving the offset alone means Kafka
    # redelivers it, and idempotency makes that safe.
    dead_letters = DeadLetterPublisher(kafka_settings, FakeProducer(fail_on_send=True))

    async def handler(_event):
        raise PersistenceError("the database is down")

    consumer = await _consumer(kafka_settings, handler, [_record(failure_event)], dead_letters)
    await consumer.run()

    assert consumer._consumer.commits == []


async def test_every_record_in_a_batch_is_processed(kafka_settings, failure_event, dead_letters):
    seen: list[str] = []

    async def handler(event):
        seen.append(event.task_id)

    records = [
        _record(failure_event, offset=1),
        _record(failure_event.model_copy(update={"task_id": "land_raw_orders"}), offset=2),
    ]
    consumer = await _consumer(kafka_settings, handler, records, dead_letters)
    await consumer.run()

    assert seen == ["build_orders_by_customer", "land_raw_orders"]
    assert len(consumer._consumer.commits) == 2


async def test_a_stop_request_finishes_the_record_in_flight_then_returns(
    kafka_settings, failure_event, dead_letters
):
    seen: list[str] = []

    async def handler(event):
        seen.append(event.run_id)

    records = [_record(failure_event, offset=1), _record(failure_event, offset=2)]
    consumer = await _consumer(kafka_settings, handler, records, dead_letters)
    consumer.request_stop()
    await consumer.run()

    assert len(seen) == 1
    assert len(consumer._consumer.commits) == 1


async def test_running_before_starting_is_refused(kafka_settings, dead_letters):
    async def handler(_event):
        return None

    with pytest.raises(MessagingError):
        await FailureConsumer(
            kafka_settings, handler, consumer=FakeConsumer(), dead_letters=dead_letters
        ).run()


async def test_an_unreachable_broker_names_its_address(kafka_settings, dead_letters):
    async def handler(_event):
        return None

    class RefusingConsumer(FakeConsumer):
        async def start(self):
            raise ConnectionRefusedError("no broker listening")

    with pytest.raises(MessagingError) as excinfo:
        await FailureConsumer(
            kafka_settings, handler, consumer=RefusingConsumer(), dead_letters=dead_letters
        ).start()

    assert excinfo.value.details["bootstrap_servers"] == kafka_settings.bootstrap_servers


async def test_the_consumer_disconnects_on_the_way_out(kafka_settings, failure_event, dead_letters):
    async def handler(_event):
        return None

    fake = FakeConsumer([_record(failure_event)])
    async with FailureConsumer(
        kafka_settings, handler, consumer=fake, dead_letters=dead_letters
    ) as consumer:
        await consumer.run()

    assert fake.stop_calls == 1


async def test_parking_before_starting_is_refused(kafka_settings, dlq_producer):
    with pytest.raises(MessagingError):
        await DeadLetterPublisher(kafka_settings, dlq_producer).park(
            b"x", reason="unparseable", error="e", topic="t", partition=0, offset=0, attempts=1
        )


async def test_stopping_a_dead_letter_publisher_that_never_started_is_harmless(
    kafka_settings, dlq_producer
):
    await DeadLetterPublisher(kafka_settings, dlq_producer).stop()

    assert dlq_producer.stop_calls == 0


async def test_starting_a_dead_letter_publisher_twice_connects_once(kafka_settings, dlq_producer):
    publisher = DeadLetterPublisher(kafka_settings, dlq_producer)

    await publisher.start()
    await publisher.start()
    await publisher.stop()

    assert dlq_producer.stop_calls == 1


async def test_an_unexpected_error_is_dead_lettered_rather_than_killing_the_worker(
    kafka_settings, failure_event, dead_letters, dlq_producer
):
    # The handler catching only DagDoctorError let a plain ValidationError escape the
    # retry path, kill the worker, and leave the offset uncommitted. Kafka redelivered and
    # it died again: 41 restarts on one message before anyone noticed. An unexpected error
    # is precisely what the dead-letter topic is for.
    async def handler(_event):
        raise ValueError("something nobody anticipated")

    consumer = await _consumer(kafka_settings, handler, [_record(failure_event)], dead_letters)
    await consumer.run()

    parked = DeadLetterMessage.model_validate_json(dlq_producer.records[0][1])
    assert parked.reason == "unexpected_error"
    assert "something nobody anticipated" in parked.error
    # The offset moves on, so the message cannot be redelivered forever.
    assert len(consumer._consumer.commits) == 1


async def test_an_unexpected_error_is_retried_before_being_given_up_on(
    kafka_settings, failure_event, dead_letters, dlq_producer
):
    attempts = {"n": 0}

    async def handler(_event):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError("transient, and not one of ours")

    consumer = await _consumer(kafka_settings, handler, [_record(failure_event)], dead_letters)
    await consumer.run()

    assert attempts["n"] == 3
    assert dlq_producer.records == []


async def test_one_poison_message_does_not_stop_the_ones_behind_it(
    kafka_settings, failure_event, dead_letters
):
    # The point of dead-lettering: a message nobody can process must not starve the
    # partition behind it.
    seen: list[str] = []

    async def handler(event):
        if event.task_id == "poison":
            raise ValueError("unprocessable")
        seen.append(event.task_id)

    records = [
        _record(failure_event.model_copy(update={"task_id": "poison"}), offset=1),
        _record(failure_event.model_copy(update={"task_id": "healthy"}), offset=2),
    ]
    consumer = await _consumer(kafka_settings, handler, records, dead_letters)
    await consumer.run()

    assert seen == ["healthy"]
    assert len(consumer._consumer.commits) == 2
