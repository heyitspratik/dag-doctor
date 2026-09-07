"""The wired path: a message on the topic becomes exactly one incident row.

These drive the real consumer over a real database, with only the broker faked. That is
what makes them a check on the phase's actual promise rather than on two halves of it.
"""

from sqlalchemy import func, select

from dag_doctor.core.models import IncidentStatus
from dag_doctor.core.settings import KafkaSettings
from dag_doctor.db.models import Incident
from dag_doctor.messaging.consumer import FailureConsumer
from dag_doctor.messaging.dlq import DeadLetterPublisher
from dag_doctor.messaging.schemas import TaskFailureMessage
from dag_doctor.worker.main import _install_signal_handlers, build_handler

from ..messaging.conftest import FakeProducer
from ..messaging.test_consumer import FakeConsumer, FakeRecord


async def _incidents(session_factory) -> list[Incident]:
    async with session_factory() as session:
        return list((await session.execute(select(Incident))).scalars())


async def _count(session_factory) -> int:
    async with session_factory() as session:
        return (await session.execute(select(func.count()).select_from(Incident))).scalar_one()


async def _run(session_factory, records, dlq_producer=None):
    settings = KafkaSettings()
    consumer = FailureConsumer(
        settings,
        build_handler(session_factory),
        consumer=FakeConsumer(records),
        dead_letters=DeadLetterPublisher(settings, dlq_producer or FakeProducer()),
    )
    await consumer.start()
    await consumer.run()
    return consumer


def _record(event, offset: int) -> FakeRecord:
    return FakeRecord(TaskFailureMessage.from_event(event).to_bytes(), offset=offset)


async def test_one_message_becomes_one_incident(session_factory, failure_event):
    await _run(session_factory, [_record(failure_event, 1)])

    incidents = await _incidents(session_factory)
    assert len(incidents) == 1
    assert incidents[0].dag_id == "schema_drift_orders"
    assert incidents[0].status is IncidentStatus.RECEIVED
    assert incidents[0].exception_message == 'column "customer_id" does not exist'


async def test_a_redelivered_message_creates_exactly_one_incident(session_factory, failure_event):
    # The phase's promise, checked through the wired path rather than at the repository
    # alone: Kafka delivers the same failure three times, one investigation results.
    consumer = await _run(
        session_factory,
        [_record(failure_event, offset) for offset in (1, 2, 3)],
    )

    incidents = await _incidents(session_factory)
    assert len(incidents) == 1
    assert incidents[0].delivery_count == 3
    # Every delivery is still acknowledged, so the partition does not stall.
    assert len(consumer._consumer.commits) == 3


async def test_distinct_failures_become_distinct_incidents(session_factory, failure_event):
    retry = failure_event.model_copy(update={"try_number": 2})

    await _run(session_factory, [_record(failure_event, 1), _record(retry, 2)])

    assert await _count(session_factory) == 2


async def test_an_unparseable_message_is_parked_and_opens_no_incident(session_factory):
    dlq_producer = FakeProducer()

    await _run(session_factory, [FakeRecord(b"{}")], dlq_producer)

    assert await _count(session_factory) == 0
    assert len(dlq_producer.records) == 1


async def test_the_incident_is_committed_before_the_offset(session_factory, failure_event):
    # If the offset moved first, a crash between the two would acknowledge a failure that
    # was never recorded. This order can only ever process a message twice, which the
    # unique constraint absorbs.
    committed_counts: list[int] = []

    class CountingConsumer(FakeConsumer):
        async def commit(self, offsets):
            committed_counts.append(await _count(session_factory))
            await super().commit(offsets)

    settings = KafkaSettings()
    consumer = FailureConsumer(
        settings,
        build_handler(session_factory),
        consumer=CountingConsumer([_record(failure_event, 1)]),
        dead_letters=DeadLetterPublisher(settings, FakeProducer()),
    )
    await consumer.start()
    await consumer.run()

    assert committed_counts == [1]


async def test_a_termination_signal_asks_the_consumer_to_drain(session_factory, failure_event):
    # The Kubernetes preStop hook depends on this: the pod stops taking new records but
    # finishes the incident it is holding rather than abandoning it mid-investigation.
    import signal

    settings = KafkaSettings()
    consumer = FailureConsumer(
        settings,
        build_handler(session_factory),
        consumer=FakeConsumer([_record(failure_event, 1), _record(failure_event, 2)]),
        dead_letters=DeadLetterPublisher(settings, FakeProducer()),
    )
    await consumer.start()
    _install_signal_handlers(consumer)

    signal.raise_signal(signal.SIGTERM)
    await consumer.run()

    assert len(consumer._consumer.commits) == 1
