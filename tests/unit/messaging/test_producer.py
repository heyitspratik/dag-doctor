import pytest

from dag_doctor.core.exceptions import MessagingError
from dag_doctor.messaging.producer import (
    FailurePublisher,
    publish_failure_event,
    publish_failure_event_blocking,
)
from dag_doctor.messaging.schemas import TaskFailureMessage

from .conftest import FakeProducer


async def test_publishing_puts_the_event_on_the_failure_topic(
    kafka_settings, producer, failure_event
):
    async with FailurePublisher(kafka_settings, producer) as publisher:
        await publisher.publish(failure_event)

    topic, value, key = producer.records[0]
    assert topic == "airflow.task.failed"
    assert TaskFailureMessage.from_bytes(value).to_event() == failure_event
    assert key == TaskFailureMessage.from_event(failure_event).partition_key


async def test_the_connection_is_closed_even_when_publishing_raises(kafka_settings, failure_event):
    producer = FakeProducer(fail_on_send=True)

    with pytest.raises(MessagingError):
        async with FailurePublisher(kafka_settings, producer) as publisher:
            await publisher.publish(failure_event)

    assert producer.stop_calls == 1


async def test_an_unreachable_broker_names_its_address(kafka_settings):
    producer = FakeProducer(fail_on_start=True)

    with pytest.raises(MessagingError) as excinfo:
        await FailurePublisher(kafka_settings, producer).start()

    assert excinfo.value.details["bootstrap_servers"] == kafka_settings.bootstrap_servers


async def test_a_broker_that_never_acknowledges_is_an_error_not_a_silent_drop(
    kafka_settings, failure_event
):
    producer = FakeProducer(fail_on_send=True)
    publisher = FailurePublisher(kafka_settings, producer)
    await publisher.start()

    with pytest.raises(MessagingError) as excinfo:
        await publisher.publish(failure_event)

    assert excinfo.value.details["dag_id"] == "schema_drift_orders"


async def test_publishing_before_starting_is_refused(kafka_settings, producer, failure_event):
    with pytest.raises(MessagingError):
        await FailurePublisher(kafka_settings, producer).publish(failure_event)


async def test_starting_twice_connects_once(kafka_settings, producer):
    publisher = FailurePublisher(kafka_settings, producer)

    await publisher.start()
    await publisher.start()
    await publisher.stop()

    assert producer.stop_calls == 1


async def test_stopping_a_publisher_that_never_started_is_harmless(kafka_settings, producer):
    await FailurePublisher(kafka_settings, producer).stop()

    assert producer.stop_calls == 0


async def test_the_one_shot_helper_opens_and_closes_around_a_single_event(
    kafka_settings, producer, failure_event
):
    await publish_failure_event(failure_event, kafka_settings, producer)

    assert len(producer.records) == 1
    assert producer.stop_calls == 1


def test_the_blocking_wrapper_works_from_synchronous_code(kafka_settings, producer, failure_event):
    # This is the path Airflow actually takes: a callback in an ordinary task process.
    publish_failure_event_blocking(failure_event, kafka_settings, producer)

    assert len(producer.records) == 1


async def test_the_blocking_wrapper_refuses_to_run_inside_a_loop(
    kafka_settings, producer, failure_event
):
    # asyncio.run inside a running loop deadlocks rather than erroring usefully, so the
    # wrapper checks first.
    with pytest.raises(MessagingError) as excinfo:
        publish_failure_event_blocking(failure_event, kafka_settings, producer)

    assert "running event loop" in excinfo.value.message


async def test_the_real_client_is_built_lazily_on_start(kafka_settings, monkeypatch):
    # Constructing a publisher must not touch the network, so importing the DAG files in
    # an Airflow parse loop cannot block on a broker.
    built: list[str] = []

    def fake_build(settings):
        built.append(settings.bootstrap_servers)
        return FakeProducer()

    monkeypatch.setattr("dag_doctor.messaging.producer._build_aiokafka_producer", fake_build)
    publisher = FailurePublisher(kafka_settings)
    assert built == []

    await publisher.start()

    assert built == [kafka_settings.bootstrap_servers]


async def test_the_real_client_matches_the_protocol_this_package_relies_on(kafka_settings):
    # Constructing the client does not connect. The value is checking the real aiokafka
    # API against the Protocol the fakes implement: a signature drift in the client would
    # otherwise surface only against a live broker.
    import inspect

    from dag_doctor.messaging.producer import AsyncProducer, _build_aiokafka_producer

    client = _build_aiokafka_producer(kafka_settings)
    try:
        assert isinstance(client, AsyncProducer)
        sent = inspect.signature(type(client).send_and_wait).parameters
        assert {"topic", "value", "key"} <= set(sent)
    finally:
        await client.stop()
