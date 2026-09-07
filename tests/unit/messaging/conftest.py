"""Fakes standing in for the broker and for Airflow, so no test needs either."""

import pytest

from dag_doctor.core.settings import KafkaSettings


class FakeProducer:
    """Records what would have been sent, and can be told to fail like a real broker."""

    def __init__(self, *, fail_on_start: bool = False, fail_on_send: bool = False) -> None:
        self.fail_on_start = fail_on_start
        self.fail_on_send = fail_on_send
        self.started = False
        self.stop_calls = 0
        self.records: list[tuple[str, bytes, bytes | None]] = []

    async def start(self) -> None:
        if self.fail_on_start:
            raise ConnectionRefusedError("no broker listening")
        self.started = True

    async def stop(self) -> None:
        self.stop_calls += 1
        self.started = False

    async def send_and_wait(self, topic: str, value: bytes, key: bytes | None = None) -> object:
        if self.fail_on_send:
            raise TimeoutError("no acknowledgement from the broker")
        self.records.append((topic, value, key))
        return object()


class FakeTaskInstance:
    """The subset of an Airflow task instance the callback reads."""

    def __init__(self, **attributes: object) -> None:
        defaults: dict[str, object] = {
            "dag_id": "schema_drift_orders",
            "task_id": "build_orders_by_customer",
            "run_id": "manual__2026-09-07T10:00:00+00:00",
            "try_number": 1,
            "map_index": -1,
            "log_url": "http://localhost:8080/log?dag_id=schema_drift_orders",
        }
        self.__dict__.update({**defaults, **attributes})


@pytest.fixture
def producer() -> FakeProducer:
    return FakeProducer()


@pytest.fixture
def kafka_settings() -> KafkaSettings:
    return KafkaSettings()
