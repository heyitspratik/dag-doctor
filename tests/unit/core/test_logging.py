from uuid import uuid4

import pytest
import structlog

from dag_doctor.core.logging import (
    configure_logging,
    get_logger,
    incident_context,
    log_context,
    reset_logging,
)


@pytest.fixture(autouse=True)
def _fresh_logging():
    reset_logging()
    yield
    reset_logging()
    structlog.reset_defaults()


def _captured() -> list[dict[str, object]]:
    """Route log events into a list instead of stderr, keeping the contextvar merge."""
    entries: list[dict[str, object]] = []

    def capture(logger, method_name, event_dict):
        entries.append(event_dict)
        raise structlog.DropEvent

    structlog.configure(
        processors=[structlog.contextvars.merge_contextvars, capture],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )
    return entries


def test_configuring_twice_leaves_the_first_configuration_in_place():
    configure_logging(app_env="prod", log_level="INFO")
    before = structlog.get_config()["processors"]

    configure_logging(app_env="dev", log_level="DEBUG")

    assert structlog.get_config()["processors"] is before


def test_an_incident_id_reaches_every_line_inside_the_block():
    entries = _captured()
    incident_id = uuid4()

    with incident_context(incident_id, node="triage"):
        get_logger(__name__).info("investigation.started")

    assert entries[0]["incident_id"] == str(incident_id)
    assert entries[0]["node"] == "triage"


def test_the_binding_is_dropped_on_the_way_out():
    entries = _captured()

    with log_context(node="conclude"):
        pass
    get_logger(__name__).info("worker.idle")

    assert "node" not in entries[0]


def test_the_binding_is_dropped_even_when_the_block_raises():
    entries = _captured()

    with pytest.raises(RuntimeError), log_context(node="gather_evidence"):
        raise RuntimeError("tool blew up")
    get_logger(__name__).info("worker.idle")

    assert "node" not in entries[0]
