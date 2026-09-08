from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr

from dag_doctor.api.main import API_PREFIX, create_app
from dag_doctor.core.settings import Settings


async def test_liveness_touches_nothing(client):
    # Wiring a dependency check into liveness is how a brief database blip becomes a
    # restart loop.
    response = await client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "alive"}


async def test_readiness_reports_each_dependency_separately(client, api_app):
    async def ok() -> None:
        return None

    async def broken() -> None:
        raise ConnectionRefusedError("no broker listening")

    api_app.state.readiness_checks = {"postgres": ok, "redpanda": broken}

    response = await client.get("/health/ready")

    assert response.status_code == 503
    body = response.json()
    assert body["ready"] is False
    assert body["checks"]["postgres"] == "ok"
    assert "no broker listening" in body["checks"]["redpanda"]


async def test_readiness_passes_when_every_dependency_answers(client, api_app):
    async def ok() -> None:
        return None

    api_app.state.readiness_checks = {"postgres": ok, "llm_provider": ok}

    response = await client.get("/health/ready")

    assert response.status_code == 200
    assert response.json()["ready"] is True


async def test_the_real_readiness_checks_cover_what_the_agent_depends_on(
    api_settings, session_factory
):
    from dag_doctor.api.v1.routes.health import _checks

    app = create_app(api_settings, session_factory)

    class FakeRequest:
        def __init__(self, application):
            self.app = application

    assert set(_checks(FakeRequest(app))) == {"postgres", "redpanda", "llm_provider"}


async def test_metrics_are_exposed_for_scraping(client):
    response = await client.get("/metrics")

    assert response.status_code == 200
    assert "dag_doctor_incidents_received_total" in response.text


async def test_the_openapi_document_describes_every_route(client):
    document = (await client.get("/openapi.json")).json()

    assert set(document["paths"]) == {
        f"{API_PREFIX}/incidents",
        f"{API_PREFIX}/incidents/{{incident_id}}",
        f"{API_PREFIX}/incidents/{{incident_id}}/investigation",
        f"{API_PREFIX}/incidents/{{incident_id}}/replay",
        f"{API_PREFIX}/diagnoses",
        f"{API_PREFIX}/diagnoses/{{diagnosis_id}}/feedback",
        "/health/live",
        "/health/ready",
    }


async def test_every_route_documents_the_error_envelope(client):
    document = (await client.get("/openapi.json")).json()

    for path, methods in document["paths"].items():
        if path.startswith("/health"):
            continue
        for method in methods.values():
            assert "404" in method["responses"], path


async def test_each_response_carries_the_request_id_that_the_logs_use(client, seeded):
    response = await client.get("/api/v1/incidents")

    assert response.headers["X-Request-ID"]


async def test_a_caller_supplied_request_id_is_honoured(client, seeded):
    # So a trace can span a gateway and this service rather than restarting at the door.
    response = await client.get("/api/v1/incidents", headers={"X-Request-ID": "abc-123"})

    assert response.headers["X-Request-ID"] == "abc-123"


async def test_an_api_key_is_not_required_when_none_is_configured(client, seeded):
    assert (await client.get("/api/v1/incidents")).status_code == 200


@pytest.fixture
async def guarded_client(session_factory):
    settings = Settings()
    settings.api_key = SecretStr("the-secret")
    app = create_app(settings, session_factory)
    app.state.readiness_checks = {}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as http:
        yield http


async def test_a_configured_api_key_guards_every_route_at_once(guarded_client, seeded):
    # Applied through a dependency rather than per route, so a route added later cannot
    # be left unprotected by omission.
    for path in ("/api/v1/incidents", "/api/v1/diagnoses"):
        response = await guarded_client.get(path)
        assert response.status_code == 401
        assert response.json()["error"]["code"] == "UNAUTHORISED"


async def test_the_right_api_key_is_accepted(guarded_client, seeded):
    response = await guarded_client.get("/api/v1/incidents", headers={"X-API-Key": "the-secret"})

    assert response.status_code == 200


async def test_health_stays_reachable_without_a_key(guarded_client):
    # A probe that needs a secret is a probe that fails during a secret rotation.
    assert (await guarded_client.get("/health/live")).status_code == 200


async def test_replay_is_accepted_and_runs_in_the_background(client, api_app, seeded):
    calls: list[tuple] = []

    async def replay(incident_id, event, model) -> None:
        calls.append((incident_id, event.dag_id, model))

    api_app.state.replay = replay

    response = await client.post(
        f"/api/v1/incidents/{seeded['diagnosed']}/replay", json={"model": "llama3.1:8b"}
    )

    assert response.status_code == 202
    assert response.json()["attempt"] == 2
    assert calls == [(seeded["diagnosed"], "schema_drift_orders", "llama3.1:8b")]


async def test_replay_says_plainly_when_the_deployment_cannot_do_it(client, api_app, seeded):
    api_app.state.replay = None

    response = await client.post(f"/api/v1/incidents/{seeded['diagnosed']}/replay", json={})

    assert response.status_code == 422
    assert "no model provider" in response.json()["error"]["message"]


async def test_replaying_an_incident_that_does_not_exist_is_a_404(client):
    response = await client.post(f"/api/v1/incidents/{uuid4()}/replay", json={})

    assert response.status_code == 404


async def test_an_unexpected_failure_does_not_leak_internals(client, monkeypatch, seeded):
    from dag_doctor.db import repositories

    def explode(*_args, **_kwargs):
        raise RuntimeError("the connection string is postgres://user:hunter2@host/db")

    monkeypatch.setattr(repositories.IncidentRepository, "get", explode)

    response = await client.get(f"/api/v1/incidents/{seeded['diagnosed']}")

    assert response.status_code == 500
    assert response.json()["error"]["code"] == "INTERNAL_ERROR"
    assert "hunter2" not in response.text


async def test_a_replay_with_a_model_override_pins_every_node_to_it(api_settings):
    # The point of a replay is comparing models against the same failure. A per-node
    # override left in place would silently defeat that comparison.
    pinned = api_settings.llm.with_model("llama3.1:8b")

    assert pinned.default_model == "llama3.1:8b"
    assert pinned.model_for_node("triage") == "llama3.1:8b"
    assert pinned.model_for_node("form_hypothesis") == "llama3.1:8b"


async def test_a_model_override_does_not_alter_the_settings_it_came_from(api_settings):
    api_settings.llm.with_model("llama3.1:8b")

    assert api_settings.llm.default_model == "llama3.2:3b"


async def test_a_deployment_builds_its_own_replay_runner(api_settings, session_factory):
    # Without this, replay would be permanently unavailable in the compose stack, which
    # is where it is meant to be demonstrated.
    app = create_app(api_settings, session_factory)

    async with app.router.lifespan_context(app):
        assert app.state.replay is not None
