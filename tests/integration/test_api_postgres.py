"""The API against a real Postgres.

The unit suite runs these routes on SQLite, which covers the logic. What it cannot cover
is the API reading rows written through the JSONB and native enum columns the application
actually uses in production.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from dag_doctor.api.main import create_app
from dag_doctor.core.models import IncidentStatus, RootCauseCategory
from dag_doctor.core.settings import Settings
from dag_doctor.db.models import DiagnosisRecord, EvidenceRecord, Incident, InvestigationStep

ROOT = Path(__file__).parents[2]

pytestmark = pytest.mark.integration


@pytest.fixture
async def api_client(migrated_session_factory, postgres_dsn):
    settings = Settings()
    settings.db.dsn = postgres_dsn
    app = create_app(settings, migrated_session_factory)
    # Nothing here should reach a broker or a model.
    app.state.readiness_checks = {}
    app.state.replay = None
    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://testserver",
    ) as client:
        yield client


@pytest.fixture
async def incident_id(migrated_session_factory):
    now = datetime.now(UTC)
    async with migrated_session_factory() as session:
        incident = Incident(
            dag_id="schema_drift_orders",
            task_id="build_orders_by_customer",
            run_id="manual__2026-09-08",
            try_number=1,
            map_index=-1,
            status=IncidentStatus.DIAGNOSED,
            failed_at=now,
            received_at=now,
            exception_type="psycopg2.errors.UndefinedColumn",
            exception_message='column "customer_id" does not exist',
        )
        session.add(incident)
        await session.flush()
        session.add_all(
            [
                InvestigationStep(
                    incident_id=incident.id,
                    node="triage",
                    sequence=1,
                    # A nested structure, so JSONB round-tripping is genuinely exercised.
                    input={"signature": "undefined_column"},
                    output={"category": "schema_drift", "scores": [0.1, 0.9]},
                    started_at=now,
                ),
                EvidenceRecord(
                    incident_id=incident.id,
                    tool_name="compare_schema_snapshot",
                    tool_input={"table": "raw.orders"},
                    result={"likely_renames": [{"from": "customer_id", "to": "customer_uuid"}]},
                    summary="raw.orders drifted",
                    succeeded=True,
                ),
                DiagnosisRecord(
                    incident_id=incident.id,
                    root_cause_category=RootCauseCategory.SCHEMA_DRIFT,
                    summary="customer_id was renamed to customer_uuid",
                    confidence=0.86,
                    halt_reason="concluded",
                    evidence_chain=[],
                    unknowns=[],
                    model_used="llama3.2:3b",
                    created_at=now,
                ),
            ]
        )
        await session.commit()
        return incident.id


async def test_an_incident_reads_back_through_the_real_columns(api_client, incident_id):
    response = await api_client.get(f"/api/v1/incidents/{incident_id}")

    body = response.json()
    assert response.status_code == 200
    assert body["diagnosis"]["root_cause_category"] == "schema_drift"
    assert body["diagnosis"]["confidence"] == 0.86


async def test_nested_jsonb_survives_the_round_trip(api_client, incident_id):
    response = await api_client.get(f"/api/v1/incidents/{incident_id}/investigation")

    trace = response.json()
    assert trace["steps"][0]["output"]["scores"] == [0.1, 0.9]
    assert trace["evidence"][0]["result"]["likely_renames"][0]["to"] == "customer_uuid"


async def test_the_native_enum_filter_works(api_client, incident_id):
    response = await api_client.get("/api/v1/diagnoses?root_cause_category=schema_drift")

    assert len(response.json()["items"]) == 1


async def test_filtering_by_incident_status_works_against_the_native_enum(api_client, incident_id):
    assert len((await api_client.get("/api/v1/incidents?status=diagnosed")).json()["items"]) == 1
    assert len((await api_client.get("/api/v1/incidents?status=received")).json()["items"]) == 0


async def test_feedback_is_written_and_read_back(api_client, incident_id):
    diagnosis_id = (await api_client.get("/api/v1/diagnoses")).json()["items"][0]["id"]

    await api_client.post(
        f"/api/v1/diagnoses/{diagnosis_id}/feedback", json={"correct": True, "note": "verified"}
    )
    listed = (await api_client.get("/api/v1/diagnoses?reviewed=true")).json()["items"]

    assert listed[0]["human_verdict"] is True
    assert listed[0]["human_note"] == "verified"


async def test_readiness_reports_a_reachable_database(api_client, migrated_session_factory):
    from sqlalchemy import text

    async def database() -> None:
        async with migrated_session_factory() as session:
            await session.execute(text("SELECT 1"))

    api_client._transport.app.state.readiness_checks = {"postgres": database}

    response = await api_client.get("/health/ready")

    assert response.status_code == 200
    assert response.json()["checks"]["postgres"] == "ok"
