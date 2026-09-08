"""An application wired to a real database, with no network anywhere."""

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from dag_doctor.api.main import create_app
from dag_doctor.core.models import IncidentStatus, RootCauseCategory
from dag_doctor.core.settings import Settings
from dag_doctor.db.models import (
    DiagnosisRecord,
    EvidenceRecord,
    HypothesisRecord,
    Incident,
    InvestigationStep,
)

NOW = datetime(2026, 9, 8, 12, 0, tzinfo=UTC)


@pytest.fixture
def api_settings() -> Settings:
    return Settings()


@pytest.fixture
def api_app(api_settings, session_factory) -> FastAPI:
    app = create_app(api_settings, session_factory)
    # Readiness would otherwise reach for a database, a broker and Ollama. Tests that
    # care about readiness replace these deliberately.
    app.state.readiness_checks = {}
    return app


@pytest.fixture
async def client(api_app: FastAPI) -> AsyncIterator[AsyncClient]:
    # raise_app_exceptions=False so an unhandled error reaches the test as the response a
    # real client would get, rather than as an exception the test framework intercepts.
    async with (
        AsyncClient(
            transport=ASGITransport(app=api_app, raise_app_exceptions=False),
            base_url="http://testserver",
        ) as http,
        api_app.router.lifespan_context(api_app),
    ):
        yield http


@pytest.fixture
async def seeded(session_factory):
    """Three incidents: one diagnosed, one inconclusive, one still waiting."""
    async with session_factory() as session:
        diagnosed = Incident(
            dag_id="schema_drift_orders",
            task_id="build_orders_by_customer",
            run_id="manual__2026-09-07",
            try_number=1,
            map_index=-1,
            status=IncidentStatus.DIAGNOSED,
            failed_at=NOW,
            received_at=NOW,
            exception_type="psycopg2.errors.UndefinedColumn",
            exception_message='column "customer_id" does not exist',
            log_url="http://localhost:8080/log",
        )
        inconclusive = Incident(
            dag_id="null_explosion_customers",
            task_id="load_customers",
            run_id="manual__2026-09-06",
            try_number=1,
            map_index=-1,
            status=IncidentStatus.INCONCLUSIVE,
            failed_at=NOW - timedelta(hours=1),
            received_at=NOW - timedelta(hours=1),
        )
        waiting = Incident(
            dag_id="schema_drift_orders",
            task_id="build_orders_by_customer",
            run_id="manual__2026-09-05",
            try_number=1,
            map_index=-1,
            status=IncidentStatus.RECEIVED,
            failed_at=NOW - timedelta(hours=2),
            received_at=NOW - timedelta(hours=2),
        )
        session.add_all([diagnosed, inconclusive, waiting])
        await session.flush()

        evidence = EvidenceRecord(
            incident_id=diagnosed.id,
            tool_name="compare_schema_snapshot",
            tool_input={"connection": "warehouse", "table": "raw.orders"},
            result={"has_drift": True},
            summary="raw.orders drifted: customer_id appears renamed to customer_uuid",
            succeeded=True,
        )
        session.add(evidence)
        session.add_all(
            [
                InvestigationStep(
                    incident_id=diagnosed.id,
                    node="triage",
                    sequence=1,
                    input={},
                    output={"category": "schema_drift"},
                    prompt_tokens=12,
                    model_used="llama3.2:3b",
                    started_at=NOW,
                ),
                InvestigationStep(
                    incident_id=diagnosed.id,
                    node="conclude",
                    sequence=2,
                    input={},
                    output={"confidence": 0.86},
                    prompt_tokens=40,
                    model_used="llama3.2:3b",
                    started_at=NOW,
                ),
                HypothesisRecord(
                    incident_id=diagnosed.id,
                    statement="customer_id was renamed to customer_uuid",
                    root_cause_category=RootCauseCategory.SCHEMA_DRIFT,
                    proposed_test="diff the schema",
                    test_call={"tool": "compare_schema_snapshot", "arguments": {}},
                    outcome="confirmed",
                    rank=0,
                    supporting_evidence_ids=[],
                ),
                HypothesisRecord(
                    incident_id=diagnosed.id,
                    statement="the warehouse was unreachable",
                    root_cause_category=RootCauseCategory.TRANSIENT_INFRASTRUCTURE,
                    proposed_test="probe the connection",
                    test_call={},
                    outcome="refuted",
                    rank=1,
                    supporting_evidence_ids=[],
                ),
                DiagnosisRecord(
                    incident_id=diagnosed.id,
                    root_cause_category=RootCauseCategory.SCHEMA_DRIFT,
                    summary="customer_id was renamed to customer_uuid upstream",
                    confidence=0.86,
                    halt_reason="concluded",
                    evidence_chain=[str(evidence.id)],
                    proposed_fix="select customer_uuid",
                    responsible_dag_id="schema_drift_orders",
                    responsible_task_id="land_raw_orders",
                    unknowns=[],
                    model_used="llama3.2:3b",
                    created_at=NOW,
                ),
                DiagnosisRecord(
                    incident_id=inconclusive.id,
                    root_cause_category=RootCauseCategory.UNKNOWN,
                    summary="ran out of iterations",
                    confidence=0.31,
                    halt_reason="iteration_budget_exhausted",
                    evidence_chain=[],
                    unknowns=["raising MAX_ITERATIONS may help"],
                    model_used="llama3.2:3b",
                    created_at=NOW - timedelta(hours=1),
                ),
            ]
        )
        await session.commit()
        return {"diagnosed": diagnosed.id, "inconclusive": inconclusive.id, "waiting": waiting.id}
