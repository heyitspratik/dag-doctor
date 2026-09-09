"""The worked-example renderer.

The README's opening section has to be genuine output. Generating it from the live API is
what keeps that honest, so this checks the rendering rather than the prose.
"""

import httpx
import pytest

from dag_doctor.evaluation.report import MAX_SUMMARY_CHARS, NoIncidentError, fetch_latest, render

INCIDENT = {
    "id": "11111111-1111-1111-1111-111111111111",
    "dag_id": "schema_drift_orders",
    "task_id": "build_orders_by_customer",
    "run_id": "manual__2026-09-09",
    "exception_type": "psycopg2.errors.UndefinedColumn",
    "exception_message": 'column "customer_id" does not exist',
    "diagnosis": {
        "root_cause_category": "schema_drift",
        "confidence": 0.86,
        "conclusive": True,
        "summary": "orders.customer_id was renamed to customer_uuid upstream",
        "proposed_fix": "Select customer_uuid, or restore the old name upstream",
        "responsible_task_id": "land_raw_orders",
        "unknowns": ["who made the upstream change"],
    },
}

TRACE = {
    "steps": [
        {
            "sequence": 1,
            "node": "triage",
            "duration_ms": 900,
            "prompt_tokens": 300,
            "completion_tokens": 40,
        },
        {
            "sequence": 2,
            "node": "gather_evidence",
            "duration_ms": 2100,
            "prompt_tokens": 800,
            "completion_tokens": 60,
        },
        {
            "sequence": 3,
            "node": "conclude",
            "duration_ms": 1500,
            "prompt_tokens": 700,
            "completion_tokens": 120,
        },
    ],
    "evidence": [
        {
            "tool_name": "fetch_task_logs",
            "summary": "UndefinedColumn on customer_id",
            "succeeded": True,
        },
        {"tool_name": "profile_table", "summary": "unavailable", "succeeded": False},
    ],
    "hypotheses": [
        {
            "outcome": "confirmed",
            "statement": "customer_id was renamed to customer_uuid",
            "proposed_test": "diff the schema against the stored snapshot",
            "test_notes": "the diff shows the rename",
        },
        {
            "outcome": "refuted",
            "statement": "the warehouse was unreachable",
            "proposed_test": "probe the connection",
            "test_notes": "the connection answered in 3ms",
        },
    ],
}


def test_the_failure_airflow_reported_comes_first():
    rendered = render(INCIDENT, TRACE)

    assert rendered.index("The failure Airflow reported") < rendered.index("The path it took")
    assert 'column "customer_id" does not exist' in rendered


def test_the_path_through_the_graph_is_shown():
    rendered = render(INCIDENT, TRACE)

    assert "`triage -> gather_evidence -> conclude`" in rendered
    assert "Total: 3 steps, 2020 tokens." in rendered


def test_a_tool_that_could_not_answer_is_shown_as_such():
    # Hiding the failed calls would make the trace look tidier than the investigation was.
    rendered = render(INCIDENT, TRACE)

    assert "`profile_table` (could not answer)" in rendered


def test_what_was_ruled_out_appears_beside_what_was_concluded():
    # What was discarded, and what discarded it, is half of why the conclusion should be
    # believed.
    rendered = render(INCIDENT, TRACE)

    assert "**refuted**: the warehouse was unreachable" in rendered
    assert "the connection answered in 3ms" in rendered


def test_the_diagnosis_carries_its_confidence_and_the_task_it_blames():
    rendered = render(INCIDENT, TRACE)

    assert "**schema_drift** (confidence 0.86, conclusive)" in rendered
    assert "`land_raw_orders`" in rendered
    assert "who made the upstream change" in rendered


def test_an_inconclusive_result_is_not_dressed_up():
    incident = {**INCIDENT, "diagnosis": {**INCIDENT["diagnosis"], "conclusive": False}}

    assert "inconclusive)" in render(incident, TRACE)


def test_an_undiagnosed_incident_says_so():
    assert "No diagnosis was produced" in render({**INCIDENT, "diagnosis": None}, TRACE)


def test_an_empty_trace_renders_without_failing():
    rendered = render(
        {**INCIDENT, "diagnosis": None}, {"steps": [], "evidence": [], "hypotheses": []}
    )

    assert "The failure Airflow reported" in rendered


def test_a_runaway_tool_payload_is_clipped():
    # A full profiling result would bury the argument the example exists to show.
    trace = {
        **TRACE,
        "evidence": [{"tool_name": "profile_table", "summary": "x" * 5000, "succeeded": True}],
    }

    rendered = render(INCIDENT, trace)

    assert "..." in rendered
    assert len(max(rendered.splitlines(), key=len)) < MAX_SUMMARY_CHARS + 100


async def test_the_latest_incident_and_its_trace_are_fetched():
    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/incidents":
            return httpx.Response(200, json={"items": [{"id": INCIDENT["id"]}]})
        if request.url.path.endswith("/investigation"):
            return httpx.Response(200, json=TRACE)
        return httpx.Response(200, json=INCIDENT)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handle), base_url="http://agent"
    ) as client:
        incident, trace = await fetch_latest(client, "schema_drift_orders")

    assert incident["dag_id"] == "schema_drift_orders"
    assert len(trace["steps"]) == 3


async def test_a_dag_with_no_incident_says_what_to_run():
    def handle(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"items": []})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handle), base_url="http://agent"
    ) as client:
        with pytest.raises(NoIncidentError) as excinfo:
            await fetch_latest(client, "schema_drift_orders")

    assert "make seed-failures" in excinfo.value.message
