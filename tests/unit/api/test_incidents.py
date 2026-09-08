from uuid import uuid4

import pytest


async def test_incidents_are_listed_newest_first(client, seeded):
    response = await client.get("/api/v1/incidents")

    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["run_id"] for item in items] == [
        "manual__2026-09-07",
        "manual__2026-09-06",
        "manual__2026-09-05",
    ]


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("dag_id=schema_drift_orders", 2),
        ("dag_id=null_explosion_customers", 1),
        ("status=diagnosed", 1),
        ("status=received", 1),
        ("task_id=load_customers", 1),
        ("dag_id=nothing_here", 0),
    ],
)
async def test_the_list_can_be_filtered(client, seeded, query, expected):
    response = await client.get(f"/api/v1/incidents?{query}")

    assert len(response.json()["items"]) == expected


async def test_the_list_can_be_bounded_by_date(client, seeded):
    response = await client.get("/api/v1/incidents?since=2026-09-08T11:30:00Z")

    assert len(response.json()["items"]) == 1


async def test_paging_walks_every_incident_exactly_once(client, seeded):
    # Cursor rather than offset, because incidents arrive continuously and an offset page
    # shifts under the reader as new rows land.
    seen: list[str] = []
    cursor = None
    for _ in range(5):
        query = f"/api/v1/incidents?limit=1{f'&cursor={cursor}' if cursor else ''}"
        page = (await client.get(query)).json()
        seen.extend(item["id"] for item in page["items"])
        cursor = page["next_cursor"]
        if cursor is None:
            break

    assert len(seen) == 3
    assert len(set(seen)) == 3


async def test_the_last_page_offers_no_cursor(client, seeded):
    page = (await client.get("/api/v1/incidents?limit=50")).json()

    assert page["next_cursor"] is None


async def test_a_cursor_this_api_did_not_issue_is_refused(client, seeded):
    response = await client.get("/api/v1/incidents?cursor=not-a-real-cursor")

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "CONFIG_INVALID"


async def test_an_incident_comes_back_with_its_diagnosis(client, seeded):
    response = await client.get(f"/api/v1/incidents/{seeded['diagnosed']}")

    body = response.json()
    assert body["exception_type"] == "psycopg2.errors.UndefinedColumn"
    assert body["diagnosis"]["root_cause_category"] == "schema_drift"
    assert body["diagnosis"]["conclusive"] is True
    assert body["diagnosis"]["responsible_task_id"] == "land_raw_orders"


async def test_an_undiagnosed_incident_says_so_rather_than_omitting_the_field(client, seeded):
    response = await client.get(f"/api/v1/incidents/{seeded['waiting']}")

    assert response.json()["diagnosis"] is None


async def test_an_inconclusive_diagnosis_is_not_presented_as_an_answer(client, seeded):
    response = await client.get(f"/api/v1/incidents/{seeded['inconclusive']}")

    diagnosis = response.json()["diagnosis"]
    assert diagnosis["conclusive"] is False
    assert diagnosis["halt_reason"] == "iteration_budget_exhausted"
    assert diagnosis["unknowns"] == ["raising MAX_ITERATIONS may help"]


async def test_an_incident_that_does_not_exist_returns_the_error_envelope(client):
    missing = uuid4()

    response = await client.get(f"/api/v1/incidents/{missing}")

    assert response.status_code == 404
    error = response.json()["error"]
    assert error["code"] == "NOT_FOUND"
    assert error["details"]["incident_id"] == str(missing)
    assert error["request_id"]


async def test_an_identifier_that_is_not_a_uuid_is_a_validation_error(client):
    response = await client.get("/api/v1/incidents/not-a-uuid")

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


async def test_the_trace_shows_the_whole_investigation(client, seeded):
    response = await client.get(f"/api/v1/incidents/{seeded['diagnosed']}/investigation")

    trace = response.json()
    assert [step["node"] for step in trace["steps"]] == ["triage", "conclude"]
    assert trace["evidence"][0]["tool_name"] == "compare_schema_snapshot"
    assert trace["diagnosis"]["confidence"] == 0.86


async def test_the_trace_keeps_what_was_ruled_out(client, seeded):
    # What was refuted, and what refuted it, is half the argument for the conclusion.
    response = await client.get(f"/api/v1/incidents/{seeded['diagnosed']}/investigation")

    outcomes = {item["statement"]: item["outcome"] for item in response.json()["hypotheses"]}
    assert outcomes["the warehouse was unreachable"] == "refuted"
    assert outcomes["customer_id was renamed to customer_uuid"] == "confirmed"


async def test_the_trace_reports_token_spend(client, seeded):
    response = await client.get(f"/api/v1/incidents/{seeded['diagnosed']}/investigation")

    assert sum(step["prompt_tokens"] for step in response.json()["steps"]) == 52


async def test_the_trace_of_an_uninvestigated_incident_is_empty_not_missing(client, seeded):
    response = await client.get(f"/api/v1/incidents/{seeded['waiting']}/investigation")

    trace = response.json()
    assert response.status_code == 200
    assert trace["steps"] == []
    assert trace["attempts"] == 0
