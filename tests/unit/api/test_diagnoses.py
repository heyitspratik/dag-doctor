from uuid import uuid4


async def test_diagnoses_are_listed_newest_first(client, seeded):
    response = await client.get("/api/v1/diagnoses")

    categories = [item["root_cause_category"] for item in response.json()["items"]]
    assert categories == ["schema_drift", "unknown"]


async def test_diagnoses_can_be_filtered_by_category(client, seeded):
    response = await client.get("/api/v1/diagnoses?root_cause_category=schema_drift")

    assert len(response.json()["items"]) == 1


async def test_diagnoses_can_be_filtered_by_confidence(client, seeded):
    response = await client.get("/api/v1/diagnoses?min_confidence=0.5")

    assert [item["confidence"] for item in response.json()["items"]] == [0.86]


async def test_unreviewed_diagnoses_can_be_found(client, seeded):
    # Unjudged is not the same as judged wrong, and the accuracy metric respects that.
    response = await client.get("/api/v1/diagnoses?reviewed=false")

    assert len(response.json()["items"]) == 2


async def test_a_human_verdict_is_recorded(client, seeded):
    diagnosis_id = (await client.get("/api/v1/diagnoses")).json()["items"][0]["id"]

    response = await client.post(
        f"/api/v1/diagnoses/{diagnosis_id}/feedback",
        json={"correct": True, "note": "confirmed against the upstream migration"},
    )

    assert response.status_code == 200
    assert response.json()["human_verdict"] is True
    assert response.json()["human_note"] == "confirmed against the upstream migration"


async def test_a_recorded_verdict_survives_a_reread(client, seeded):
    diagnosis_id = (await client.get("/api/v1/diagnoses")).json()["items"][0]["id"]
    await client.post(f"/api/v1/diagnoses/{diagnosis_id}/feedback", json={"correct": False})

    listed = (await client.get("/api/v1/diagnoses?reviewed=true")).json()["items"]

    assert [item["id"] for item in listed] == [diagnosis_id]
    assert listed[0]["human_verdict"] is False


async def test_a_verdict_can_be_revised(client, seeded):
    # People change their minds once they have looked properly. Refusing the correction
    # would leave the accuracy number wrong on purpose.
    diagnosis_id = (await client.get("/api/v1/diagnoses")).json()["items"][0]["id"]

    await client.post(f"/api/v1/diagnoses/{diagnosis_id}/feedback", json={"correct": False})
    response = await client.post(
        f"/api/v1/diagnoses/{diagnosis_id}/feedback", json={"correct": True}
    )

    assert response.json()["human_verdict"] is True


async def test_feedback_on_a_diagnosis_that_does_not_exist_is_a_404(client):
    response = await client.post(f"/api/v1/diagnoses/{uuid4()}/feedback", json={"correct": True})

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "NOT_FOUND"


async def test_feedback_without_a_verdict_is_refused(client, seeded):
    diagnosis_id = (await client.get("/api/v1/diagnoses")).json()["items"][0]["id"]

    response = await client.post(f"/api/v1/diagnoses/{diagnosis_id}/feedback", json={})

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"
