"""Turning a real investigation into the worked example the README shows.

The README's opening section has to be genuine output, not something written by hand to
look like output. Generating it from the live API is what keeps that honest: if the agent
stops finding the rename, the example stops saying it did.
"""

from typing import Any

import httpx

from dag_doctor.core.exceptions import DagDoctorError
from dag_doctor.core.logging import get_logger

logger = get_logger(__name__)

#: Truncation for evidence payloads. A full profiling result is thousands of characters
#: and would bury the argument the example is meant to show.
MAX_SUMMARY_CHARS = 220


class NoIncidentError(DagDoctorError):
    """The DAG asked about has not produced a diagnosed incident."""

    code = "NO_INCIDENT"
    http_status = 404


async def fetch_latest(
    client: httpx.AsyncClient, dag_id: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Fetch the most recent diagnosed incident for a DAG, and its trace.

    Args:
        client: A client pointed at the agent's API.
        dag_id: Which seeded DAG to write up.

    Returns:
        The incident detail and its investigation trace.

    Raises:
        NoIncidentError: If the DAG has no incident yet, which usually means the stack is
            up but nothing has been triggered.
    """
    listing = await client.get("/api/v1/incidents", params={"dag_id": dag_id, "limit": 1})
    listing.raise_for_status()
    items = listing.json()["items"]
    if not items:
        raise NoIncidentError(
            f"No incident recorded for {dag_id!r}. Run `make seed-failures` first.",
            details={"dag_id": dag_id},
        )

    incident_id = items[0]["id"]
    detail = await client.get(f"/api/v1/incidents/{incident_id}")
    detail.raise_for_status()
    trace = await client.get(f"/api/v1/incidents/{incident_id}/investigation")
    trace.raise_for_status()
    return detail.json(), trace.json()


def render(incident: dict[str, Any], trace: dict[str, Any]) -> str:
    """Render one investigation as markdown, ready to paste into the README.

    Args:
        incident: The incident detail from the API.
        trace: The investigation trace from the API.

    Returns:
        The worked example.
    """
    sections = [
        _failure(incident),
        _steps(trace),
        _evidence(trace),
        _hypotheses(trace),
        _diagnosis(incident.get("diagnosis")),
    ]
    return "\n".join(section for section in sections if section)


def _failure(incident: dict[str, Any]) -> str:
    """What Airflow reported."""
    return (
        "**The failure Airflow reported**\n\n"
        "```\n"
        f"dag_id    {incident['dag_id']}\n"
        f"task_id   {incident['task_id']}\n"
        f"run_id    {incident['run_id']}\n"
        f"exception {incident.get('exception_type') or '(none recorded)'}\n"
        f"message   {incident.get('exception_message') or '(none recorded)'}\n"
        "```\n"
    )


def _steps(trace: dict[str, Any]) -> str:
    """The path the investigation took through the graph."""
    steps = trace.get("steps") or []
    if not steps:
        return ""
    path = " -> ".join(step["node"] for step in steps)
    rows = "".join(
        f"| {step['sequence']} | `{step['node']}` | {step['duration_ms']} ms "
        f"| {step['prompt_tokens'] + step['completion_tokens']} |\n"
        for step in steps
    )
    tokens = sum(step["prompt_tokens"] + step["completion_tokens"] for step in steps)
    return (
        f"**The path it took**: `{path}`\n\n"
        "| # | Node | Duration | Tokens |\n|---|---|---|---|\n"
        f"{rows}\n"
        f"Total: {len(steps)} steps, {tokens} tokens.\n"
    )


def _evidence(trace: dict[str, Any]) -> str:
    """What each tool found, including the ones that could not answer."""
    evidence = trace.get("evidence") or []
    if not evidence:
        return ""
    lines = "".join(
        f"{index}. `{item['tool_name']}`{'' if item['succeeded'] else ' (could not answer)'}: "
        f"{_clip(item['summary'])}\n"
        for index, item in enumerate(evidence, start=1)
    )
    return f"**What it looked at**\n\n{lines}\n"


def _hypotheses(trace: dict[str, Any]) -> str:
    """What it believed and what it ruled out.

    Refuted hypotheses are included deliberately. What was discarded, and what discarded
    it, is half of why the conclusion should be believed.
    """
    hypotheses = trace.get("hypotheses") or []
    if not hypotheses:
        return ""
    lines = "".join(
        f"- **{item['outcome']}**: {item['statement']}\n"
        f"  - test: {item['proposed_test']}\n"
        + (f"  - result: {_clip(item['test_notes'])}\n" if item.get("test_notes") else "")
        for item in hypotheses
    )
    return f"**What it believed, and what it ruled out**\n\n{lines}\n"


def _diagnosis(diagnosis: dict[str, Any] | None) -> str:
    """The conclusion, with the confidence computed for it."""
    if diagnosis is None:
        return "**No diagnosis was produced.**\n"
    fix = diagnosis.get("proposed_fix") or "(none proposed)"
    responsible = diagnosis.get("responsible_task_id")
    blame = f"\nResponsible task: `{responsible}`" if responsible else ""
    unknowns = diagnosis.get("unknowns") or []
    gaps = (
        "\n\nWhat it could not establish:\n" + "".join(f"- {item}\n" for item in unknowns)
        if unknowns
        else ""
    )
    return (
        "**The diagnosis**\n\n"
        f"> **{diagnosis['root_cause_category']}** "
        f"(confidence {diagnosis['confidence']:.2f}, "
        f"{'conclusive' if diagnosis['conclusive'] else 'inconclusive'})\n>\n"
        f"> {diagnosis['summary']}\n>\n"
        f"> **Proposed fix:** {fix}{blame}\n"
        f"{gaps}"
    )


def _clip(text: str | None) -> str:
    """Shorten a payload so it informs rather than buries."""
    if not text:
        return ""
    cleaned = " ".join(text.split())
    if len(cleaned) <= MAX_SUMMARY_CHARS:
        return cleaned
    return f"{cleaned[:MAX_SUMMARY_CHARS]}..."
