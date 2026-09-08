"""What the API returns.

Response models are separate from both the database rows and the domain models. A column
rename should not become a breaking API change, and the API should be free to omit things
the database happens to store.
"""

from collections.abc import Mapping
from datetime import datetime
from typing import Self
from uuid import UUID

from pydantic import BaseModel, Field, JsonValue, TypeAdapter

from dag_doctor.core.models import HaltReason, IncidentStatus, RootCauseCategory
from dag_doctor.db.models import (
    DiagnosisRecord,
    EvidenceRecord,
    HypothesisRecord,
    Incident,
    InvestigationStep,
)

#: Everything in a JSON column is JSON by construction, but the ORM types those columns
#: loosely. Validating here states the invariant instead of asserting it with a cast.
_JSON_OBJECT = TypeAdapter(dict[str, JsonValue])


def _json_object(value: Mapping[str, object]) -> dict[str, JsonValue]:
    """Read a JSON column as a JSON object."""
    return _JSON_OBJECT.validate_python(dict(value))


class DiagnosisOut(BaseModel):
    """A diagnosis as the API returns it."""

    id: UUID
    incident_id: UUID
    attempt: int
    root_cause_category: RootCauseCategory
    summary: str
    confidence: float
    halt_reason: str
    conclusive: bool
    proposed_fix: str | None
    responsible_dag_id: str | None
    responsible_task_id: str | None
    unknowns: list[str]
    evidence_chain: list[str]
    model_used: str
    human_verdict: bool | None
    human_note: str | None
    created_at: datetime

    @classmethod
    def of(cls, record: DiagnosisRecord) -> Self:
        """Build the response from a stored diagnosis."""
        return cls(
            id=record.id,
            incident_id=record.incident_id,
            attempt=record.attempt,
            root_cause_category=record.root_cause_category,
            summary=record.summary,
            confidence=record.confidence,
            halt_reason=record.halt_reason,
            # Derived rather than stored, so the rule for what counts as a real answer
            # lives in one place.
            conclusive=record.halt_reason == HaltReason.CONCLUDED.value
            and record.root_cause_category is not RootCauseCategory.UNKNOWN,
            proposed_fix=record.proposed_fix,
            responsible_dag_id=record.responsible_dag_id,
            responsible_task_id=record.responsible_task_id,
            unknowns=[str(item) for item in record.unknowns],
            evidence_chain=[str(item) for item in record.evidence_chain],
            model_used=record.model_used,
            human_verdict=record.human_verdict,
            human_note=record.human_note,
            created_at=record.created_at,
        )


class IncidentSummary(BaseModel):
    """An incident in a list."""

    id: UUID
    dag_id: str
    task_id: str
    run_id: str
    try_number: int
    map_index: int
    status: IncidentStatus
    failed_at: datetime
    received_at: datetime
    delivery_count: int
    exception_type: str | None

    @classmethod
    def of(cls, record: Incident) -> Self:
        """Build the response from a stored incident."""
        return cls(
            id=record.id,
            dag_id=record.dag_id,
            task_id=record.task_id,
            run_id=record.run_id,
            try_number=record.try_number,
            map_index=record.map_index,
            status=record.status,
            failed_at=record.failed_at,
            received_at=record.received_at,
            delivery_count=record.delivery_count,
            exception_type=record.exception_type,
        )


class IncidentDetail(IncidentSummary):
    """An incident with the answer, if it has one."""

    log_url: str | None = None
    exception_message: str | None = None
    diagnosis: DiagnosisOut | None = None

    @classmethod
    def of_incident(cls, record: Incident, diagnosis: DiagnosisRecord | None) -> Self:
        """Build the response from an incident and its most recent diagnosis."""
        summary = IncidentSummary.of(record)
        return cls(
            **summary.model_dump(),
            log_url=record.log_url,
            exception_message=record.exception_message,
            diagnosis=DiagnosisOut.of(diagnosis) if diagnosis is not None else None,
        )


class StepOut(BaseModel):
    """One node execution in the trace."""

    node: str
    attempt: int
    sequence: int
    input: dict[str, JsonValue]
    output: dict[str, JsonValue]
    duration_ms: int
    prompt_tokens: int
    completion_tokens: int
    model_used: str | None
    started_at: datetime

    @classmethod
    def of(cls, record: InvestigationStep) -> Self:
        """Build the response from a stored step."""
        return cls(
            node=record.node,
            attempt=record.attempt,
            sequence=record.sequence,
            input=_json_object(record.input),
            output=_json_object(record.output),
            duration_ms=record.duration_ms,
            prompt_tokens=record.prompt_tokens,
            completion_tokens=record.completion_tokens,
            model_used=record.model_used,
            started_at=record.started_at,
        )


class EvidenceOut(BaseModel):
    """One tool result in the trace."""

    id: UUID
    attempt: int
    tool_name: str
    tool_input: dict[str, JsonValue]
    result: dict[str, JsonValue]
    summary: str
    succeeded: bool
    duration_ms: int
    collected_at: datetime

    @classmethod
    def of(cls, record: EvidenceRecord) -> Self:
        """Build the response from a stored evidence row."""
        return cls(
            id=record.id,
            attempt=record.attempt,
            tool_name=record.tool_name,
            tool_input=_json_object(record.tool_input),
            result=_json_object(record.result),
            summary=record.summary,
            succeeded=record.succeeded,
            duration_ms=record.duration_ms,
            collected_at=record.collected_at,
        )


class HypothesisOut(BaseModel):
    """One candidate cause in the trace, including the ones that were ruled out."""

    id: UUID
    attempt: int
    statement: str
    root_cause_category: RootCauseCategory
    proposed_test: str
    test_call: dict[str, JsonValue]
    outcome: str
    rank: int
    test_notes: str | None
    supporting_evidence_ids: list[str]
    responsible_dag_id: str | None
    responsible_task_id: str | None

    @classmethod
    def of(cls, record: HypothesisRecord) -> Self:
        """Build the response from a stored hypothesis."""
        return cls(
            id=record.id,
            attempt=record.attempt,
            statement=record.statement,
            root_cause_category=record.root_cause_category,
            proposed_test=record.proposed_test,
            test_call=_json_object(record.test_call),
            outcome=record.outcome.value,
            rank=record.rank,
            test_notes=record.test_notes,
            supporting_evidence_ids=[str(item) for item in record.supporting_evidence_ids],
            responsible_dag_id=record.responsible_dag_id,
            responsible_task_id=record.responsible_task_id,
        )


class InvestigationTrace(BaseModel):
    """The whole investigation, step by step.

    This is the endpoint that turns the agent from a black box into something a person can
    audit: what it looked at, what it believed, what it tried to disprove, and what it
    concluded. Refuted hypotheses are included on purpose, because what was ruled out is
    half the argument.
    """

    incident_id: UUID
    attempts: int
    steps: list[StepOut]
    evidence: list[EvidenceOut]
    hypotheses: list[HypothesisOut]
    diagnosis: DiagnosisOut | None


class Page[T](BaseModel):
    """One page of results, with the cursor for the next.

    Cursor rather than offset: incidents arrive continuously, and an offset page shifts
    under the reader as new rows land, silently skipping or repeating items.
    """

    items: list[T]
    next_cursor: str | None = None


class FeedbackIn(BaseModel):
    """A human's verdict on a diagnosis."""

    correct: bool
    note: str | None = Field(default=None, max_length=2000)


class ReplayIn(BaseModel):
    """Options for re-running an investigation."""

    #: Overrides the provider default for every node in this run. The point of a replay is
    #: comparing models and prompts against the same failure.
    model: str | None = Field(default=None, max_length=120)


class ReplayAccepted(BaseModel):
    """The acknowledgement that a replay has been queued."""

    incident_id: UUID
    attempt: int
    status: str = "accepted"
