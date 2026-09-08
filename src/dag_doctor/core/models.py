"""The domain models threaded through the investigation.

These are the vocabulary of the whole application: the failure that starts it, the
evidence gathered along the way, the hypotheses formed and tested, and the diagnosis that
comes out. They are deliberately independent of both the wire format (see
:mod:`dag_doctor.messaging.schemas`) and the database rows (see :mod:`dag_doctor.db`), so
a change to either does not ripple into the graph.
"""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, JsonValue

#: The nodes of the investigation graph. Declared here rather than in the graph package
#: because settings and persistence both name nodes, and neither should import the graph.
type NodeName = Literal[
    "triage",
    "gather_evidence",
    "form_hypothesis",
    "test_hypothesis",
    "conclude",
    "escalate",
]


def _utc_now() -> datetime:
    """Timezone-aware now, so a timestamp never depends on the container's clock setting."""
    return datetime.now(UTC)


class RootCauseCategory(StrEnum):
    """The closed set of root causes a diagnosis may assert.

    This is a closed enum, and persisted as a database enum, precisely so that accuracy is
    computable by comparison rather than by a human reading free text. A model that cannot
    fit a failure into one of these must say ``UNKNOWN`` rather than inventing a category.
    """

    SCHEMA_DRIFT = "schema_drift"
    DATA_QUALITY_REGRESSION = "data_quality_regression"
    UPSTREAM_DEPENDENCY_FAILURE = "upstream_dependency_failure"
    MISSING_UPSTREAM_DATA = "missing_upstream_data"
    TRANSIENT_INFRASTRUCTURE = "transient_infrastructure"
    QUERY_DEFECT = "query_defect"
    TYPE_MISMATCH = "type_mismatch"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    CONFIGURATION_ERROR = "configuration_error"
    UNKNOWN = "unknown"


class HaltReason(StrEnum):
    """Why the investigation stopped.

    Everything except :attr:`CONCLUDED` produces an inconclusive diagnosis. Running out of
    budget is an ordinary outcome recorded honestly, not a crash and not a guess.
    """

    CONCLUDED = "concluded"
    ITERATION_BUDGET_EXHAUSTED = "iteration_budget_exhausted"
    TOOL_CALL_BUDGET_EXHAUSTED = "tool_call_budget_exhausted"
    CONFIDENCE_TOO_LOW = "confidence_too_low"
    NO_HYPOTHESIS_FORMED = "no_hypothesis_formed"
    INVESTIGATION_ERROR = "investigation_error"


class IncidentStatus(StrEnum):
    """Where an incident is in its lifecycle.

    ``RECEIVED`` and ``INVESTIGATING`` are the only non-terminal states. ``FAILED`` means
    the investigation itself broke, which is distinct from ``INCONCLUSIVE``, where the
    investigation ran correctly and honestly could not decide.
    """

    RECEIVED = "received"
    INVESTIGATING = "investigating"
    DIAGNOSED = "diagnosed"
    INCONCLUSIVE = "inconclusive"
    FAILED = "failed"


class HypothesisOutcome(StrEnum):
    """The result of executing a hypothesis's falsifiable test."""

    UNTESTED = "untested"
    CONFIRMED = "confirmed"
    REFUTED = "refuted"
    INCONCLUSIVE = "inconclusive"


class FailureEvent(BaseModel):
    """One Airflow task failure, as delivered by the event log.

    Frozen: the event is a historical fact. Anything the investigation learns about it
    belongs on the state, not written back over the event.
    """

    model_config = ConfigDict(frozen=True)

    dag_id: str = Field(min_length=1)
    task_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    try_number: int = Field(ge=1)
    map_index: int = -1
    logical_date: datetime | None = None
    failed_at: datetime = Field(default_factory=_utc_now)
    log_url: str | None = None
    exception_type: str | None = None
    exception_message: str | None = None

    @property
    def idempotency_key(self) -> str:
        """The four-tuple that identifies a failure, used to deduplicate redeliveries.

        Kafka delivery is at-least-once, so the same failure can arrive more than once.
        Keying on this makes a redelivery update an existing incident rather than open a
        second one. ``map_index`` is included because each mapped task instance fails
        independently and deserves its own incident.
        """
        return f"{self.dag_id}/{self.task_id}/{self.run_id}/{self.try_number}/{self.map_index}"


class Evidence(BaseModel):
    """One structured observation returned by a tool.

    A tool that failed still produces evidence: knowing that the metadata database was
    unreachable is information the graph should reason over, not an absence.
    """

    id: UUID = Field(default_factory=uuid4)
    tool_name: str
    tool_input: dict[str, JsonValue] = Field(default_factory=dict)
    result: dict[str, JsonValue] = Field(default_factory=dict)
    summary: str = ""
    succeeded: bool = True
    duration_ms: int = 0
    collected_at: datetime = Field(default_factory=_utc_now)


class Hypothesis(BaseModel):
    """A candidate root cause paired with the check that would refute it.

    ``proposed_test`` is not decoration. A hypothesis without a falsifiable test cannot be
    tested, and an investigation that cannot refute itself is just a guess with citations.
    """

    id: UUID = Field(default_factory=uuid4)
    statement: str
    root_cause_category: RootCauseCategory
    proposed_test: str
    #: The tool call that would refute this hypothesis. Structured rather than prose so
    #: the test node can actually execute it: a test nobody can run is not a test.
    test_tool: str | None = None
    test_arguments: dict[str, JsonValue] = Field(default_factory=dict)
    rank: int = 0
    outcome: HypothesisOutcome = HypothesisOutcome.UNTESTED
    test_notes: str | None = None
    supporting_evidence_ids: list[UUID] = Field(default_factory=list)
    responsible_dag_id: str | None = None
    responsible_task_id: str | None = None
    created_at: datetime = Field(default_factory=_utc_now)


class Diagnosis(BaseModel):
    """The investigation's conclusion, with the evidence that supports it.

    ``confidence`` is computed by :mod:`dag_doctor.graph.confidence` from the shape of the
    investigation. It is never the model's self-reported number, which is not calibrated.
    """

    model_config = ConfigDict(frozen=True)

    incident_id: UUID
    root_cause_category: RootCauseCategory
    summary: str
    confidence: float = Field(ge=0.0, le=1.0)
    halt_reason: HaltReason = HaltReason.CONCLUDED
    evidence_chain: list[UUID] = Field(default_factory=list)
    proposed_fix: str | None = None
    responsible_dag_id: str | None = None
    responsible_task_id: str | None = None
    unknowns: list[str] = Field(default_factory=list)
    model_used: str = ""
    created_at: datetime = Field(default_factory=_utc_now)

    @property
    def is_conclusive(self) -> bool:
        """Whether the agent is actually asserting a root cause."""
        return (
            self.halt_reason is HaltReason.CONCLUDED
            and self.root_cause_category is not RootCauseCategory.UNKNOWN
        )
