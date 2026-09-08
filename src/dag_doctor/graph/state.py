"""The state carried through the investigation.

Two things here are load-bearing rather than bookkeeping.

The budgets are hard limits, and hitting one is a legitimate terminal state producing an
inconclusive diagnosis with whatever evidence was gathered. An agent without a budget is
an agent that loops forever on an ambiguous failure and bills for the privilege.

The step trace records every node execution with its inputs, outputs and cost. It is what
turns the agent from a black box into something a human can audit, and it is the reason
the investigation is worth trusting at all.
"""

from datetime import UTC, datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from dag_doctor.core.models import (
    Diagnosis,
    Evidence,
    FailureEvent,
    HaltReason,
    Hypothesis,
    HypothesisOutcome,
    NodeName,
    RootCauseCategory,
)


class TriageResult(BaseModel):
    """What can be said from the failure event and its exception alone.

    ``confidence`` is computed in code from whether the signature matches a known pattern,
    never taken from the model. A model asked how sure it is will say ninety percent
    regardless, which is precisely the failure mode this design avoids.
    """

    category: RootCauseCategory
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str
    matched_signature: str | None = None
    needs_investigation: bool = True


class StepRecord(BaseModel):
    """One node execution, for the trace."""

    node: NodeName
    sequence: int
    input: dict[str, JsonValue] = Field(default_factory=dict)
    output: dict[str, JsonValue] = Field(default_factory=dict)
    duration_ms: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    model_used: str | None = None
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class InvestigationState(BaseModel):
    """State threaded through every node of the graph."""

    model_config = ConfigDict(arbitrary_types_allowed=False)

    incident_id: UUID
    failure: FailureEvent

    triage: TriageResult | None = None
    evidence: list[Evidence] = Field(default_factory=list)
    hypotheses: list[Hypothesis] = Field(default_factory=list)
    current_hypothesis: Hypothesis | None = None

    iteration: int = 0
    max_iterations: int = 5
    tool_calls_made: int = 0
    max_tool_calls: int = 20

    diagnosis: Diagnosis | None = None
    halt_reason: HaltReason | None = None

    steps: list[StepRecord] = Field(default_factory=list)
    unknowns: list[str] = Field(default_factory=list)
    model_used: str = ""

    @property
    def iterations_remaining(self) -> int:
        """How many more gather-and-test cycles the budget allows."""
        return max(0, self.max_iterations - self.iteration)

    @property
    def tool_calls_remaining(self) -> int:
        """How many more tool calls the budget allows."""
        return max(0, self.max_tool_calls - self.tool_calls_made)

    @property
    def budget_exhausted(self) -> bool:
        """Whether either budget has run out."""
        return self.iterations_remaining == 0 or self.tool_calls_remaining == 0

    def exhaustion_reason(self) -> HaltReason | None:
        """Which budget ran out, if either has.

        Distinguished rather than collapsed, because they mean different things: out of
        iterations is an ambiguous failure, out of tool calls is an expensive one.
        """
        if self.iterations_remaining == 0:
            return HaltReason.ITERATION_BUDGET_EXHAUSTED
        if self.tool_calls_remaining == 0:
            return HaltReason.TOOL_CALL_BUDGET_EXHAUSTED
        return None

    @property
    def successful_evidence(self) -> list[Evidence]:
        """Evidence from tool calls that actually returned data.

        A tool that failed is still recorded, because knowing the metadata database was
        unreachable matters, but it does not support a conclusion.
        """
        return [item for item in self.evidence if item.succeeded]

    @property
    def refuted_hypotheses(self) -> list[Hypothesis]:
        """Hypotheses that were tested and did not survive."""
        return [
            hypothesis
            for hypothesis in self.hypotheses
            if hypothesis.outcome is HypothesisOutcome.REFUTED
        ]

    @property
    def untested_hypotheses(self) -> list[Hypothesis]:
        """Hypotheses still awaiting their test, best-ranked first."""
        return sorted(
            (
                hypothesis
                for hypothesis in self.hypotheses
                if hypothesis.outcome is HypothesisOutcome.UNTESTED
            ),
            key=lambda hypothesis: hypothesis.rank,
        )

    def next_sequence(self) -> int:
        """The sequence number for the next step in the trace."""
        return len(self.steps) + 1
