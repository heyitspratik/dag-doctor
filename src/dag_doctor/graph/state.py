"""The state carried through the investigation.

Two things here are load-bearing rather than bookkeeping.

The budgets are hard limits, and hitting one is a legitimate terminal state producing an
inconclusive diagnosis with whatever evidence was gathered. An agent without a budget is
an agent that loops forever on an ambiguous failure and bills for the privilege.

The step trace records every node execution with its inputs, outputs and cost. It is what
turns the agent from a black box into something a human can audit, and it is the reason
the investigation is worth trusting at all.
"""

import json
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

    #: Set when a gather round produced no evidence the investigation did not already
    #: hold. Running out of information is a different condition from running out of
    #: budget, and only this one means another round would return the same answers.
    evidence_exhausted: bool = False

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
    def known_task_ids(self) -> set[str]:
        """Task ids named outright by the upstream state tool, for prompting.

        A hint rather than the rule: it is the set a conclusion can be steered towards,
        but a task learned from the DAG source instead is just as real, so enforcement
        uses :meth:`has_seen_task`.
        """
        found = {self.failure.task_id}
        for item in self.evidence:
            upstream = item.result.get("upstream")
            if isinstance(upstream, list):
                for entry in upstream:
                    if not isinstance(entry, dict):
                        continue
                    task_id = entry.get("task_id")
                    if isinstance(task_id, str):
                        found.add(task_id)
            root_failure = item.result.get("root_failure")
            if isinstance(root_failure, str):
                found.add(root_failure)
        return found

    def has_seen_task(self, task_id: str) -> bool:
        """Whether any evidence actually mentions a task by this name.

        Attribution is only meaningful against a task that exists. Asked to name the task
        responsible, a model will otherwise answer with whatever string is to hand: one
        live run blamed the tool ``fetch_task_logs``.

        The test is deliberately loose. Narrowing it to tasks the upstream tool listed
        would reject a correct answer read out of the DAG source, and discarding a right
        answer is worse than the fabrication this guards against.

        Only what a tool reported is searched, never the envelope around it. The envelope
        carries the tool's own name, so searching it would accept ``fetch_task_logs`` as a
        task and defeat the entire check.

        Args:
            task_id: The task name the model proposed.

        Returns:
            Whether the investigation saw that name anywhere it looked.
        """
        if task_id == self.failure.task_id:
            return True
        return any(task_id in _reported(item) for item in self.evidence)

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

    def tool_context(self) -> dict[str, JsonValue]:
        """The facts every tool call about this incident should start from.

        Handed to the toolbox so a model does not have to copy the identifiers into each
        call. They describe the incident rather than anything the model decides, and a
        model that wants a different task can still say so.
        """
        return {
            "dag_id": self.failure.dag_id,
            "task_id": self.failure.task_id,
            "run_id": self.failure.run_id,
            "try_number": self.failure.try_number,
        }

    def next_sequence(self) -> int:
        """The sequence number for the next step in the trace."""
        return len(self.steps) + 1


def _reported(item: Evidence) -> str:
    """What one tool actually said, without the envelope that names the tool.

    The payload wraps every result in ``tool``, ``status`` and ``error`` keys. Searching
    those for a task name would match the tool's own name, which is exactly the mistake
    this is here to catch.
    """
    return f"{item.summary}\n{json.dumps(item.result.get('data'), default=str)}"
