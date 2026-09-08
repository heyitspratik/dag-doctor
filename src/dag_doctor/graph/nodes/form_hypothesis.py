"""Forming hypotheses: turn evidence into candidate causes that could be wrong.

Every hypothesis must carry a test, and the test is a tool call rather than a sentence.
That constraint is what stops this node producing a confident narrative: if there is no
call whose result could contradict the story, the story is not a hypothesis.

Hypotheses already refuted are named in the prompt so the investigation does not circle
back to them, which is the failure mode a loop invites.
"""

from typing import ClassVar

from pydantic import BaseModel, Field, JsonValue

from dag_doctor.core.models import Hypothesis, NodeName, RootCauseCategory
from dag_doctor.graph import prompts
from dag_doctor.graph.model import ModelCaller
from dag_doctor.graph.nodes.base import InvestigationNode, NodeUpdate
from dag_doctor.graph.state import InvestigationState
from dag_doctor.graph.toolbox import Toolbox

MAX_HYPOTHESES = 3


class ProposedHypothesis(BaseModel):
    """One candidate cause, as the model proposes it."""

    statement: str = Field(min_length=1)
    category: RootCauseCategory
    proposed_test: str = Field(min_length=1)
    test_tool: str | None = None
    test_arguments: dict[str, JsonValue] = Field(default_factory=dict)
    #: Evidence numbers as shown in the prompt, one-based. Numbers rather than identifiers
    #: because a model asked for a UUID will cheerfully invent one.
    supporting_evidence: list[int] = Field(default_factory=list)
    responsible_dag_id: str | None = None
    responsible_task_id: str | None = None


class HypothesisSet(BaseModel):
    """What the model is asked for when forming hypotheses."""

    hypotheses: list[ProposedHypothesis] = Field(default_factory=list)


class FormHypothesisNode(InvestigationNode):
    """Propose ranked root causes with falsifiable tests."""

    node: ClassVar[NodeName] = "form_hypothesis"

    def __init__(self, caller: ModelCaller, toolbox: Toolbox) -> None:
        """Initialise the node.

        Args:
            caller: How this node reaches a model.
            toolbox: The tools a proposed test may name.
        """
        self._caller = caller
        self._toolbox = toolbox

    async def run(self, state: InvestigationState) -> NodeUpdate:
        """Propose hypotheses and select the best untested one to test next."""
        prompt = prompts.render(
            "form_hypothesis",
            dag_id=state.failure.dag_id,
            task_id=state.failure.task_id,
            exception_type=state.failure.exception_type or "(none recorded)",
            exception_message=state.failure.exception_message or "(none recorded)",
            evidence=_render_evidence(state),
            refuted=_render_refuted(state),
            tools=self._toolbox.describe(),
            max_hypotheses=MAX_HYPOTHESES,
            categories="\n".join(f"  - {member.value}" for member in RootCauseCategory),
        )
        answer = await self._caller.call(self.node, prompt, HypothesisSet)

        existing = len(state.hypotheses)
        proposed = [
            _to_hypothesis(candidate, state, rank=existing + offset)
            for offset, candidate in enumerate(answer.value.hypotheses[:MAX_HYPOTHESES])
        ]
        hypotheses = [*state.hypotheses, *proposed]
        current = min(proposed, key=lambda item: item.rank) if proposed else None

        return NodeUpdate(
            updates={
                "hypotheses": hypotheses,
                "current_hypothesis": current,
            },
            step_input={
                "evidence_items": len(state.evidence),
                "already_refuted": len(state.refuted_hypotheses),
            },
            step_output={
                "proposed": [item.statement for item in proposed],
                "testing": current.statement if current else None,
            },
            model_used=answer.model_used,
            prompt_tokens=answer.prompt_tokens,
            completion_tokens=answer.completion_tokens,
        )


def _to_hypothesis(
    candidate: ProposedHypothesis, state: InvestigationState, rank: int
) -> Hypothesis:
    """Convert a proposal into a hypothesis, resolving evidence numbers to identifiers.

    Numbers outside the range are dropped rather than rejected. A model miscounting its
    citations is not a reason to discard an otherwise sound hypothesis, and the count of
    genuinely supporting evidence feeds the confidence score anyway.
    """
    supporting = [
        state.evidence[number - 1].id
        for number in candidate.supporting_evidence
        if 1 <= number <= len(state.evidence)
    ]
    return Hypothesis(
        statement=candidate.statement,
        root_cause_category=candidate.category,
        proposed_test=candidate.proposed_test,
        test_tool=candidate.test_tool,
        test_arguments=candidate.test_arguments,
        rank=rank,
        supporting_evidence_ids=supporting,
        responsible_dag_id=candidate.responsible_dag_id,
        responsible_task_id=candidate.responsible_task_id,
    )


def _render_evidence(state: InvestigationState) -> str:
    """Number the evidence so hypotheses can cite it."""
    if not state.evidence:
        return "  (nothing gathered yet)"
    return "\n".join(
        f"  {index}. [{item.tool_name}] {item.summary}"
        for index, item in enumerate(state.evidence, start=1)
    )


def _render_refuted(state: InvestigationState) -> str:
    """List what has already been ruled out."""
    refuted = state.refuted_hypotheses
    if not refuted:
        return "  (none yet)"
    return "\n".join(
        f"  - {item.statement} ({item.test_notes or 'refuted by its test'})" for item in refuted
    )
