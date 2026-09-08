"""Concluding: write the diagnosis, and score it in code.

The model writes the prose. The confidence is computed from the shape of the
investigation, and the prompt tells the model not to supply one, because a number it
invents would be uncalibrated and would look exactly as authoritative as a real one.
"""

from typing import ClassVar

from pydantic import BaseModel, Field

from dag_doctor.core.models import Diagnosis, HaltReason, NodeName, RootCauseCategory
from dag_doctor.graph import prompts
from dag_doctor.graph.confidence import compute_confidence
from dag_doctor.graph.model import ModelCaller
from dag_doctor.graph.nodes.base import InvestigationNode, NodeUpdate
from dag_doctor.graph.state import InvestigationState


class Conclusion(BaseModel):
    """What the model is asked for at the end."""

    root_cause_category: RootCauseCategory
    summary: str = Field(min_length=1)
    proposed_fix: str | None = None
    responsible_dag_id: str | None = None
    responsible_task_id: str | None = None
    unknowns: list[str] = Field(default_factory=list)


class ConcludeNode(InvestigationNode):
    """Produce the final diagnosis."""

    node: ClassVar[NodeName] = "conclude"

    def __init__(self, caller: ModelCaller) -> None:
        """Initialise the node.

        Args:
            caller: How this node reaches a model.
        """
        self._caller = caller

    async def run(self, state: InvestigationState) -> NodeUpdate:
        """Write the diagnosis and attach a confidence computed from the evidence."""
        hypothesis = state.current_hypothesis
        prompt = prompts.render(
            "conclude",
            dag_id=state.failure.dag_id,
            task_id=state.failure.task_id,
            exception_type=state.failure.exception_type or "(none recorded)",
            exception_message=state.failure.exception_message or "(none recorded)",
            hypothesis=hypothesis.statement if hypothesis else "none formed; triage alone",
            outcome=hypothesis.outcome.value if hypothesis else "untested",
            test_notes=hypothesis.test_notes or "" if hypothesis else "",
            evidence=_render_evidence(state),
        )
        answer = await self._caller.call(self.node, prompt, Conclusion)
        breakdown = compute_confidence(state)

        diagnosis = Diagnosis(
            incident_id=state.incident_id,
            root_cause_category=answer.value.root_cause_category,
            summary=answer.value.summary,
            confidence=breakdown.total,
            halt_reason=HaltReason.CONCLUDED,
            evidence_chain=[item.id for item in state.successful_evidence],
            proposed_fix=answer.value.proposed_fix,
            responsible_dag_id=answer.value.responsible_dag_id
            or (hypothesis.responsible_dag_id if hypothesis else None),
            responsible_task_id=answer.value.responsible_task_id
            or (hypothesis.responsible_task_id if hypothesis else None),
            unknowns=answer.value.unknowns,
            model_used=answer.model_used,
        )
        return NodeUpdate(
            updates={
                "diagnosis": diagnosis,
                "halt_reason": HaltReason.CONCLUDED,
                "unknowns": answer.value.unknowns,
            },
            step_input={"evidence_items": len(state.successful_evidence)},
            step_output={
                "category": diagnosis.root_cause_category.value,
                "confidence": diagnosis.confidence,
                "confidence_breakdown": breakdown.explain(),
            },
            model_used=answer.model_used,
            prompt_tokens=answer.prompt_tokens,
            completion_tokens=answer.completion_tokens,
        )


def _render_evidence(state: InvestigationState) -> str:
    """Number the evidence that actually returned something."""
    items = state.successful_evidence
    if not items:
        return "  (no tool returned usable evidence)"
    return "\n".join(
        f"  {index}. [{item.tool_name}] {item.summary}" for index, item in enumerate(items, start=1)
    )
