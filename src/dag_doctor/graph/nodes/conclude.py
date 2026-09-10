"""Concluding: write the diagnosis, and score it in code.

The model writes the prose. The confidence is computed from the shape of the
investigation, and the prompt tells the model not to supply one, because a number it
invents would be uncalibrated and would look exactly as authoritative as a real one.
"""

from typing import ClassVar

from pydantic import BaseModel, Field, JsonValue

from dag_doctor.core.logging import get_logger
from dag_doctor.core.models import (
    Diagnosis,
    HaltReason,
    Hypothesis,
    NodeName,
    RootCauseCategory,
)
from dag_doctor.graph import prompts
from dag_doctor.graph.confidence import compute_confidence
from dag_doctor.graph.model import ModelCaller
from dag_doctor.graph.nodes.base import InvestigationNode, NodeUpdate
from dag_doctor.graph.state import InvestigationState

logger = get_logger(__name__)


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
            known_tasks=_render_tasks(state),
        )
        candidates: list[JsonValue] = [*sorted(state.known_task_ids)]
        answer = await self._caller.call(self.node, prompt, Conclusion)
        breakdown = compute_confidence(state)
        responsible_task_id = self._attributed_task(answer.value, hypothesis, state)

        diagnosis = Diagnosis(
            incident_id=state.incident_id,
            root_cause_category=answer.value.root_cause_category,
            summary=answer.value.summary,
            confidence=breakdown.total,
            halt_reason=HaltReason.CONCLUDED,
            evidence_chain=[item.id for item in state.successful_evidence],
            proposed_fix=answer.value.proposed_fix,
            # Attribution is published as a pair or not at all. A task named without its
            # DAG cannot be looked up, so the incident supplies the DAG rather than the
            # model guessing at it; and a DAG named alongside a task that was rejected as
            # invented is no more trustworthy than the task was.
            responsible_dag_id=(
                answer.value.responsible_dag_id
                or (hypothesis.responsible_dag_id if hypothesis else None)
                or state.failure.dag_id
            )
            if responsible_task_id
            else None,
            responsible_task_id=responsible_task_id,
            unknowns=answer.value.unknowns,
            model_used=answer.model_used,
        )
        return NodeUpdate(
            updates={
                "diagnosis": diagnosis,
                "halt_reason": HaltReason.CONCLUDED,
                "unknowns": answer.value.unknowns,
            },
            step_input={
                "evidence_items": len(state.successful_evidence),
                "candidate_tasks": candidates,
            },
            step_output={
                "category": diagnosis.root_cause_category.value,
                "confidence": diagnosis.confidence,
                "confidence_breakdown": breakdown.explain(),
            },
            model_used=answer.model_used,
            prompt_tokens=answer.prompt_tokens,
            completion_tokens=answer.completion_tokens,
        )

    def _attributed_task(
        self,
        answer: Conclusion,
        hypothesis: Hypothesis | None,
        state: InvestigationState,
    ) -> str | None:
        """The responsible task, but only if it is a task that exists.

        Blaming a task nobody can open is worse than blaming nothing: it reads as a
        finding, and a reader has to go and disprove it. Dropped rather than corrected,
        because there is no honest way to guess which task was meant.

        Args:
            answer: What the model wrote.
            hypothesis: The leading hypothesis, which may carry its own attribution.
            state: The investigation, for the tasks it has seen.

        Returns:
            The responsible task id, or None if none was named or the name was not real.
        """
        proposed = answer.responsible_task_id or (
            hypothesis.responsible_task_id if hypothesis else None
        )
        if proposed is None or state.has_seen_task(proposed):
            return proposed
        logger.warning(
            "conclude.responsible_task_unknown",
            proposed=proposed,
            incident_id=str(state.incident_id),
            candidates=sorted(state.known_task_ids),
        )
        return None


def _render_tasks(state: InvestigationState) -> str:
    """List the tasks an attribution may name."""
    return "\n".join(f"  - {task_id}" for task_id in sorted(state.known_task_ids))


def _render_evidence(state: InvestigationState) -> str:
    """Number the evidence that actually returned something."""
    items = state.successful_evidence
    if not items:
        return "  (no tool returned usable evidence)"
    return "\n".join(
        f"  {index}. [{item.tool_name}] {item.summary}" for index, item in enumerate(items, start=1)
    )
