"""Triage: what can be said before spending anything.

Cheap, no tools, one small model call. Its job is to decide whether the investigation is
needed at all, and the bar for saying no is deliberately high: two independent things must
agree, a failure pattern the code already recognises and a model that classifies it the
same way. Either alone is not enough to skip an investigation.
"""

from typing import ClassVar

from pydantic import BaseModel, Field

from dag_doctor.core.models import NodeName, RootCauseCategory
from dag_doctor.graph import prompts
from dag_doctor.graph.confidence import match_signature, triage_confidence
from dag_doctor.graph.model import ModelCaller
from dag_doctor.graph.nodes.base import InvestigationNode, NodeUpdate
from dag_doctor.graph.state import InvestigationState, TriageResult


class TriageAnswer(BaseModel):
    """What the model is asked for at triage."""

    category: RootCauseCategory
    rationale: str = Field(min_length=1)
    needs_investigation: bool = True


class TriageNode(InvestigationNode):
    """Classify the failure from the event alone."""

    node: ClassVar[NodeName] = "triage"

    def __init__(self, caller: ModelCaller) -> None:
        """Initialise the node.

        Args:
            caller: How this node reaches a model.
        """
        self._caller = caller

    async def run(self, state: InvestigationState) -> NodeUpdate:
        """Classify the failure and score how far that classification can be trusted."""
        failure = state.failure
        signature = match_signature(failure.exception_type, failure.exception_message)
        hint = (
            f"A known failure pattern matched: {signature.name}, which usually means "
            f"{signature.category.value}. Treat this as a strong prior, not a conclusion."
            if signature
            else "No known failure pattern matched this message."
        )

        prompt = prompts.render(
            "triage",
            dag_id=failure.dag_id,
            task_id=failure.task_id,
            run_id=failure.run_id,
            try_number=failure.try_number,
            exception_type=failure.exception_type or "(none recorded)",
            exception_message=failure.exception_message or "(none recorded)",
            signature_hint=hint,
            categories="\n".join(f"  - {member.value}" for member in RootCauseCategory),
        )
        answer = await self._caller.call(self.node, prompt, TriageAnswer)

        agrees = signature is not None and answer.value.category is signature.category
        confidence = triage_confidence(signature, model_agrees=agrees)
        triage = TriageResult(
            category=answer.value.category,
            confidence=confidence,
            rationale=answer.value.rationale,
            matched_signature=signature.name if signature else None,
            # A model that wants to stop still cannot, unless the code agrees the pattern
            # is decisive. The shortcut is guarded on both sides.
            needs_investigation=answer.value.needs_investigation or not agrees,
        )
        return NodeUpdate(
            updates={"triage": triage},
            step_input={
                "exception_type": failure.exception_type,
                "signature": triage.matched_signature,
            },
            step_output={
                "category": triage.category.value,
                "confidence": triage.confidence,
                "needs_investigation": triage.needs_investigation,
            },
            model_used=answer.model_used,
            prompt_tokens=answer.prompt_tokens,
            completion_tokens=answer.completion_tokens,
        )
