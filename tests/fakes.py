"""Fakes shared by the graph and worker suites.

This is the mechanism the build spec asks for: every node reaches a model through one
interface, and here that interface hands back prepared answers. Routing, the loop and
budget enforcement are therefore checked against the real nodes and the real edges, with
only the model and the tools replaced. Nothing here touches a network.
"""

from collections.abc import Mapping, Sequence

from pydantic import BaseModel

from dag_doctor.core.models import NodeName, RootCauseCategory
from dag_doctor.graph.model import ModelCall
from dag_doctor.graph.nodes.conclude import Conclusion
from dag_doctor.graph.nodes.form_hypothesis import HypothesisSet, ProposedHypothesis
from dag_doctor.graph.nodes.gather_evidence import PlannedCall, ToolPlan
from dag_doctor.graph.nodes.test_hypothesis import TestVerdict
from dag_doctor.graph.nodes.triage import TriageAnswer
from dag_doctor.tools.base import BaseTool, ToolInput, ToolOutput


class ScriptedCaller:
    """Returns prepared answers per node, and records what it was asked.

    Exhausting a node's script repeats its last answer, which is what makes a loop easy to
    script without pre-counting iterations. Every prompt is kept so a test can assert what
    a node actually told the model.
    """

    def __init__(
        self,
        script: Mapping[str, Sequence[BaseModel]],
        model_used: str = "scripted-model",
    ) -> None:
        self._script = {node: list(answers) for node, answers in script.items()}
        self._model_used = model_used
        self.calls: list[tuple[str, str]] = []
        self.failures: dict[str, Exception] = {}

    def fail_at(self, node: str, error: Exception) -> None:
        """Make one node's model call raise, standing in for a provider outage."""
        self.failures[node] = error

    def call_count(self, node: str) -> int:
        """How many times a node asked the model."""
        return sum(1 for called, _prompt in self.calls if called == node)

    def prompt_for(self, node: str) -> str:
        """The most recent prompt a node sent."""
        return next(prompt for called, prompt in reversed(self.calls) if called == node)

    async def call[T: BaseModel](
        self, node: NodeName, prompt: str, output_model: type[T]
    ) -> ModelCall[T]:
        self.calls.append((node, prompt))
        if node in self.failures:
            raise self.failures[node]

        answers = self._script.get(node)
        if not answers:
            raise AssertionError(f"the test script has no answer for node {node!r}")
        answer = answers.pop(0) if len(answers) > 1 else answers[0]
        if not isinstance(answer, output_model):
            raise AssertionError(
                f"script for {node!r} returns {type(answer).__name__}, "
                f"but the node asked for {output_model.__name__}"
            )
        return ModelCall(
            value=answer, model_used=self._model_used, prompt_tokens=11, completion_tokens=7
        )


class StubInput(ToolInput):
    """Accepts anything a scripted plan might send."""

    dag_id: str = ""
    task_id: str = ""
    run_id: str = ""
    table: str = ""
    connection: str = ""


class StubOutput(ToolOutput):
    finding: str

    def summarise(self) -> str:
        return self.finding


def make_tool(name: str, finding: str, *, fails: bool = False) -> BaseTool[StubInput, StubOutput]:
    """Build a tool that reports a fixed finding, or refuses to answer."""

    class Stub(BaseTool[StubInput, StubOutput]):
        input_model = StubInput

        async def execute(self, tool_input: StubInput) -> StubOutput:
            if fails:
                raise RuntimeError(f"{name} could not answer")
            return StubOutput(finding=finding)

    Stub.name = name  # type: ignore[misc]
    Stub.description = f"Stub tool {name} used by the graph tests."  # type: ignore[misc]
    return Stub()


def triage_answer(
    category: RootCauseCategory = RootCauseCategory.SCHEMA_DRIFT,
    needs_investigation: bool = True,
) -> TriageAnswer:
    return TriageAnswer(
        category=category,
        rationale="the message names a column that is not there",
        needs_investigation=needs_investigation,
    )


def tool_plan(*tools: str) -> ToolPlan:
    return ToolPlan(
        calls=[PlannedCall(tool=tool, arguments={"dag_id": "d"}, why="because") for tool in tools]
    )


def hypothesis_set(
    statement: str = "orders.customer_id was renamed to customer_uuid upstream",
    test_tool: str | None = "compare_schema_snapshot",
    category: RootCauseCategory = RootCauseCategory.SCHEMA_DRIFT,
    supporting: Sequence[int] = (1,),
) -> HypothesisSet:
    return HypothesisSet(
        hypotheses=[
            ProposedHypothesis(
                statement=statement,
                category=category,
                proposed_test="diff the current orders schema against the stored snapshot",
                test_tool=test_tool,
                test_arguments={"connection": "warehouse", "table": "orders"},
                supporting_evidence=list(supporting),
                responsible_dag_id="schema_drift_orders",
                responsible_task_id="land_raw_orders",
            )
        ]
    )


def verdict(outcome: str, notes: str = "the snapshot diff shows the rename") -> TestVerdict:
    return TestVerdict(outcome=outcome, notes=notes)


def conclusion(
    category: RootCauseCategory = RootCauseCategory.SCHEMA_DRIFT,
) -> Conclusion:
    return Conclusion(
        root_cause_category=category,
        summary="orders.customer_id was renamed to customer_uuid, breaking the aggregate",
        proposed_fix="Select customer_uuid, or restore the old name upstream",
        responsible_dag_id="schema_drift_orders",
        responsible_task_id="land_raw_orders",
        unknowns=["who made the upstream change"],
    )
