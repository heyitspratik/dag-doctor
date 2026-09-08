"""The whole graph, driven by a scripted model.

This is the most important test file in the repository. It runs the real nodes over the
real edges with only the model and the tools replaced, so routing, the loop, budget
exhaustion and the step trace are all verified without a network call, an API key, or a
running Ollama.
"""

from uuid import uuid4

import pytest

from dag_doctor.core.exceptions import ProviderUnavailableError
from dag_doctor.core.models import HaltReason, HypothesisOutcome, RootCauseCategory
from dag_doctor.core.settings import BudgetSettings
from dag_doctor.graph.builder import build_graph, initial_state
from dag_doctor.graph.state import InvestigationState

from .conftest import (
    ScriptedCaller,
    conclusion,
    hypothesis_set,
    tool_plan,
    triage_answer,
    verdict,
)


async def _run(caller, toolbox, budgets, failure) -> InvestigationState:
    graph = build_graph(caller, toolbox, budgets)
    result = await graph.ainvoke(initial_state(uuid4(), failure, budgets))
    return InvestigationState.model_validate(result)


def _full_script(*, outcome: str = "confirmed") -> dict[str, list]:
    return {
        "triage": [triage_answer()],
        "gather_evidence": [tool_plan("fetch_task_logs", "compare_schema_snapshot")],
        "form_hypothesis": [hypothesis_set()],
        "test_hypothesis": [verdict(outcome)],
        "conclude": [conclusion()],
    }


async def test_an_unmistakable_failure_concludes_straight_from_triage(
    toolbox, budgets, drift_failure
):
    caller = ScriptedCaller(
        {
            "triage": [triage_answer(needs_investigation=False)],
            "conclude": [conclusion()],
        }
    )

    state = await _run(caller, toolbox, budgets, drift_failure)

    assert state.diagnosis is not None
    assert state.diagnosis.is_conclusive
    assert caller.call_count("gather_evidence") == 0
    assert [step.node for step in state.steps] == ["triage", "conclude"]


async def test_the_ordinary_path_gathers_forms_tests_and_concludes(toolbox, budgets, drift_failure):
    caller = ScriptedCaller(_full_script())

    state = await _run(caller, toolbox, budgets, drift_failure)

    assert [step.node for step in state.steps] == [
        "triage",
        "gather_evidence",
        "form_hypothesis",
        "test_hypothesis",
        "conclude",
    ]
    assert state.diagnosis is not None
    assert state.diagnosis.root_cause_category is RootCauseCategory.SCHEMA_DRIFT
    assert state.diagnosis.proposed_fix is not None
    assert state.diagnosis.responsible_task_id == "land_raw_orders"


async def test_a_refuted_hypothesis_sends_the_investigation_round_again(
    toolbox, budgets, drift_failure
):
    # The loop, end to end. A chain could not express this, which is the honest
    # justification for building the investigation as a state machine.
    caller = ScriptedCaller(
        {
            "triage": [triage_answer()],
            "gather_evidence": [tool_plan("fetch_task_logs"), tool_plan("get_dag_run_history")],
            "form_hypothesis": [
                hypothesis_set(statement="the warehouse was unreachable"),
                hypothesis_set(),
            ],
            "test_hypothesis": [verdict("refuted"), verdict("confirmed")],
            "conclude": [conclusion()],
        }
    )

    state = await _run(caller, toolbox, budgets, drift_failure)

    assert caller.call_count("gather_evidence") == 2
    assert [step.node for step in state.steps] == [
        "triage",
        "gather_evidence",
        "form_hypothesis",
        "test_hypothesis",
        "gather_evidence",
        "form_hypothesis",
        "test_hypothesis",
        "conclude",
    ]
    assert len(state.refuted_hypotheses) == 1
    assert state.diagnosis is not None
    assert state.diagnosis.is_conclusive


async def test_a_refuted_hypothesis_is_not_proposed_again(toolbox, budgets, drift_failure):
    caller = ScriptedCaller(
        {
            "triage": [triage_answer()],
            "gather_evidence": [tool_plan("fetch_task_logs")],
            "form_hypothesis": [
                hypothesis_set(statement="the warehouse was unreachable"),
                hypothesis_set(),
            ],
            "test_hypothesis": [verdict("refuted"), verdict("confirmed")],
            "conclude": [conclusion()],
        }
    )

    await _run(caller, toolbox, budgets, drift_failure)

    assert "the warehouse was unreachable" in caller.prompt_for("form_hypothesis")


async def test_running_out_of_iterations_produces_an_inconclusive_diagnosis(toolbox, drift_failure):
    # A budget that runs out is an ordinary outcome recorded honestly, not a crash and
    # not a guess dressed up as an answer.
    budgets = BudgetSettings(max_iterations=2)
    caller = ScriptedCaller(
        {
            "triage": [triage_answer()],
            "gather_evidence": [tool_plan("fetch_task_logs")],
            "form_hypothesis": [hypothesis_set()],
            "test_hypothesis": [verdict("refuted")],
        }
    )

    state = await _run(caller, toolbox, budgets, drift_failure)

    assert state.halt_reason is HaltReason.ITERATION_BUDGET_EXHAUSTED
    assert state.diagnosis is not None
    assert not state.diagnosis.is_conclusive
    assert state.diagnosis.root_cause_category is RootCauseCategory.UNKNOWN
    assert caller.call_count("conclude") == 0


async def test_running_out_of_tool_calls_produces_an_inconclusive_diagnosis(toolbox, drift_failure):
    budgets = BudgetSettings(max_tool_calls=2)
    caller = ScriptedCaller(
        {
            "triage": [triage_answer()],
            "gather_evidence": [tool_plan("fetch_task_logs", "get_dag_run_history")],
            "form_hypothesis": [hypothesis_set()],
            "test_hypothesis": [verdict("refuted")],
        }
    )

    state = await _run(caller, toolbox, budgets, drift_failure)

    assert state.halt_reason is HaltReason.TOOL_CALL_BUDGET_EXHAUSTED
    assert state.diagnosis is not None
    assert not state.diagnosis.is_conclusive


async def test_an_exhausted_budget_says_which_knob_to_turn(toolbox, drift_failure):
    budgets = BudgetSettings(max_iterations=2)
    caller = ScriptedCaller(
        {
            "triage": [triage_answer()],
            "gather_evidence": [tool_plan("fetch_task_logs")],
            "form_hypothesis": [hypothesis_set()],
            "test_hypothesis": [verdict("refuted")],
        }
    )

    state = await _run(caller, toolbox, budgets, drift_failure)

    assert any("MAX_ITERATIONS" in unknown for unknown in state.diagnosis.unknowns)


async def test_an_inconclusive_diagnosis_still_reports_what_was_found(toolbox, drift_failure):
    budgets = BudgetSettings(max_iterations=2)
    caller = ScriptedCaller(
        {
            "triage": [triage_answer()],
            "gather_evidence": [tool_plan("fetch_task_logs")],
            "form_hypothesis": [hypothesis_set(statement="a column was renamed")],
            "test_hypothesis": [verdict("refuted")],
        }
    )

    state = await _run(caller, toolbox, budgets, drift_failure)

    assert "Ruled out: a column was renamed" in state.diagnosis.summary
    assert state.diagnosis.evidence_chain


async def test_forming_no_hypothesis_escalates_rather_than_looping(toolbox, budgets, drift_failure):
    from dag_doctor.graph.nodes.form_hypothesis import HypothesisSet

    caller = ScriptedCaller(
        {
            "triage": [triage_answer()],
            "gather_evidence": [tool_plan("fetch_task_logs")],
            "form_hypothesis": [HypothesisSet(hypotheses=[])],
        }
    )

    state = await _run(caller, toolbox, budgets, drift_failure)

    assert state.halt_reason is HaltReason.NO_HYPOTHESIS_FORMED
    assert state.diagnosis is not None
    assert not state.diagnosis.is_conclusive


async def test_a_provider_outage_halts_rather_than_stranding_the_incident(
    toolbox, budgets, drift_failure
):
    # An exception escaping the graph would leave the incident stuck in investigating
    # with nothing to show. A halt produces a diagnosis saying what went wrong.
    caller = ScriptedCaller(_full_script())
    caller.fail_at("form_hypothesis", ProviderUnavailableError("ollama is down"))

    state = await _run(caller, toolbox, budgets, drift_failure)

    assert state.halt_reason is HaltReason.INVESTIGATION_ERROR
    assert state.diagnosis is not None
    assert not state.diagnosis.is_conclusive
    assert any("internal error" in unknown for unknown in state.diagnosis.unknowns)


async def test_a_tool_that_cannot_answer_is_recorded_rather_than_fatal(
    toolbox, budgets, drift_failure
):
    caller = ScriptedCaller(
        {
            **_full_script(),
            "gather_evidence": [tool_plan("profile_table", "fetch_task_logs")],
        }
    )

    state = await _run(caller, toolbox, budgets, drift_failure)

    assert [item.succeeded for item in state.evidence[:2]] == [False, True]
    assert state.diagnosis is not None
    assert state.diagnosis.is_conclusive


async def test_a_tool_the_model_invented_is_recorded_rather_than_fatal(
    toolbox, budgets, drift_failure
):
    caller = ScriptedCaller(
        {**_full_script(), "gather_evidence": [tool_plan("read_the_engineers_mind")]}
    )

    state = await _run(caller, toolbox, budgets, drift_failure)

    assert state.evidence[0].succeeded is False
    assert "no tool named" in state.evidence[0].summary
    assert state.diagnosis is not None


async def test_more_calls_than_the_budget_allows_are_not_made(toolbox, drift_failure):
    budgets = BudgetSettings(max_tool_calls=2)
    caller = ScriptedCaller(
        {
            "triage": [triage_answer()],
            "gather_evidence": [
                tool_plan(
                    "fetch_task_logs",
                    "get_dag_run_history",
                    "check_connection_health",
                    "compare_schema_snapshot",
                )
            ],
            "form_hypothesis": [hypothesis_set()],
            "test_hypothesis": [verdict("refuted")],
        }
    )

    state = await _run(caller, toolbox, budgets, drift_failure)

    assert state.tool_calls_made == 2


async def test_every_node_leaves_a_step_in_the_trace(toolbox, budgets, drift_failure):
    # The trace is what turns the agent from a black box into something auditable, and
    # it is the endpoint worth demonstrating.
    caller = ScriptedCaller(_full_script())

    state = await _run(caller, toolbox, budgets, drift_failure)

    assert [step.sequence for step in state.steps] == [1, 2, 3, 4, 5]
    assert all(step.duration_ms >= 0 for step in state.steps)
    assert all(step.model_used == "scripted-model" for step in state.steps[:-1])


async def test_the_trace_records_what_each_node_decided(toolbox, budgets, drift_failure):
    caller = ScriptedCaller(_full_script())

    state = await _run(caller, toolbox, budgets, drift_failure)

    by_node = {step.node: step for step in state.steps}
    assert by_node["gather_evidence"].output["ran"] == [
        "fetch_task_logs",
        "compare_schema_snapshot",
    ]
    assert by_node["test_hypothesis"].output["outcome"] == "confirmed"
    assert "signature" in by_node["conclude"].output["confidence_breakdown"]


async def test_token_spend_is_recorded_per_node(toolbox, budgets, drift_failure):
    caller = ScriptedCaller(_full_script())

    state = await _run(caller, toolbox, budgets, drift_failure)

    model_steps = [step for step in state.steps if step.model_used]
    assert all(step.prompt_tokens == 11 for step in model_steps)


async def test_the_hypothesis_is_marked_tested_in_the_record(toolbox, budgets, drift_failure):
    caller = ScriptedCaller(_full_script())

    state = await _run(caller, toolbox, budgets, drift_failure)

    assert state.hypotheses[0].outcome is HypothesisOutcome.CONFIRMED
    assert state.hypotheses[0].test_notes


@pytest.mark.parametrize("outcome", ["refuted", "inconclusive"])
async def test_a_hypothesis_that_did_not_survive_never_reaches_a_conclusion(
    toolbox, drift_failure, outcome
):
    budgets = BudgetSettings(max_iterations=1)
    caller = ScriptedCaller(
        {
            "triage": [triage_answer()],
            "gather_evidence": [tool_plan("fetch_task_logs")],
            "form_hypothesis": [hypothesis_set()],
            "test_hypothesis": [verdict(outcome)],
        }
    )

    state = await _run(caller, toolbox, budgets, drift_failure)

    assert caller.call_count("conclude") == 0
    assert not state.diagnosis.is_conclusive
