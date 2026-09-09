"""The prompt templates.

Prompts are the part of an agent most likely to rot silently: a renamed variable produces
a prompt with a hole in it, and the model answers anyway.
"""

import pytest

from dag_doctor.graph import prompts

EXPECTED = {
    "triage",
    "gather_evidence",
    "form_hypothesis",
    "test_hypothesis",
    "conclude",
    "tool_arguments",
}


def test_every_node_that_calls_a_model_has_a_template():
    assert set(prompts.names()) == EXPECTED


@pytest.mark.parametrize("name", sorted(EXPECTED))
def test_a_template_can_be_loaded(name):
    assert prompts.load(name).strip()


def test_a_missing_substitution_raises_rather_than_rendering_a_hole():
    with pytest.raises(KeyError):
        prompts.render("triage", dag_id="d")


def test_templates_are_versioned_so_a_trace_can_say_which_wording_ran():
    assert prompts.PROMPT_VERSION == "v3"


def test_a_template_that_does_not_exist_is_a_packaging_error():
    with pytest.raises(FileNotFoundError):
        prompts.load("does_not_exist")


def test_the_conclude_prompt_refuses_to_let_the_model_score_itself():
    # Confidence is computed from the shape of the investigation. A number the model
    # supplied would look exactly as authoritative and would not be calibrated.
    assert "Do not state a confidence level" in prompts.load("conclude")


def test_the_hypothesis_prompt_demands_a_falsifiable_test():
    assert "refute" in prompts.load("form_hypothesis")


def test_the_hypothesis_prompt_warns_against_blaming_the_visible_task():
    assert "upstream" in prompts.load("form_hypothesis")


@pytest.mark.parametrize("name", ["gather_evidence", "form_hypothesis"])
def test_the_planning_prompts_ask_for_tool_names_not_arguments(name):
    # Asking for arguments against a free-form object gives a model no schema to follow,
    # and a small one answers by inventing a shape. Arguments are asked for separately,
    # against each tool's real model.
    assert "name the tool" in prompts.load(name).lower()


def test_the_argument_prompt_names_the_connections_that_exist():
    # A model that invents a connection name gets a refusal and wastes a call.
    assert "$connections" in prompts.load("tool_arguments")
