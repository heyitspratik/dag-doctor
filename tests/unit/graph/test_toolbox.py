from dag_doctor.core.exceptions import UnknownToolError
from dag_doctor.graph.toolbox import Toolbox
from tests.fakes import make_tool


async def test_a_tool_result_becomes_evidence(toolbox):
    evidence = await toolbox.run("fetch_task_logs", {"dag_id": "d"})

    assert evidence.succeeded is True
    assert evidence.tool_name == "fetch_task_logs"
    assert evidence.tool_input == {"dag_id": "d"}
    assert "UndefinedColumn" in evidence.summary


async def test_a_tool_that_could_not_answer_is_still_evidence(toolbox):
    # Knowing a tool failed is information the investigation should weigh, not an absence.
    evidence = await toolbox.run("profile_table", {})

    assert evidence.succeeded is False
    assert evidence.result["status"] == "error"


async def test_a_tool_the_model_invented_does_not_crash_the_graph(toolbox):
    evidence = await toolbox.run("read_the_engineers_mind", {"x": 1})

    assert evidence.succeeded is False
    assert "no tool named" in evidence.summary
    assert "fetch_task_logs" in evidence.summary


async def test_arguments_that_do_not_fit_are_recorded_as_a_failed_call(toolbox):
    evidence = await toolbox.run("fetch_task_logs", {"nonsense": True})

    assert evidence.succeeded is False
    assert evidence.result["status"] == "invalid_input"


def test_the_catalogue_lists_arguments_so_the_model_does_not_invent_them(toolbox):
    described = toolbox.describe()

    assert "fetch_task_logs:" in described
    assert "arguments: dag_id?: string" in described


def test_a_tool_with_no_arguments_says_so():
    from dag_doctor.tools.base import BaseTool, ToolInput, ToolOutput

    class Empty(ToolInput):
        pass

    class Out(ToolOutput):
        def summarise(self) -> str:
            return "nothing"

    class NoArgs(BaseTool[Empty, Out]):
        name = "no_args"
        description = "Takes nothing."
        input_model = Empty

        async def execute(self, tool_input: Empty) -> Out:
            return Out()

    assert "arguments: none" in Toolbox([NoArgs()]).describe()


def test_only_the_tools_given_are_reachable():
    box = Toolbox([make_tool("only_this", "finding")])

    assert box.names == ["only_this"]
    assert box.require("only_this").name == "only_this"


def test_requiring_a_tool_that_is_not_there_names_the_ones_that_are():
    box = Toolbox([make_tool("only_this", "finding")])

    try:
        box.require("something_else")
    except UnknownToolError as exc:
        assert exc.details["available"] == ["only_this"]
    else:
        raise AssertionError("expected UnknownToolError")


async def test_the_incident_identifiers_are_filled_in_for_the_model(toolbox):
    # Which DAG, task and run failed are properties of the incident, not choices a model
    # should make. Requiring it to copy them into every call adds a failure mode with no
    # upside, and small models get them wrong often enough to lose an investigation.
    context = {"dag_id": "schema_drift_orders", "task_id": "build", "run_id": "manual__1"}

    evidence = await toolbox.run("fetch_task_logs", {}, context)

    assert evidence.succeeded is True
    assert evidence.tool_input == context


async def test_only_arguments_the_tool_declares_are_filled(toolbox):
    context = {"dag_id": "d", "task_id": "t", "run_id": "r", "not_a_parameter": "x"}

    evidence = await toolbox.run("fetch_task_logs", {}, context)

    assert "not_a_parameter" not in evidence.tool_input


async def test_the_model_can_still_point_a_tool_at_a_different_task(toolbox):
    # It has to be able to ask about an upstream task's history, so anything it supplies
    # wins over what the incident says.
    context = {"dag_id": "schema_drift_orders", "task_id": "build_orders_by_customer"}

    evidence = await toolbox.run("fetch_task_logs", {"task_id": "land_raw_orders"}, context)

    assert evidence.tool_input["task_id"] == "land_raw_orders"
    assert evidence.tool_input["dag_id"] == "schema_drift_orders"


async def test_no_context_leaves_the_arguments_exactly_as_given(toolbox):
    evidence = await toolbox.run("fetch_task_logs", {"dag_id": "d"})

    assert evidence.tool_input == {"dag_id": "d"}


async def test_a_rejected_call_says_what_was_wrong_and_what_the_tool_takes(toolbox):
    # The model reads this error and may retry. A count of invalid arguments gives it
    # nothing to correct.
    evidence = await toolbox.run("fetch_task_logs", {"nonsense": True})

    assert evidence.succeeded is False
    assert "nonsense" in evidence.summary
    assert "It takes:" in evidence.summary


async def test_a_null_the_model_could_not_fill_is_treated_as_absent(toolbox):
    # Small models emit null for fields they do not know rather than omitting them. Read
    # literally that is a value, and it fails validation on every required argument, which
    # cost a whole investigation before it was noticed.
    context = {"dag_id": "schema_drift_orders", "task_id": "build", "run_id": "manual__1"}

    evidence = await toolbox.run(
        "fetch_task_logs", {"dag_id": None, "task_id": None, "run_id": None}, context
    )

    assert evidence.succeeded is True
    assert evidence.tool_input == context


async def test_an_empty_string_is_treated_as_absent_too(toolbox):
    evidence = await toolbox.run(
        "get_dag_run_history", {"dag_id": ""}, {"dag_id": "schema_drift_orders"}
    )

    assert evidence.tool_input["dag_id"] == "schema_drift_orders"


async def test_a_null_optional_argument_falls_back_to_its_default(toolbox):
    # An optional as_of set to null is a datetime validation error rather than "use the
    # most recent snapshot".
    evidence = await toolbox.run(
        "compare_schema_snapshot",
        {"connection": "warehouse", "table": "orders", "as_of": None},
    )

    assert "as_of" not in evidence.tool_input
    assert evidence.succeeded is True
