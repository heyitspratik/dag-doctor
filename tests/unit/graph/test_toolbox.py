from dag_doctor.core.exceptions import UnknownToolError
from dag_doctor.graph.toolbox import Toolbox

from .conftest import make_tool


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
