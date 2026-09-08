import asyncio
from typing import ClassVar

import pytest

from dag_doctor.core.exceptions import PersistenceError, ReadOnlyViolationError, UnknownToolError
from dag_doctor.tools import registry
from dag_doctor.tools.base import (
    BaseTool,
    TargetNotFoundError,
    ToolInput,
    ToolOutput,
    ToolStatus,
)


class EchoInput(ToolInput):
    text: str
    times: int = 1


class EchoOutput(ToolOutput):
    said: str

    def summarise(self) -> str:
        return f"said {self.said!r}"


class EchoTool(BaseTool[EchoInput, EchoOutput]):
    name: ClassVar[str] = "echo"
    description: ClassVar[str] = "Repeats its input."
    input_model = EchoInput

    def __init__(self, timeout_s: float = 5.0, raises: Exception | None = None) -> None:
        super().__init__(timeout_s)
        self._raises = raises

    async def execute(self, tool_input: EchoInput) -> EchoOutput:
        if self._raises is not None:
            raise self._raises
        return EchoOutput(said=tool_input.text * tool_input.times)


class SlowTool(EchoTool):
    name: ClassVar[str] = "slow"

    async def execute(self, tool_input: EchoInput) -> EchoOutput:
        await asyncio.sleep(10)
        return EchoOutput(said="never")


@pytest.fixture(autouse=True)
def _empty_registry():
    registry.clear()
    yield
    registry.clear()


async def test_a_valid_call_returns_data_and_a_summary():
    result = await EchoTool().run({"text": "ab", "times": 2})

    assert result.ok
    assert result.status is ToolStatus.OK
    assert result.summarise() == "said 'abab'"


async def test_the_result_serialises_to_plain_json_for_the_evidence_record():
    payload = (await EchoTool().run({"text": "x"})).payload()

    assert payload["tool"] == "echo"
    assert payload["status"] == "ok"


@pytest.mark.parametrize(
    "raw_input",
    [{}, {"text": 1}, {"times": 2}, {"text": "x", "surprise": True}],
)
async def test_arguments_that_do_not_fit_the_schema_are_refused(raw_input):
    # The arguments come from a language model. Accepting a field the tool does not read
    # would turn a hallucinated parameter into a query answering a different question.
    result = await EchoTool().run(raw_input)

    assert result.status is ToolStatus.INVALID_INPUT
    assert result.data is None


async def test_a_hanging_tool_is_abandoned_rather_than_consuming_the_budget():
    result = await SlowTool(timeout_s=0.01).run({"text": "x"})

    assert result.status is ToolStatus.TIMEOUT
    assert "timed out" in (result.error or "")


async def test_a_missing_target_is_a_finding_not_a_fault():
    result = await EchoTool(raises=TargetNotFoundError("no such table")).run({"text": "x"})

    assert result.status is ToolStatus.NOT_FOUND


async def test_a_refused_connection_surfaces_as_forbidden():
    result = await EchoTool(raises=ReadOnlyViolationError("nope")).run({"text": "x"})

    assert result.status is ToolStatus.FORBIDDEN


async def test_an_unreachable_backend_surfaces_as_unavailable():
    result = await EchoTool(raises=PersistenceError("connection refused")).run({"text": "x"})

    assert result.status is ToolStatus.UNAVAILABLE


async def test_an_undocumented_driver_error_does_not_reach_the_graph():
    # A tool that raises would abandon an incident other tools could still have explained.
    result = await EchoTool(raises=ZeroDivisionError("surprise")).run({"text": "x"})

    assert result.status is ToolStatus.ERROR
    assert "ZeroDivisionError" in (result.error or "")


async def test_a_failure_still_summarises_itself():
    result = await EchoTool(raises=TargetNotFoundError("gone")).run({"text": "x"})

    assert "not_found" in result.summarise()


async def test_every_call_is_timed():
    assert (await EchoTool().run({"text": "x"})).duration_ms >= 0


def test_the_input_schema_is_published_for_the_model():
    schema = EchoTool().input_schema()

    assert schema["properties"] == {
        "text": {"title": "Text", "type": "string"},
        "times": {"default": 1, "title": "Times", "type": "integer"},
    }


def test_a_tool_can_be_looked_up_by_name():
    tool = registry.register(EchoTool())

    assert registry.get("echo") is tool
    assert registry.names() == ["echo"]


def test_an_invented_tool_name_is_refused_with_the_real_ones():
    registry.register(EchoTool())

    with pytest.raises(UnknownToolError) as excinfo:
        registry.get("fetch_the_moon")

    assert excinfo.value.details["available"] == ["echo"]


def test_two_tools_cannot_share_one_name():
    # Which one runs would otherwise depend on import order.
    registry.register(EchoTool())

    with pytest.raises(UnknownToolError):
        registry.register(EchoTool())


def test_the_catalogue_carries_a_description_for_each_tool():
    registry.register(EchoTool())

    assert list(registry.catalogue()) == [("echo", "Repeats its input.")]
