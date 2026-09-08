"""Fetching and parsing the failing task's log.

The log is the one piece of evidence that is not in a database, so this tool goes to
Airflow's REST API. What comes back is megabytes of prose, and handing that to a model is
how an investigation loses its budget in one call. The parsing here is the tool's real
work: the exception type, the traceback frames, and the tail, as fields.
"""

import re
from typing import ClassVar

import httpx

from dag_doctor.core.exceptions import ProviderUnavailableError
from dag_doctor.core.settings import AirflowSettings
from dag_doctor.tools.base import BaseTool, TargetNotFoundError, ToolInput, ToolOutput

#: Matches a traceback frame header written by CPython.
_FRAME = re.compile(r'\s*File "(?P<file>[^"]+)", line (?P<line>\d+), in (?P<function>\S+)')

#: Matches the exception line that closes a traceback. Deliberately not restricted to
#: names ending in Error or Exception: the ones that matter most here do not, and
#: psycopg2.errors.UndefinedColumn is exactly the failure this agent exists to explain.
#: Requiring the final component to be capitalised is what keeps ordinary prose out.
_EXCEPTION = re.compile(
    r"^(?P<module>[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*\.)?(?P<cls>[A-Z]\w*)\s*:\s*(?P<message>.*)$"
)

#: Airflow prefixes every log line with a timestamp and a level. Stripping them makes the
#: traceback parseable and cuts the payload roughly in half.
_LOG_PREFIX = re.compile(
    r"^\[?\d{4}-\d{2}-\d{2}[ T][\d:,.]+(?:[+-]\d{2}:?\d{2}|Z)?\]?\s*"
    r"(?:\{[^}]*\}\s*)?(?:[A-Z]+\s*-\s*)?"
)

MAX_TAIL_LINES = 500


class TracebackFrame(ToolOutput):
    """One frame of a Python traceback."""

    file: str
    line: int
    function: str
    code: str | None = None

    def summarise(self) -> str:
        """One line describing this frame."""
        return f"{self.file}:{self.line} in {self.function}"


class ParsedLog(ToolOutput):
    """A task log reduced to the parts that identify a failure."""

    dag_id: str
    task_id: str
    run_id: str
    try_number: int
    exception_type: str | None
    exception_message: str | None
    traceback_frames: list[TracebackFrame]
    final_lines: list[str]
    total_lines: int

    def summarise(self) -> str:
        """One line describing what the log says went wrong."""
        if self.exception_type:
            return f"{self.exception_type}: {self.exception_message or '(no message)'}"
        return f"log has {self.total_lines} lines and no recognisable exception"


class TaskLogsInput(ToolInput):
    """Arguments for :class:`FetchTaskLogs`."""

    dag_id: str
    task_id: str
    run_id: str
    try_number: int = 1
    tail_lines: int = 80


class FetchTaskLogs(BaseTool[TaskLogsInput, ParsedLog]):
    """What did the task actually print before it died?"""

    name: ClassVar[str] = "fetch_task_logs"
    description: ClassVar[str] = (
        "The failing task's log, parsed into the exception type, its message, the "
        "traceback frames and the final lines. Usually the first call worth making."
    )
    input_model = TaskLogsInput

    def __init__(
        self,
        settings: AirflowSettings,
        timeout_s: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """Initialise the tool.

        Args:
            settings: How to reach Airflow's REST API.
            timeout_s: Per-call timeout.
            transport: Injected by tests, so no test needs a running Airflow.
        """
        super().__init__(timeout_s)
        self._settings = settings
        self._transport = transport

    async def execute(self, tool_input: TaskLogsInput) -> ParsedLog:
        """Fetch the log and parse it.

        Raises:
            TargetNotFoundError: If Airflow has no log for that attempt.
            ProviderUnavailableError: If Airflow could not be reached.
        """
        raw = await self._fetch(tool_input)
        return _parse_log(raw, tool_input)

    async def _fetch(self, tool_input: TaskLogsInput) -> str:
        """Ask Airflow for one attempt's log."""
        base = self._settings.base_url.rstrip("/")
        url = (
            f"{base}/api/v1/dags/{tool_input.dag_id}/dagRuns/{tool_input.run_id}"
            f"/taskInstances/{tool_input.task_id}/logs/{tool_input.try_number}"
        )
        auth = (self._settings.username, self._settings.password.get_secret_value())
        try:
            async with httpx.AsyncClient(
                timeout=self._settings.request_timeout_s, transport=self._transport
            ) as client:
                response = await client.get(url, auth=auth, params={"full_content": "true"})
        except httpx.HTTPError as exc:
            raise ProviderUnavailableError(
                f"Airflow is not reachable at {self._settings.base_url}",
                details={"base_url": self._settings.base_url},
            ) from exc

        if response.status_code == httpx.codes.NOT_FOUND:
            raise TargetNotFoundError(
                f"No log for {tool_input.dag_id}.{tool_input.task_id} "
                f"attempt {tool_input.try_number}"
            )
        if response.status_code >= httpx.codes.BAD_REQUEST:
            raise ProviderUnavailableError(
                f"Airflow returned {response.status_code} for the task log",
                details={"status": response.status_code},
            )
        return response.text


def _parse_log(raw: str, tool_input: TaskLogsInput) -> ParsedLog:
    """Reduce a raw log to its exception, its frames, and its tail."""
    lines = [_LOG_PREFIX.sub("", line.rstrip()) for line in raw.splitlines()]
    frames = _traceback_frames(lines)
    exception_type, exception_message = _final_exception(lines)
    tail = min(max(tool_input.tail_lines, 1), MAX_TAIL_LINES)
    return ParsedLog(
        dag_id=tool_input.dag_id,
        task_id=tool_input.task_id,
        run_id=tool_input.run_id,
        try_number=tool_input.try_number,
        exception_type=exception_type,
        exception_message=exception_message,
        traceback_frames=frames,
        final_lines=[line for line in lines[-tail:] if line.strip()],
        total_lines=len(lines),
    )


def _traceback_frames(lines: list[str]) -> list[TracebackFrame]:
    """Pull the frames out of every traceback in the log.

    The last traceback is the one that killed the task, so later frames win when a log
    carries several, for example from a retry inside the task itself.
    """
    frames: list[TracebackFrame] = []
    for index, line in enumerate(lines):
        match = _FRAME.match(line)
        if match is None:
            continue
        following = lines[index + 1] if index + 1 < len(lines) else ""
        code = following.strip() or None
        if code is not None and _FRAME.match(following):
            code = None
        frames.append(
            TracebackFrame(
                file=match.group("file"),
                line=int(match.group("line")),
                function=match.group("function"),
                code=code,
            )
        )
    return frames


def _final_exception(lines: list[str]) -> tuple[str | None, str | None]:
    """Find the last line that looks like an exception being raised."""
    for line in reversed(lines):
        stripped = line.strip()
        if not stripped:
            continue
        match = _EXCEPTION.match(stripped)
        if match is not None:
            message = (match.group("message") or "").strip() or None
            return f"{match.group('module') or ''}{match.group('cls')}", message
    return None, None
