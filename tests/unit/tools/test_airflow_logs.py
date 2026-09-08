import httpx
import pytest

from dag_doctor.core.settings import AirflowSettings
from dag_doctor.tools.airflow_logs import FetchTaskLogs
from dag_doctor.tools.base import ToolStatus

REAL_LOG = """\
[2026-09-07T10:00:01.123+0000] {taskinstance.py:2613} INFO - Starting attempt 1 of 1
[2026-09-07T10:00:01.456+0000] {sql.py:265} INFO - Running statement: DROP TABLE IF EXISTS analytics.orders_by_customer
[2026-09-07T10:00:02.001+0000] {taskinstance.py:2917} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/models/taskinstance.py", line 465, in _execute_task
    result = _execute_callable(context=context, **execute_callable_kwargs)
  File "/opt/airflow/dags/schema_drift_orders.py", line 61, in execute
    hook.run(self.sql, autocommit=True)
  File "/home/airflow/.local/lib/python3.12/site-packages/psycopg2/extensions.py", line 12, in run
    cursor.execute(statement)
psycopg2.errors.UndefinedColumn: column "customer_id" does not exist
LINE 3:     customer_id,
            ^
[2026-09-07T10:00:02.100+0000] {taskinstance.py:1206} INFO - Marking task as FAILED.
"""


def _transport(text: str, status: int = 200) -> httpx.MockTransport:
    return httpx.MockTransport(lambda _request: httpx.Response(status, text=text))


@pytest.fixture
def airflow_settings() -> AirflowSettings:
    return AirflowSettings()


async def test_the_exception_that_killed_the_task_is_extracted(airflow_settings):
    tool = FetchTaskLogs(airflow_settings, transport=_transport(REAL_LOG))

    result = await tool.run(
        {"dag_id": "schema_drift_orders", "task_id": "build", "run_id": "manual__1"}
    )

    assert result.ok
    assert result.data.exception_type == "psycopg2.errors.UndefinedColumn"
    assert result.data.exception_message == 'column "customer_id" does not exist'


async def test_the_summary_leads_with_what_went_wrong(airflow_settings):
    tool = FetchTaskLogs(airflow_settings, transport=_transport(REAL_LOG))

    result = await tool.run({"dag_id": "d", "task_id": "t", "run_id": "r"})

    assert result.data.summarise().startswith("psycopg2.errors.UndefinedColumn:")


async def test_the_traceback_points_at_the_dag_file(airflow_settings):
    # The frame in the user's DAG is the one worth reading; the Airflow internals above
    # it are the same on every failure.
    tool = FetchTaskLogs(airflow_settings, transport=_transport(REAL_LOG))

    result = await tool.run({"dag_id": "d", "task_id": "t", "run_id": "r"})

    dag_frames = [
        frame for frame in result.data.traceback_frames if "/opt/airflow/dags/" in frame.file
    ]
    assert dag_frames[0].line == 61
    assert dag_frames[0].function == "execute"
    assert dag_frames[0].code == "hook.run(self.sql, autocommit=True)"


async def test_airflow_line_prefixes_are_stripped(airflow_settings):
    # Without this the traceback does not parse and half the payload is timestamps.
    tool = FetchTaskLogs(airflow_settings, transport=_transport(REAL_LOG))

    result = await tool.run({"dag_id": "d", "task_id": "t", "run_id": "r"})

    assert not any(line.startswith("[2026-") for line in result.data.final_lines)


async def test_the_tail_is_bounded_by_the_requested_number_of_lines(airflow_settings):
    tool = FetchTaskLogs(airflow_settings, transport=_transport("noise\n" * 5000))

    result = await tool.run({"dag_id": "d", "task_id": "t", "run_id": "r", "tail_lines": 10})

    assert len(result.data.final_lines) == 10
    assert result.data.total_lines == 5000


async def test_a_log_with_no_exception_says_so_rather_than_inventing_one(airflow_settings):
    tool = FetchTaskLogs(airflow_settings, transport=_transport("all fine\ndone\n"))

    result = await tool.run({"dag_id": "d", "task_id": "t", "run_id": "r"})

    assert result.data.exception_type is None
    assert "no recognisable exception" in result.data.summarise()


async def test_an_attempt_with_no_log_is_reported_as_not_found(airflow_settings):
    tool = FetchTaskLogs(airflow_settings, transport=_transport("", status=404))

    result = await tool.run({"dag_id": "d", "task_id": "t", "run_id": "r"})

    assert result.status is ToolStatus.NOT_FOUND


async def test_an_unreachable_airflow_is_reported_as_unavailable(airflow_settings):
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    tool = FetchTaskLogs(airflow_settings, transport=httpx.MockTransport(refuse))

    result = await tool.run({"dag_id": "d", "task_id": "t", "run_id": "r"})

    assert result.status is ToolStatus.UNAVAILABLE
    assert "not reachable" in (result.error or "")


async def test_an_airflow_error_response_is_reported_as_unavailable(airflow_settings):
    tool = FetchTaskLogs(airflow_settings, transport=_transport("boom", status=500))

    result = await tool.run({"dag_id": "d", "task_id": "t", "run_id": "r"})

    assert result.status is ToolStatus.UNAVAILABLE


async def test_the_log_is_requested_for_the_attempt_that_failed(airflow_settings):
    seen: list[str] = []

    def record(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(200, text=REAL_LOG)

    tool = FetchTaskLogs(airflow_settings, transport=httpx.MockTransport(record))

    await tool.run({"dag_id": "d", "task_id": "t", "run_id": "r", "try_number": 3})

    assert "/taskInstances/t/logs/3" in seen[0]
