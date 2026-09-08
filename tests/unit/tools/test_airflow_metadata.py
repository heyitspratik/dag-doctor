from dag_doctor.tools.airflow_metadata import GetDagRunHistory, GetUpstreamTaskState
from dag_doctor.tools.base import ToolStatus


async def test_a_task_with_a_clean_record_reads_as_newly_failing(connections):
    result = await GetDagRunHistory(connections).run(
        {"dag_id": "schema_drift_orders", "task_id": "build_orders_by_customer"}
    )

    assert result.ok
    history = result.data
    assert history.is_new_failure is True
    assert history.consecutive_failures == 1
    assert history.failure_rate < 0.2
    assert "newly failing" in history.summarise()


async def test_the_most_recent_attempt_comes_first(connections):
    result = await GetDagRunHistory(connections).run(
        {"dag_id": "schema_drift_orders", "task_id": "build_orders_by_customer"}
    )

    assert result.data.runs[0].state == "failed"
    assert result.data.runs[1].state == "success"


async def test_durations_are_summarised_so_a_slowdown_is_visible(connections):
    result = await GetDagRunHistory(connections).run(
        {"dag_id": "schema_drift_orders", "task_id": "build_orders_by_customer"}
    )

    assert result.data.median_duration_s is not None
    assert result.data.median_duration_s > 0


async def test_the_history_limit_is_respected(connections):
    result = await GetDagRunHistory(connections).run(
        {"dag_id": "schema_drift_orders", "task_id": "build_orders_by_customer", "limit": 3}
    )

    assert len(result.data.runs) == 3


async def test_a_task_nobody_has_run_reports_no_history_rather_than_failing(connections):
    result = await GetDagRunHistory(connections).run(
        {"dag_id": "schema_drift_orders", "task_id": "never_existed"}
    )

    assert result.ok
    assert result.data.runs == []
    assert "no recorded run history" in result.data.summarise()


async def test_upstream_dependencies_are_walked_transitively(connections):
    # build_orders_by_customer depends on land_raw_orders, which depends on
    # extract_orders. Stopping at the direct parent is how a diagnosis blames the
    # symptom rather than the cause.
    result = await GetUpstreamTaskState(connections).run(
        {
            "dag_id": "schema_drift_orders",
            "task_id": "build_orders_by_customer",
            "run_id": "manual__2026-09-07T10:00:00+00:00",
        }
    )

    assert result.ok
    assert [task.task_id for task in result.data.upstream] == [
        "extract_orders",
        "land_raw_orders",
    ]


async def test_a_task_on_another_branch_is_not_counted_as_upstream(connections):
    # unrelated_branch failed in the same run but is not a dependency, so blaming it
    # would be a coincidence dressed up as a finding.
    result = await GetUpstreamTaskState(connections).run(
        {
            "dag_id": "schema_drift_orders",
            "task_id": "build_orders_by_customer",
            "run_id": "manual__2026-09-07T10:00:00+00:00",
        }
    )

    assert "unrelated_branch" not in [task.task_id for task in result.data.upstream]
    assert result.data.all_upstream_succeeded is True
    assert result.data.failed_upstream == []


async def test_healthy_upstream_tasks_say_so_plainly(connections):
    result = await GetUpstreamTaskState(connections).run(
        {
            "dag_id": "schema_drift_orders",
            "task_id": "build_orders_by_customer",
            "run_id": "manual__2026-09-07T10:00:00+00:00",
        }
    )

    assert "all 2 upstream tasks" in result.data.summarise()


async def test_a_dag_the_scheduler_has_not_parsed_is_reported_as_not_found(connections):
    result = await GetUpstreamTaskState(connections).run(
        {"dag_id": "never_parsed", "task_id": "t", "run_id": "r"}
    )

    assert result.status is ToolStatus.NOT_FOUND


async def test_naming_a_connection_the_agent_may_not_read_is_refused(connections):
    from dag_doctor.tools.connection_check import CheckConnectionHealth

    result = await CheckConnectionHealth(connections).run({"conn_id": "production_primary"})

    assert result.status is ToolStatus.FORBIDDEN
