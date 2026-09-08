"""Seeded failure: an external API that does not answer.

The enrichment step calls a host that is not routable, so the connection attempt times
out. Retries are on, with backoff, because that is what the correct fix looks like and the
agent should recognise it as already in place rather than propose it as news.

Correct diagnosis: transient infrastructure. The recommendation is about retry and timeout
policy, not a code change, and the tell is that the task's history shows it succeeding on
other runs.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_doctor.messaging.airflow_callback import on_task_failure

# The only scenario that retries. A deterministic failure gains nothing from retrying, but
# a transient one is exactly the case retries exist for, and the agent should see the
# attempts in the task history.
DEFAULT_ARGS = {
    "owner": "dag-doctor",
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "on_failure_callback": on_task_failure,
}

# Reserved as non-routable, so the connection hangs rather than being refused outright,
# which is what a real network partition looks like.
UNREACHABLE_ENDPOINT = "http://10.255.255.1:8080/partners/enrich"
CONNECT_TIMEOUT_S = 5.0


def enrich_from_partner_api() -> None:
    """Call the partner API, which is not reachable from here."""
    import httpx

    with httpx.Client(timeout=CONNECT_TIMEOUT_S) as client:
        response = client.get(UNREACHABLE_ENDPOINT)
        response.raise_for_status()


with DAG(
    dag_id="connection_timeout_api",
    description="An external API call times out; retries do not help.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["dag-doctor", "seeded-failure", "transient"],
) as dag:
    PythonOperator(
        task_id="enrich_from_partner_api",
        python_callable=enrich_from_partner_api,
    )
