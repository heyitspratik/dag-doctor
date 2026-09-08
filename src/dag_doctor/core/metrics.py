"""Prometheus collectors, defined once and shared by both processes.

The worker and the API are separate processes with separate registries, so each exposes
its own ``/metrics``: the API serves the endpoint, and the worker runs a small exporter of
its own. The alternative, multiprocess mode with a shared directory, buys nothing here and
costs a deployment constraint.

What is measured follows from what a reader would actually ask: how many incidents came
through, how long a diagnosis takes, how often each tool fails, what it cost in tokens,
and how confident the answers were.
"""

from prometheus_client import Counter, Gauge, Histogram

#: Diagnosis latency in seconds. The buckets run to five minutes because a small local
#: model doing five iterations genuinely takes minutes, and a histogram whose top bucket
#: is thirty seconds would report only that everything is slow.
_LATENCY_BUCKETS = (1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0)

INCIDENTS_RECEIVED = Counter(
    "dag_doctor_incidents_received_total",
    "Failure events consumed, whether or not they opened a new incident.",
    ["dag_id"],
)

INVESTIGATIONS_COMPLETED = Counter(
    "dag_doctor_investigations_completed_total",
    "Investigations that reached a terminal node.",
    ["root_cause_category", "conclusive", "halt_reason"],
)

DIAGNOSIS_LATENCY = Histogram(
    "dag_doctor_diagnosis_duration_seconds",
    "Time from starting an investigation to persisting its diagnosis.",
    buckets=_LATENCY_BUCKETS,
)

TOOL_CALLS = Counter(
    "dag_doctor_tool_calls_total",
    "Tool calls made, by tool and outcome.",
    ["tool", "status"],
)

TOOL_DURATION = Histogram(
    "dag_doctor_tool_duration_seconds",
    "How long each tool takes.",
    ["tool"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)

TOKENS_SPENT = Counter(
    "dag_doctor_tokens_total",
    "Tokens spent, by graph node and direction.",
    ["node", "direction"],
)

CONFIDENCE = Histogram(
    "dag_doctor_confidence",
    "Confidence of completed diagnoses.",
    # Fixed tenths rather than the default latency buckets, so the distribution can be
    # read directly against the thresholds that gate concluding.
    buckets=(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0),
)

MESSAGES_DEAD_LETTERED = Counter(
    "dag_doctor_dead_lettered_total",
    "Messages parked on the dead-letter topic.",
    ["reason"],
)

WORKER_UP = Gauge(
    "dag_doctor_worker_up",
    "One while the worker's consume loop is running.",
)
