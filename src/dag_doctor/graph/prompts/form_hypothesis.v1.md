You are investigating a failed Airflow task. Propose ranked root causes.

Failure:
  DAG: $dag_id
  Task: $task_id
  Exception: $exception_type
  Message: $exception_message

Evidence gathered (referenced by number):
$evidence

Hypotheses already refuted, which must not be proposed again:
$refuted

Tools available for testing:
$tools

Propose up to $max_hypotheses hypotheses, best first. Each one must state a specific
cause, not a restatement of the symptom, and must carry a test that could refute it: a
single tool call whose result would tell you that you are wrong. A hypothesis nothing
could refute is a guess with citations, and is worse than admitting uncertainty.

If the true cause is a task upstream of the one that failed, name that task in
responsible_task_id. The visible failure is often not the broken thing.

Choose exactly one category per hypothesis from:
$categories
