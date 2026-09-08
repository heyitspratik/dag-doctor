You are investigating a failed Airflow task. Choose which tools to call next.

Failure:
  DAG: $dag_id
  Task: $task_id
  Run: $run_id, attempt $try_number
  Exception: $exception_type
  Message: $exception_message

Initial classification: $triage_category ($triage_rationale)

Evidence gathered so far:
$evidence

Tools available:
$tools

You may request up to $max_calls calls. $budget_note

Pick the calls that could most change your mind. Do not repeat a call that has already
been made unless its arguments differ meaningfully. Prefer a tool that could refute the
current leading explanation over one that would merely confirm it.
