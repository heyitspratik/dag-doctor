#!/usr/bin/env bash
# Import every seeded DAG against a real Airflow, and fail if any of them cannot be.
#
# A file rather than a heredoc inside the workflow. Embedding this in YAML meant a Python
# heredoc inside a bash -c inside a block scalar, and the indentation YAML requires stops
# bash finding the terminator, so the whole script reached Python indented. Keeping it here
# means one level of quoting and something that can be run locally exactly as CI runs it.
set -euo pipefail

# DagBag wants somewhere to look up connections and variables; nothing here uses them.
airflow db migrate >/dev/null 2>&1

python - <<'PY'
from airflow.models import DagBag

bag = DagBag("/opt/airflow/dags", include_examples=False)
for path, error in bag.import_errors.items():
    print(f"::error file={path}::{error}")
if bag.import_errors:
    raise SystemExit(1)

if not bag.dag_ids:
    # An empty DagBag is not a pass. It is what a mounting mistake looks like, and it
    # would quietly report success while checking nothing at all.
    print("::error::no DAGs were found, which means nothing was actually checked")
    raise SystemExit(1)

print(f"Parsed {len(bag.dag_ids)}: {', '.join(sorted(bag.dag_ids))}")
PY
