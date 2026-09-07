#!/bin/bash
# Seeds the fake warehouse the broken DAGs read and write. This is the data the agent's
# schema and profiling tools inspect: it stands in for a real analytics database.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username warehouse --dbname warehouse \
    -f /docker-entrypoint-initdb.d/warehouse/01_baseline.sql
