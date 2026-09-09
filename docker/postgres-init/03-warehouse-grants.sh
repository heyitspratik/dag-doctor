#!/bin/bash
# Let the agent's read-only role actually read the warehouse.
#
# Runs after 02 creates the schemas, because a grant cannot name a schema that does not
# exist yet. 01 grants on `public` only, which is enough for Airflow's metadata but not for
# the warehouse, whose tables live in `raw` and `analytics`. Without this, schema
# reflection still works, since that reads the catalogs, while anything that selects a row
# fails with a permission error that looks like a broken tool rather than a missing grant.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname warehouse <<-SQL
    GRANT USAGE ON SCHEMA raw, analytics TO dagdoctor_ro;
    GRANT SELECT ON ALL TABLES IN SCHEMA raw, analytics TO dagdoctor_ro;

    -- The seeded DAGs create tables after this runs, so the grant above would not cover
    -- them. Default privileges are per granting role, hence naming warehouse explicitly.
    ALTER DEFAULT PRIVILEGES FOR ROLE warehouse IN SCHEMA raw
        GRANT SELECT ON TABLES TO dagdoctor_ro;
    ALTER DEFAULT PRIVILEGES FOR ROLE warehouse IN SCHEMA analytics
        GRANT SELECT ON TABLES TO dagdoctor_ro;
SQL
