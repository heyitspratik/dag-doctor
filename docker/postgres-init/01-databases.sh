#!/bin/bash
# Creates the three logical databases and the SELECT-only role the agent's tools use.
# Runs once, on an empty data volume, from the postgres image's entrypoint.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-SQL
    CREATE ROLE airflow LOGIN PASSWORD 'airflow';
    CREATE ROLE warehouse LOGIN PASSWORD 'warehouse';
    CREATE ROLE dagdoctor_ro LOGIN PASSWORD 'dagdoctor_ro';

    CREATE DATABASE airflow OWNER airflow;
    CREATE DATABASE warehouse OWNER warehouse;
    CREATE DATABASE dag_doctor OWNER ${POSTGRES_USER};

    -- The agent reads Airflow's metadata and the warehouse, and writes only to its own
    -- database. The read-only role is the outer guarantee; the tools enforce read-only in
    -- code as well, because a demo repository gets cloned and reconfigured by people who
    -- will not read this grant.
    REVOKE ALL ON DATABASE airflow FROM PUBLIC;
    REVOKE ALL ON DATABASE warehouse FROM PUBLIC;
    GRANT CONNECT ON DATABASE airflow TO dagdoctor_ro;
    GRANT CONNECT ON DATABASE warehouse TO dagdoctor_ro;
SQL

for db in airflow warehouse; do
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db" <<-SQL
        GRANT USAGE ON SCHEMA public TO dagdoctor_ro;
        GRANT SELECT ON ALL TABLES IN SCHEMA public TO dagdoctor_ro;

        -- Airflow and the DAGs create their tables after this script runs, so the grant
        -- above would cover nothing without this. Default privileges are applied per
        -- granting role, hence one statement for each writer.
        ALTER DEFAULT PRIVILEGES FOR ROLE ${POSTGRES_USER} IN SCHEMA public
            GRANT SELECT ON TABLES TO dagdoctor_ro;
        ALTER DEFAULT PRIVILEGES FOR ROLE airflow IN SCHEMA public
            GRANT SELECT ON TABLES TO dagdoctor_ro;
        ALTER DEFAULT PRIVILEGES FOR ROLE warehouse IN SCHEMA public
            GRANT SELECT ON TABLES TO dagdoctor_ro;
SQL
done
