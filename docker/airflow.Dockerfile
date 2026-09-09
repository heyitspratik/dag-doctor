# Airflow plus the one thing the failure callback needs: a Kafka client.
#
# The agent's source is bind-mounted rather than installed, so editing the callback does
# not mean rebuilding this image. Only the callback's runtime dependencies are baked in,
# not the agent's own (langgraph, fastapi, sqlalchemy), which have no business inside a
# task process.
ARG AIRFLOW_IMAGE_TAG=2.10.5
FROM apache/airflow:${AIRFLOW_IMAGE_TAG}

USER airflow

# Pinned, and deliberately few: anything installed here shares a resolver with Airflow's
# own pins, so a loose constraint is how an Airflow image quietly stops booting.
# Exactly what the callback's import closure needs beyond what Airflow already ships.
# tests/unit/test_airflow_image.py computes that closure and fails if this list drifts
# from it, because the symptom of a missing one is every DAG failing to import.
RUN pip install --no-cache-dir \
        "aiokafka==0.14.0" \
        "pydantic-settings>=2.6,<3.0" \
        "structlog>=24.4,<27"
