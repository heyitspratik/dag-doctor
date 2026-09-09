UV      ?= uv
COMPOSE ?= docker compose -f docker/docker-compose.yml
CHART   ?= deploy/helm/dag-doctor
KIND_CLUSTER ?= dag-doctor
NAMESPACE    ?= dag-doctor
KIND_MODEL   ?= qwen2.5:0.5b
AIRFLOW_URL  ?= http://localhost:8080
AIRFLOW_AUTH ?= airflow:airflow
AGENT_URL    ?= http://localhost:8000
EVAL_OUTPUT  ?= results/accuracy.md
EXAMPLE_DAG  ?= schema_drift_orders
EXAMPLE_OUTPUT ?= results/worked-example.md

.DEFAULT_GOAL := help
.PHONY: help install dev up down logs migrate pull-models trigger seed-failures \
        topic-tail example evaluate test test-integration lint format typecheck check \
        helm-lint kind-deploy kind-destroy clean

help:  ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

install:  ## Sync the runtime environment only
	$(UV) sync --frozen --no-dev

dev:  ## Sync every dependency group and install the pre-commit hooks
	$(UV) sync --all-groups
	$(UV) run pre-commit install
	@test -f .env || cp .env.example .env

up:  ## Start the full stack (airflow, redpanda, postgres, agent, ollama)
	$(COMPOSE) up -d --wait

down:  ## Stop the stack and drop its volumes
	$(COMPOSE) down -v

logs:  ## Follow the agent worker and API logs
	$(COMPOSE) logs -f agent-worker agent-api

migrate:  ## Apply database migrations
	$(UV) run alembic upgrade head

pull-models:  ## Pull the default Ollama model (several GB on first run)
	$(COMPOSE) exec ollama ollama pull $${OLLAMA_MODEL:-llama3.2:3b}

trigger:  ## Trigger one DAG: make trigger DAG=schema_drift_orders
	@test -n "$(DAG)" || { echo "usage: make trigger DAG=<dag_id>"; exit 2; }
	@curl -sS -u $(AIRFLOW_AUTH) -X POST -H 'Content-Type: application/json' \
		-d '{"conf": {}}' $(AIRFLOW_URL)/api/v1/dags/$(DAG)/dagRuns \
		| head -c 400
	@echo

seed-failures:  ## Trigger every seeded DAG so the agent has incidents to diagnose
	$(UV) run python -m dag_doctor.evaluation.runner seed --agent-url $(AGENT_URL)

topic-tail:  ## Print what is currently on the failure topic
	$(COMPOSE) exec redpanda rpk topic consume airflow.task.failed \
		--brokers localhost:9092 --num 10 --offset start

example:  ## Render the latest investigation of $(EXAMPLE_DAG) as markdown
	$(UV) run python -m dag_doctor.evaluation.runner trace \
		--agent-url $(AGENT_URL) --dag-id $(EXAMPLE_DAG) --output $(EXAMPLE_OUTPUT)

evaluate:  ## Trigger the scenarios, wait for diagnoses, print the accuracy table
	$(UV) run python -m dag_doctor.evaluation.runner evaluate \
		--agent-url $(AGENT_URL) --output $(EVAL_OUTPUT)

test:  ## Run unit tests with the coverage gate
	$(UV) run pytest --cov --cov-report=term-missing --cov-report=xml

test-integration:  ## Run integration tests (needs Docker)
	$(UV) run pytest -m integration -p no:cacheprovider

lint:  ## Check formatting and lint rules
	$(UV) run ruff format --check .
	$(UV) run ruff check .

format:  ## Apply formatting and autofixes
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

typecheck:  ## Type-check the source tree
	$(UV) run mypy

check: lint typecheck test  ## The full quality gate

helm-lint:  ## Lint and render the chart against every values file
	helm lint $(CHART)
	helm lint $(CHART) -f $(CHART)/values-local.yaml
	helm lint $(CHART) -f $(CHART)/values-prod.yaml
	helm template dag-doctor $(CHART) >/dev/null
	helm template dag-doctor $(CHART) -f $(CHART)/values-local.yaml >/dev/null
	helm template dag-doctor $(CHART) -f $(CHART)/values-prod.yaml >/dev/null

kind-deploy:  ## Create a kind cluster, load the image, and deploy the chart
	kind create cluster --name $(KIND_CLUSTER) --wait 120s || true
	kubectl apply -f deploy/kind/dependencies.yaml
	kubectl -n $(NAMESPACE) wait --for=condition=available --timeout=300s \
		deploy/postgres deploy/redpanda deploy/ollama
	# The model has to exist before the agent starts, or readiness correctly reports
	# that the provider is not usable and the rollout never completes.
	kubectl -n $(NAMESPACE) exec deploy/ollama -- ollama pull $(KIND_MODEL)
	docker build -f docker/Dockerfile -t dag-doctor:local .
	kind load docker-image dag-doctor:local --name $(KIND_CLUSTER)
	helm upgrade --install dag-doctor $(CHART) \
		-f $(CHART)/values-local.yaml \
		--namespace $(NAMESPACE) --wait --timeout 10m
	kubectl -n $(NAMESPACE) get pods

kind-destroy:  ## Delete the kind cluster
	kind delete cluster --name $(KIND_CLUSTER)

clean:  ## Remove caches and build artefacts
	rm -rf .mypy_cache .pytest_cache .ruff_cache htmlcov .coverage coverage.xml dist build
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
