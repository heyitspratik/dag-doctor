"""Environment-backed settings.

Everything secret or machine-specific lives here, loaded from ``.env``. The values that
shape an investigation's behaviour, the budgets and confidence thresholds, live here too
rather than in the graph code, because they are the knobs an operator turns without
redeploying.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated, Literal, Self

from pydantic import BeforeValidator, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from dag_doctor.core.models import NodeName

type LLMProvider = Literal["ollama", "anthropic", "openai"]
type AppEnv = Literal["dev", "prod"]


def _blank_to_none(value: object) -> object:
    """Treat an empty environment variable as unset.

    ``.env.example`` ships keys with empty values as placeholders, and an empty
    ``SecretStr`` is truthy, so without this an unset key would pass validation.
    """
    if isinstance(value, str) and not value.strip():
        return None
    return value


type OptionalSecret = Annotated[SecretStr | None, BeforeValidator(_blank_to_none)]
type OptionalName = Annotated[str | None, BeforeValidator(_blank_to_none)]

_ENV_CONFIG = SettingsConfigDict(
    env_file=".env",
    env_file_encoding="utf-8",
    extra="ignore",
    populate_by_name=True,
)


class NodeModelOverrides(BaseSettings):
    """Per-node model choices, each falling back to the provider default when unset.

    Triage is a cheap classification a 3B model handles fine, while forming a hypothesis
    from a dozen pieces of evidence is where a stronger model earns its cost. Spending the
    same model on both is the easy choice, not the right one. ``escalate`` is absent by
    design: it summarises what was already found, in code, and calling a model after the
    budget is exhausted would defeat the budget.
    """

    model_config = _ENV_CONFIG

    triage: OptionalName = Field(default=None, validation_alias="LLM_MODEL_TRIAGE")
    gather_evidence: OptionalName = Field(
        default=None, validation_alias="LLM_MODEL_GATHER_EVIDENCE"
    )
    form_hypothesis: OptionalName = Field(
        default=None, validation_alias="LLM_MODEL_FORM_HYPOTHESIS"
    )
    test_hypothesis: OptionalName = Field(
        default=None, validation_alias="LLM_MODEL_TEST_HYPOTHESIS"
    )
    conclude: OptionalName = Field(default=None, validation_alias="LLM_MODEL_CONCLUDE")

    def get(self, node: NodeName) -> str | None:
        """Return the override for a node, or ``None`` to use the provider default."""
        override: object = getattr(self, node, None)
        return override if isinstance(override, str) else None


class LLMSettings(BaseSettings):
    """Which chat model to talk to, and how to reach it.

    Model names are deliberately per-provider rather than one shared ``LLM_MODEL``:
    ``llama3.2:3b`` and ``gpt-4o-mini`` are not interchangeable strings, and sharing one
    variable across providers makes switching provider silently produce a broken name.
    """

    model_config = _ENV_CONFIG

    provider: LLMProvider = Field(default="ollama", validation_alias="LLM_PROVIDER")

    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "llama3.2:3b"

    anthropic_api_key: OptionalSecret = None
    anthropic_model: str = "claude-sonnet-5"

    openai_api_key: OptionalSecret = None
    openai_model: str = "gpt-4o-mini"

    temperature: float = Field(default=0.0, ge=0.0, le=2.0, validation_alias="LLM_TEMPERATURE")
    max_output_tokens: int = Field(default=2048, gt=0, validation_alias="LLM_MAX_OUTPUT_TOKENS")
    request_timeout_s: float = Field(default=120.0, gt=0.0)
    max_retries: int = Field(default=2, ge=0)

    node_models: NodeModelOverrides = Field(default_factory=NodeModelOverrides)

    @property
    def default_model(self) -> str:
        """The model identifier for the selected provider."""
        return {
            "ollama": self.ollama_model,
            "anthropic": self.anthropic_model,
            "openai": self.openai_model,
        }[self.provider]

    @property
    def api_key(self) -> SecretStr | None:
        """The API key for the selected provider, or ``None`` for key-less providers."""
        return {
            "ollama": None,
            "anthropic": self.anthropic_api_key,
            "openai": self.openai_api_key,
        }[self.provider]

    def model_for_node(self, node: NodeName) -> str:
        """Return the model a given graph node should use.

        Args:
            node: The graph node about to make a call.

        Returns:
            The node's configured override, or the provider default.
        """
        return self.node_models.get(node) or self.default_model

    @model_validator(mode="after")
    def _require_provider_credentials(self) -> Self:
        """Fail at load time, with the offending variable named, rather than mid-diagnosis."""
        key_variables: dict[str, tuple[str, SecretStr | None]] = {
            "anthropic": ("ANTHROPIC_API_KEY", self.anthropic_api_key),
            "openai": ("OPENAI_API_KEY", self.openai_api_key),
        }
        required = key_variables.get(self.provider)
        if required is not None and required[1] is None:
            raise ValueError(f"{required[0]} is required when LLM_PROVIDER={self.provider}")

        # Ollama is the default precisely because it needs no key; it needs a reachable
        # base URL instead, which differs between host and container.
        if self.provider == "ollama" and not self.ollama_base_url.strip():
            raise ValueError("OLLAMA_BASE_URL is required when LLM_PROVIDER=ollama")
        return self


class BudgetSettings(BaseSettings):
    """Hard limits on one investigation, and the bar a conclusion has to clear.

    An agent without a budget is an agent that loops forever on an ambiguous failure. Both
    limits are enforced by routing: hitting one ends the investigation with an inconclusive
    diagnosis carrying whatever evidence was gathered.
    """

    model_config = _ENV_CONFIG

    max_iterations: int = Field(default=5, ge=1, le=50, validation_alias="MAX_ITERATIONS")
    max_tool_calls: int = Field(default=20, ge=1, le=200, validation_alias="MAX_TOOL_CALLS")
    tool_timeout_s: float = Field(default=30.0, gt=0.0, validation_alias="TOOL_TIMEOUT_SECONDS")

    #: Above this, triage alone is trusted and the graph skips straight to a conclusion.
    #: Set high: shortcutting the investigation is only safe for unmistakable signatures.
    triage_shortcut_confidence: float = Field(default=0.9, ge=0.0, le=1.0)

    #: Below this, a confirmed hypothesis still escalates rather than asserting a cause.
    min_conclude_confidence: float = Field(default=0.5, ge=0.0, le=1.0)

    @model_validator(mode="after")
    def _shortcut_must_be_stricter_than_conclusion(self) -> Self:
        """A triage shortcut that is easier to clear than a tested conclusion is a bug."""
        if self.triage_shortcut_confidence < self.min_conclude_confidence:
            raise ValueError(
                "triage_shortcut_confidence must be at least min_conclude_confidence, "
                "otherwise triage would conclude on evidence a tested hypothesis could not"
            )
        return self


class DatabaseSettings(BaseSettings):
    """Connection details for the agent's own database."""

    model_config = _ENV_CONFIG

    dsn: str = Field(
        default="postgresql+psycopg://dagdoctor:dagdoctor@localhost:5432/dag_doctor",
        validation_alias="POSTGRES_DSN",
    )
    echo: bool = Field(default=False, validation_alias="POSTGRES_ECHO")
    pool_size: int = Field(default=5, ge=1)


class AirflowSettings(BaseSettings):
    """How the tools reach Airflow, as an outsider reading it.

    The DSN must point at a ``SELECT``-only role. That grant is the outer guarantee; the
    tools enforce read-only in code as well, because a demo repository is cloned and
    reconfigured by people who will not read the grant.
    """

    model_config = _ENV_CONFIG

    dsn: str = Field(
        default="postgresql+psycopg://dagdoctor_ro:dagdoctor_ro@localhost:5432/airflow",
        validation_alias="AIRFLOW_DSN",
    )
    base_url: str = Field(default="http://localhost:8080", validation_alias="AIRFLOW_BASE_URL")
    username: str = Field(default="airflow", validation_alias="AIRFLOW_USERNAME")
    password: SecretStr = Field(default=SecretStr("airflow"), validation_alias="AIRFLOW_PASSWORD")
    request_timeout_s: float = Field(default=30.0, gt=0.0)


class KafkaSettings(BaseSettings):
    """Redpanda connection details and topic names.

    Redpanda speaks the Kafka protocol, so this is ordinary Kafka configuration: the same
    client library, the same consumer groups, the same offsets.
    """

    model_config = _ENV_CONFIG

    bootstrap_servers: str = Field(
        default="localhost:19092", validation_alias="KAFKA_BOOTSTRAP_SERVERS"
    )
    consumer_group: str = Field(
        default="dag-doctor-worker", validation_alias="KAFKA_CONSUMER_GROUP"
    )
    topic_failures: str = Field(
        default="airflow.task.failed", validation_alias="KAFKA_TOPIC_FAILURES"
    )
    topic_diagnoses: str = Field(
        default="agent.diagnosis.completed", validation_alias="KAFKA_TOPIC_DIAGNOSES"
    )
    topic_dlq: str = Field(default="airflow.task.failed.dlq", validation_alias="KAFKA_TOPIC_DLQ")

    #: Attempts before a message is parked on the dead-letter topic and the offset moves
    #: on. Without a ceiling, one poison message blocks the partition indefinitely.
    max_delivery_attempts: int = Field(default=3, ge=1)


class Settings(BaseSettings):
    """Top-level application settings, composed from the per-concern groups above."""

    model_config = _ENV_CONFIG

    app_env: AppEnv = "dev"
    log_level: str = "INFO"
    api_key: OptionalSecret = Field(default=None, validation_alias="API_KEY")

    llm: LLMSettings = Field(default_factory=LLMSettings)
    budgets: BudgetSettings = Field(default_factory=BudgetSettings)
    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    airflow: AirflowSettings = Field(default_factory=AirflowSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load settings once per process.

    Returns:
        The cached settings instance.
    """
    return Settings()
