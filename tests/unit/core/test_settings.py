from pathlib import Path

import pytest
from pydantic import ValidationError

from dag_doctor.core.settings import (
    AirflowSettings,
    BudgetSettings,
    DatabaseSettings,
    KafkaSettings,
    LLMSettings,
    NodeModelOverrides,
    Settings,
    get_settings,
)


def test_ollama_is_the_default_and_needs_no_key():
    settings = LLMSettings()

    assert settings.provider == "ollama"
    assert settings.api_key is None
    assert settings.default_model == "llama3.2:3b"


@pytest.mark.parametrize(
    ("provider", "variable"),
    [("anthropic", "ANTHROPIC_API_KEY"), ("openai", "OPENAI_API_KEY")],
)
def test_keyed_provider_without_its_key_is_refused(provider, variable):
    with pytest.raises(ValidationError) as excinfo:
        LLMSettings(provider=provider)

    assert variable in str(excinfo.value)


@pytest.mark.parametrize(
    ("provider", "field", "expected_model"),
    [
        ("anthropic", "anthropic_api_key", "claude-sonnet-5"),
        ("openai", "openai_api_key", "gpt-4o-mini"),
    ],
)
def test_keyed_provider_with_its_key_loads(provider, field, expected_model):
    settings = LLMSettings(provider=provider, **{field: "secret-value"})

    assert settings.default_model == expected_model
    assert settings.api_key is not None
    assert settings.api_key.get_secret_value() == "secret-value"


def test_a_blank_key_counts_as_unset():
    # .env.example ships every key present but empty, and an empty SecretStr is truthy.
    with pytest.raises(ValidationError):
        LLMSettings(provider="anthropic", anthropic_api_key="   ")


def test_ollama_needs_a_base_url():
    with pytest.raises(ValidationError) as excinfo:
        LLMSettings(provider="ollama", ollama_base_url="  ")

    assert "OLLAMA_BASE_URL" in str(excinfo.value)


def test_an_unknown_provider_is_refused():
    with pytest.raises(ValidationError):
        LLMSettings(provider="bedrock")


def test_api_key_is_not_printed_by_repr():
    settings = LLMSettings(provider="openai", openai_api_key="sk-do-not-log-me")

    assert "sk-do-not-log-me" not in repr(settings)


def test_a_node_without_an_override_uses_the_provider_default():
    settings = LLMSettings()

    assert settings.model_for_node("form_hypothesis") == "llama3.2:3b"


def test_a_node_override_wins_over_the_provider_default():
    settings = LLMSettings(node_models=NodeModelOverrides(triage="qwen2.5:0.5b"))

    assert settings.model_for_node("triage") == "qwen2.5:0.5b"
    assert settings.model_for_node("conclude") == "llama3.2:3b"


def test_a_blank_override_falls_back_rather_than_selecting_an_empty_model():
    settings = LLMSettings(node_models=NodeModelOverrides(triage="  "))

    assert settings.model_for_node("triage") == "llama3.2:3b"


def test_escalate_has_no_override_and_falls_back():
    # escalate is deliberately code-only, so it has no environment variable of its own.
    assert LLMSettings().model_for_node("escalate") == "llama3.2:3b"


def test_node_overrides_are_read_from_the_environment(monkeypatch):
    monkeypatch.setenv("LLM_MODEL_FORM_HYPOTHESIS", "llama3.1:8b")

    assert LLMSettings().model_for_node("form_hypothesis") == "llama3.1:8b"


def test_budgets_default_to_the_documented_limits():
    budgets = BudgetSettings()

    assert budgets.max_iterations == 5
    assert budgets.max_tool_calls == 20


@pytest.mark.parametrize("field", ["max_iterations", "max_tool_calls"])
def test_a_budget_of_zero_is_refused(field):
    with pytest.raises(ValidationError):
        BudgetSettings(**{field: 0})


def test_a_triage_shortcut_easier_than_a_conclusion_is_refused():
    with pytest.raises(ValidationError) as excinfo:
        BudgetSettings(triage_shortcut_confidence=0.3, min_conclude_confidence=0.8)

    assert "triage_shortcut_confidence" in str(excinfo.value)


def test_settings_compose_every_group():
    settings = Settings()

    assert settings.llm.provider == "ollama"
    assert settings.kafka.topic_failures == "airflow.task.failed"
    assert settings.airflow.password.get_secret_value() == "airflow"
    assert settings.db.dsn.startswith("postgresql+psycopg://")


def test_settings_are_loaded_once_per_process():
    assert get_settings() is get_settings()


def _documented_variables() -> set[str]:
    env_example = Path(__file__).parents[3] / ".env.example"
    names = set()
    for line in env_example.read_text().splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            names.add(stripped.split("=", 1)[0].strip())
    return names


def _known_variables() -> set[str]:
    classes = [
        Settings,
        LLMSettings,
        NodeModelOverrides,
        BudgetSettings,
        DatabaseSettings,
        AirflowSettings,
        KafkaSettings,
    ]
    names = set()
    for cls in classes:
        for field_name, field in cls.model_fields.items():
            alias = field.validation_alias
            names.add(alias if isinstance(alias, str) else field_name.upper())
    return names


def test_every_documented_variable_is_one_the_settings_actually_read():
    # .env.example is the only configuration reference a reader gets, so a variable that
    # was renamed in code and left behind here is a documentation bug that silently does
    # nothing at runtime.
    assert _documented_variables() <= _known_variables()
