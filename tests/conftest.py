"""Fixtures shared by the whole suite.

Nothing here may reach the network. No test in this repository is allowed to need an API
key or a running Ollama, so every LLM is scripted and every service is a fixture.
"""

import os

import pytest

from dag_doctor.core import settings as settings_module

#: Environment variables the settings classes read. A developer's shell, or the .env file
#: sitting in the repository root, must never be able to make a test pass or fail.
_SETTINGS_PREFIXES = (
    "APP_ENV",
    "LOG_LEVEL",
    "API_KEY",
    "LLM_",
    "OLLAMA_",
    "ANTHROPIC_",
    "OPENAI_",
    "MAX_",
    "TOOL_",
    "POSTGRES_",
    "AIRFLOW_",
    "KAFKA_",
)


@pytest.fixture(autouse=True)
def hermetic_settings(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """Isolate settings from the ambient environment and from the repository's .env.

    Changing directory is what handles the .env file: pydantic-settings resolves the
    relative env_file against the working directory, and the nested settings groups build
    themselves through default factories that would otherwise each reread it.
    """
    for name in list(os.environ):
        if name.startswith(_SETTINGS_PREFIXES):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
    settings_module.get_settings.cache_clear()
    yield
    settings_module.get_settings.cache_clear()
