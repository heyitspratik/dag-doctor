"""Fixtures shared by the whole suite.

Nothing here may reach the network. No test in this repository is allowed to need an
API key or a running Ollama, so every LLM is scripted and every service is a fixture.
"""

import pytest


@pytest.fixture(autouse=True)
def _no_provider_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip provider keys so a developer's shell cannot make a test pass by accident."""
    for name in ("ANTHROPIC_API_KEY", "OPENAI_API_KEY"):
        monkeypatch.delenv(name, raising=False)
