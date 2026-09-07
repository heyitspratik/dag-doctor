import sys

import httpx
import pytest

from dag_doctor.core.exceptions import ProviderUnavailableError
from dag_doctor.core.llm import build_chat_model, check_llm_health
from dag_doctor.core.settings import LLMSettings, NodeModelOverrides


def _tags_transport(*names: str) -> httpx.MockTransport:
    payload = {"models": [{"name": name} for name in names]}
    return httpx.MockTransport(lambda _request: httpx.Response(200, json=payload))


def test_ollama_model_is_built_without_a_key():
    model = build_chat_model(LLMSettings())

    assert model.model == "llama3.2:3b"


def test_a_node_gets_its_own_model():
    settings = LLMSettings(node_models=NodeModelOverrides(triage="qwen2.5:0.5b"))

    assert build_chat_model(settings, "triage").model == "qwen2.5:0.5b"
    assert build_chat_model(settings, "form_hypothesis").model == "llama3.2:3b"


def test_anthropic_model_is_built_with_its_key():
    settings = LLMSettings(provider="anthropic", anthropic_api_key="test-key")

    assert build_chat_model(settings).model == "claude-sonnet-5"


def test_openai_model_is_built_with_its_key():
    settings = LLMSettings(provider="openai", openai_api_key="test-key")

    assert build_chat_model(settings).model_name == "gpt-4o-mini"


async def test_health_passes_when_the_model_is_pulled():
    await check_llm_health(LLMSettings(), transport=_tags_transport("llama3.2:3b"))


async def test_health_checks_every_node_override_not_just_the_default():
    settings = LLMSettings(node_models=NodeModelOverrides(form_hypothesis="llama3.1:8b"))

    with pytest.raises(ProviderUnavailableError) as excinfo:
        await check_llm_health(settings, transport=_tags_transport("llama3.2:3b"))

    assert excinfo.value.details["missing"] == ["llama3.1:8b"]


async def test_an_untagged_model_name_matches_the_latest_tag():
    settings = LLMSettings(ollama_model="llama3.2")

    await check_llm_health(settings, transport=_tags_transport("llama3.2:latest"))


async def test_health_fails_with_a_pull_hint_when_the_model_is_absent():
    with pytest.raises(ProviderUnavailableError) as excinfo:
        await check_llm_health(LLMSettings(), transport=_tags_transport("mistral:7b"))

    assert "make pull-models" in excinfo.value.message
    assert excinfo.value.details["missing"] == ["llama3.2:3b"]


async def test_health_fails_with_the_base_url_when_ollama_is_unreachable():
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    with pytest.raises(ProviderUnavailableError) as excinfo:
        await check_llm_health(LLMSettings(), transport=httpx.MockTransport(refuse))

    assert "not reachable" in excinfo.value.message
    assert excinfo.value.details["base_url"] == "http://localhost:11434"


async def test_health_fails_when_ollama_returns_an_error_status():
    transport = httpx.MockTransport(lambda _request: httpx.Response(500))

    with pytest.raises(ProviderUnavailableError):
        await check_llm_health(LLMSettings(), transport=transport)


async def test_an_unexpected_tags_payload_does_not_crash_the_probe():
    transport = httpx.MockTransport(lambda _request: httpx.Response(200, json={"models": "nope"}))

    with pytest.raises(ProviderUnavailableError) as excinfo:
        await check_llm_health(LLMSettings(), transport=transport)

    assert excinfo.value.details["installed"] == []


async def test_a_keyed_provider_is_not_probed():
    # No transport is supplied, so any HTTP call at all would fail the test.
    settings = LLMSettings(provider="anthropic", anthropic_api_key="test-key")

    await check_llm_health(settings)


async def test_a_tags_payload_of_the_wrong_shape_entirely_is_tolerated():
    transport = httpx.MockTransport(lambda _request: httpx.Response(200, json=["surprise"]))

    with pytest.raises(ProviderUnavailableError) as excinfo:
        await check_llm_health(LLMSettings(), transport=transport)

    assert excinfo.value.details["installed"] == []


def test_a_missing_provider_package_is_reported_as_provider_unavailable(monkeypatch):
    # Setting the entry to None is what makes the import statement itself raise.
    monkeypatch.setitem(sys.modules, "langchain_ollama", None)

    with pytest.raises(ProviderUnavailableError) as excinfo:
        build_chat_model(LLMSettings())

    assert excinfo.value.details["provider"] == "ollama"


def test_a_keyed_provider_smuggled_past_validation_still_refuses_to_build():
    # model_construct skips validation, standing in for any future path that bypasses it.
    settings = LLMSettings.model_construct(provider="openai", openai_api_key=None)

    with pytest.raises(ProviderUnavailableError) as excinfo:
        build_chat_model(settings)

    assert "No API key" in excinfo.value.message
