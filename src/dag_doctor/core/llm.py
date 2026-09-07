"""The single place a chat model is constructed, for every supported provider.

Ollama is the committed default because it needs no API key and no signup, so a fresh
clone can run the quickstart at zero cost. It is not simply "OpenAI with a different
environment variable", and the differences are handled here rather than leaking into
callers: there is no key to validate, the base URL differs between host and container,
model names are not portable between providers, and the model must be pulled before first
use, which is worth detecting at startup instead of halfway through an investigation.

Models are selected per node. A node passes its own name and gets the model configured for
it, falling back to the provider default.
"""

from typing import TYPE_CHECKING

import httpx
from pydantic import SecretStr

from dag_doctor.core.exceptions import ProviderUnavailableError
from dag_doctor.core.logging import get_logger
from dag_doctor.core.models import NodeName
from dag_doctor.core.settings import LLMSettings

if TYPE_CHECKING:
    from langchain_core.language_models.chat_models import BaseChatModel

logger = get_logger(__name__)

#: How long the startup reachability probe waits before declaring Ollama down.
HEALTH_TIMEOUT_S = 5.0

#: Ollama's own default tag, appended when a configured model name carries none.
_DEFAULT_OLLAMA_TAG = "latest"

_PULL_HINT = "Start it with `make up`, then pull the model with `make pull-models`."


def build_chat_model(settings: LLMSettings, node: NodeName | None = None) -> "BaseChatModel":
    """Build the LangChain chat model for the configured provider.

    Provider packages are imported lazily so that installing the project does not force
    every provider's SDK to be importable at startup.

    Args:
        settings: Validated LLM settings; credential rules are enforced on load.
        node: The graph node the model is for, which may carry a model override. ``None``
            uses the provider default.

    Returns:
        A ready-to-use chat model.

    Raises:
        ProviderUnavailableError: If the provider package is not installed.
    """
    model = settings.model_for_node(node) if node is not None else settings.default_model
    try:
        match settings.provider:
            case "ollama":
                from langchain_ollama import ChatOllama

                return ChatOllama(
                    model=model,
                    base_url=settings.ollama_base_url,
                    temperature=settings.temperature,
                    num_predict=settings.max_output_tokens,
                )
            case "anthropic":
                from langchain_anthropic import ChatAnthropic

                return ChatAnthropic(
                    model=model,
                    anthropic_api_key=_required_key(settings),
                    temperature=settings.temperature,
                    max_tokens=settings.max_output_tokens,
                    default_request_timeout=settings.request_timeout_s,
                    max_retries=settings.max_retries,
                )
            case "openai":
                from langchain_openai import ChatOpenAI

                return ChatOpenAI(
                    model_name=model,
                    openai_api_key=_required_key(settings),
                    temperature=settings.temperature,
                    max_tokens=settings.max_output_tokens,
                    request_timeout=settings.request_timeout_s,
                    max_retries=settings.max_retries,
                )
    except ImportError as exc:
        raise ProviderUnavailableError(
            f"The {settings.provider} provider package is not installed: {exc}",
            details={"provider": settings.provider},
        ) from exc


async def check_llm_health(
    settings: LLMSettings,
    *,
    timeout_s: float = HEALTH_TIMEOUT_S,
    transport: httpx.AsyncBaseTransport | None = None,
) -> None:
    """Verify the configured provider is usable, failing fast with an actionable message.

    Only Ollama is probed. The keyed providers are validated by their credential rules at
    settings load time, and probing them would mean paying for a request on every start.

    Every model the investigation could select is checked, not just the default, so a
    missing per-node override is caught at startup rather than three nodes into a graph.

    Args:
        settings: Validated LLM settings.
        timeout_s: How long to wait for Ollama to respond.
        transport: Injected by tests so no test needs a running Ollama.

    Raises:
        ProviderUnavailableError: If Ollama is unreachable or a configured model is absent.
    """
    if settings.provider != "ollama":
        return

    tags_url = f"{settings.ollama_base_url.rstrip('/')}/api/tags"
    try:
        # Context-managed so the socket closes. A bare httpx.get leaks the connection,
        # which only shows up once something is actually listening on the port.
        async with httpx.AsyncClient(timeout=timeout_s, transport=transport) as client:
            response = await client.get(tags_url)
            response.raise_for_status()
            payload = response.json()
    except httpx.HTTPError as exc:
        raise ProviderUnavailableError(
            f"Ollama is not reachable at {settings.ollama_base_url}. {_PULL_HINT}",
            details={"provider": "ollama", "base_url": settings.ollama_base_url},
        ) from exc

    installed = _installed_model_names(payload)
    missing = sorted(_configured_model_names(settings) - installed)
    if missing:
        raise ProviderUnavailableError(
            f"Ollama models {', '.join(missing)} are not pulled. {_PULL_HINT}",
            details={"provider": "ollama", "missing": missing, "installed": sorted(installed)},
        )
    logger.debug("llm.health_ok", provider="ollama", models=sorted(installed))


def _configured_model_names(settings: LLMSettings) -> set[str]:
    """Every Ollama model this configuration could ask for, default and overrides alike."""
    names = {settings.default_model}
    overrides = settings.node_models.model_dump()
    names.update(value for value in overrides.values() if isinstance(value, str))
    return {_qualified_model_name(name) for name in names}


def _required_key(settings: LLMSettings) -> SecretStr:
    """Return the selected provider's API key.

    Settings validation already refuses to load a keyed provider without its key; this
    restates the invariant where the type checker can see it.
    """
    key = settings.api_key
    if key is None:
        raise ProviderUnavailableError(
            f"No API key configured for provider {settings.provider!r}",
            details={"provider": settings.provider},
        )
    return key


def _qualified_model_name(model: str) -> str:
    """Append Ollama's implicit ``:latest`` tag so name comparisons are exact."""
    return model if ":" in model else f"{model}:{_DEFAULT_OLLAMA_TAG}"


def _installed_model_names(payload: object) -> set[str]:
    """Extract model names from an ``/api/tags`` response, tolerating shape drift."""
    if not isinstance(payload, dict):
        return set()
    models = payload.get("models")
    if not isinstance(models, list):
        return set()
    return {
        _qualified_model_name(entry["name"])
        for entry in models
        if isinstance(entry, dict) and isinstance(entry.get("name"), str)
    }
