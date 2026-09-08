"""The seam between the graph and a language model.

Every node reaches a model through this one interface, which is what makes the whole
graph testable without a network call: the tests supply a scripted caller that returns
prepared answers, and the nodes cannot tell the difference. A node that cannot be driven
this way has a design problem rather than a testing problem.

Structured output is required, not requested. Nodes receive a validated Pydantic model or
a typed failure, never prose they have to parse.
"""

from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Protocol, Self

from pydantic import BaseModel, ValidationError

from dag_doctor.core.exceptions import ProviderUnavailableError
from dag_doctor.core.llm import build_chat_model
from dag_doctor.core.logging import get_logger
from dag_doctor.core.models import NodeName
from dag_doctor.core.settings import LLMSettings

if TYPE_CHECKING:
    from langchain_core.language_models.chat_models import BaseChatModel

logger = get_logger(__name__)


@dataclass(frozen=True)
class ModelCall[T: BaseModel]:
    """One model response, with what it cost.

    The cost travels with the value so the step trace can report token spend per node
    without every node threading usage accounting through its own code.
    """

    value: T
    model_used: str
    prompt_tokens: int = 0
    completion_tokens: int = 0


class ModelCaller(Protocol):
    """What a node needs from a model."""

    async def call[T: BaseModel](
        self, node: NodeName, prompt: str, output_model: type[T]
    ) -> ModelCall[T]:
        """Ask for one structured answer.

        Args:
            node: Which node is asking, so the configured per-node model is used.
            prompt: The fully rendered prompt.
            output_model: The shape the answer must take.

        Returns:
            The validated answer and what it cost.
        """
        ...


class LangChainCaller:
    """The real caller, over whichever provider is configured."""

    def __init__(self, settings: LLMSettings) -> None:
        """Initialise the caller.

        Args:
            settings: Provider, credentials, and any per-node model overrides.
        """
        self._settings = settings
        self._models: dict[str, BaseChatModel] = {}

    async def __aenter__(self) -> Self:
        """Nothing to open; present so callers can treat every caller alike."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Nothing to close."""
        return None

    async def call[T: BaseModel](
        self, node: NodeName, prompt: str, output_model: type[T]
    ) -> ModelCall[T]:
        """Ask the node's configured model for one structured answer.

        Args:
            node: Which node is asking.
            prompt: The fully rendered prompt.
            output_model: The shape the answer must take.

        Returns:
            The validated answer and what it cost.

        Raises:
            ProviderUnavailableError: If the provider failed, or answered with something
                that does not fit the requested shape even after its own retries. Small
                local models do fail this way, and a node must be told rather than handed
                a half-parsed object.
        """
        model_name = self._settings.model_for_node(node)
        chat = self._models.get(model_name)
        if chat is None:
            chat = build_chat_model(self._settings, node)
            self._models[model_name] = chat

        try:
            raw = await chat.with_structured_output(output_model).ainvoke(prompt)
        except Exception as exc:
            raise ProviderUnavailableError(
                f"The model failed while answering for {node}: {exc}",
                details={"node": node, "model": model_name},
            ) from exc

        return ModelCall(
            value=_coerce(raw, output_model, node, model_name),
            model_used=model_name,
        )


def _coerce[T: BaseModel](raw: object, output_model: type[T], node: str, model: str) -> T:
    """Turn whatever the provider returned into the requested model.

    Providers differ: some return the model instance, some a dict. Both are accepted, and
    anything else is a provider problem rather than something to paper over.
    """
    if isinstance(raw, output_model):
        return raw
    if isinstance(raw, dict):
        try:
            return output_model.model_validate(raw)
        except ValidationError as exc:
            raise ProviderUnavailableError(
                f"The model's answer for {node} did not fit {output_model.__name__}",
                details={"node": node, "model": model, "errors": exc.error_count()},
            ) from exc
    raise ProviderUnavailableError(
        f"The model returned {type(raw).__name__} for {node}, not a structured answer",
        details={"node": node, "model": model},
    )
