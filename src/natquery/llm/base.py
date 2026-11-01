"""LLM provider interface + registry. Add a provider = implement `complete` and register it."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from natquery.errors import ConfigError


class LLMClient(ABC):
    @abstractmethod
    def complete(
        self, messages: list[dict], *, temperature: float = 0.0, max_tokens: int = 800
    ) -> str:
        """Return the assistant text. Raise LLMUnavailable on any provider failure."""


_REGISTRY: dict[str, Callable[[dict[str, Any]], LLMClient]] = {}


def register_llm(name: str, factory: Callable[[dict[str, Any]], LLMClient]) -> None:
    _REGISTRY[name.lower()] = factory


def create_llm(llm_config: dict[str, Any]) -> LLMClient:
    provider = (llm_config.get("provider") or "").lower()
    if provider not in _REGISTRY:
        raise ConfigError(
            internal=f"unknown LLM provider '{provider}'. Known: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[provider](llm_config)
