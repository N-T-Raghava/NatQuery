from __future__ import annotations

from typing import Any

from groq import Groq

from natquery.errors import LLMUnavailable
from natquery.llm.base import LLMClient, register_llm


class GroqClient(LLMClient):
    def __init__(self, config: dict[str, Any]):
        # bounded wait + a single retry: a hung provider must not hang the terminal
        self._client = Groq(api_key=config["api_key"], timeout=30.0, max_retries=1)
        self._model = config["model"]
        # Reasoning models (gpt-oss) spend max_tokens on hidden reasoning first; with the
        # generic 800-token budget harder questions end with EMPTY output (finish=length).
        self._reasoning = "gpt-oss" in self._model
        self._effort = config.get("reasoning_effort") or (
            "low" if self._reasoning else None
        )
        self._min_tokens = int(
            config.get("min_tokens") or (4096 if self._reasoning else 0)
        )

    def complete(self, messages, *, temperature=0.0, max_tokens=800) -> str:
        extra = {"reasoning_effort": self._effort} if self._effort else {}
        try:
            resp = self._client.chat.completions.create(
                model=self._model,
                messages=messages,
                temperature=temperature,
                max_tokens=max(max_tokens, self._min_tokens),
                **extra,
            )
            choice = resp.choices[0]
            text = choice.message.content or ""
            if not text and getattr(choice, "finish_reason", None) == "length":
                raise RuntimeError(
                    "output budget exhausted before any SQL was produced"
                )
            return text
        except Exception as e:
            raise LLMUnavailable(internal=f"{type(e).__name__}: {str(e)[:200]}") from e


register_llm("groq", lambda cfg: GroqClient(cfg))
