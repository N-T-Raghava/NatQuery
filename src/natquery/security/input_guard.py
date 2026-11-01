"""Input screening BEFORE any text reaches the LLM.

These are heuristics - a speed bump and an audit signal, not the security
boundary. The real boundary is access.enforcer, which assumes the LLM may
already have been talked into producing hostile SQL.

Add your own guard by subclassing InputGuard and passing it to the service.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod

from natquery.errors import InputRejected


class InputGuard(ABC):
    @abstractmethod
    def check(self, text: str) -> None:
        """Raise InputRejected(reason=...) to refuse the input."""


class LengthAndCharsGuard(InputGuard):
    def __init__(self, max_chars: int = 1000):
        self.max_chars = max_chars

    def check(self, text: str) -> None:
        if not text or not text.strip():
            raise InputRejected("Please type a question.", reason="empty_input")
        if len(text) > self.max_chars:
            raise InputRejected(
                f"Please keep questions under {self.max_chars} characters.",
                reason="too_long",
            )
        if any(ord(c) < 32 and c not in "\n\t" for c in text):
            raise InputRejected(reason="control_chars")


_PATTERNS = [
    (
        r"ignore\s+(all\s+|any\s+|the\s+)?(previous|prior|above|earlier|your)\s+(instructions?|rules?|prompts?)",
        "override_instructions",
    ),
    (
        r"disregard\s+(all\s+|any\s+|the\s+)?(previous|prior|above|your)?\s*(instructions?|rules?|polic(y|ies)|restrictions?)",
        "override_instructions",
    ),
    (
        r"(reveal|show|print|repeat|leak)\s+.{0,30}(system\s+prompt|your\s+instructions|hidden\s+prompt|api[\s_-]?key|password)",
        "prompt_extraction",
    ),
    (
        r"\b(you\s+are\s+now|act\s+as|pretend\s+(to\s+be|you)|developer\s+mode|jailbreak|dan\s+mode)\b",
        "role_hijack",
    ),
    (
        r"\b(bypass|override|disable|turn\s+off)\s+.{0,20}(security|polic(y|ies)|restrictions?|permissions?|filters?|access\s+control)",
        "bypass_attempt",
    ),
    (
        r"\b(information_schema|pg_catalog|pg_shadow|pg_roles|pg_user|pg_authid|pg_read_file|pg_sleep|dblink|lo_import)\b",
        "system_object",
    ),
    (
        r";\s*(drop|delete|update|insert|alter|truncate|grant|create)\b",
        "stacked_statement",
    ),
    (r"(^|\s)(drop|truncate)\s+(table|database|schema)\b", "destructive_sql"),
    (r"\bunion\s+select\b", "sqli_pattern"),
]
_COMPILED = [(re.compile(p, re.I | re.S), reason) for p, reason in _PATTERNS]


class InjectionPatternGuard(InputGuard):
    def check(self, text: str) -> None:
        for rx, reason in _COMPILED:
            if rx.search(text):
                raise InputRejected(
                    "That request looks like an attempt to bypass safety rules, so it was not processed. "
                    "If this is a mistake, rephrase your question about the data.",
                    internal=f"matched '{reason}'",
                    reason=reason,
                )


def default_guards() -> list[InputGuard]:
    return [LengthAndCharsGuard(), InjectionPatternGuard()]
