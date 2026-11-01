"""Secret scrubbing for logs and error text that may reach an LLM."""

from __future__ import annotations

import re
from typing import Any

_SENSITIVE_KEY = re.compile(
    r"(pass(word|wd)?|pwd|secret|api_?key|token|dsn|authorization|credential)", re.I
)
_URL_CRED = re.compile(r"(\b[a-z][a-z0-9+.-]*://[^:/\s@]+:)([^@\s]+)(@)", re.I)
_KV_SECRET = re.compile(
    r"(\b(?:password|passwd|pwd|api_?key|secret|token)\s*=\s*)(\S+)", re.I
)
_MAX_STR = 2000


def _scrub_str(value: str) -> str:
    value = _URL_CRED.sub(r"\1[REDACTED]\3", value)
    value = _KV_SECRET.sub(r"\1[REDACTED]", value)
    if len(value) > _MAX_STR:
        value = value[:_MAX_STR] + "...[truncated]"
    return value


def redact_secrets(obj: Any) -> Any:
    """Return a deep copy of *obj* with credentials masked."""
    if isinstance(obj, dict):
        return {
            k: ("[REDACTED]" if _SENSITIVE_KEY.search(str(k)) else redact_secrets(v))
            for k, v in obj.items()
        }
    if isinstance(obj, (list, tuple, set)):
        return [redact_secrets(v) for v in obj]
    if isinstance(obj, str):
        return _scrub_str(obj)
    return obj


_SQ_LITERAL = re.compile(r"'(?:[^']|'')*'")


def sanitize_db_error(message: str) -> str:
    """Make a database error safe to hand to the repair LLM.

    PostgreSQL errors can embed row values (``Key (roll_no)=(21CS10001)``,
    ``invalid input syntax ... "abc"``). Keep only the first line, blank out
    quoted literals and cap the length.
    """
    first = (message or "").strip().splitlines()[0] if message else ""
    first = _SQ_LITERAL.sub("'?'", first)
    return first[:300]
