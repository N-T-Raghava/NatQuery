"""Last line of defence on the way OUT: runs on rows after the DB returns them.

The enforcer decides what may be queried; this layer catches what slipped through
anyway (an ID number sitting inside a free-text column, a binary blob, a runaway
cell) and redacts it using the role's configured patterns.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any

REDACTED = "[REDACTED]"
MAX_CELL_CHARS = 2000


class ResultGuard:
    def __init__(self, patterns: Mapping[str, str], names: Sequence[str] = ()):
        self._rx = [re.compile(patterns[n]) for n in names if n in patterns]

    def _clean(self, value: Any) -> tuple[Any, int]:
        if value is None or isinstance(value, bool):
            return value, 0
        if isinstance(value, (bytes, bytearray, memoryview)):
            return f"<binary {len(value)} bytes>", 0
        if isinstance(value, (str, int, Decimal)):
            text = value if isinstance(value, str) else str(value)
            hits = 0
            for rx in self._rx:
                text, n = rx.subn(REDACTED, text)
                hits += n
            if hits:
                return text, hits
            if isinstance(value, str) and len(value) > MAX_CELL_CHARS:
                return value[:MAX_CELL_CHARS] + "...[truncated]", 0
        return value, 0

    def apply(self, rows: list[dict]) -> tuple[list[dict], int]:
        """Return (clean_rows, number_of_redactions)."""
        total = 0
        out = []
        for row in rows:
            clean = {}
            for k, v in row.items():
                clean[k], n = self._clean(v)
                total += n
            out.append(clean)
        return out, total
