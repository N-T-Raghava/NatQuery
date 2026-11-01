from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class QueryResponse:
    """What a caller gets back. Fields a role may not see are simply left empty."""

    ref: str  # audit reference id (quote it to an admin)
    rows: list[dict[str, Any]]
    columns: list[str]
    rows_returned: int
    truncated: bool
    execution_time_ms: float
    role: str
    generated_sql: str | None = None  # only for roles with can_view_sql
    plan_summary: dict | None = None  # only for roles with can_view_plan
    suggestions: list[dict] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
