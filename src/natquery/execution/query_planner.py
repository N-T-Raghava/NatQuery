from __future__ import annotations

import re


def generate_rewrite_candidates(
    sql: str, schema_context: str | None = None
) -> list[str]:
    """Return conservative rewrites that preserve query semantics."""
    year_pattern = re.compile(
        r"EXTRACT\(YEAR\s+FROM\s+([\w.]+)\)\s*=\s*(\d{4})", re.IGNORECASE
    )
    candidates = []
    match = year_pattern.search(sql)
    if match:
        column, year = match.groups()
        replacement = (
            f"{column} >= DATE '{year}-01-01' AND "
            f"{column} < DATE '{int(year) + 1}-01-01'"
        )
        candidates.append(year_pattern.sub(replacement, sql, count=1))
    return candidates
