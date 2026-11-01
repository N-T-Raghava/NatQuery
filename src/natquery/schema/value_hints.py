"""Collect example values of low-cardinality text columns (admin-time, generic).

A model that cannot see that a status column holds 'PAID' / 'PENDING' will guess ('paid', 'Success'),
and the query then returns nothing. Listing the few distinct values of categorical columns fixes that.

Run this as a database administrator (it reads data); the result goes into the semantics file and is
shown to the model only for columns the asking role can see and that are not masked.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

TEXT_TYPES = ("character varying", "varchar", "text", "character", "char", "bpchar")


def collect_value_hints(
    run: Callable[[str], list[tuple]],
    schema: dict[str, Any],
    *,
    max_distinct: int = 12,
    min_rows: int = 30,
    max_len: int = 24,
    skip_columns: set[str] | None = None,
) -> dict[str, dict[str, list[str]]]:
    """Return {table: {column: [values]}} for text columns with at most `max_distinct` values.

    Skips key columns, columns in `skip_columns` (e.g. secrets), long values, and tables with fewer
    than `min_rows` rows (a tiny table's distinct values are its rows, not a category list).
    `run(sql)` executes a statement and returns rows. Identifiers are quoted from the extracted schema.
    """
    skip = {c.lower() for c in (skip_columns or set())}
    personal = (
        "name",
        "email",
        "mail",
        "phone",
        "mobile",
        "address",
        "password",
        "hash",
        "token",
        "otp",
        "secret",
        "remark",
        "note",
        "reason",
        "title",
        "pwd",
        "disab",
    )
    out: dict[str, dict[str, list[str]]] = {}
    for tname, meta in schema.get("tables", {}).items():
        keys = set(meta.get("primary_key", [])) | {
            fk["column"] for fk in meta.get("foreign_keys", [])
        }
        cols = [
            c
            for c, t in meta.get("columns", {}).items()
            if c not in keys
            and c.lower() not in skip
            and not any(w in c.lower() for w in personal)
            and any(t.lower().startswith(x) for x in TEXT_TYPES)
        ]
        if not cols:
            continue
        qt = ".".join('"' + p.replace('"', '""') + '"' for p in tname.split("."))
        if int(run(f"SELECT count(*) FROM {qt}")[0][0]) < min_rows:
            continue
        for c in cols:
            qc = '"' + c.replace('"', '""') + '"'
            rows = run(
                f"SELECT DISTINCT {qc} FROM {qt} WHERE {qc} IS NOT NULL LIMIT {max_distinct + 1}"
            )
            vals = sorted(str(r[0]) for r in rows)
            if 1 <= len(vals) <= max_distinct and all(len(v) <= max_len for v in vals):
                out.setdefault(tname, {})[c] = vals
    return out
