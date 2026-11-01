"""Per-role view of the schema.

The SAME view object drives three things, so they can never disagree:
  1. what the LLM is told exists (hidden tables/columns are never in the prompt),
  2. the schema sqlglot uses to resolve columns (a hidden column simply fails to resolve),
  3. which columns get masked and which tables get row filters at enforcement time.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field

from natquery.access.policy import ALL, Policy, ResolvedRole, TableGrant


@dataclass
class SchemaView:
    role: str
    tables: dict[str, dict[str, str]] = field(
        default_factory=dict
    )  # table -> {col: type}
    masked: dict[str, dict[str, str]] = field(
        default_factory=dict
    )  # table -> {col: strategy}
    row_filters: dict[str, str] = field(default_factory=dict)  # table -> template
    prompt_schema: dict = field(
        default_factory=lambda: {"tables": {}}
    )  # schema.json shape

    @property
    def multi(self) -> bool:
        """True when table keys are 'schema.table' (multi-schema mode)."""
        return any("." in t for t in self.tables)

    def sqlglot_schema(self) -> dict:
        if not self.multi:
            return {t: dict(cols) for t, cols in self.tables.items()}
        nested: dict[str, dict[str, dict[str, str]]] = {}
        for key, cols in self.tables.items():
            ns, _, table = key.partition(".")
            nested.setdefault(ns, {})[table] = dict(cols)
        return nested

    def is_empty(self) -> bool:
        return not self.tables


def _grant_for(role: ResolvedRole, table: str) -> TableGrant | None:
    if table in role.deny_tables or _schema_wildcard(table) in role.deny_tables:
        return None
    return (
        role.tables.get(table)
        or role.tables.get(_schema_wildcard(table))  # "people.*"
        or role.tables.get(ALL)
    )


def _schema_wildcard(table: str) -> str:
    return table.split(".", 1)[0] + ".*" if "." in table else ""


def build_view(full_schema: dict, policy: Policy, role: ResolvedRole) -> SchemaView:
    view = SchemaView(role=role.name)
    src_tables = full_schema.get("tables", {})

    for tname, meta in src_tables.items():
        if policy.table_globally_denied(tname):
            continue
        grant = _grant_for(role, tname)
        if grant is None:
            continue
        all_cols = meta.get("columns", {})
        wanted = set(all_cols) if grant.columns is None else set(grant.columns)
        visible = {
            c: all_cols[c]
            for c in all_cols
            if c in wanted
            and c not in grant.deny_columns
            and not policy.column_globally_denied(tname, c)
        }
        if not visible:
            continue
        view.tables[tname] = visible
        masked = {c: s for c, s in grant.masked.items() if c in visible}
        if masked:
            view.masked[tname] = masked
        if grant.row_filter:
            view.row_filters[tname] = grant.row_filter

    # prompt-facing copy: same shape the formatter / retriever already understand
    for tname, cols in view.tables.items():
        meta = src_tables[tname]
        entry = {
            "columns": dict(cols),
            "primary_key": [c for c in meta.get("primary_key", []) if c in cols],
            "foreign_keys": [
                fk
                for fk in meta.get("foreign_keys", [])
                if fk["column"] in cols
                and fk["references"]["table"] in view.tables
                and fk["references"]["column"] in view.tables[fk["references"]["table"]]
            ],
            "masked_columns": sorted(view.masked.get(tname, {})),
        }
        for extra in (
            "description",
            "semantic_tags",
            "synonyms",
        ):  # semantic hints are safe to share
            if extra in meta:
                entry[extra] = copy.deepcopy(meta[extra])
        # Example values of low-cardinality columns: only for columns this role can see and that are
        # not masked (a masked column's values must never reach the prompt).
        hints = {
            c: list(v)
            for c, v in (meta.get("value_hints") or {}).items()
            if c in cols and c not in view.masked.get(tname, {})
        }
        if hints:
            entry["value_hints"] = hints
        view.prompt_schema["tables"][tname] = entry  # row_count deliberately dropped
    return view
