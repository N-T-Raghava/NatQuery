from __future__ import annotations

from collections import deque
from typing import Any

from natquery.schema.knowledge_base import search_schema_memory


def _related_tables(schema: dict[str, Any], selected: set[str]) -> set[str]:
    """One hop along foreign keys in either direction."""
    related = set(selected)
    for table_name, metadata in schema.get("tables", {}).items():
        for foreign_key in metadata.get("foreign_keys", []):
            target = foreign_key.get("references", {}).get("table")
            if table_name in selected or target in selected:
                related.update({table_name, target})
    return {table for table in related if table}


def _graph(schema: dict[str, Any]) -> dict[str, set[str]]:
    tables = schema.get("tables", {})
    graph: dict[str, set[str]] = {name: set() for name in tables}
    for name, metadata in tables.items():
        for fk in metadata.get("foreign_keys", []):
            target = fk.get("references", {}).get("table")
            if target in graph:
                graph[name].add(target)
                graph[target].add(name)
    return graph


def _bridge_tables(
    schema: dict[str, Any], core: list[str], max_hops: int = 3
) -> set[str]:
    """Tables that connect the chosen tables to each other along foreign keys.

    A question about "customers" and "transactions" often needs the table between them even
    though no word in the question names it; one-hop neighbours alone miss longer chains.
    """
    graph = _graph(schema)
    bridges: set[str] = set()
    for i, start in enumerate(core):
        if start not in graph:
            continue
        goals = {c for c in core[i + 1 :] if c in graph}
        if not goals:
            continue
        seen = {start: None}
        queue = deque([(start, 0)])
        while queue:
            node, depth = queue.popleft()
            if depth == max_hops:
                continue
            for nxt in sorted(graph[node]):
                if nxt in seen:
                    continue
                seen[nxt] = node
                if nxt in goals:
                    walk = seen[nxt]
                    while walk is not None and walk != start:
                        bridges.add(walk)
                        walk = seen[walk]
                queue.append((nxt, depth + 1))
    return bridges


def _column_count(schema: dict[str, Any]) -> int:
    return sum(len(m.get("columns", {})) for m in schema.get("tables", {}).values())


def retrieve_relevant_schema(
    question: str,
    schema: dict[str, Any],
    top_k: int = 5,
    max_tables: int = 20,
    full_if_columns: int = 0,
    lookup_max_columns: int = 8,
) -> dict[str, Any]:
    """Pick the tables most relevant to *question* (+ FK neighbours and bridges), bounded in size.

    If the whole schema is small (at most `full_if_columns` columns and `max_tables` tables) it
    is returned unchanged: pruning a small schema saves little and risks dropping a table the
    question needs.

    Operates on the caller's role view, so it can never surface a hidden table.
    """
    tables = schema.get("tables", {})
    if (
        full_if_columns
        and len(tables) <= max_tables
        and _column_count(schema) <= full_if_columns
    ):
        return {**schema, "tables": {n: tables[n] for n in sorted(tables)}}
    ranked = search_schema_memory(question, schema)
    if ranked:
        core = ranked[:top_k]
        selected = _related_tables(schema, set(core)) | _bridge_tables(schema, core)
        if len(selected) > max_tables:
            # Keep the best-ranked tables, then fill with the neighbours that connect to the most core
            # tables, then by their own rank. (Alphabetical filling used to drop whole schemas such as
            # org.* and people.* just because "acad" sorts first.)
            graph = _graph(schema)
            rank = {t: i for i, t in enumerate(ranked)}
            rest = sorted(
                selected - set(core),
                key=lambda t: (
                    -len(graph.get(t, set()) & set(core)),
                    rank.get(t, len(rank)),
                    t,
                ),
            )
            selected = set((core + rest)[:max_tables])
        # Small lookup tables (department, program, session ...) that a chosen table points at: cheap in
        # tokens and the usual way to turn an id into a name or to filter by "CS" / "this semester".
        for name in sorted(selected):
            for fk in tables.get(name, {}).get("foreign_keys", []):
                target = fk.get("references", {}).get("table")
                if (
                    target in tables
                    and target not in selected
                    and len(tables[target].get("columns", {})) <= lookup_max_columns
                ):
                    selected.add(target)
    else:
        selected = set(sorted(tables)[:max_tables])
    return {**schema, "tables": {n: tables[n] for n in sorted(selected) if n in tables}}
