from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class SchemaEntry:
    table_name: str
    terms: frozenset[str]
    weights: dict = field(
        default_factory=dict, compare=False, hash=False
    )  # term -> weight


def _terms(value: str) -> set[str]:
    terms = {term.lower() for term in re.findall(r"[a-zA-Z0-9]+", value)}
    terms.update(
        term[:-1] for term in list(terms) if len(term) > 3 and term.endswith("s")
    )
    return terms


_NAME_W, _DESC_W, _COL_W = 3.0, 2.0, 1.0


def _add(weights: dict[str, float], words: set[str], w: float) -> None:
    for t in words:
        weights[t] = max(weights.get(t, 0.0), w)


def build_schema_index(schema: dict[str, Any]) -> list[SchemaEntry]:
    """One entry per table. A word in the table's own name or description points at the table far more
    reliably than the same word appearing as a column somewhere, so those matches weigh more.
    """
    entries = []
    for table_name, metadata in schema.get("tables", {}).items():
        weights: dict[str, float] = {}

        _add(weights, _terms(table_name.rpartition(".")[2]), _NAME_W)
        _add(weights, _terms(metadata.get("description", "")), _DESC_W)
        for synonym in metadata.get("synonyms", []):
            _add(weights, _terms(synonym), _DESC_W)
        for tag in metadata.get("semantic_tags", []):
            _add(weights, {t.lower() for t in _terms(tag)}, _DESC_W)
        for column_name in metadata.get("columns", {}):
            _add(weights, _terms(column_name), _COL_W)
        _add(
            weights,
            _terms(table_name.partition(".")[0]) if "." in table_name else set(),
            _COL_W,
        )
        entries.append(SchemaEntry(table_name, frozenset(weights), weights))
    return entries


def search_schema_memory(question: str, schema: dict[str, Any]) -> list[str]:
    """Rank tables by how well their names/columns/synonyms match the question.

    Matches are weighted by rarity (inverse document frequency): a word that appears in
    nearly every table ("id", "name", "date") counts for little, a word that points at one
    table ("tuition", "salary") counts for a lot.
    """
    query_terms = _terms(question)
    entries = build_schema_index(schema)
    n = len(entries)
    df: dict[str, int] = {}
    for entry in entries:
        for term in entry.terms:
            df[term] = df.get(term, 0) + 1
    ranked = []
    for entry in entries:
        score = sum(
            entry.weights[t] * math.log(1 + n / df[t])
            for t in query_terms & entry.terms
        )
        ranked.append((score, entry.table_name))
    return [
        table
        for score, table in sorted(ranked, key=lambda item: (-item[0], item[1]))
        if score
    ]
