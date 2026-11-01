"""Turns a question (+ the CALLER'S role-filtered schema text) into SQL, and repairs it.

This module never reads the schema from disk: the schema text is always passed in
by the service after access.schema_view has removed everything the user may not see.
"""

from __future__ import annotations

import re

from natquery.errors import QueryRejected
from natquery.llm.base import LLMClient
from natquery.orchestration.error_classifier import classify_error
from natquery.prompt.builder import build_prompt, build_repair_prompt

UNANSWERABLE = "UNANSWERABLE"

_HINTS = {
    "SYNTAX_ERROR": "Fix the SQL syntax.",
    "UNDEFINED_TABLE": "Use only table names that appear in the schema.",
    "UNDEFINED_COLUMN": "Use only columns that appear in the schema. If the data is not in the schema, answer -- UNANSWERABLE.",
    "AMBIGUOUS_COLUMN": "Disambiguate columns with table aliases.",
    "TYPE_ERROR": "Fix type mismatches and operators.",
    "GROUP_BY_ERROR": "Ensure every selected column is grouped or aggregated.",
    "JOIN_ERROR": "Fix joins using the listed RELATIONSHIPS; every join needs ON.",
    "POLICY": "Follow the ACCESS RULES exactly.",
    "UNKNOWN": "Fix the query so it is valid and follows the rules.",
}
_FENCE = re.compile(r"```(?:sql)?", re.I)


def normalize_sql(text: str) -> str:
    return _FENCE.sub("", text or "").strip()


def is_unanswerable(sql: str) -> bool:
    return UNANSWERABLE in sql.upper() and "SELECT" not in sql.upper().replace(
        UNANSWERABLE, ""
    )


class SQLGenerator:
    def __init__(self, llm: LLMClient):
        self.llm = llm

    def generate(self, question: str, schema_text: str) -> str:
        sql = normalize_sql(self.llm.complete(build_prompt(question, schema_text)))
        if not sql:
            raise QueryRejected(
                internal="LLM returned empty output",
                reason="empty_generation",
                fixable=False,
            )
        return sql

    def repair(
        self, question: str, failed_sql: str, error_hint: str, schema_text: str
    ) -> str:
        kind = classify_error(error_hint)
        messages = build_repair_prompt(question, failed_sql, error_hint, schema_text)
        messages[0]["content"] += f"\n\nERROR TYPE: {kind}\nINSTRUCTION: {_HINTS[kind]}"
        sql = normalize_sql(self.llm.complete(messages))
        if not sql:
            raise QueryRejected(
                internal="LLM returned empty repair",
                reason="empty_generation",
                fixable=False,
            )
        return sql
