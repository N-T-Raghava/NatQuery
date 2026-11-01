"""Database backend interface.

The rest of NatQuery only talks to this ABC, so supporting another database
(Oracle, MySQL, SQL Server ...) means adding ONE class here plus the matching
sqlglot dialect name; the auth, policy, enforcer and pipeline stay unchanged.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from natquery.errors import ConfigError


@dataclass
class QueryOutput:
    rows: list[dict]
    truncated: bool
    elapsed_ms: float


class QueryBackend(ABC):
    dialect: str = "postgres"  # sqlglot dialect used to parse/regenerate SQL
    allowed_schema: str = "public"
    # Every schema NatQuery may read. One entry = classic single-schema mode (table keys are bare
    # names); several entries = multi-schema mode (table keys are "schema.table").
    schemas: tuple[str, ...] = ("public",)

    @property
    def multi_schema(self) -> bool:
        return len(self.schemas) > 1

    @abstractmethod
    def execute_readonly(
        self,
        sql: str,
        *,
        max_rows: int,
        timeout_ms: int,
        db_role: str | None = None,
        session_vars: dict[str, str] | None = None,
    ) -> QueryOutput:
        """Run ONE already-enforced SELECT in a read-only transaction and always roll back.

        session_vars become transaction-local settings (``set_config(name, value, true)``) so that
        database row-level-security policies can see who is asking."""

    @abstractmethod
    def explain(self, sql: str, *, timeout_ms: int) -> dict:
        """Plan only (never ANALYZE: that would execute the query a second time)."""

    @abstractmethod
    def extract_schema(self) -> dict:
        """Return {"tables": {name: {columns, primary_key, foreign_keys, row_estimate}}}."""

    @abstractmethod
    def close(self) -> None: ...


_REGISTRY: dict[str, Callable[[dict[str, Any]], QueryBackend]] = {}


def register_backend(
    name: str, factory: Callable[[dict[str, Any]], QueryBackend]
) -> None:
    _REGISTRY[name] = factory


def create_backend(db_config: dict[str, Any], kind: str = "postgres") -> QueryBackend:
    if kind not in _REGISTRY:
        raise ConfigError(
            internal=f"unknown database backend '{kind}'. Known: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[kind](db_config)
