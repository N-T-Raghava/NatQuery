"""PostgreSQL backend: pooled, read-only, time- and row-bounded."""

from __future__ import annotations

import re
import threading
import time
from typing import Any

import psycopg2
from psycopg2 import errors as pgerr
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

from natquery.backends.base import QueryBackend, QueryOutput, register_backend
from natquery.errors import ConfigError, ExecutionFailed
from natquery.observability.logger import NatQueryLogger
from natquery.security.redaction import sanitize_db_error

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
_GUC = re.compile(
    r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$"
)  # custom setting: prefix.name


class PostgresBackend(QueryBackend):
    dialect = "postgres"

    def __init__(
        self,
        db_config: dict[str, Any],
        min_conn: int = 1,
        max_conn: int = 10,
        acquire_timeout: float = 10.0,
    ):
        self.db_name = db_config.get("dbname", "unknown_db")
        schemas = tuple(db_config.get("schemas") or ("public",))
        for s in schemas:
            if not _IDENT.fullmatch(s):
                raise ConfigError(internal=f"invalid schema name '{s}'")
        self.schemas = schemas
        self.allowed_schema = schemas[0]
        # Back-pressure: callers queue for a connection instead of failing when the pool is busy.
        self._slots = threading.BoundedSemaphore(max_conn)
        self._acquire_timeout = acquire_timeout
        # default_transaction_read_only is a belt-and-braces server-side setting in
        # addition to the read-only transactions opened per query.
        options = "-c default_transaction_read_only=on -c idle_in_transaction_session_timeout=15000"
        try:
            if db_config["type"] == "dsn":
                self._pool = ThreadedConnectionPool(
                    min_conn,
                    max_conn,
                    db_config["dsn"],
                    connect_timeout=5,
                    options=options,
                    application_name="natquery",
                )
            else:
                args = dict(
                    host=db_config["host"],
                    port=db_config["port"],
                    dbname=db_config["dbname"],
                    user=db_config["user"],
                    password=db_config["password"],
                    connect_timeout=5,
                    options=options,
                    application_name="natquery",
                )
                if db_config.get("sslmode"):
                    args["sslmode"] = db_config["sslmode"]
                self._pool = ThreadedConnectionPool(min_conn, max_conn, **args)
        except psycopg2.OperationalError as e:
            NatQueryLogger.log_event(
                "ERROR", "db_connection_failed", self.db_name, details={"error": str(e)}
            )
            raise ConfigError(
                "Cannot reach the database. Contact an administrator.",
                internal=f"connect failed: {e}",
            ) from e
        self._lock = threading.Lock()
        NatQueryLogger.log_event("INFO", "db_pool_ready", self.db_name)

    # ------------------------------------------------------------------ helpers
    def _checkout(self):
        if not self._slots.acquire(timeout=self._acquire_timeout):
            raise ExecutionFailed(
                "The system is busy right now. Please retry in a moment.",
                internal="connection pool wait timed out",
                reason="pool_timeout",
            )
        try:
            conn = self._pool.getconn()
            conn.rollback()
            conn.set_session(readonly=True, autocommit=False)
            return conn
        except Exception:
            self._slots.release()
            raise

    def _release(self, conn) -> None:
        try:
            conn.rollback()  # NEVER commit: nothing this tool runs may persist
        finally:
            try:
                self._pool.putconn(conn)
            finally:
                self._slots.release()

    @staticmethod
    def _setup_txn(
        cur,
        timeout_ms: int,
        db_role: str | None,
        session_vars: dict[str, str] | None = None,
    ) -> None:
        cur.execute(f"SET LOCAL statement_timeout = {int(timeout_ms)}")
        cur.execute("SET LOCAL lock_timeout = 2000")
        # Set the caller's identity BEFORE dropping to the restricted role (RLS policies read it).
        for name, value in (session_vars or {}).items():
            if not _GUC.fullmatch(name):
                raise ConfigError(internal=f"invalid session variable '{name}'")
            cur.execute("SELECT set_config(%s, %s, true)", (name, str(value)))
        if db_role:
            if not _IDENT.fullmatch(db_role):
                raise ConfigError(internal=f"invalid db_role '{db_role}'")
            cur.execute(f'SET LOCAL ROLE "{db_role}"')

    @staticmethod
    def _wrap(e: Exception) -> ExecutionFailed:
        if isinstance(e, pgerr.QueryCanceled):
            return ExecutionFailed(
                "The query took too long and was cancelled. Try a narrower question.",
                internal=str(e),
                reason="timeout",
            )
        code = getattr(e, "pgcode", "") or ""
        if code.startswith(
            "42"
        ):  # syntax error / access rule: SQL-level, LLM may repair
            return ExecutionFailed(
                internal=str(e),
                reason="sql_error",
                fixable=True,
                db_hint=sanitize_db_error(str(e)),
            )
        if isinstance(e, psycopg2.OperationalError):
            return ExecutionFailed(
                "The database is temporarily unavailable.",
                internal=str(e),
                reason="db_unavailable",
            )
        return ExecutionFailed(internal=str(e), reason="db_error")

    # ---------------------------------------------------------------- interface
    def execute_readonly(
        self, sql, *, max_rows, timeout_ms, db_role=None, session_vars=None
    ) -> QueryOutput:
        conn = self._checkout()
        started = time.time()
        try:
            with conn.cursor() as cur:
                self._setup_txn(cur, timeout_ms, db_role, session_vars)
            # server-side cursor: at most max_rows+1 rows ever cross the wire
            with conn.cursor(name="nq_cur", cursor_factory=RealDictCursor) as cur:
                cur.itersize = min(max_rows + 1, 1000)
                cur.execute(sql)
                rows = [dict(r) for r in cur.fetchmany(max_rows + 1)]
            truncated = len(rows) > max_rows
            return QueryOutput(
                rows[:max_rows], truncated, (time.time() - started) * 1000
            )
        except ConfigError:
            raise
        except psycopg2.Error as e:
            raise self._wrap(e) from e
        finally:
            self._release(conn)

    def explain(self, sql, *, timeout_ms) -> dict:
        conn = self._checkout()
        try:
            with conn.cursor() as cur:
                self._setup_txn(cur, timeout_ms, None)
                cur.execute(f"EXPLAIN (FORMAT JSON) {sql}")
                result = cur.fetchone()[0]
            return result[0] if isinstance(result, list) else result
        except psycopg2.Error as e:
            raise self._wrap(e) from e
        finally:
            self._release(conn)

    def extract_schema(self) -> dict:
        schemas = list(self.schemas)
        multi = len(schemas) > 1

        def key(ns: str, table: str) -> str:
            return f"{ns}.{table}" if multi else table

        conn = self._checkout()
        try:
            cur = conn.cursor()
            out: dict[str, Any] = {"tables": {}}
            tables = out["tables"]
            cur.execute(
                """SELECT n.nspname, c.relname, a.attname, pg_catalog.format_type(a.atttypid, NULL)
                   FROM pg_class c
                   JOIN pg_namespace n ON n.oid = c.relnamespace
                   JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                   WHERE n.nspname = ANY(%s) AND c.relkind IN ('r', 'v', 'm', 'p')
                   ORDER BY n.nspname, c.relname, a.attnum""",
                (schemas,),
            )
            for ns, table, col, dtype in cur.fetchall():
                entry = tables.setdefault(
                    key(ns, table),
                    {
                        "columns": {},
                        "primary_key": [],
                        "foreign_keys": [],
                        "row_estimate": None,
                    },
                )
                entry["columns"][col] = dtype

            # pg_catalog (not information_schema): information_schema hides tables, columns and
            # constraints from accounts without privileges on them, which is exactly the login
            # NatQuery should run as (it only gets SELECT after SET ROLE).
            cur.execute(
                """SELECT n.nspname, cl.relname, a.attname
                   FROM pg_constraint con
                   JOIN pg_class cl ON cl.oid = con.conrelid
                   JOIN pg_namespace n ON n.oid = cl.relnamespace
                   CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord)
                   JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
                   WHERE con.contype = 'p' AND n.nspname = ANY(%s)
                   ORDER BY n.nspname, cl.relname, k.ord""",
                (schemas,),
            )
            for ns, table, col in cur.fetchall():
                if key(ns, table) in tables:
                    tables[key(ns, table)]["primary_key"].append(col)

            cur.execute(
                """SELECT n.nspname, cl.relname, a.attname, fn.nspname, fcl.relname, fa.attname
                   FROM pg_constraint con
                   JOIN pg_class cl  ON cl.oid  = con.conrelid
                   JOIN pg_namespace n ON n.oid = cl.relnamespace
                   JOIN pg_class fcl ON fcl.oid = con.confrelid
                   JOIN pg_namespace fn ON fn.oid = fcl.relnamespace
                   CROSS JOIN LATERAL unnest(con.conkey, con.confkey) AS k(attnum, fattnum)
                   JOIN pg_attribute a  ON a.attrelid  = con.conrelid  AND a.attnum  = k.attnum
                   JOIN pg_attribute fa ON fa.attrelid = con.confrelid AND fa.attnum = k.fattnum
                   WHERE con.contype = 'f' AND n.nspname = ANY(%s)""",
                (schemas,),
            )
            for ns, table, col, fns, ftable, fcol in cur.fetchall():
                if key(ns, table) in tables and fns in schemas:
                    tables[key(ns, table)]["foreign_keys"].append(
                        {
                            "column": col,
                            "references": {"table": key(fns, ftable), "column": fcol},
                        }
                    )

            # planner estimate instead of COUNT(*): O(1) per table, safe on huge ERP tables
            cur.execute(
                """SELECT n.nspname, c.relname, GREATEST(c.reltuples, 0)::bigint
                   FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                   WHERE c.relkind IN ('r', 'p') AND n.nspname = ANY(%s)""",
                (schemas,),
            )
            for ns, table, est in cur.fetchall():
                if key(ns, table) in tables:
                    tables[key(ns, table)]["row_estimate"] = est
            cur.close()
            return out
        finally:
            self._release(conn)

    def close(self) -> None:
        self._pool.closeall()


register_backend("postgres", lambda cfg: PostgresBackend(cfg))
