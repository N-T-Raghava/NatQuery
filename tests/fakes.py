"""Shared test doubles. Plain classes (no mocking magic) so tests read clearly."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from natquery.access.policy import load_policy
from natquery.audit.log import AuditLog
from natquery.auth import hashing
from natquery.auth.local import LocalPasswordProvider
from natquery.auth.session import SessionManager
from natquery.auth.user_store import UserStore
from natquery.llm.base import LLMClient
from natquery.llm.generator import SQLGenerator
from natquery.orchestration.pipeline import QueryService

EXAMPLE_POLICY = (
    Path(__file__).resolve().parents[1] / "src/natquery/examples/policy.example.yaml"
)
PASSWORD = "Passw0rd!x1"


def _cols(**kw: str) -> dict[str, str]:
    return dict(kw)


# Mirrors examples/erp_sample.sql so enforcer tests need no database.
ERP_SCHEMA: dict[str, Any] = {
    "tables": {
        "departments": {
            "columns": _cols(dept_id="text", dept_name="text"),
            "primary_key": ["dept_id"],
            "foreign_keys": [],
            "row_count": 3,
        },
        "faculty": {
            "columns": _cols(
                emp_id="text",
                name="text",
                email="text",
                phone="text",
                department="text",
                designation="text",
            ),
            "primary_key": ["emp_id"],
            "foreign_keys": [],
            "row_count": 3,
        },
        "students": {
            "columns": _cols(
                roll_no="text",
                name="text",
                email="text",
                phone="text",
                aadhaar_no="text",
                department="text",
                year="integer",
                cgpa="numeric",
                hall="text",
                advisor_emp_id="text",
            ),
            "primary_key": ["roll_no"],
            "foreign_keys": [],
            "row_count": 6,
        },
        "courses": {
            "columns": _cols(
                course_id="text",
                title="text",
                credits="integer",
                department="text",
                instructor_emp_id="text",
                semester="text",
            ),
            "primary_key": ["course_id"],
            "foreign_keys": [],
            "row_count": 4,
        },
        "enrollments": {
            "columns": _cols(
                roll_no="text",
                course_id="text",
                semester="text",
                grade="text",
                marks="integer",
            ),
            "primary_key": ["roll_no", "course_id", "semester"],
            "foreign_keys": [],
            "row_count": 8,
        },
        "fees": {
            "columns": _cols(
                roll_no="text",
                semester="text",
                amount_due="numeric",
                amount_paid="numeric",
                status="text",
            ),
            "primary_key": [],
            "foreign_keys": [],
            "row_count": 4,
        },
        "salaries": {
            "columns": _cols(
                emp_id="text", month="text", gross="numeric", net="numeric"
            ),
            "primary_key": [],
            "foreign_keys": [],
            "row_count": 3,
        },
        "erp_users": {
            "columns": _cols(username="text", password_hash="text", role="text"),
            "primary_key": ["username"],
            "foreign_keys": [],
            "row_count": 2,
        },
    }
}


def erp_policy():
    return load_policy(EXAMPLE_POLICY)


class FakeLLM(LLMClient):
    """Returns whatever `responder(messages)` says; records every prompt it was shown."""

    def __init__(self, responder: Callable[[list[dict]], str] | str = "SELECT 1"):
        self.responder = (
            responder if callable(responder) else (lambda _m, _r=responder: _r)
        )
        self.calls: list[list[dict]] = []

    def complete(self, messages, **kw):
        self.calls.append(messages)
        return self.responder(messages)

    @property
    def prompts(self) -> str:
        return "\n".join(m["content"] for call in self.calls for m in call)


class ScriptedLLM(FakeLLM):
    """Hands out queued SQL strings in order; fails loudly if the pipeline asks for more."""

    def __init__(self, *sqls: str):
        self.queue = list(sqls)
        super().__init__(self._next)

    def _next(self, _messages):
        assert self.queue, "pipeline asked the LLM for more SQL than the test scripted"
        return self.queue.pop(0)


@dataclass
class FakeBackend:
    """Records the SQL it is asked to run and returns canned rows."""

    rows: list[dict] = field(default_factory=lambda: [{"n": 1}])
    truncated: bool = False
    dialect: str = "postgres"
    allowed_schema: str = "public"
    raise_on_execute: Exception | None = None
    executed: list[dict] = field(default_factory=list)
    closed: bool = False

    def execute_readonly(
        self, sql, *, max_rows, timeout_ms, db_role=None, session_vars=None
    ):
        self.executed.append(
            {
                "session_vars": session_vars,
                "sql": sql,
                "max_rows": max_rows,
                "timeout_ms": timeout_ms,
                "db_role": db_role,
            }
        )
        if self.raise_on_execute is not None:
            raise self.raise_on_execute
        return SimpleNamespace(
            rows=list(self.rows), truncated=self.truncated, elapsed_ms=1.5
        )

    def explain(self, sql, *, timeout_ms):
        return {
            "Plan": {
                "Node Type": "Seq Scan",
                "Total Cost": 10,
                "Plan Rows": 1,
                "Relation Name": "students",
            }
        }

    def close(self):
        self.closed = True


def make_service(
    tmp_path,
    *,
    llm=None,
    backend=None,
    policy=None,
    schema=None,
    security=None,
    users=None,
    guards=None,
    limiter=None,
):
    """Build a QueryService wired to fakes. Returns (service, ctx)."""
    policy = policy or erp_policy()
    schema = schema or ERP_SCHEMA
    llm = llm or FakeLLM()
    backend = backend or FakeBackend()
    store = UserStore(tmp_path / "auth.db")
    provider = LocalPasswordProvider({"lock_threshold": 3}, store=store)
    pw_hash = hashing.hash_password(PASSWORD)
    for username, roles, attrs in users or [
        ("alice", ["student"], {"roll_no": "21CS10001"}),
        ("rao", ["teacher"], {"emp_id": "F1001"}),
        ("root", ["admin"], {"emp_id": "F1001"}),
    ]:
        store.add(username, pw_hash, roles, attrs)
    sec = {
        "rate_limit_per_minute": 100,
        "llm_calls_per_hour": 1000,
        "max_repair_attempts": 2,
        "enable_optimizer": False,
        "log_question_text": True,
    }
    sec.update(security or {})
    sessions = SessionManager(provider, idle_seconds=600, max_seconds=3600)
    audit = AuditLog(tmp_path / "audit.log")
    kwargs = {}
    if guards is not None:
        kwargs["guards"] = guards
    if limiter is not None:
        kwargs["limiter"] = limiter
    svc = QueryService(
        backend=backend,
        policy=policy,
        schema=schema,
        generator=SQLGenerator(llm),
        audit=audit,
        sessions=sessions,
        security=sec,
        db_name="erp",
        **kwargs,
    )
    return svc, SimpleNamespace(
        llm=llm,
        backend=backend,
        store=store,
        provider=provider,
        sessions=sessions,
        audit=audit,
        policy=policy,
    )


def login(svc, username="alice", password=PASSWORD):
    return svc.login({"username": username, "password": password})
