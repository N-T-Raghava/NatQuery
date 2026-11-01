from types import SimpleNamespace

import pytest

from natquery.access.enforcer import SQLEnforcer
from natquery.access.policy import parse_policy
from natquery.access.schema_view import build_view
from natquery.app import NatQueryApp
from natquery.audit.log import AuditLog
from natquery.auth.hashing import hash_password
from natquery.auth.local import LocalPasswordProvider
from natquery.auth.models import Identity
from natquery.auth.session import SessionManager
from natquery.auth.user_store import UserStore
from natquery.errors import PermissionDenied


@pytest.fixture
def sample_policy():
    return parse_policy(
        {
            "version": 1,
            "defaults": {"max_rows": 10, "statement_timeout_ms": 5000},
            "roles": {
                "student": {
                    "priority": 10,
                    "tables": {
                        "students": {
                            "columns": ["id", "name", "dept_id", "email", "status"],
                            "masked": {"email": "redacted"},
                            "row_filter": "dept_id = :dept",
                        },
                        "departments": {"columns": ["id", "name"]},
                    },
                    "permissions": {"can_view_sql": True, "can_view_plan": True},
                },
                "viewer": {
                    "inherits": ["student"],
                    "permissions": {"can_view_sql": False, "can_view_plan": False},
                },
            },
            "global_deny": {"tables": ["secret_table"], "columns": ["students.ssn"]},
            "masking": {
                "redacted": "CASE WHEN {col} IS NULL THEN NULL ELSE '[REDACTED]' END"
            },
        }
    )


@pytest.fixture
def sample_schema():
    return {
        "tables": {
            "students": {
                "columns": {
                    "id": "INTEGER",
                    "name": "TEXT",
                    "dept_id": "INTEGER",
                    "email": "TEXT",
                    "status": "TEXT",
                    "ssn": "TEXT",
                },
                "primary_key": ["id"],
                "description": "student roster",
            },
            "departments": {
                "columns": {"id": "INTEGER", "name": "TEXT"},
                "primary_key": ["id"],
                "description": "department details",
            },
            "secret_table": {"columns": {"id": "INTEGER"}, "primary_key": ["id"]},
        }
    }


def test_sqlenforcer_and_queryservice(sample_policy, sample_schema, tmp_path):
    role = sample_policy.resolve("student")
    view = build_view(sample_schema, sample_policy, role)
    identity = Identity(
        user_id="u1",
        username="alice",
        roles=frozenset({"student"}),
        attributes={"dept": "42"},
    )
    enforcer = SQLEnforcer(sample_policy)

    enforced = enforcer.enforce(
        "SELECT id, name FROM students WHERE dept_id = 42", identity, role, view
    )
    assert enforced.role == "student"
    assert "FROM students" in enforced.sql

    with pytest.raises(PermissionDenied):
        enforcer.enforce("SELECT pg_sleep(1)", identity, role, view)

    class FakeGenerator:
        def __init__(self):
            self.calls = 0

        def generate(self, question, schema_text):
            self.calls += 1
            return "SELECT id, name FROM students"

        def repair(self, question, failed_sql, error_hint, schema_text):
            return "SELECT id, name FROM students"

    class FakeBackend:
        dialect = "postgres"
        allowed_schema = "public"

        def execute_readonly(self, sql, *, max_rows, timeout_ms, db_role=None):
            return SimpleNamespace(
                rows=[{"id": 1, "name": "Ada"}], truncated=False, elapsed_ms=7.5
            )

        def explain(self, sql, *, timeout_ms):
            return {"Plan": {"Total Cost": 10, "Plan Rows": 1, "Node Type": "Seq Scan"}}

        def close(self):
            return None

    backend = FakeBackend()
    audit = AuditLog(path=tmp_path / "audit.log")
    provider = LocalPasswordProvider(store=UserStore(tmp_path / "auth.db"))
    provider.store.add(
        "alice", hash_password("Str0ngPass123!"), ["student"], {"dept": "42"}
    )
    identity = provider.authenticate(
        {"username": "alice", "password": "Str0ngPass123!"}
    )
    sessions = SessionManager(provider, idle_seconds=60, max_seconds=300)
    session = sessions.create(identity)
    security = {
        "rate_limit_per_minute": 10,
        "llm_calls_per_hour": 10,
        "max_repair_attempts": 0,
        "enable_optimizer": False,
        "log_question_text": True,
    }

    from natquery.orchestration.pipeline import QueryService

    service = QueryService(
        backend=backend,
        policy=sample_policy,
        schema=sample_schema,
        generator=FakeGenerator(),
        audit=audit,
        sessions=sessions,
        security=security,
        db_name="demo",
    )
    response = service.ask(session.token, "show students")
    assert response.rows[0]["name"] == "Ada"
    assert response.role == "student"
    service.close()


def test_natquery_app_wiring(sample_policy, sample_schema, tmp_path):
    class DummyProvider:
        name = "dummy"

        def credential_fields(self):
            return []

        def authenticate(self, credentials):
            return Identity(
                user_id="u5",
                username="alice",
                roles=frozenset({"student"}),
                attributes={"dept": "42"},
            )

        def refresh(self, identity):
            return identity

    class DummyGenerator:
        def generate(self, question, schema_text):
            return "SELECT 1"

        def repair(self, question, failed_sql, error_hint, schema_text):
            return "SELECT 1"

    class DummyBackend:
        dialect = "postgres"
        allowed_schema = "public"

        def execute_readonly(self, sql, *, max_rows, timeout_ms, db_role=None):
            return SimpleNamespace(rows=[{"_col": 1}], truncated=False, elapsed_ms=5.0)

        def explain(self, sql, *, timeout_ms):
            return {"Plan": {"Total Cost": 5, "Plan Rows": 1}}

        def close(self):
            return None

    provider = DummyProvider()
    sessions = SessionManager(provider, idle_seconds=3600, max_seconds=3600)
    service = __import__(
        "natquery.orchestration.pipeline", fromlist=["QueryService"]
    ).QueryService(
        backend=DummyBackend(),
        policy=sample_policy,
        schema=sample_schema,
        generator=DummyGenerator(),
        audit=AuditLog(path=tmp_path / "audit.log"),
        sessions=sessions,
        security={
            "rate_limit_per_minute": 10,
            "llm_calls_per_hour": 10,
            "max_repair_attempts": 0,
            "enable_optimizer": False,
            "log_question_text": True,
        },
        db_name="demo",
    )
    app = NatQueryApp(
        service, provider, sample_policy, AuditLog(path=tmp_path / "audit.log")
    )
    session = sessions.create(
        Identity(
            user_id="u5",
            username="alice",
            roles=frozenset({"student"}),
            attributes={"dept": "42"},
        )
    )
    response = app.ask(session, "show students")
    assert response.rows
    app.close()
