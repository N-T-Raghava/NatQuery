import json
from types import SimpleNamespace

import pytest

from natquery.access.policy import parse_policy
from natquery.app import NatQueryApp
from natquery.audit.log import AuditLog
from natquery.auth.local import LocalPasswordProvider
from natquery.auth.models import CredentialField, Identity
from natquery.auth.session import SessionManager
from natquery.auth.user_store import UserStore
from natquery.cli import admin, formatter, shell
from natquery.cli.commands import (
    connect_command,
    extract_db_name_from_dsn,
    reset_command,
    select_database_command,
    show_config_command,
)
from natquery.cli.main import _build_parser, main
from natquery.errors import QueryRejected
from natquery.orchestration.types import QueryResponse


def test_package_registry_and_llm_factory():
    import natquery
    from natquery.errors import ConfigError
    from natquery.llm.base import LLMClient, create_llm, register_llm

    assert natquery.NatQuery is NatQueryApp

    class FakeLLM(LLMClient):
        def complete(self, messages, *, temperature=0.0, max_tokens=800):
            return "ok"

    register_llm("dummy", lambda cfg: FakeLLM())
    assert isinstance(create_llm({"provider": "dummy"}), FakeLLM)
    with pytest.raises(ConfigError):
        create_llm({"provider": "missing"})


def test_admin_helpers_formatter_and_shell(monkeypatch, tmp_path):
    assert admin._parse_attrs(["dept=cs", "roll_no=42", "team=alpha-1"]) == {
        "dept": "cs",
        "roll_no": "42",
        "team": "alpha-1",
    }
    with pytest.raises(ValueError):
        admin._parse_attrs(["not_valid"])

    monkeypatch.setattr(admin, "getpass", lambda prompt="": "StrongPass123!")
    monkeypatch.setattr(
        admin.hashing, "check_password_strength", lambda pw, username: None
    )
    assert admin._new_password("alice") == "StrongPass123!"

    monkeypatch.setenv("NATQUERY_HOME", str(tmp_path))
    monkeypatch.setattr(
        admin.Settings,
        "get_security_config",
        lambda: {"policy_file": str(tmp_path / "policy.yaml")},
    )
    monkeypatch.setattr(admin.Settings, "get_db_config", lambda: {"dbname": "demo"})
    assert admin.policy_init_command() == 0
    assert (tmp_path / "policy.yaml").exists()
    assert (tmp_path / "demo" / "semantics.json").exists()

    local_provider = LocalPasswordProvider(store=UserStore(tmp_path / "auth.db"))
    assert admin._local(local_provider) is local_provider
    assert admin._local(SimpleNamespace(name="ldap")) is None

    formatter.print_table([{"id": 1, "name": "Ada"}])
    formatter.print_table([], title="empty")

    class FakeProvider:
        name = "local"

        def credential_fields(self):
            return [
                CredentialField("username", "Username"),
                CredentialField("password", "Password", secret=True),
            ]

        def authenticate(self, credentials):
            return Identity(
                user_id="u1",
                username="alice",
                roles=frozenset({"student"}),
                attributes={},
            )

    class FakeService:
        def role_for(self, token, requested=None):
            return SimpleNamespace(name="student", can=lambda permission: True)

    class FakeApp:
        provider = FakeProvider()
        policy = SimpleNamespace(roles={"student": object()})
        service = FakeService()

        def login(self, credentials):
            return SimpleNamespace(
                identity=Identity(
                    user_id="u1",
                    username="alice",
                    roles=frozenset({"student"}),
                    attributes={},
                ),
                token="t1",
            )

        def logout(self, session):
            return None

        def ask(self, session, question, role=None):
            return QueryResponse(
                ref="abc",
                rows=[{"id": 1, "name": "Ada"}],
                columns=["id", "name"],
                rows_returned=1,
                truncated=False,
                execution_time_ms=12.3,
                role="student",
                generated_sql="SELECT 1",
                plan_summary={
                    "total_cost": 7.5,
                    "planning_time_ms": 2.0,
                    "plan_rows": 1,
                },
                suggestions=[
                    {"type": "index", "reason": "faster", "sql": "CREATE INDEX"}
                ],
                notes=["ok"],
            )

    fake_app = FakeApp()
    shell.show_banner()
    shell._show_error(QueryRejected("nope"))
    shell.format_plan_output(
        {"total_cost": 5.0, "planning_time_ms": 2.0, "plan_rows": 1},
        [{"type": "index", "reason": "faster"}],
    )
    shell.format_plan_output({}, [])

    values = iter(["alice", "secret", "/whoami", "/sql", "/plan", "exit"])
    monkeypatch.setattr("builtins.input", lambda *args, **kwargs: next(values))
    monkeypatch.setattr(
        shell, "console", SimpleNamespace(input=lambda prompt="": next(values))
    )
    shell.start_shell(fake_app)


def test_natquery_app_direct_construction_and_cli(monkeypatch, tmp_path):
    from natquery.auth.base import AuthProvider

    class DummyProvider(AuthProvider):
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

    policy = parse_policy(
        {
            "version": 1,
            "roles": {
                "student": {
                    "tables": {"students": {"columns": ["id"]}},
                    "permissions": {"can_view_sql": True, "can_view_plan": True},
                }
            },
            "defaults": {"max_rows": 10, "statement_timeout_ms": 1000},
        }
    )
    schema = {
        "tables": {"students": {"columns": {"id": "INTEGER"}, "primary_key": ["id"]}}
    }
    audit = AuditLog(path=tmp_path / "audit.log")
    provider = DummyProvider()
    sessions = SessionManager(provider, idle_seconds=3600, max_seconds=3600)

    from natquery.orchestration.pipeline import QueryService

    service = QueryService(
        backend=DummyBackend(),
        policy=policy,
        schema=schema,
        generator=DummyGenerator(),
        audit=audit,
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
    app = NatQueryApp(service, provider, policy, audit)
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

    parser = _build_parser()
    assert parser.parse_args(["connect", "postgresql://user:pass@localhost/db"])
    assert parser.parse_args(["audit", "tail", "-n", "5"])

    monkeypatch.setattr(
        "natquery.cli.commands.Settings.save_config", lambda db_name, config: None
    )
    monkeypatch.setattr(
        "natquery.cli.commands.paths.write_private", lambda path, data: None
    )
    monkeypatch.setattr("natquery.cli.commands.paths.base_dir", lambda: tmp_path)
    monkeypatch.setattr(
        "natquery.cli.commands._ask_workspace_name", lambda suggested="": "demo"
    )
    monkeypatch.setattr("builtins.input", lambda *args, **kwargs: "demo")
    monkeypatch.setattr("natquery.cli.commands.getpass", lambda *args, **kwargs: "")
    connect_command("postgresql://user:pass@localhost/demo")

    assert extract_db_name_from_dsn("postgresql://user:pass@localhost/mydb") == "mydb"
    assert extract_db_name_from_dsn("dbname=mydb host=localhost") == "mydb"

    current = tmp_path / "current_db"
    current.write_text("demo", encoding="utf-8")
    (tmp_path / "demo").mkdir(exist_ok=True)
    (tmp_path / "demo" / "config.json").write_text(
        json.dumps({"db_name": "demo"}), encoding="utf-8"
    )
    monkeypatch.setattr("builtins.input", lambda *args, **kwargs: "1")
    select_database_command()

    (tmp_path / "demo" / "config.json").write_text(
        json.dumps({"db_name": "demo"}), encoding="utf-8"
    )
    monkeypatch.setattr(
        "natquery.cli.commands.Settings.load_config",
        lambda: {"db_name": "demo", "security": {}},
    )
    monkeypatch.setattr("builtins.print", lambda *args, **kwargs: None)
    show_config_command()

    monkeypatch.setattr(
        "natquery.cli.commands.Settings.workspace", lambda: tmp_path / "demo"
    )
    (tmp_path / "demo" / "config.json").write_text("{}", encoding="utf-8")
    reset_command()

    args = parser.parse_args(["user", "add", "alice", "--roles", "student"])
    assert args.user_cmd == "add"
    assert main.__name__ == "main"
