import pytest

from natquery.access.enforcer import SQLEnforcer
from natquery.access.policy import (
    ConfigError,
    PermissionDenied,
    check_row_filter,
    parse_policy,
)
from natquery.access.schema_view import build_view
from natquery.auth.models import Identity
from natquery.config.settings import Settings
from natquery.errors import QueryRejected
from natquery.schema.formatter import format_schema
from natquery.schema.retriever import retrieve_relevant_schema
from natquery.schema.store import apply_semantics, save_schema


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
                "foreign_keys": [
                    {
                        "column": "dept_id",
                        "references": {"table": "departments", "column": "id"},
                    }
                ],
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


def test_paths_and_settings_roundtrip(isolated_natquery_home, monkeypatch):
    from natquery.config.paths import base_dir, safe_name, workspace_dir

    assert base_dir() == isolated_natquery_home
    assert safe_name("prod-db_1") == "prod-db_1"
    with pytest.raises(ValueError):
        safe_name("../oops")

    db_dir = workspace_dir("demo")
    db_dir.mkdir(exist_ok=True)
    (isolated_natquery_home / "current_db").write_text("demo", encoding="utf-8")
    Settings.save_config(
        "demo",
        {
            "db_host": "localhost",
            "db_port": "5432",
            "db_name": "demo",
            "db_user": "natuser",
            "db_password": "supersecret",
            "llm_provider": "groq",
            "llm_api_key": "abc123",
            "llm_model": "llama-3.3-70b",
        },
    )

    config = Settings.get_db_config()
    assert config["dbname"] == "demo"
    assert config["password"] == "supersecret"
    assert Settings.get_llm_config()["api_key"] == "abc123"
    assert Settings.exists()

    monkeypatch.setenv("NATQUERY_DB_DSN", "postgresql://user:pw@localhost:5432/tenant")
    monkeypatch.delenv("NATQUERY_DB_PASSWORD", raising=False)
    cfg = Settings.get_db_config()
    assert cfg["dsn"].endswith("tenant")
    assert cfg["type"] == "dsn"

    from natquery.security.redaction import redact_secrets, sanitize_db_error

    redacted = redact_secrets({"password": "p@ss", "api_key": "sk-123", "safe": "ok"})
    assert redacted["password"] == "[REDACTED]"
    assert redacted["api_key"] == "[REDACTED]"
    assert sanitize_db_error("Bad input 'abc' and 'def'") == "Bad input '?' and '?'"


def test_policy_validation_and_schema_view(sample_policy, sample_schema):
    identity = Identity(
        user_id="u1",
        username="alice",
        roles=frozenset({"student"}),
        attributes={"dept": "42"},
    )
    role = sample_policy.pick_role(identity)
    assert role.name == "student"
    assert role.can("can_view_sql")

    warnings = sample_policy.validate_against_schema(sample_schema)
    assert warnings == []

    view = build_view(sample_schema, sample_policy, role)
    assert "students" in view.tables
    assert "email" in view.masked["students"]
    assert view.row_filters["students"] == "dept_id = :dept"
    assert "secret_table" not in view.tables

    with pytest.raises(PermissionDenied):
        sample_policy.pick_role(
            Identity(
                user_id="u2", username="bob", roles=frozenset({"other"}), attributes={}
            )
        )

    check_row_filter("dept_id = :dept_id", "students")
    with pytest.raises(ConfigError):
        check_row_filter("dept_id = :BadName", "students")


def test_schema_formatter_and_retriever(sample_schema):
    formatted = format_schema(sample_schema)
    assert "TABLE departments" in formatted
    assert "RELATIONSHIPS:" in formatted
    assert "students.dept_id -> departments.id" in formatted

    selected = retrieve_relevant_schema(
        "which students are in department 42", sample_schema, top_k=1, max_tables=10
    )
    assert "students" in selected["tables"]
    assert "departments" in selected["tables"]

    semantics = {
        "tables": {
            "students": {
                "description": "roster",
                "synonyms": ["pupils"],
                "semantic_tags": ["academic"],
            }
        }
    }
    merged = apply_semantics(sample_schema, semantics)
    assert merged["tables"]["students"]["description"] == "roster"

    save_schema("demo", sample_schema)
    from natquery.config.paths import workspace_dir

    assert (workspace_dir("demo") / "schema.json").exists()


def test_policy_and_enforcer_edge_cases(sample_policy, sample_schema):
    identity = Identity(
        user_id="u1",
        username="alice",
        roles=frozenset({"student"}),
        attributes={"dept": "42"},
    )
    role = sample_policy.resolve("student")
    view = build_view(sample_schema, sample_policy, role)
    enforcer = SQLEnforcer(sample_policy)

    # the globally denied column is invisible to the role and cannot be selected
    assert "ssn" not in view.tables["students"]
    with pytest.raises(QueryRejected) as hidden:
        enforcer.enforce("SELECT ssn FROM students", identity, role, view)
    assert hidden.value.reason == "unresolved_reference"
    assert sample_policy.column_globally_denied("students", "ssn")
    with pytest.raises(PermissionDenied):
        sample_policy.pick_role(
            Identity(
                user_id="u2",
                username="bob",
                roles=frozenset({"student"}),
                attributes={},
            ),
            requested="other",
        )
    with pytest.raises(PermissionDenied):
        sample_policy.resolve("missing")

    assert sample_policy.validate_against_schema(
        {"tables": {"students": {"columns": {"id": "INTEGER"}}}}
    )
    with pytest.raises(ConfigError):
        parse_policy({"version": 1, "roles": {"bad": {"inherits": ["missing"]}}})
    with pytest.raises(ConfigError):
        parse_policy(
            {
                "version": 1,
                "roles": {
                    "bad": {
                        "tables": {
                            "students": {
                                "columns": ["id"],
                                "masked": {"name": "unknown"},
                            }
                        }
                    }
                },
            }
        )
    with pytest.raises(ConfigError):
        check_row_filter("dept_id = :BadName", "students")

    with pytest.raises(QueryRejected):
        enforcer._parse("", role.limits)
    with pytest.raises(QueryRejected):
        enforcer._parse("SELECT 1; SELECT 2;", role.limits)
    with pytest.raises(PermissionDenied):
        enforcer._check_constructs(
            __import__("sqlglot").parse_one("INSERT INTO students VALUES (1)")
        )
    with pytest.raises(PermissionDenied):
        enforcer._check_functions(__import__("sqlglot").parse_one("SELECT pg_sleep(1)"))
    with pytest.raises(QueryRejected):
        enforcer._check_complexity(
            __import__("sqlglot").parse_one(
                "SELECT * FROM students a JOIN students b ON a.id = b.id JOIN students c ON a.id = c.id "
                "JOIN students d ON a.id = d.id JOIN students e ON a.id = e.id JOIN students f ON a.id = f.id "
                "JOIN students g ON a.id = g.id"
            ),
            {
                "max_ast_nodes": 50,
                "max_joins": 5,
                "allow_cross_join": False,
                "max_subquery_depth": 2,
            },
        )
    with pytest.raises(PermissionDenied):
        enforcer._bind_filter(
            "dept_id = :dept",
            Identity(
                user_id="u2",
                username="bob",
                roles=frozenset({"student"}),
                attributes={},
            ),
        )
    assert (
        enforcer._apply_limit(
            __import__("sqlglot").parse_one("SELECT * FROM students LIMIT 100"), 10
        ).sql()
        == "SELECT * FROM students LIMIT 10"
    )
    with pytest.raises(PermissionDenied):
        enforcer._recheck("DROP TABLE students")
