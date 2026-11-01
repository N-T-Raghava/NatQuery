import json

import pytest

from natquery.access.policy import (
    BUILTIN_MASKS,
    DEFAULT_LIMITS,
    check_row_filter,
    load_policy,
    parse_policy,
)
from natquery.access.schema_view import build_view
from natquery.auth.models import Identity
from natquery.errors import ConfigError, PermissionDenied


def cfg(**roles):
    return {"roles": roles}


@pytest.mark.parametrize(
    "bad,needle",
    [
        ([], "mapping"),
        ("x", "mapping"),
        ({"version": 2}, "version"),
        ({"bogus": 1}, "unknown key"),
        ({"defaults": {"max_rowz": 1}}, "unknown key"),
        ({"global_deny": {"tabels": []}}, "unknown key"),
        ({"roles": {"a": {"tabels": {}}}}, "unknown key"),
        ({"roles": {"a": {"tables": {"t": {"colums": []}}}}}, "unknown key"),
        ({"roles": {"a": {"permissions": {"can_fly": True}}}}, "unknown key"),
        ({"roles": {"a": {"limits": {"max_rowz": 1}}}}, "unknown key"),
        ({"roles": {"a": {"tables": {"t": {"columns": "name"}}}}}, "list of strings"),
        ({"roles": {"a": {"tables": {"t": {"columns": [1]}}}}}, "list of strings"),
        (
            {"roles": {"a": {"tables": {"t": {"masked": {"c": "nope"}}}}}},
            "unknown strategy",
        ),
        ({"roles": {"a": {"tables": {"t": {"row_filter": "x = "}}}}}, "does not parse"),
        (
            {"roles": {"a": {"tables": {"t": {"row_filter": "x = :Bad"}}}}},
            "placeholder",
        ),
        ({"roles": {"a": {"tables": {"t": {"row_filter": "x = %s"}}}}}, None),
        ({"roles": {"a": {"redact": ["ghost"]}}}, "unknown pattern"),
        ({"roles": {"a": {"inherits": ["ghost"]}}}, "unknown role"),
        ({"masking": {"m": "no placeholder"}}, "{col}"),
        ({"redaction_patterns": {"p": "(unclosed"}}, "redaction_patterns"),
        ({"roles": {"a": {"inherits": ["b"]}, "b": {"inherits": ["a"]}}}, "cycle"),
        ({"roles": {"a": {"inherits": ["a"]}}}, "cycle"),
    ],
)
def test_invalid_policies_fail_loudly(bad, needle):
    with pytest.raises(ConfigError) as exc:
        parse_policy(bad)
    if needle:
        assert needle in exc.value.internal


def test_typos_in_security_file_never_silently_ignored():
    """A misspelt `deny_columns` must not quietly grant the column."""
    with pytest.raises(ConfigError):
        parse_policy(cfg(a={"tables": {"t": {"columns": "*", "deny_column": ["ssn"]}}}))


def test_empty_policy_and_empty_role_are_valid():
    p = parse_policy({})
    assert p.roles == {}
    p = parse_policy({"roles": {"a": None}})
    assert p.resolve("a").tables == {}


def test_defaults_and_limits_layering():
    p = parse_policy(
        {
            "defaults": {"max_rows": 50, "max_joins": 2},
            "roles": {"a": {"limits": {"max_rows": 5}}, "b": {}},
        }
    )
    assert (
        p.resolve("a").limits["max_rows"] == 5
        and p.resolve("a").limits["max_joins"] == 2
    )
    assert p.resolve("b").limits["max_rows"] == 50
    assert p.resolve("b").limits["max_ast_nodes"] == DEFAULT_LIMITS["max_ast_nodes"]


def test_permissions_default_to_false():
    r = parse_policy(cfg(a={"permissions": {"can_view_sql": True}})).resolve("a")
    assert (
        r.can("can_view_sql")
        and not r.can("can_manage_users")
        and not r.can("nonexistent")
    )


def test_builtin_masks_available_and_custom_override():
    p = parse_policy({"masking": {"redacted": "'X' || LEFT({col}, 0)"}})
    assert (
        p.mask_template("redacted").startswith("'X'")
        and p.mask_template("last4") == BUILTIN_MASKS["last4"]
    )


# ---------------------------------------------------------------- inheritance
def inherit_policy():
    return parse_policy(
        {
            "roles": {
                "base": {
                    "priority": 1,
                    "permissions": {"can_view_sql": True},
                    "limits": {"max_rows": 10},
                    "redact": ["p"],
                    "tables": {"t": {"columns": ["a", "b"]}, "u": {"columns": ["x"]}},
                },
                "mid": {
                    "inherits": ["base"],
                    "priority": 2,
                    "deny_tables": ["u"],
                    "tables": {"t": {"columns": ["a"]}},
                },
                "wild": {"inherits": ["base"], "tables": {"*": {"columns": "*"}}},
                "multi": {"inherits": ["base", "wild"]},
            },
            "redaction_patterns": {"p": r"\d+"},
        }
    )


def test_child_grant_overrides_parent_per_table():
    r = inherit_policy().resolve("mid")
    assert r.tables["t"].columns == frozenset({"a"})  # narrowed, not unioned


def test_parent_tables_and_permissions_flow_down():
    r = inherit_policy().resolve("mid")
    assert "u" in r.tables and "u" in r.deny_tables
    assert r.can("can_view_sql") and r.limits["max_rows"] == 10 and r.redact == ("p",)


def test_wildcard_child_supersedes_inherited_grants():
    r = inherit_policy().resolve("wild")
    assert set(r.tables) == {"*"}


def test_diamond_inheritance_resolves():
    assert inherit_policy().resolve("multi").name == "multi"


def test_deny_tables_beat_grants_in_view(erp_schema):
    p = parse_policy(
        {
            "roles": {
                "a": {"deny_tables": ["students"], "tables": {"*": {"columns": "*"}}}
            }
        }
    )
    view = build_view(erp_schema, p, p.resolve("a"))
    assert "students" not in view.tables and "courses" in view.tables


def test_unknown_role_resolve_denied():
    with pytest.raises(PermissionDenied):
        inherit_policy().resolve("ghost")


# ---------------------------------------------------------------- pick_role
def test_pick_role_highest_priority_wins_never_union():
    p = inherit_policy()
    ident = Identity("1", "u", frozenset({"base", "mid"}))
    assert p.pick_role(ident).name == "mid"
    assert p.pick_role(ident, "base").name == "base"


def test_pick_role_tiebreak_is_deterministic():
    p = parse_policy({"roles": {"b": {"priority": 1}, "a": {"priority": 1}}})
    ids = {
        p.pick_role(Identity("1", "u", frozenset({"a", "b"}))).name for _ in range(20)
    }
    assert ids == {"a"}


def test_pick_role_ignores_unknown_and_denies_when_none():
    p = inherit_policy()
    assert p.pick_role(Identity("1", "u", frozenset({"base", "ghost"}))).name == "base"
    with pytest.raises(PermissionDenied):
        p.pick_role(Identity("1", "u", frozenset({"ghost"})))
    with pytest.raises(PermissionDenied):
        p.pick_role(Identity("1", "u", frozenset()))


def test_requested_role_must_be_held_even_if_it_exists():
    p = inherit_policy()
    with pytest.raises(PermissionDenied):
        p.pick_role(Identity("1", "u", frozenset({"base"})), "mid")


# ---------------------------------------------------------------- global deny
def test_global_deny_column_patterns():
    p = parse_policy(
        {"global_deny": {"columns": ["*.password_hash", "students.ssn", "pay*.*"]}}
    )
    assert p.column_globally_denied("erp_users", "password_hash")
    assert p.column_globally_denied("students", "ssn")
    assert p.column_globally_denied("payroll", "anything")
    assert not p.column_globally_denied("students", "name")
    assert not p.column_globally_denied("other", "ssn")


def test_global_deny_applies_even_to_wildcard_grants(erp_policy, erp_schema):
    view = build_view(erp_schema, erp_policy, erp_policy.resolve("admin"))
    assert "erp_users" not in view.tables
    assert "aadhaar_no" not in view.tables["students"]
    assert "password_hash" not in str(view.prompt_schema)


# ---------------------------------------------------------------- row filter validation
@pytest.mark.parametrize(
    "ok",
    [
        "a = :roll_no",
        "a IN (SELECT x FROM t WHERE y = :emp_id)",
        "a = :x AND b = :y_2",
        "TRUE",
    ],
)
def test_row_filter_accepts(ok):
    check_row_filter(ok, "here")


@pytest.mark.parametrize("bad", ["a = ", "a = :1", "a = :UPPER", "a = $1", ")("])
def test_row_filter_rejects(bad):
    with pytest.raises(ConfigError):
        check_row_filter(bad, "here")


# ---------------------------------------------------------------- loading from disk
def test_load_yaml_and_json_equivalent(tmp_path):
    data = {"roles": {"a": {"tables": {"t": {"columns": ["x"]}}}}}
    (tmp_path / "p.json").write_text(json.dumps(data))
    import yaml

    (tmp_path / "p.yaml").write_text(yaml.safe_dump(data))
    (tmp_path / "p.yml").write_text(yaml.safe_dump(data))
    for name in ("p.json", "p.yaml", "p.yml"):
        assert load_policy(tmp_path / name).resolve("a").tables[
            "t"
        ].columns == frozenset({"x"})


def test_load_errors(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_policy(tmp_path / "missing.yaml")
    (tmp_path / "bad.yaml").write_text("roles: [unclosed")
    with pytest.raises(ConfigError, match="not valid"):
        load_policy(tmp_path / "bad.yaml")
    (tmp_path / "bad.json").write_text("{nope")
    with pytest.raises(ConfigError):
        load_policy(tmp_path / "bad.json")
    (tmp_path / "list.yaml").write_text("- a\n- b\n")
    with pytest.raises(ConfigError, match="mapping"):
        load_policy(tmp_path / "list.yaml")


def test_yaml_is_loaded_safely(tmp_path):
    (tmp_path / "evil.yaml").write_text(
        "roles: !!python/object/apply:os.system ['echo pwned']\n"
    )
    with pytest.raises(ConfigError):
        load_policy(tmp_path / "evil.yaml")


def test_example_policy_is_valid_and_matches_sample_schema(erp_policy, erp_schema):
    assert set(erp_policy.roles) == {"student", "teacher", "admin"}
    assert erp_policy.validate_against_schema(erp_schema) == []


def test_validate_against_schema_reports_typos(erp_schema):
    p = parse_policy(
        {
            "roles": {
                "a": {
                    "tables": {
                        "studentz": {"columns": ["x"]},
                        "students": {
                            "columns": ["name", "nme"],
                            "masked": {"email": "null"},
                            "deny_columns": ["zz"],
                        },
                    }
                }
            }
        }
    )
    w = "\n".join(p.validate_against_schema(erp_schema))
    assert "studentz" in w and "students.nme" in w and "students.zz" in w
    assert "masked column 'students.email' is not granted" in w
