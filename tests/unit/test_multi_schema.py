"""Multi-schema mode: table keys are 'schema.table'; grants, masks, row filters and the enforcer follow."""

import pytest

from natquery.access.enforcer import SQLEnforcer
from natquery.access.policy import parse_policy
from natquery.access.schema_view import build_view
from natquery.auth.models import Identity
from natquery.errors import NatQueryError, PermissionDenied, QueryRejected

SCHEMA = {
    "tables": {
        "people.student": {
            "columns": {
                "student_id": "integer",
                "roll_no": "text",
                "mobile_no": "text",
                "dept": "text",
            },
            "primary_key": ["student_id"],
            "foreign_keys": [],
        },
        "acad.result": {
            "columns": {
                "result_id": "integer",
                "student_id": "integer",
                "grade": "text",
            },
            "primary_key": ["result_id"],
            "foreign_keys": [
                {
                    "column": "student_id",
                    "references": {"table": "people.student", "column": "student_id"},
                }
            ],
        },
        "auth.account": {
            "columns": {
                "account_id": "integer",
                "login_id": "text",
                "password_hash": "text",
            },
            "primary_key": ["account_id"],
            "foreign_keys": [],
        },
        "fin.payment": {
            "columns": {
                "payment_id": "integer",
                "student_id": "integer",
                "amount": "numeric",
            },
            "primary_key": ["payment_id"],
            "foreign_keys": [],
        },
        "lib.book": {
            "columns": {"book_id": "integer", "title": "text"},
            "primary_key": ["book_id"],
            "foreign_keys": [],
        },
        "gh.book": {
            "columns": {"book_id": "integer", "guest": "text"},
            "primary_key": ["book_id"],
            "foreign_keys": [],
        },
    }
}
POLICY = {
    "global_deny": {
        "tables": ["auth.*"],
        "columns": ["*.password_hash", "*.mobile_no"],
    },
    "db_session_vars": {"erp.person_id": "student_id"},
    "roles": {
        "student": {
            "tables": {
                "people.student": {
                    "columns": "*",
                    "row_filter": "student_id = :student_id",
                },
                "acad.result": {
                    "columns": "*",
                    "row_filter": "student_id = :student_id",
                },
                "lib.*": {"columns": "*"},
            }
        },
        "accounts": {
            "tables": {
                "fin.*": {"columns": "*"},
                "people.student": {"columns": ["student_id", "roll_no"]},
            }
        },
    },
}
ALICE = Identity("u1", "alice", frozenset({"student"}), {"student_id": "42"})
ACC = Identity("u2", "acct", frozenset({"accounts"}), {})


@pytest.fixture
def run():
    pol = parse_policy(POLICY)
    enf = SQLEnforcer(
        pol, allowed_schemas=["people", "acad", "auth", "fin", "lib", "gh"]
    )

    def go(sql, ident=ALICE):
        role = pol.pick_role(ident)
        return enf.enforce(sql, ident, role, build_view(SCHEMA, pol, role))

    return go


def test_view_is_multi_and_wildcards_and_global_deny_apply():
    pol = parse_policy(POLICY)
    v = build_view(SCHEMA, pol, pol.resolve("student"))
    assert v.multi
    assert set(v.tables) == {
        "people.student",
        "acad.result",
        "lib.book",
    }  # auth.* denied, gh not granted
    assert "mobile_no" not in v.tables["people.student"]
    assert v.sqlglot_schema()["people"]["student"]["roll_no"] == "text"


def test_qualified_query_gets_row_filter(run):
    q = run("SELECT roll_no FROM people.student")
    assert "student_id = '42'" in q.sql and "people.student" in q.sql
    assert q.row_filtered_tables == ("people.student",)


def test_unqualified_unique_table_is_resolved(run):
    q = run("SELECT grade FROM result")
    assert "acad.result" in q.sql and "'42'" in q.sql


def test_join_across_schemas_filters_both(run):
    q = run(
        "SELECT s.roll_no, r.grade FROM people.student s JOIN acad.result r ON r.student_id = s.student_id"
    )
    assert q.sql.count("'42'") == 2 and set(q.tables) == {
        "people.student",
        "acad.result",
    }


def test_cte_name_is_not_resolved_as_a_table(run):
    q = run("WITH x AS (SELECT grade FROM acad.result) SELECT grade FROM x")
    assert "'42'" in q.sql


def test_ambiguous_bare_name_must_be_qualified():
    pol = parse_policy({"roles": {"r": {"tables": {"*": {"columns": "*"}}}}})
    enf = SQLEnforcer(pol, allowed_schemas=["lib", "gh"])
    ident = Identity("u", "u", frozenset({"r"}))
    role = pol.resolve("r")
    view = build_view(SCHEMA, pol, role)
    with pytest.raises(QueryRejected) as e:
        enf.enforce("SELECT title FROM book", ident, role, view)
    assert e.value.reason == "ambiguous_table"
    assert (
        "lib.book" in enf.enforce("SELECT title FROM lib.book", ident, role, view).sql
    )


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM auth.account",
        "SELECT password_hash FROM auth.account",
        "SELECT * FROM gh.book",  # exists but not granted
        "SELECT mobile_no FROM people.student",
        "SELECT * FROM pg_catalog.pg_user",
        "SELECT * FROM information_schema.tables",
        "SELECT * FROM other.student",
    ],
)
def test_hidden_or_foreign_things_fail(run, sql):
    with pytest.raises(NatQueryError):
        run(sql)


def test_whole_row_reference_blocked_with_qualified_tables(run):
    with pytest.raises(PermissionDenied):
        run("SELECT s::text FROM people.student s")


def test_subquery_cannot_bypass_filter_in_other_schema(run):
    q = run(
        "SELECT roll_no FROM people.student WHERE student_id IN (SELECT student_id FROM acad.result)"
    )
    assert q.sql.count("'42'") == 2


def test_other_role_sees_only_its_schema_and_columns(run):
    assert "payment" in run("SELECT amount FROM fin.payment", ACC).sql
    with pytest.raises(NatQueryError):
        run("SELECT mobile_no FROM people.student", ACC)
    with pytest.raises(NatQueryError):
        run("SELECT grade FROM acad.result", ACC)


def test_session_vars_come_from_identity_attributes():
    pol = parse_policy(POLICY)
    assert pol.session_vars_for(ALICE) == {"erp.person_id": "42"}
    pol2 = parse_policy(
        {
            "db_session_vars": {"erp.role": "@role", "erp.student_id": "student_id"},
            "roles": {},
        }
    )
    assert pol2.session_vars_for(ALICE, "student") == {
        "erp.role": "student",
        "erp.student_id": "42",
    }
    assert pol.session_vars_for(ACC) == {"erp.person_id": ""}


@pytest.mark.parametrize("bad", [{"nodot": "a"}, {"erp.x": "Bad Attr"}])
def test_bad_session_var_config_rejected(bad):
    from natquery.errors import ConfigError

    with pytest.raises(ConfigError):
        parse_policy({"db_session_vars": bad, "roles": {}})
