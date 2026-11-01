"""Edge cases of the enforcer: scoping, limits, aliasing, and fixable-vs-fatal classification."""

import pytest

from natquery.access.enforcer import SQLEnforcer
from natquery.access.policy import parse_policy
from natquery.access.schema_view import build_view
from natquery.auth.models import Identity
from natquery.errors import NatQueryError, PermissionDenied, QueryRejected

STUDENT = Identity("u1", "alice", frozenset({"student"}), {"roll_no": "21CS10001"})
TEACHER = Identity("u2", "rao", frozenset({"teacher"}), {"emp_id": "F1001"})


@pytest.fixture
def run(erp_policy, erp_schema):
    enf = SQLEnforcer(erp_policy)

    def go(sql, ident=STUDENT):
        role = erp_policy.pick_role(ident)
        return enf.enforce(sql, ident, role, build_view(erp_schema, erp_policy, role))

    return go


def test_role_with_empty_view_is_denied_outright(erp_schema):
    policy = parse_policy({"roles": {"nothing": {}}})
    role = policy.resolve("nothing")
    with pytest.raises(PermissionDenied) as e:
        SQLEnforcer(policy).enforce(
            "SELECT 1",
            Identity("1", "u", frozenset({"nothing"})),
            role,
            build_view(erp_schema, policy, role),
        )
    assert e.value.reason == "empty_view"


def test_parenthesised_statement_is_unwrapped_and_still_enforced(run):
    q = run("(SELECT name FROM students)")
    assert "21CS10001" in q.sql
    assert run("((SELECT name FROM students))").sql


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT name FROM students ORDER BY nope",
        "SELECT x.nope FROM (SELECT name FROM students) x",
        "SELECT zz.name FROM students",
        "SELECT students.nope FROM students",
        "SELECT name FROM students s WHERE s.nope = 1",
        "SELECT name FROM students WHERE EXISTS (SELECT 1 FROM courses c WHERE c.nope = students.roll_no)",
        "SELECT name FROM students GROUP BY nope",
    ],
)
def test_unresolved_names_are_fixable_rejections(run, sql):
    with pytest.raises(QueryRejected) as e:
        run(sql)
    assert e.value.fixable and e.value.reason == "unresolved_reference"


def test_correlated_subquery_resolves_outer_columns(run):
    q = run(
        "SELECT name FROM students s WHERE EXISTS (SELECT 1 FROM enrollments e WHERE e.roll_no = s.roll_no)"
    )
    assert q.sql.count("'21CS10001'") == 2


def test_order_by_output_alias_is_allowed(run):
    assert run("SELECT cgpa AS score FROM students ORDER BY score DESC").sql


def test_masked_column_with_explicit_alias_keeps_alias(run):
    q = run("SELECT email AS contact FROM students", TEACHER)
    assert "'***@'" in q.sql and q.sql.lower().count("as contact") == 1


def test_masked_column_without_alias_keeps_output_name(run):
    assert "AS email" in run("SELECT email FROM students", TEACHER).sql


@pytest.mark.parametrize(
    "limit,expected",
    [
        ("LIMIT 5", 5),
        ("LIMIT 100", 100),
        ("LIMIT 101", 100),
        ("LIMIT 1e3", 100),
        ("LIMIT 0", 0),
        ("LIMIT ALL", 100),
        ("LIMIT NULL", 100),
        ("", 100),
        ("LIMIT 5 OFFSET 10", 5),
        ("LIMIT 99999999999999999999", 100),
        ("LIMIT (SELECT 5)", 100),
    ],
)
def test_limit_normalisation(run, limit, expected):
    q = run(f"SELECT name FROM students {limit}")
    assert f"LIMIT {expected}" in q.sql.upper()


def test_offset_cannot_be_used_to_scan_beyond_cap(run):
    q = run("SELECT name FROM students LIMIT 5 OFFSET 10")
    assert "LIMIT 5" in q.sql.upper()


def test_distinct_and_aggregates_survive(run):
    q = run("SELECT DISTINCT department FROM students")
    assert "DISTINCT" in q.sql.upper()


def test_enforced_query_reports_what_it_touched(run):
    q = run(
        "SELECT s.name FROM students s JOIN courses c ON c.department = s.department"
    )
    assert set(q.tables) == {"students", "courses"} and q.row_filtered_tables == (
        "students",
    )
    assert q.statement_timeout_ms == 5000 and q.redact == ("aadhaar", "phone")


def test_mask_is_reported(run):
    assert run("SELECT email FROM students", TEACHER).masked_columns


def test_same_table_twice_with_different_aliases_both_filtered(run):
    q = run("SELECT a.name FROM students a JOIN students b ON a.roll_no = b.roll_no")
    assert q.sql.count("'21CS10001'") == 2


def test_ambiguous_columns_are_a_fixable_error(run):
    with pytest.raises(QueryRejected) as e:
        run(
            "SELECT roll_no FROM students s JOIN enrollments e ON s.roll_no = e.roll_no"
        )
    assert e.value.fixable


def test_postgres_specific_syntax_parses(run):
    for sql in [
        "SELECT name::text FROM students",
        "SELECT DATE_TRUNC('month', NOW()) FROM students",
        "SELECT name ILIKE 'a%' AS m FROM students",
        "SELECT ARRAY[1,2] FROM students",
        "SELECT EXTRACT(YEAR FROM NOW()) FROM students",
        "SELECT COUNT(*) FILTER (WHERE cgpa > 5) FROM students",
        "SELECT ROW_NUMBER() OVER (ORDER BY cgpa) FROM students",
    ]:
        run(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT name FROM students WHERE name = 'x' OR TRUE",
        "SELECT name FROM students WHERE NOT (roll_no = '21CS10001')",
        "SELECT name FROM students WHERE roll_no IN (SELECT roll_no FROM students)",
        "SELECT name FROM students WHERE roll_no = ANY(ARRAY['a','b'])",
    ],
)
def test_user_predicates_can_only_narrow_never_widen(run, sql):
    try:
        q = run(sql)
    except NatQueryError:
        return
    # the filter lives inside the table's own subquery, so no outer predicate can bypass it
    assert "(SELECT * FROM students WHERE roll_no = '21CS10001') AS students" in q.sql


def test_enforce_is_reentrant_across_roles(erp_policy, erp_schema):
    enf = SQLEnforcer(erp_policy)
    sv = erp_policy.pick_role(STUDENT)
    tv = erp_policy.pick_role(TEACHER)
    a = enf.enforce(
        "SELECT name FROM students", STUDENT, sv, build_view(erp_schema, erp_policy, sv)
    )
    b = enf.enforce(
        "SELECT name FROM students", TEACHER, tv, build_view(erp_schema, erp_policy, tv)
    )
    assert "21CS10001" in a.sql and "F1001" in b.sql and "F1001" not in a.sql


def test_enforcer_does_not_mutate_view_or_policy(erp_policy, erp_schema):
    import copy

    role = erp_policy.pick_role(TEACHER)
    view = build_view(erp_schema, erp_policy, role)
    before = copy.deepcopy(
        (view.tables, view.masked, view.row_filters, view.prompt_schema)
    )
    for _ in range(3):
        SQLEnforcer(erp_policy).enforce(
            "SELECT email FROM students", TEACHER, role, view
        )
    assert (view.tables, view.masked, view.row_filters, view.prompt_schema) == before


def test_custom_function_allow_list_extends_builtins(erp_schema):
    base = {"roles": {"r": {"tables": {"courses": {"columns": "*"}}}}}
    ident = Identity("1", "u", frozenset({"r"}))
    sql = "SELECT SOUNDEX(title) FROM courses"
    role = parse_policy(base).resolve("r")
    view = build_view(erp_schema, parse_policy(base), role)
    with pytest.raises(NatQueryError):
        SQLEnforcer(parse_policy(base)).enforce(sql, ident, role, view)
    wider = parse_policy({**base, "allowed_functions": ["soundex"]})
    assert SQLEnforcer(wider).enforce(sql, ident, wider.resolve("r"), view).sql


def test_allow_list_cannot_unlock_never_allowed_functions(erp_schema):
    base = {
        "allowed_functions": ["pg_sleep", "pg_read_file", "set_config"],
        "roles": {"r": {"tables": {"courses": {"columns": "*"}}}},
    }
    p = parse_policy(base)
    ident = Identity("1", "u", frozenset({"r"}))
    view = build_view(erp_schema, p, p.resolve("r"))
    for fn in (
        "pg_sleep(1)",
        "pg_read_file('/etc/passwd')",
        "set_config('a','b',false)",
    ):
        with pytest.raises(PermissionDenied):
            SQLEnforcer(p).enforce(
                f"SELECT {fn} FROM courses", ident, p.resolve("r"), view
            )


def test_sql_length_cap_is_independent_of_ast_size(run):
    """A short AST with a huge literal must still be refused (cap is on characters)."""
    with pytest.raises(QueryRejected) as e:
        run("SELECT name FROM students WHERE name = '" + "a" * 7000 + "'")
    assert e.value.reason == "too_long"


def test_final_output_is_revalidated_as_defence_in_depth(
    erp_policy, erp_schema, monkeypatch
):
    """If a rewrite step ever produced something that is not a single SELECT, it must not escape."""
    from sqlglot import exp, parse_one

    role = erp_policy.pick_role(STUDENT)
    view = build_view(erp_schema, erp_policy, role)
    enf = SQLEnforcer(erp_policy)
    monkeypatch.setattr(
        SQLEnforcer,
        "_apply_limit",
        staticmethod(lambda ast, n: parse_one("DROP TABLE students")),
    )
    with pytest.raises(PermissionDenied) as e:
        enf.enforce("SELECT name FROM students", STUDENT, role, view)
    assert e.value.reason == "regen_shape"
    monkeypatch.setattr(
        SQLEnforcer, "_apply_limit", staticmethod(lambda ast, n: exp.select("1"))
    )
    assert (
        enf.enforce("SELECT name FROM students", STUDENT, role, view).sql == "SELECT 1"
    )  # control
