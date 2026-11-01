"""Security regression corpus for SQLEnforcer against the example ERP policy.

The enforcer is the trust boundary: it must hold even if the LLM is fully hostile.
Every row in the DENY tables is an attack that must never reach the database.
"""

import pytest
import sqlglot
from corpus import (
    ATTACKS,
    LEGIT,
)  # noqa: E402  (shared with the Postgres integration tests)
from sqlglot import exp

from natquery.access.enforcer import SQLEnforcer
from natquery.access.schema_view import build_view
from natquery.auth.models import Identity
from natquery.errors import NatQueryError, PermissionDenied, QueryRejected

IDS = {
    "student": Identity(
        "u1", "alice", frozenset({"student"}), {"roll_no": "21CS10001"}
    ),
    "teacher": Identity("u2", "rao", frozenset({"teacher"}), {"emp_id": "F1001"}),
    "admin": Identity("u3", "root", frozenset({"admin"}), {"emp_id": "F1001"}),
    "student_noattr": Identity("u4", "bob", frozenset({"student"}), {}),
}


@pytest.fixture
def enforce(erp_policy, erp_schema):
    enforcer = SQLEnforcer(erp_policy)
    views = {}

    def run(who, sql):
        ident = IDS[who]
        role = erp_policy.pick_role(ident)
        view = views.setdefault(role.name, build_view(erp_schema, erp_policy, role))
        return enforcer.enforce(sql, ident, role, view)

    return run


def assert_every_scan_filtered(sql, tables, needle):
    """Each base-table reference must sit inside a subquery whose WHERE carries the filter."""
    tree = sqlglot.parse_one(sql, dialect="postgres")
    scans = [t for t in tree.find_all(exp.Table) if t.name in tables]
    assert scans, sql
    for t in scans:
        sel = t.find_ancestor(exp.Select)
        assert sel is not None and sel.args.get("where") is not None, sql
        assert needle in sel.args["where"].sql(dialect="postgres"), (t.name, sql)


def refused(enforce, who, sql):
    with pytest.raises(NatQueryError) as exc:
        enforce(who, sql)
    return exc.value


# --------------------------------------------------------------------------- legit


@pytest.mark.parametrize("who,sql", LEGIT)
def test_legitimate_queries_pass(enforce, who, sql):
    q = enforce(who, sql)
    assert q.role == who
    assert "limit" in q.sql.lower()


# --------------------------------------------------------------------------- isolation
def test_explicit_public_schema_is_allowed_and_still_filtered(enforce):
    assert "21CS10001" in enforce("student", "SELECT * FROM public.students").sql


def test_row_filter_is_injected_with_bound_identity_value(enforce):
    q = enforce("student", "SELECT * FROM students")
    assert "roll_no = '21CS10001'" in q.sql
    assert q.row_filtered_tables == ("students",) or "students" in q.row_filtered_tables


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT roll_no FROM students WHERE roll_no <> '21CS10001'",
        "SELECT roll_no FROM students WHERE 1=1 OR roll_no = '21CS10002'",
        "SELECT roll_no FROM students UNION SELECT roll_no FROM enrollments",
        "SELECT (SELECT COUNT(*) FROM students) AS everyone",
        "SELECT * FROM students s1 JOIN students s2 ON TRUE",
        "SELECT * FROM (SELECT * FROM students) t",
        "WITH a AS (SELECT * FROM students) SELECT * FROM a",
    ],
)
def test_user_cannot_widen_row_scope(enforce, sql):
    """OR tricks, unions, subqueries, CTEs and self-joins: filter must wrap EVERY access."""
    try:
        q = enforce("student", sql)
    except NatQueryError:
        return  # refusing is also safe
    assert_every_scan_filtered(q.sql, {"students", "enrollments"}, "'21CS10001'")


def test_every_reference_of_a_table_is_filtered(enforce):
    q = enforce(
        "student",
        "SELECT a.name FROM students a WHERE a.cgpa > (SELECT AVG(cgpa) FROM students)",
    )
    assert q.sql.count("'21CS10001'") == 2


def test_filter_value_cannot_break_out_of_quoting(erp_policy, erp_schema):
    evil = Identity(
        "u9", "mallory", frozenset({"student"}), {"roll_no": "x' OR '1'='1"}
    )
    role = erp_policy.pick_role(evil)
    view = build_view(erp_schema, erp_policy, role)
    with pytest.raises(NatQueryError) as exc:
        SQLEnforcer(erp_policy).enforce("SELECT * FROM students", evil, role, view)
    assert exc.value.reason == "unsafe_attribute"


@pytest.mark.parametrize(
    "value",
    ["a;b", "a'b", 'a"b', "a\nb", "0\n", "a\n", "\n", "a" * 200, "", "a/*b*/", "a\\b'"],
)
def test_hostile_attribute_values_rejected(erp_policy, erp_schema, value):
    ident = Identity("u9", "mallory", frozenset({"student"}), {"roll_no": value})
    role = erp_policy.pick_role(ident)
    view = build_view(erp_schema, erp_policy, role)
    with pytest.raises(NatQueryError):
        SQLEnforcer(erp_policy).enforce("SELECT * FROM students", ident, role, view)


def test_missing_identity_attribute_fails_closed(enforce):
    assert (
        refused(enforce, "student_noattr", "SELECT * FROM students").reason
        == "missing_attribute"
    )


def test_tables_without_filter_work_without_attribute(enforce):
    assert enforce("student_noattr", "SELECT title FROM courses").sql


# --------------------------------------------------------------------------- attacks


@pytest.mark.parametrize("sql,reason", ATTACKS)
def test_attack_is_refused(enforce, sql, reason):
    err = refused(enforce, "student", sql)
    if reason:
        assert err.reason == reason, (err.reason, err.internal)


def test_hard_violations_are_permission_denied_not_fixable(enforce):
    """Only structural mistakes may be sent back to the LLM; security violations never."""
    for sql in [
        "SELECT pg_read_file('/etc/passwd')",
        "SELECT * INTO t FROM students",
        "SELECT * FROM students FOR UPDATE",
    ]:
        err = refused(enforce, "student", sql)
        assert isinstance(err, PermissionDenied), sql
        assert not getattr(err, "fixable", False)


def test_unresolved_names_are_fixable_but_leak_nothing(enforce):
    err = refused(enforce, "student", "SELECT * FROM salaries")
    assert isinstance(err, QueryRejected) and err.fixable
    assert "salar" not in err.user_message.lower()


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT PG_READ_FILE('/etc/passwd')",
        "SELECT pg_catalog.pg_read_file('/etc/passwd')",
        "SELECT \"pg_read_file\"('/etc/passwd')",
        "SELECT Pg_Sleep(1)",
        "SELECT/**/pg_sleep(1)",
        "SELECT (SELECT pg_sleep(1))",
        "SELECT name FROM students WHERE (SELECT pg_sleep(1)) IS NULL",
        "SELECT CASE WHEN true THEN pg_sleep(1) END",
        "SELECT COALESCE(pg_read_file('/etc/passwd'), 'x')",
        "SELECT * FROM students ORDER BY pg_sleep(1)",
    ],
)
def test_obfuscated_dangerous_calls_refused(enforce, sql):
    assert refused(enforce, "student", sql)


def test_stacked_statement_hidden_after_comment(enforce):
    assert refused(enforce, "student", "SELECT 1 /* x */ ; /* y */ DROP TABLE students")


@pytest.mark.parametrize(
    "sql", ["SELECT 'é' FROM students", "SELECT 'ＤＲＯＰ' FROM students"]
)
def test_unicode_literals_are_just_data(enforce, sql):
    assert enforce("student", sql).sql


def test_null_byte_is_refused_not_crashed(enforce):
    assert refused(enforce, "student", "SELECT \x00 FROM students")


def test_oversized_sql_refused(enforce):
    sql = "SELECT " + ", ".join(f"name AS a{i}" for i in range(2000)) + " FROM students"
    assert refused(enforce, "student", sql)


def test_deep_nesting_refused(enforce):
    sql = "SELECT name FROM students"
    for _ in range(8):
        sql = f"SELECT name FROM ({sql}) t"
    assert refused(enforce, "student", sql)


def test_too_many_joins_refused(enforce):
    joins = " ".join(
        f"JOIN courses c{i} ON c{i}.course_id = c.course_id" for i in range(10)
    )
    assert refused(enforce, "student", f"SELECT 1 FROM courses c {joins}")


# --------------------------------------------------------------------------- masking
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT name FROM students WHERE email LIKE 'a%'",
        "SELECT name FROM students ORDER BY email",
        "SELECT LOWER(email) FROM students",
        "SELECT COUNT(DISTINCT email) FROM students",
        "SELECT email, COUNT(*) FROM students GROUP BY email",
        "SELECT name FROM students WHERE email = 'asha@kgpian.iitkgp.ac.in'",
        "SELECT SUBSTRING(email, 1, 1) FROM students",
        "SELECT email || 'x' FROM students",
        "SELECT MIN(email) FROM students",
        "SELECT name FROM students WHERE email IN (SELECT email FROM students)",
        "SELECT name FROM students WHERE LENGTH(email) > 3",
    ],
)
def test_masked_column_cannot_be_used_as_oracle(enforce, sql):
    err = refused(enforce, "teacher", sql)
    assert err.reason == "masked_misuse", (err.reason, err.internal)


def test_masked_column_direct_select_is_rewritten(enforce):
    q = enforce("teacher", "SELECT name, email FROM students")
    assert "'***@'" in q.sql and "SPLIT_PART" in q.sql.upper()
    assert q.masked_columns


def test_masked_column_via_join_still_masked(enforce):
    q = enforce(
        "teacher",
        "SELECT s.email FROM students s JOIN enrollments e ON e.roll_no = s.roll_no",
    )
    assert "'***@'" in q.sql


def test_masked_column_via_cte_and_subquery_still_masked(enforce):
    for sql in [
        "WITH x AS (SELECT email FROM students) SELECT email FROM x",
        "SELECT email FROM (SELECT email FROM students) t",
    ]:
        q = enforce("teacher", sql)
        assert "'***@'" in q.sql, sql


def test_star_on_masked_table_masks_email(enforce):
    assert "'***@'" in enforce("teacher", "SELECT * FROM students").sql


def test_teacher_cannot_join_on_masked_columns_with_other_table(enforce):
    assert refused(
        enforce,
        "teacher",
        "SELECT s.name FROM students s JOIN faculty f ON f.email = s.email",
    )


def test_filtering_a_cte_column_only_sees_the_masked_value(enforce):
    """The CTE already holds the masked expression, so an outer filter cannot reach the raw value."""
    q = enforce(
        "teacher",
        "WITH x AS (SELECT email FROM students) SELECT * FROM x WHERE email LIKE 'a%'",
    )
    cte = q.sql.split(") SELECT")[0]
    assert "'***@'" in cte and "email LIKE" not in cte


# --------------------------------------------------------------------------- roles
def test_teacher_cannot_see_fees_or_other_roles_columns(enforce):
    assert (
        refused(enforce, "teacher", "SELECT * FROM fees").reason
        == "unresolved_reference"
    )


def test_admin_still_denied_globally_denied_objects(enforce):
    for sql in [
        "SELECT * FROM erp_users",
        "SELECT aadhaar_no FROM students",
        "SELECT password_hash FROM erp_users",
    ]:
        assert refused(enforce, "admin", sql)


def test_admin_wildcard_overrides_inherited_teacher_filters(enforce):
    q = enforce("admin", "SELECT * FROM students")
    assert "F1001" not in q.sql  # no teacher row filter
    assert "email" in q.sql.lower() and "***@" not in q.sql  # nor teacher masking


def test_teacher_row_filter_is_a_subquery_bound_to_emp_id(enforce):
    q = enforce("teacher", "SELECT name FROM students")
    assert "'F1001'" in q.sql and "instructor_emp_id" in q.sql


def test_limit_is_capped_by_role(enforce):
    q = enforce("student", "SELECT * FROM students LIMIT 100000")
    assert q.max_rows == 100 and "100000" not in q.sql
    assert enforce("admin", "SELECT * FROM students").max_rows == 1000


def test_small_user_limit_is_respected(enforce):
    assert "LIMIT 3" in enforce("admin", "SELECT * FROM students LIMIT 3").sql.upper()


def test_limit_applied_to_union_and_set_operations(enforce):
    q = enforce(
        "student",
        "SELECT title FROM courses UNION ALL SELECT dept_name FROM departments",
    )
    assert q.sql.upper().count("LIMIT") >= 1


def test_enforced_sql_is_regenerated_not_passed_through(enforce):
    q = enforce("student", "select   name   from students   -- trailing comment")
    assert "--" not in q.sql and "trailing" not in q.sql


def test_comments_stripped_from_enforced_sql(enforce):
    q = enforce("student", "SELECT /* sneaky */ name FROM students")
    assert "sneaky" not in q.sql


def test_enforcement_is_idempotent_and_safe_on_its_own_output(enforce):
    q1 = enforce("student", "SELECT name FROM students")
    q2 = enforce("student", q1.sql)
    assert q2.sql.count("'21CS10001'") >= 1


def test_role_switch_requires_holding_role(erp_policy):
    with pytest.raises(PermissionDenied):
        erp_policy.pick_role(IDS["student"], "admin")


def test_unknown_role_identity_denied(erp_policy):
    with pytest.raises(PermissionDenied):
        erp_policy.pick_role(Identity("u", "x", frozenset({"alumnus"}), {}))
