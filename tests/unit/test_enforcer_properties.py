"""Property-based tests: whatever the LLM emits, the enforcer must hold its invariants.

Invariants for ANY input string:
  I1  it either raises NatQueryError or returns SQL (never leaks another exception type);
  I2  returned SQL is exactly one SELECT-type statement;
  I3  returned SQL carries a LIMIT no larger than the role's cap;
  I4  returned SQL only references tables of the role's view, only in the allowed schema;
  I5  every scan of a row-filtered table is inside the filter, bound to the caller's own value;
  I6  no hidden column name (aadhaar_no, password_hash, ...) appears in the returned SQL;
  I7  no dangerous function appears in the returned SQL.
"""

import os
import re

import pytest
import sqlglot
from corpus import ATTACKS, LEGIT
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from sqlglot import exp

from natquery.access.enforcer import SQLEnforcer
from natquery.access.functions import NEVER_ALLOWED
from natquery.access.schema_view import build_view
from natquery.auth.models import Identity
from natquery.errors import NatQueryError

pytestmark = pytest.mark.timeout(120)

SETTINGS = settings(
    max_examples=int(os.environ.get("NATQUERY_FUZZ_EXAMPLES", "300")),
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
IDENT = Identity("u1", "alice", frozenset({"student"}), {"roll_no": "21CS10001"})
HIDDEN = [
    "aadhaar_no",
    "password_hash",
    "erp_users",
    "salaries",
    "advisor_emp_id",
    "phone",
]
FILTERED = {"students", "enrollments", "fees"}


@pytest.fixture(scope="module")
def ctx():
    import fakes

    policy = fakes.erp_policy()
    role = policy.pick_role(IDENT)
    view = build_view(fakes.ERP_SCHEMA, policy, role)
    return SQLEnforcer(policy), role, view


def check_invariants(sql, enforcer, role, view):
    try:
        q = enforcer.enforce(sql, IDENT, role, view)
    except NatQueryError:
        return None  # I1: refusing is always fine
    trees = sqlglot.parse(q.sql, dialect="postgres")
    assert len(trees) == 1 and isinstance(
        trees[0], (exp.Select, exp.Union, exp.Subquery)
    ), q.sql  # I2
    tree = trees[0]
    limit = tree.args.get("limit")
    assert (
        limit is not None and int(limit.expression.name) <= role.limits["max_rows"]
    ), q.sql  # I3
    for t in tree.find_all(exp.Table):
        assert (t.name in view.tables) or t.name in {
            c.alias_or_name for c in tree.find_all(exp.CTE)
        }, q.sql  # I4
        assert not t.args.get("db") or t.args["db"].name == "public", q.sql
    for t in tree.find_all(exp.Table):  # I5
        if t.name in FILTERED:
            sel = t.find_ancestor(exp.Select)
            assert sel is not None and "'21CS10001'" in sel.sql(
                dialect="postgres"
            ), q.sql
    lowered = q.sql.lower()
    for h in HIDDEN:
        if h not in view.tables and not any(h in cols for cols in view.tables.values()):
            assert h not in lowered, (h, q.sql)  # I6
    for fn in tree.find_all(exp.Anonymous):  # I7
        assert fn.name.lower() not in NEVER_ALLOWED, q.sql
    return q


# ---------------------------------------------------------------------------- raw noise
@SETTINGS
@given(st.text(max_size=300))
def test_arbitrary_text(ctx, text):
    check_invariants(text, *ctx)


@SETTINGS
@given(
    st.text(
        alphabet="SELECT FROMWHEREJOINONANDORNOT*(),.'=<>;-/ studentsfeesroll_noname0123456789",
        max_size=200,
    )
)
def test_sql_flavoured_noise(ctx, text):
    check_invariants(text, *ctx)


# ---------------------------------------------------------------------------- mutated corpus
SEEDS = [sql for _, sql in LEGIT] + [sql for sql, _ in ATTACKS]


def mutate(sql, ops):
    chars = list(sql)
    for kind, pos, payload in ops:
        if not chars:
            break
        i = pos % len(chars)
        if kind == "delete":
            del chars[i]
        elif kind == "insert":
            chars.insert(i, payload)
        elif kind == "case":
            chars[i] = chars[i].swapcase()
        elif kind == "dup":
            chars[i:i] = chars[i : i + 5]
        elif kind == "comment":
            chars[i:i] = list("/**/")
    return "".join(chars)


OPS = st.lists(
    st.tuples(
        st.sampled_from(["delete", "insert", "case", "dup", "comment"]),
        st.integers(0, 10_000),
        st.sampled_from(list(";'\"-*( )\n,.%$:\\0x1")),
    ),
    min_size=1,
    max_size=8,
)


@SETTINGS
@given(st.sampled_from(SEEDS), OPS)
def test_mutated_known_queries(ctx, seed, ops):
    check_invariants(mutate(seed, ops), *ctx)


# ---------------------------------------------------------------------------- grammar-generated SELECTs
tables = st.sampled_from(
    [
        "students",
        "enrollments",
        "fees",
        "courses",
        "departments",
        "faculty",
        "salaries",
        "erp_users",
        "pg_shadow",
        "information_schema.tables",
        "public.students",
    ]
)
columns = st.sampled_from(
    [
        "*",
        "name",
        "roll_no",
        "cgpa",
        "email",
        "phone",
        "aadhaar_no",
        "password_hash",
        "grade",
        "marks",
        "amount_due",
        "title",
        "gross",
        "1",
        "'x'",
    ]
)
funcs = st.sampled_from(
    [
        "COUNT",
        "SUM",
        "AVG",
        "MAX",
        "UPPER",
        "LOWER",
        "ROUND",
        "pg_sleep",
        "pg_read_file",
        "version",
        "current_user",
        "set_config",
        "nextval",
        "COALESCE",
        "generate_series",
    ]
)


@st.composite
def selects(draw, depth=0):
    cols = ", ".join(
        draw(
            st.lists(
                st.one_of(columns, st.builds(lambda f, c: f"{f}({c})", funcs, columns)),
                min_size=1,
                max_size=3,
            )
        )
    )
    from_ = draw(tables)
    sql = f"SELECT {cols} FROM {from_}"
    if draw(st.booleans()):
        sql += f" a JOIN {draw(tables)} b ON a.roll_no = b.roll_no"
    if draw(st.booleans()):
        sql += (
            f" WHERE {draw(columns)} {draw(st.sampled_from(['=', '<>', '>', 'LIKE', 'IN']))} "
            f"{draw(st.sampled_from(['1', chr(39) + 'x' + chr(39), '(SELECT 1)', '(SELECT roll_no FROM students)']))}"
        )
    if draw(st.booleans()):
        sql += f" GROUP BY {draw(columns)}"
    if draw(st.booleans()):
        sql += f" ORDER BY {draw(columns)}"
    if draw(st.booleans()):
        sql += f" LIMIT {draw(st.integers(0, 10**7))}"
    if depth < 2 and draw(st.booleans()):
        sql = f"{sql} {draw(st.sampled_from(['UNION', 'UNION ALL', 'EXCEPT', 'INTERSECT']))} {draw(selects(depth + 1))}"
    if depth < 2 and draw(st.booleans()):
        sql = f"WITH c AS ({draw(selects(depth + 1))}) {sql}"
    if draw(st.booleans()):
        sql = f"SELECT * FROM ({sql}) t"
    return sql


@SETTINGS
@given(selects())
def test_generated_selects(ctx, sql):
    check_invariants(sql, *ctx)


def test_generator_produces_both_accepted_and_refused_queries(ctx):
    """Guards the guard: the property tests are only meaningful if both outcomes occur."""
    from hypothesis import find

    ok = find(
        selects(),
        lambda s: check_invariants(s, *ctx) is not None,
        settings=settings(max_examples=2000),
    )
    bad = find(
        selects(),
        lambda s: check_invariants(s, *ctx) is None,
        settings=settings(max_examples=2000),
    )
    assert ok and bad


# ---------------------------------------------------------------------------- attribute value fuzz
@SETTINGS
@given(st.text(max_size=60))
def test_identity_attribute_never_breaks_out_of_its_literal(ctx, value):

    enforcer, role, view = ctx
    ident = Identity("u9", "m", frozenset({"student"}), {"roll_no": value})
    try:
        q = enforcer.enforce("SELECT * FROM students", ident, role, view)
    except NatQueryError:
        return
    tree = sqlglot.parse_one(q.sql, dialect="postgres")
    lits = [l.name for l in tree.find_all(exp.Literal) if l.is_string]
    assert value in lits  # the value is carried only as a single string literal
    assert len(sqlglot.parse(q.sql, dialect="postgres")) == 1
    assert re.fullmatch(r"[\w@.\- ]{1,128}", value)


# ---------------------------------------------------------------------------- never raises odd exceptions
@SETTINGS
@given(
    st.one_of(
        st.binary(max_size=80).map(lambda b: b.decode("latin-1")),
        st.text(alphabet=st.characters(blacklist_categories=("Cs",)), max_size=80),
    )
)
def test_unicode_and_binary_garbage(ctx, text):
    check_invariants(text, *ctx)
