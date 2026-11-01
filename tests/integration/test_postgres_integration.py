"""Everything here runs against a real PostgreSQL (see conftest.py for how to enable it)."""

import threading
import time

import fakes
import pytest
from corpus import ATTACKS, LEGIT

from natquery.access.enforcer import SQLEnforcer
from natquery.access.schema_view import build_view
from natquery.auth.models import Identity
from natquery.errors import ExecutionFailed, NatQueryError, PermissionDenied

IDS = {
    "student": Identity(
        "u1", "alice", frozenset({"student"}), {"roll_no": "21CS10001"}
    ),
    "teacher": Identity("u2", "rao", frozenset({"teacher"}), {"emp_id": "F1001"}),
    "admin": Identity("u3", "root", frozenset({"admin"}), {"emp_id": "F1001"}),
}


# ====================================================================== backend guarantees
def test_extract_schema_matches_sample(backend):
    s = backend.extract_schema()["tables"]
    assert set(s) == {
        "departments",
        "faculty",
        "students",
        "courses",
        "enrollments",
        "fees",
        "salaries",
        "erp_users",
    }
    assert s["students"]["primary_key"] == ["roll_no"]
    assert s["enrollments"]["primary_key"] == ["roll_no", "course_id", "semester"]
    fk = {
        (f["column"], f["references"]["table"])
        for f in s["enrollments"]["foreign_keys"]
    }
    assert fk == {("roll_no", "students"), ("course_id", "courses")}
    assert (
        s["students"]["columns"]["cgpa"] == "numeric"
        and s["students"]["row_estimate"] is not None
    )


def test_row_cap_is_enforced_by_the_server_side_cursor(backend):
    out = backend.execute_readonly(
        "SELECT generate_series(1, 5000) AS g", max_rows=100, timeout_ms=2000
    )
    assert len(out.rows) == 100 and out.truncated and out.rows[0] == {"g": 1}
    exact = backend.execute_readonly(
        "SELECT generate_series(1, 100) AS g", max_rows=100, timeout_ms=2000
    )
    assert len(exact.rows) == 100 and not exact.truncated


def test_statement_timeout_cancels_and_connection_is_reusable(backend):
    with pytest.raises(ExecutionFailed) as e:
        backend.execute_readonly("SELECT pg_sleep(5)", max_rows=1, timeout_ms=300)
    assert e.value.reason == "timeout" and not e.value.fixable
    assert backend.execute_readonly(
        "SELECT 1 AS x", max_rows=1, timeout_ms=1000
    ).rows == [{"x": 1}]


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO departments VALUES ('ZZ','x')",
        "UPDATE students SET cgpa = 10",
        "DELETE FROM fees",
        "DROP TABLE salaries",
        "CREATE TABLE evil (a int)",
        "TRUNCATE enrollments",
    ],
)
def test_database_refuses_writes_even_if_the_enforcer_were_bypassed(
    backend, snapshot, sql
):
    before = snapshot()
    with pytest.raises(ExecutionFailed):
        backend.execute_readonly(sql, max_rows=1, timeout_ms=1000)
    assert snapshot() == before


def test_side_effecting_select_blocked_and_nothing_persists(backend, pg_env):
    with pytest.raises(ExecutionFailed):
        backend.execute_readonly(
            "SELECT nextval('nq_seq')", max_rows=1, timeout_ms=1000
        )
    c = pg_env["seeded"].cursor()
    c.execute("SELECT last_value, is_called FROM nq_seq")
    assert c.fetchone() == (1, False)


def test_session_state_does_not_leak_between_calls(backend):
    for _ in range(3):  # tiny pool => connections are reused
        with pytest.raises(ExecutionFailed):
            backend.execute_readonly("SELECT pg_sleep(5)", max_rows=1, timeout_ms=100)
    out = backend.execute_readonly(
        "SELECT current_setting('statement_timeout') AS t", max_rows=1, timeout_ms=7000
    )
    assert out.rows[0]["t"] == "7s"
    after = backend.execute_readonly(
        "SELECT current_setting('statement_timeout') AS t", max_rows=1, timeout_ms=9000
    )
    assert after.rows[0]["t"] == "9s"


def test_db_role_limits_what_sql_can_read(backend, pg_env):
    ok = backend.execute_readonly(
        "SELECT count(*) AS n FROM courses",
        max_rows=5,
        timeout_ms=2000,
        db_role=pg_env["limited"],
    )
    assert ok.rows == [{"n": 4}]
    with pytest.raises(
        ExecutionFailed
    ) as e:  # second layer: DB itself says no, regardless of the enforcer
        backend.execute_readonly(
            "SELECT * FROM salaries",
            max_rows=5,
            timeout_ms=2000,
            db_role=pg_env["limited"],
        )
    assert e.value.reason == "sql_error"
    after = backend.execute_readonly(
        "SELECT count(*) AS n FROM salaries", max_rows=5, timeout_ms=2000
    )
    assert after.rows == [
        {"n": 3}
    ]  # role reset: SET LOCAL did not stick to the pooled connection


def test_pool_exhaustion_is_back_pressure_not_failure(db_config):
    from natquery.backends.postgres import PostgresBackend

    b = PostgresBackend(db_config, max_conn=2, acquire_timeout=20)
    errors, results = [], []

    def work():
        try:
            results.append(
                b.execute_readonly(
                    "SELECT pg_sleep(0.05), 1 AS x", max_rows=1, timeout_ms=3000
                )
            )
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    ts = [threading.Thread(target=work) for _ in range(30)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    b.close()
    assert not errors and len(results) == 30


def test_bad_credentials_surface_as_config_error_without_password(db_config):
    from natquery.backends.postgres import PostgresBackend
    from natquery.errors import ConfigError

    with pytest.raises(ConfigError) as e:
        PostgresBackend({**db_config, "password": "definitely-wrong-pw"})
    assert "definitely-wrong-pw" not in e.value.user_message


def test_explain_plans_without_executing(backend, pg_env):
    plan = backend.explain("SELECT * FROM students WHERE cgpa > 5", timeout_ms=2000)
    assert "Plan" in plan and "Execution Time" not in plan  # ANALYZE would add it


# ====================================================================== enforced SQL is valid Postgres
@pytest.fixture
def stack(backend, db_config):
    schema = backend.extract_schema()
    policy = fakes.erp_policy()
    assert policy.validate_against_schema(schema) == []
    enforcer = SQLEnforcer(policy)

    def run(who, sql):
        ident = IDS[who]
        role = policy.pick_role(ident)
        view = build_view(schema, policy, role)
        q = enforcer.enforce(sql, ident, role, view)
        return q, backend.execute_readonly(
            q.sql, max_rows=q.max_rows, timeout_ms=q.statement_timeout_ms
        )

    return run


@pytest.mark.parametrize("who,sql", LEGIT)
def test_every_accepted_query_runs_on_postgres(stack, who, sql):
    """The SQL we regenerate (row filters, masks, LIMIT) must be real, executable PostgreSQL."""
    q, out = stack(who, sql)
    assert len(out.rows) <= q.max_rows


@pytest.mark.parametrize("sql,reason", ATTACKS)
def test_attacks_never_reach_postgres(stack, snapshot, sql, reason):
    before = snapshot()
    with pytest.raises(NatQueryError):
        stack("student", sql)
    assert snapshot() == before


def test_row_level_isolation_with_real_data(stack):
    _, out = stack("student", "SELECT roll_no FROM students")
    assert out.rows == [{"roll_no": "21CS10001"}]
    _, out = stack(
        "student", "SELECT roll_no FROM students WHERE roll_no <> '21CS10001'"
    )
    assert out.rows == []
    _, out = stack(
        "student", "SELECT roll_no FROM students WHERE 1=1 OR roll_no = '21CS10002'"
    )
    assert out.rows == [{"roll_no": "21CS10001"}]
    _, out = stack("student", "SELECT (SELECT COUNT(*) FROM students) AS everyone")
    assert out.rows == [{"everyone": 1}]
    _, out = stack(
        "student", "SELECT roll_no FROM students UNION SELECT roll_no FROM enrollments"
    )
    assert out.rows == [{"roll_no": "21CS10001"}]
    _, out = stack("student", "SELECT SUM(amount_due - amount_paid) AS owed FROM fees")
    assert out.rows == [{"owed": 0}]  # only her own fee row (fully paid)


def test_teacher_sees_only_own_students_with_masked_emails(stack):
    _, out = stack("teacher", "SELECT roll_no, email FROM students ORDER BY roll_no")
    assert [r["roll_no"] for r in out.rows] == ["21CS10001", "21CS10002", "22CS10005"]
    assert all(
        r["email"].startswith("***@") and "asha" not in r["email"] for r in out.rows
    )
    _, out = stack("teacher", "SELECT gross FROM salaries")
    assert [r["gross"] for r in out.rows] == [250000]  # only F1001's own


def test_admin_sees_everything_except_globally_denied(stack):
    _, out = stack("admin", "SELECT COUNT(*) AS n FROM students")
    assert out.rows == [{"n": 6}]
    _, out = stack("admin", "SELECT email FROM students ORDER BY roll_no LIMIT 1")
    assert out.rows == [{"email": "asha@kgpian.iitkgp.ac.in"}]
    for sql in ("SELECT * FROM erp_users", "SELECT aadhaar_no FROM students"):
        with pytest.raises(NatQueryError):
            stack("admin", sql)


def test_max_rows_cap_applies_to_real_results(stack):
    q, out = stack("admin", "SELECT * FROM enrollments LIMIT 100000")
    assert q.max_rows == 1000 and len(out.rows) == 8


# ====================================================================== whole application on Postgres
@pytest.fixture
def app(db_config, backend, tmp_path):

    llm = fakes.FakeLLM("SELECT roll_no, grade FROM enrollments")
    svc, ctx = fakes.make_service(
        tmp_path,
        llm=llm,
        backend=backend,
        schema=backend.extract_schema(),
        users=[
            ("alice", ["student"], {"roll_no": "21CS10001"}),
            ("bhaskar", ["student"], {"roll_no": "21CS10002"}),
            ("rao", ["teacher"], {"emp_id": "F1001"}),
            ("root", ["admin"], {}),
        ],
        security={"rate_limit_per_minute": 10_000, "llm_calls_per_hour": 100_000},
    )
    svc._ctx = ctx
    yield svc
    # backend is closed by its own fixture


def test_end_to_end_isolation_between_users(app):
    a = fakes.login(app, "alice")
    b = fakes.login(app, "bhaskar")
    ra, rb = app.ask(a.token, "my grades"), app.ask(b.token, "my grades")
    assert sorted(r["grade"] for r in ra.rows) == ["A", "EX"] and {
        r["roll_no"] for r in ra.rows
    } == {"21CS10001"}
    assert sorted(r["grade"] for r in rb.rows) == ["A", "B"] and {
        r["roll_no"] for r in rb.rows
    } == {"21CS10002"}


def test_end_to_end_redaction_applies_to_real_rows(app):
    app._ctx.llm.responder = (
        lambda m: "SELECT name || ' call 9876543210' AS note FROM students"
    )
    resp = app.ask(fakes.login(app, "alice").token, "students")
    assert resp.rows == [{"note": "Asha Verma call [REDACTED]"}]
    assert any("redacted" in n for n in resp.notes)
    assert app._ctx.audit.tail(5)[-1]["details"]["redactions"] == 1


def test_concurrent_users_never_see_each_others_rows(app):
    tokens = {
        "alice": fakes.login(app, "alice").token,
        "bhaskar": fakes.login(app, "bhaskar").token,
    }
    expect = {"alice": "21CS10001", "bhaskar": "21CS10002"}
    bad, lock, lat = [], threading.Lock(), []

    def worker(user):
        for _ in range(5):
            t0 = time.time()
            try:
                rows = app.ask(tokens[user], "my grades").rows
                if not rows or {r["roll_no"] for r in rows} != {expect[user]}:
                    with lock:
                        bad.append((user, rows))
            except Exception as e:  # noqa: BLE001
                with lock:
                    bad.append((user, repr(e)[:100]))
            with lock:
                lat.append(time.time() - t0)

    ts = [
        threading.Thread(target=worker, args=("alice" if i % 2 == 0 else "bhaskar",))
        for i in range(40)
    ]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert not bad, bad[:3]
    assert (
        sorted(lat)[int(len(lat) * 0.95)] < 5
    )  # generous: guards against pathological regressions only


def test_end_to_end_attack_leaves_database_unchanged_and_is_audited(app, snapshot):
    before = snapshot()
    app._ctx.llm.responder = lambda m: "DELETE FROM students"
    with pytest.raises(PermissionDenied):
        app.ask(fakes.login(app, "alice").token, "wipe it")
    assert snapshot() == before
    assert any(e["event"] == "query_denied" for e in app._ctx.audit.tail(50))
    assert app._ctx.audit.verify()[0]
