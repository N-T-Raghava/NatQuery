"""PostgresBackend against a fake psycopg2 pool: checks the safety protocol, not the database."""

import threading

import psycopg2
import pytest
from psycopg2 import errors as pgerr

import natquery.backends.postgres as pg
from natquery.backends.base import create_backend
from natquery.errors import ConfigError, ExecutionFailed


def pgerror(code, message="boom"):
    """psycopg2 only fills pgcode from a live server, so fake it with a subclass."""
    cls = type(f"Pg{code}", (psycopg2.Error,), {"pgcode": code})
    return cls(message)


class FakeCursor:
    def __init__(self, conn, name=None):
        self.conn, self.name, self.itersize = conn, name, None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.conn.log.append(sql)
        if self.conn.fail_on and self.conn.fail_on[0] in sql:
            raise self.conn.fail_on[1]
        self.last = sql

    def fetchmany(self, n):
        self.conn.fetch_n = n
        return [{"i": i} for i in range(min(n, self.conn.available))]

    def fetchone(self):
        return ([{"Plan": {"Total Cost": 1}}],)

    def fetchall(self):
        return self.conn.schema_rows.pop(0)

    def close(self):
        pass


class FakeConn:
    def __init__(self):
        self.log, self.fail_on, self.available, self.fetch_n = [], None, 10_000, None
        self.rollbacks, self.commits, self.session = 0, 0, None
        self.schema_rows = []
        self.cursor_names = []

    def cursor(self, name=None, cursor_factory=None):
        self.cursor_names.append(name)
        return FakeCursor(self, name)

    def rollback(self):
        self.rollbacks += 1

    def commit(self):
        self.commits += 1

    def set_session(self, **kw):
        self.session = kw


class FakePool:
    instances = []

    def __init__(self, minc, maxc, *a, **kw):
        self.args, self.kw = a, kw
        self.conn = FakeConn()
        self.out, self.closed = 0, False
        FakePool.instances.append(self)

    def getconn(self):
        self.out += 1
        return self.conn

    def putconn(self, c):
        self.out -= 1

    def closeall(self):
        self.closed = True


@pytest.fixture
def backend(monkeypatch):
    monkeypatch.setattr(pg, "ThreadedConnectionPool", FakePool)
    b = pg.PostgresBackend(
        {
            "type": "standard",
            "host": "h",
            "port": 1,
            "dbname": "d",
            "user": "u",
            "password": "p",
        },
        max_conn=2,
        acquire_timeout=0.2,
    )
    return b, b._pool.conn


def test_read_only_session_and_always_rolls_back(backend):
    b, conn = backend
    b.execute_readonly("SELECT 1", max_rows=5, timeout_ms=1234)
    assert conn.session == {"readonly": True, "autocommit": False}
    assert conn.commits == 0 and conn.rollbacks >= 2  # on checkout and on release
    assert b._pool.out == 0  # connection returned


def test_timeouts_set_locally_before_query(backend):
    b, conn = backend
    b.execute_readonly("SELECT 1", max_rows=5, timeout_ms=1234)
    assert conn.log[:2] == [
        "SET LOCAL statement_timeout = 1234",
        "SET LOCAL lock_timeout = 2000",
    ]
    assert conn.log[-1] == "SELECT 1"


def test_timeout_value_is_coerced_to_int(backend):
    b, conn = backend
    with pytest.raises(ValueError):
        b.execute_readonly("SELECT 1", max_rows=5, timeout_ms="1; DROP TABLE x")
    assert b._pool.out == 0 and conn.commits == 0


def test_server_side_cursor_and_row_cap(backend):
    b, conn = backend
    out = b.execute_readonly("SELECT 1", max_rows=5, timeout_ms=100)
    assert "nq_cur" in conn.cursor_names and conn.fetch_n == 6
    assert len(out.rows) == 5 and out.truncated is True


def test_not_truncated_when_fewer_rows(backend):
    b, conn = backend
    conn.available = 3
    out = b.execute_readonly("SELECT 1", max_rows=5, timeout_ms=100)
    assert len(out.rows) == 3 and out.truncated is False and out.elapsed_ms >= 0


def test_exactly_max_rows_is_not_truncated(backend):
    b, conn = backend
    conn.available = 5
    assert b.execute_readonly("SELECT 1", max_rows=5, timeout_ms=100).truncated is False


def test_db_role_applied_and_validated(backend):
    b, conn = backend
    b.execute_readonly("SELECT 1", max_rows=1, timeout_ms=1, db_role="nq_student")
    assert 'SET LOCAL ROLE "nq_student"' in conn.log
    for evil in ['x"; DROP TABLE t; --', "a b", "1abc", "", "a" * 64, "role\n"]:
        conn.log.clear()
        if evil == "":
            b.execute_readonly(
                "SELECT 1", max_rows=1, timeout_ms=1, db_role=evil
            )  # falsy: ignored
            assert not any("ROLE" in s for s in conn.log)
            continue
        with pytest.raises(ConfigError):
            b.execute_readonly("SELECT 1", max_rows=1, timeout_ms=1, db_role=evil)
        assert "SELECT 1" not in conn.log and b._pool.out == 0


@pytest.mark.parametrize(
    "exc,reason,fixable",
    [
        (
            pgerr.QueryCanceled("canceling statement due to statement timeout"),
            "timeout",
            False,
        ),
        (pgerror("42601", 'syntax error at or near "x"'), "sql_error", True),
        (pgerror("42703", 'column "x" does not exist'), "sql_error", True),
        (pgerror("42501", "permission denied"), "sql_error", True),
        (
            psycopg2.OperationalError("server closed the connection"),
            "db_unavailable",
            False,
        ),
        (pgerror("22012", "division by zero"), "db_error", False),
    ],
)
def test_database_errors_are_classified(backend, exc, reason, fixable):
    b, conn = backend
    conn.fail_on = ("SELECT boom", exc)
    with pytest.raises(ExecutionFailed) as e:
        b.execute_readonly("SELECT boom", max_rows=1, timeout_ms=1)
    assert e.value.reason == reason and e.value.fixable is fixable
    assert b._pool.out == 0 and conn.commits == 0 and conn.rollbacks >= 2


def test_db_hint_is_sanitised(backend):
    b, conn = backend
    conn.fail_on = ("SELECT boom", pgerror("42601", "bad near 'secret'\nLINE 1: x"))
    with pytest.raises(ExecutionFailed) as e:
        b.execute_readonly("SELECT boom", max_rows=1, timeout_ms=1)
    assert "secret" not in e.value.db_hint and "LINE" not in e.value.db_hint


def test_user_message_never_contains_database_text(backend):
    b, conn = backend
    conn.fail_on = (
        "SELECT boom",
        pgerror("42P01", 'relation "salaries" does not exist'),
    )
    with pytest.raises(ExecutionFailed) as e:
        b.execute_readonly("SELECT boom", max_rows=1, timeout_ms=1)
    assert "salaries" not in e.value.user_message


def test_pool_backpressure_times_out_cleanly(backend):
    b, _ = backend
    held = [b._checkout(), b._checkout()]  # max_conn == 2
    with pytest.raises(ExecutionFailed) as e:
        b.execute_readonly("SELECT 1", max_rows=1, timeout_ms=1)
    assert e.value.reason == "pool_timeout"
    for c in held:
        b._release(c)
    b.execute_readonly("SELECT 1", max_rows=1, timeout_ms=1)  # recovered


def test_slot_released_when_checkout_itself_fails(backend):
    b, conn = backend
    conn.rollback = lambda: (_ for _ in ()).throw(psycopg2.OperationalError("dead"))
    for _ in range(5):  # would deadlock the pool if the slot leaked
        with pytest.raises(psycopg2.OperationalError):
            b._checkout()
    assert b._slots._value == 2


def test_explain_is_plan_only_and_parsed(backend):
    b, conn = backend
    plan = b.explain("SELECT 1", timeout_ms=50)
    assert plan == {"Plan": {"Total Cost": 1}}
    assert conn.log[
        -1
    ] == "EXPLAIN (FORMAT JSON) SELECT 1" and "ANALYZE" not in " ".join(conn.log)
    assert conn.commits == 0


def test_explain_errors_wrapped(backend):
    b, conn = backend
    conn.fail_on = ("EXPLAIN", pgerror("42601", "x"))
    with pytest.raises(ExecutionFailed):
        b.explain("SELECT 1", timeout_ms=50)
    assert b._pool.out == 0


def test_extract_schema_assembles_tables(backend):
    b, conn = backend
    conn.schema_rows = [
        [
            ("public", "t", "id", "integer"),
            ("public", "t", "name", "text"),
            ("public", "u", "t_id", "integer"),
        ],
        [("public", "t", "id")],
        [("public", "u", "t_id", "public", "t", "id")],
        [("public", "t", 7), ("public", "u", 0), ("public", "ghost", 1)],
    ]
    s = b.extract_schema()["tables"]
    assert s["t"] == {
        "columns": {"id": "integer", "name": "text"},
        "primary_key": ["id"],
        "foreign_keys": [],
        "row_estimate": 7,
    }
    assert s["u"]["foreign_keys"] == [
        {"column": "t_id", "references": {"table": "t", "column": "id"}}
    ]
    assert "ghost" not in s and b._pool.out == 0


def test_extract_schema_multi_schema_qualifies_keys(backend):
    b, conn = backend
    b.schemas = ("people", "acad")
    conn.schema_rows = [
        [
            ("people", "student", "id", "integer"),
            ("acad", "result", "student_id", "integer"),
        ],
        [("people", "student", "id")],
        [("acad", "result", "student_id", "people", "student", "id")],
        [("people", "student", 5)],
    ]
    s = b.extract_schema()["tables"]
    assert set(s) == {"people.student", "acad.result"}
    assert s["acad.result"]["foreign_keys"] == [
        {
            "column": "student_id",
            "references": {"table": "people.student", "column": "id"},
        }
    ]
    assert s["people.student"]["row_estimate"] == 5


def test_session_vars_set_before_role_and_validated(backend):
    b, conn = backend
    b.execute_readonly(
        "SELECT 1",
        max_rows=1,
        timeout_ms=5,
        db_role="ro",
        session_vars={"erp.person_id": "7"},
    )
    log = [x for x in conn.log if "set_config" in x or "SET LOCAL ROLE" in x]
    assert "set_config" in log[0] and "SET LOCAL ROLE" in log[1]
    with pytest.raises(ConfigError):
        b.execute_readonly(
            "SELECT 1", max_rows=1, timeout_ms=5, session_vars={"x; DROP": "1"}
        )


def test_extract_schema_never_counts_rows(backend):
    b, conn = backend
    conn.schema_rows = [[], [], [], []]
    b.extract_schema()
    assert not any("COUNT(" in s.upper() for s in conn.log)


def test_connection_failure_is_a_config_error_without_password(monkeypatch):
    def boom(*a, **k):
        raise psycopg2.OperationalError("could not connect")

    monkeypatch.setattr(pg, "ThreadedConnectionPool", boom)
    with pytest.raises(ConfigError) as e:
        pg.PostgresBackend(
            {
                "type": "standard",
                "host": "h",
                "port": 1,
                "dbname": "d",
                "user": "u",
                "password": "TOPSECRET",
            }
        )
    assert "TOPSECRET" not in e.value.user_message


def test_connection_options_are_hardened(monkeypatch):
    monkeypatch.setattr(pg, "ThreadedConnectionPool", FakePool)
    pg.PostgresBackend(
        {
            "type": "standard",
            "host": "h",
            "port": 1,
            "dbname": "d",
            "user": "u",
            "password": "p",
            "sslmode": "require",
        }
    )
    kw = FakePool.instances[-1].kw
    assert (
        "default_transaction_read_only=on" in kw["options"]
        and kw["connect_timeout"] <= 10
    )
    assert kw["sslmode"] == "require" and kw["application_name"] == "natquery"


def test_dsn_mode(monkeypatch):
    monkeypatch.setattr(pg, "ThreadedConnectionPool", FakePool)
    pg.PostgresBackend({"type": "dsn", "dsn": "postgres://x/y", "dbname": "y"})
    assert FakePool.instances[-1].args == ("postgres://x/y",)


def test_close_and_registry(backend):
    b, _ = backend
    b.close()
    assert b._pool.closed
    from natquery.backends.base import QueryBackend

    with pytest.raises(ConfigError):
        create_backend({}, kind="oracle")
    with pytest.raises(TypeError):
        QueryBackend()


def test_concurrent_use_never_exceeds_pool(backend):
    b, conn = backend
    peak, lock = [0], threading.Lock()
    orig = b._pool.getconn

    def tracked():
        with lock:
            peak[0] = max(peak[0], b._pool.out + 1)
        return orig()

    b._pool.getconn = tracked
    b._acquire_timeout = 5
    ts = [
        threading.Thread(
            target=lambda: b.execute_readonly("SELECT 1", max_rows=1, timeout_ms=1)
        )
        for _ in range(20)
    ]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert peak[0] <= 2 and b._pool.out == 0


def test_create_backend_uses_registered_factory():
    from natquery.backends.base import QueryBackend, register_backend

    class Dummy(QueryBackend):
        def execute_readonly(self, *a, **k): ...
        def explain(self, *a, **k): ...
        def extract_schema(self): ...
        def close(self): ...

    register_backend("dummy-db", lambda cfg: Dummy())
    assert isinstance(create_backend({}, kind="dummy-db"), Dummy)


def test_failed_getconn_releases_slot_and_does_not_hold_the_pool(backend):
    b, _ = backend

    def broken():
        raise psycopg2.pool.PoolError("exhausted")

    b._pool.getconn = broken
    for _ in range(5):
        with pytest.raises(psycopg2.pool.PoolError):
            b._checkout()
    assert b._slots._value == 2


def test_release_returns_slot_even_if_rollback_fails(backend):
    b, conn = backend
    c = b._checkout()
    conn.rollback = lambda: (_ for _ in ()).throw(psycopg2.OperationalError("dead"))
    with pytest.raises(psycopg2.OperationalError):
        b._release(c)
    assert b._slots._value == 2 and b._pool.out == 0
