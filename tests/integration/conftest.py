"""Real-PostgreSQL fixtures. Skipped unless a superuser connection is configured:

    NATQUERY_TEST_PG_HOST=127.0.0.1 NATQUERY_TEST_PG_PORT=5432 \
    NATQUERY_TEST_PG_USER=postgres  NATQUERY_TEST_PG_PASSWORD=...  pytest -m integration

Each session creates a throw-away database (loaded from examples/erp_sample.sql) and a
SELECT-only role, and drops both afterwards. Nothing else on the server is touched.
"""

import os
import uuid
from pathlib import Path

import pytest

psycopg2 = pytest.importorskip("psycopg2")

SAMPLE_SQL = (
    Path(__file__).resolve().parents[2] / "src/natquery/examples/erp_sample.sql"
)
READER_PW = "reader-pw-1"


def pytest_collection_modifyitems(items):
    for item in items:
        if "/integration/" in str(item.fspath):
            item.add_marker(pytest.mark.integration)


def _admin(dbname="postgres"):
    return psycopg2.connect(
        host=os.environ["NATQUERY_TEST_PG_HOST"],
        port=os.environ.get("NATQUERY_TEST_PG_PORT", "5432"),
        user=os.environ["NATQUERY_TEST_PG_USER"],
        password=os.environ.get("NATQUERY_TEST_PG_PASSWORD", ""),
        dbname=dbname,
        connect_timeout=3,
    )


@pytest.fixture(scope="session")
def pg_env():
    if not os.environ.get("NATQUERY_TEST_PG_HOST"):
        pytest.skip(
            "set NATQUERY_TEST_PG_HOST/USER/PASSWORD to run the PostgreSQL integration tests"
        )
    try:
        _admin().close()
    except psycopg2.Error as e:
        pytest.skip(f"PostgreSQL not reachable: {e}")
    suffix = uuid.uuid4().hex[:8]
    db, reader, limited = (
        f"nq_it_{suffix}",
        f"nq_it_reader_{suffix}",
        f"nq_it_limited_{suffix}",
    )
    root = _admin()
    root.autocommit = True
    cur = root.cursor()
    cur.execute(f'CREATE DATABASE "{db}"')
    cur.execute(f"CREATE ROLE \"{reader}\" LOGIN PASSWORD '{READER_PW}'")
    cur.execute(f'CREATE ROLE "{limited}" NOLOGIN')
    cur.execute(f'GRANT "{limited}" TO "{reader}"')
    seeded = _admin(db)
    seeded.autocommit = True
    scur = seeded.cursor()
    scur.execute(SAMPLE_SQL.read_text())
    scur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA public TO "{reader}"')
    scur.execute(f'GRANT SELECT ON courses, departments TO "{limited}"')
    scur.execute(f'GRANT USAGE ON SCHEMA public TO "{limited}"')
    scur.execute("CREATE SEQUENCE nq_seq")
    scur.execute(f'GRANT USAGE ON SEQUENCE nq_seq TO "{reader}"')
    info = {"db": db, "reader": reader, "limited": limited, "seeded": seeded}
    yield info
    seeded.close()
    cur.execute(f'DROP DATABASE IF EXISTS "{db}" WITH (FORCE)')
    cur.execute(f'DROP ROLE IF EXISTS "{limited}"')
    cur.execute(f'DROP ROLE IF EXISTS "{reader}"')
    root.close()


@pytest.fixture
def db_config(pg_env):
    return {
        "type": "standard",
        "host": os.environ["NATQUERY_TEST_PG_HOST"],
        "port": os.environ.get("NATQUERY_TEST_PG_PORT", "5432"),
        "dbname": pg_env["db"],
        "user": pg_env["reader"],
        "password": READER_PW,
    }


@pytest.fixture
def backend(db_config):
    from natquery.backends.postgres import PostgresBackend

    b = PostgresBackend(db_config, max_conn=5)
    yield b
    b.close()


@pytest.fixture
def snapshot(pg_env):
    """Returns a callable giving a comparable fingerprint of all ERP data (detects any write)."""
    tables = [
        "students",
        "faculty",
        "courses",
        "enrollments",
        "fees",
        "salaries",
        "erp_users",
        "departments",
    ]

    def take():
        c = pg_env["seeded"].cursor()
        out = {}
        for t in tables:
            c.execute(
                f"SELECT md5(string_agg(x::text, ',' ORDER BY x::text)) FROM {t} x"
            )
            out[t] = c.fetchone()[0]
        return out

    return take
