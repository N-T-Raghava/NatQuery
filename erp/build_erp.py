"""Build the synthetic ERP database from scratch.

    python build_erp.py --host 127.0.0.1 --port 5432 --user postgres [--password ...] [--dbname erp]

Needs a PostgreSQL superuser (it creates roles, the database and the pgcrypto extension).
Steps: roles -> database -> DDL (generated from erp_schema.dsl) -> auth functions -> seed -> grants + RLS.
Passwords of the roles it creates come from the environment (ERP_APP_PW, NATQUERY_PW) or default to demo values.
"""

import argparse
import os
import pathlib
import subprocess
import sys

import psycopg2

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", default="5432")
    ap.add_argument("--user", default="postgres")
    ap.add_argument("--password", default=os.environ.get("PGPASSWORD", ""))
    ap.add_argument("--dbname", default="erp")
    ap.add_argument("--no-seed", action="store_true")
    a = ap.parse_args()
    base = dict(host=a.host, port=a.port, user=a.user, password=a.password)

    subprocess.run(
        [sys.executable, str(HERE / "gen_ddl.py")], check=True
    )  # re-validate and regenerate DDL

    c = psycopg2.connect(dbname="postgres", **base)
    c.autocommit = True
    cur = c.cursor()
    app_pw = os.environ.get("ERP_APP_PW", "erp-app-demo-pw")
    nq_pw = os.environ.get("NATQUERY_PW", "natquery-demo-pw")
    for role, extra in (
        ("erp_owner", "NOLOGIN"),
        ("erp_ro", "NOLOGIN"),
        ("erp_app", f"LOGIN PASSWORD '{app_pw}'"),
        ("natquery", f"LOGIN NOINHERIT PASSWORD '{nq_pw}'"),
    ):
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
        cur.execute(f"{'ALTER' if cur.fetchone() else 'CREATE'} ROLE {role} {extra}")
    cur.execute(f'DROP DATABASE IF EXISTS "{a.dbname}"')
    cur.execute(f'CREATE DATABASE "{a.dbname}" OWNER erp_owner')
    cur.execute(f'REVOKE ALL ON DATABASE "{a.dbname}" FROM PUBLIC')
    cur.execute(f'GRANT CONNECT ON DATABASE "{a.dbname}" TO erp_app, natquery, erp_ro')
    c.close()

    c = psycopg2.connect(dbname=a.dbname, **base)
    c.autocommit = True
    cur = c.cursor()
    for f in ("01_ddl.sql", "02_auth.sql"):
        cur.execute((HERE / "sql" / f).read_text())
        print("applied", f)
    if not a.no_seed:
        import seed

        dsn = f"dbname={a.dbname} user={a.user} host={a.host} port={a.port} password={a.password}"
        counts = seed.seed(dsn)
        (HERE / "seed_row_counts.txt").write_text(
            "".join(f"{t:40s} {n:>9,}\n" for t, n in sorted(counts.items()))
        )
    cur.execute((HERE / "sql" / "04_roles_rls.sql").read_text())
    print("applied 04_roles_rls.sql")
    cur.execute("ANALYZE")
    print("done")


if __name__ == "__main__":
    main()
