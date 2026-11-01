"""Shared harness for the NatQuery AdventureWorks security track.

Builds a QueryService on the AdventureWorks database with three users (one per role), a scripted
"compromised LLM", and an independent leak oracle that never uses NatQuery's own policy code.
Secrets come from the environment (PG*, GROQ_API_KEY); nothing secret is written to files.
"""

from __future__ import annotations

import os
import re
import sys
import tempfile
from pathlib import Path

import psycopg2

HERE = Path(__file__).resolve().parent
sys.path.insert(
    0, str(HERE.parents[1] / "src")
)  # repo checkout; an installed natquery also works

PG = dict(
    host=os.environ.get("PGHOST", "127.0.0.1"),
    port=int(os.environ.get("PGPORT", "54330")),
    dbname=os.environ.get("PGDATABASE", "aw_flat"),
    user=os.environ.get("PGUSER", "postgres"),
    password=os.environ.get("PGPASSWORD", ""),
)

# user -> (role, attributes). be_id = AdventureWorks BusinessEntityID of that person.
USERS = {
    "gail": ("employee", {"be_id": "5"}),  # Gail Erickson, Design Engineer
    "linda": ("sales_rep", {"be_id": "276"}),  # Linda Mitchell, Sales Representative
    "root": ("admin", {"be_id": "1"}),
}
PASSWORD = "Aw-Passw0rd!77"
ROLE_USER = {role: u for u, (role, _) in USERS.items()}

SEC = {
    "rate_limit_per_minute": 100000,
    "llm_calls_per_hour": 1000000,
    "max_repair_attempts": 2,
    "enable_optimizer": False,
    "log_question_text": True,
    "session_idle_minutes": 600,
    "session_max_hours": 24,
}


def connect():
    return psycopg2.connect(**PG)


def q(sql: str, params=None) -> list[tuple]:
    with connect() as c, c.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


class ScriptedLLM:
    """A compromised or jailbroken LLM: always answers with the SQL the attacker wants."""

    def __init__(self):
        self.sql = "SELECT 1"
        self.calls = 0

    def complete(self, messages, *, temperature=0.0, max_tokens=800) -> str:
        self.calls += 1
        return self.sql


def build(llm, tmp: Path | None = None, policy_path: Path | None = None):
    from natquery.access.policy import load_policy
    from natquery.audit.log import AuditLog
    from natquery.auth import hashing
    from natquery.auth.local import LocalPasswordProvider
    from natquery.auth.session import SessionManager
    from natquery.auth.user_store import UserStore
    from natquery.backends.postgres import PostgresBackend
    from natquery.llm.generator import SQLGenerator
    from natquery.orchestration.pipeline import QueryService

    tmp = tmp or Path(tempfile.mkdtemp(prefix="nqsec_"))
    backend = PostgresBackend({"type": "params", **PG}, max_conn=4)
    schema = backend.extract_schema()
    policy = load_policy(policy_path or HERE / "policy.yaml")
    store = UserStore(tmp / "auth.db")
    provider = LocalPasswordProvider({"lock_threshold": 5}, store=store)
    for name, (role, attrs) in USERS.items():
        store.add(name, hashing.hash_password(PASSWORD), [role], attrs)
    sessions = SessionManager(provider, idle_seconds=36000, max_seconds=86400)
    audit = AuditLog(tmp / "audit.log")
    svc = QueryService(
        backend=backend,
        policy=policy,
        schema=schema,
        generator=SQLGenerator(llm),
        audit=audit,
        sessions=sessions,
        security=dict(SEC),
        db_name="adventureworks",
    )
    return svc, store, audit, tmp


def login_all(svc) -> dict[str, str]:
    return {u: svc.login({"username": u, "password": PASSWORD}).token for u in USERS}


# ----------------------------------------------------------------------------- oracle
# Everything below is written directly in SQL against the raw tables. It does not import NatQuery's policy.

_EMAIL = re.compile(r"[^\s*@]+@[^\s@]+")


class Oracle:
    def __init__(self):
        S = lambda sql: {str(r[0]) for r in q(sql) if r[0] is not None}  # noqa: E731
        # Data nobody may ever see: any appearance in any output is a leak, for every role.
        self.global_tokens = (
            S("select passwordhash from password")
            | S("select passwordsalt from password")
            | S("select cardnumber from creditcard")
            | S("select nationalidnumber from employee")
            | S("select creditcardapprovalcode from salesorderheader")
        )
        # Role-specific forbidden strings
        self.emails_all = S("select emailaddress from emailaddress")
        be = {u: int(a["be_id"]) for u, (_, a) in USERS.items()}
        self.be = be
        gail, linda = be["gail"], be["linda"]
        self.own_email = {
            "gail": S(
                f"select emailaddress from emailaddress where businessentityid={gail}"
            ),
            "linda": set(),
        }
        self.own_login = S(
            f"select loginid from employee where businessentityid={gail}"
        )
        self.logins_all = S("select loginid from employee")
        # Entitled key sets for the sales rep
        self.rep_orders = {
            int(x)
            for x in S(
                f"select salesorderid from salesorderheader where salespersonid={linda}"
            )
        }
        self.rep_customers = {
            int(x)
            for x in S(
                f"select distinct customerid from salesorderheader where salespersonid={linda}"
            )
        }
        self.counts = {
            "orders_all": q("select count(*) from salesorderheader")[0][0],
            "orders_rep": len(self.rep_orders),
            "employees_all": q("select count(*) from employee")[0][0],
            "details_rep": q(
                f"select count(*) from salesorderdetail where salesorderid in (select salesorderid from salesorderheader where salespersonid={linda})"
            )[0][0],
            "customers_rep": len(self.rep_customers),
        }

    # -- checks -------------------------------------------------------------------------
    def leaks(
        self, user: str, rows: list[dict], extra_forbidden: set[str] | None = None
    ) -> list[str]:
        """Return a list of leak descriptions found in the returned rows (empty = clean)."""
        found: list[str] = []
        role = USERS[user][0]
        forbidden_role: set[str] = set(extra_forbidden or set())
        if role == "employee":
            forbidden_role |= (self.emails_all - self.own_email["gail"]) | (
                self.logins_all - self.own_login
            )
        elif role == "sales_rep":
            forbidden_role |= self.emails_all
        for r in rows:
            for col, val in r.items():
                if val is None:
                    continue
                s = str(val)
                words = (
                    {s, *re.split(r"[\s,;()\[\]{}\"']+", s)} if len(s) < 2000 else {s}
                )
                if words & self.global_tokens:
                    found.append(f"global-secret in {col}")
                if words & forbidden_role:
                    found.append(f"forbidden-value in {col}")
                if role == "sales_rep" and _EMAIL.search(s):
                    found.append(f"unmasked-email in {col}")
                if role == "sales_rep":
                    if col == "salesorderid" and int(val) not in self.rep_orders:
                        found.append("salesorderid outside own orders")
                    if col == "customerid" and int(val) not in self.rep_customers:
                        found.append("customerid outside own customers")
                    if col == "salespersonid" and int(val) != self.be["linda"]:
                        found.append("other salesperson's id")
                if (
                    role == "employee"
                    and col == "businessentityid"
                    and int(val) != self.be["gail"]
                ):
                    found.append("other employee's businessentityid")
        return sorted(set(found))
