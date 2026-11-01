"""Shared harness for the ERP track: builds a NatQuery service on the ERP database with one user per role."""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import psycopg2

HERE = Path(__file__).resolve().parent
ERP = HERE.parent
for cand in (ERP.parent / "src", HERE.parent.parent.parent / "work" / "src"):
    if cand.exists():
        sys.path.insert(0, str(cand))

SCHEMAS = ["auth", "org", "people", "acad", "fin", "lib", "svc", "gh", "cdc", "sys"]
PG = dict(
    host=os.environ.get("PGHOST", "127.0.0.1"),
    port=int(os.environ.get("PGPORT", "5432")),
    dbname=os.environ.get("ERP_DB", "erp"),
)
SUPER = dict(
    PG,
    user=os.environ.get("PGUSER", "postgres"),
    password=os.environ.get("PGPASSWORD", ""),
)
NQ = dict(
    PG, user="natquery", password=os.environ.get("NATQUERY_PW", "natquery-demo-pw")
)
PASSWORD = "Erp-Bench-Passw0rd!"  # the NatQuery-side login of the benchmark users (not the ERP database passwords)

SEC = {
    "rate_limit_per_minute": 100000,
    "llm_calls_per_hour": 1000000,
    "max_repair_attempts": 2,
    "enable_optimizer": False,
    "log_question_text": True,
    "session_idle_minutes": 600,
    "session_max_hours": 24,
    "schema_full_max_columns": 250,
}


def sq(sql, params=None):
    """Run SQL as the database superuser (ground truth; never goes through NatQuery)."""
    with psycopg2.connect(**SUPER) as c, c.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def discover_users() -> dict[str, tuple[str, dict]]:
    """One benchmark user per role, chosen deterministically from the data."""
    stu = sq(
        """SELECT s.student_id, s.person_id, s.department_id, s.hall_id, a.account_id, s.roll_no FROM people.student s JOIN auth.account a ON a.person_id=s.person_id AND a.account_type='STUDENT'
                WHERE s.current_status='ON ROLL' AND s.admission_year=2025 AND s.program_id=(SELECT program_id FROM org.program WHERE program_code='BT-CS') AND s.department_id=(SELECT department_id FROM org.department WHERE dept_code='CS') AND s.hall_id IS NOT NULL
                  AND EXISTS (SELECT 1 FROM acad.semester_performance sp WHERE sp.student_id=s.student_id) AND EXISTS (SELECT 1 FROM lib.loan l WHERE l.person_id=s.person_id AND l.returned_on IS NULL) AND EXISTS (SELECT 1 FROM lib.library_due d WHERE d.person_id=s.person_id AND d.status='UNPAID')
                ORDER BY s.student_id LIMIT 2"""
    )

    def emp(role, dept=None):
        q = """SELECT e.employee_id, e.person_id, e.department_id, a.account_id, e.emp_code FROM people.employee e JOIN auth.account a ON a.person_id=e.person_id AND a.account_type='STAFF'
               JOIN auth.account_role ar ON ar.account_id=a.account_id JOIN auth.role r USING(role_id) WHERE r.role_code=%s"""
        args = [role]
        if dept:
            q += " AND e.department_id=(SELECT department_id FROM org.department WHERE dept_code=%s)"
            args.append(dept)
        return sq(q + " ORDER BY e.employee_id LIMIT 1", args)[0]

    users = {}
    for i, (sid, pid, did, hid, aid, roll) in enumerate(stu):
        users[f"student{i + 1}"] = (
            "student",
            dict(
                student_id=sid,
                person_id=pid,
                dept_id=did,
                hall_id=hid,
                account_id=aid,
                login=roll,
            ),
        )
    # a CS faculty who teaches this semester and advises someone
    fac = sq(
        """SELECT e.employee_id, e.person_id, e.department_id, a.account_id, e.emp_code FROM people.employee e JOIN auth.account a ON a.person_id=e.person_id AND a.account_type='STAFF'
                JOIN auth.account_role ar ON ar.account_id=a.account_id JOIN auth.role r USING(role_id) WHERE r.role_code='faculty' AND e.department_id=(SELECT department_id FROM org.department WHERE dept_code='CS')
                  AND EXISTS (SELECT 1 FROM people.faculty_advisor fa WHERE fa.employee_id=e.employee_id AND fa.to_session_id IS NULL)
                  AND EXISTS (SELECT 1 FROM acad.offering_instructor oi JOIN acad.course_offering o USING(offering_id) JOIN org.academic_session s ON s.session_id=o.session_id WHERE oi.employee_id=e.employee_id AND s.is_current)
                  AND EXISTS (SELECT 1 FROM acad.offering_instructor oi JOIN acad.course_offering o USING(offering_id) JOIN acad.registration g ON g.offering_id=o.offering_id JOIN acad.result r ON r.registration_id=g.registration_id
                              JOIN org.academic_session s ON s.session_id=o.session_id WHERE oi.employee_id=e.employee_id AND s.session_id=(SELECT session_id FROM org.academic_session WHERE is_current)-1 AND r.marks IS NOT NULL)
                  AND NOT EXISTS (SELECT 1 FROM org.department d WHERE d.hod_employee_id=e.employee_id) ORDER BY e.employee_id LIMIT 1"""
    )[0]
    users["faculty1"] = (
        "faculty",
        dict(
            employee_id=fac[0],
            person_id=fac[1],
            dept_id=fac[2],
            account_id=fac[3],
            login=fac[4],
        ),
    )
    for role, dept in (
        ("hod", "CS"),
        ("dept_office", "CS"),
        ("acad_section", None),
        ("accounts", None),
        ("library", None),
        ("hall_office", None),
        ("cdc", None),
        ("admin", None),
    ):
        e = emp(role, dept)
        attrs = dict(
            employee_id=e[0], person_id=e[1], dept_id=e[2], account_id=e[3], login=e[4]
        )
        if role == "hall_office":
            attrs["hall_id"] = sq(
                "SELECT scope_id FROM auth.account_role WHERE account_id=%s AND scope_type='HALL'",
                (e[3],),
            )[0][0]
        users[f"{role}1"] = (role, attrs)
    return users


class ScriptedLLM:
    """A compromised LLM that returns whatever SQL it is told to."""

    def __init__(self):
        self.sql, self.calls = "SELECT 1", 0

    def complete(self, messages, *, temperature=0.0, max_tokens=800) -> str:
        self.calls += 1
        return self.sql


def build(llm, users, tmp: Path | None = None, policy_path: Path | None = None):
    import json

    from natquery.access.policy import load_policy
    from natquery.audit.log import AuditLog
    from natquery.auth import hashing
    from natquery.auth.local import LocalPasswordProvider
    from natquery.auth.session import SessionManager
    from natquery.auth.user_store import UserStore
    from natquery.backends.postgres import PostgresBackend
    from natquery.llm.generator import SQLGenerator
    from natquery.orchestration.pipeline import QueryService
    from natquery.schema.store import apply_semantics

    tmp = tmp or Path(tempfile.mkdtemp(prefix="nqerp_"))
    backend = PostgresBackend({"type": "params", **NQ, "schemas": SCHEMAS}, max_conn=4)
    schema = backend.extract_schema()
    apply_semantics(
        schema, json.loads((ERP / "natquery" / "semantics.json").read_text())
    )
    policy = load_policy(policy_path or ERP / "natquery" / "policy.yaml")
    store = UserStore(tmp / "auth.db")
    provider = LocalPasswordProvider({"lock_threshold": 5}, store=store)
    for name, (role, attrs) in users.items():
        store.add(
            name,
            hashing.hash_password(PASSWORD),
            [role],
            {k: str(v) for k, v in attrs.items() if k != "login"},
        )
    sessions = SessionManager(provider, idle_seconds=36000, max_seconds=86400)
    svc = QueryService(
        backend=backend,
        policy=policy,
        schema=schema,
        generator=SQLGenerator(llm),
        audit=AuditLog(tmp / "audit.log"),
        sessions=sessions,
        security=dict(SEC),
        db_name="erp",
    )
    return svc, tmp


def login_all(svc, users) -> dict[str, str]:
    return {u: svc.login({"username": u, "password": PASSWORD}).token for u in users}
