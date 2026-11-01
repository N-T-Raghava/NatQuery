"""Checks of the ERP database itself (no NatQuery involved): authentication functions, grants and row-level security.

    python test_erp.py --port 5432 --superuser-password ... [--dbname erp]

The 'oracle' numbers are computed as the superuser with plain SQL, then compared with what each database role sees
through RLS. Exit code 1 if any check fails; results are also written to results/erp_db_checks.json.
"""

import argparse
import json
import os
import pathlib

import psycopg2

HERE = pathlib.Path(__file__).resolve().parent
ap = argparse.ArgumentParser()
ap.add_argument("--host", default="127.0.0.1")
ap.add_argument("--port", default="5432")
ap.add_argument("--dbname", default="erp")
ap.add_argument("--superuser", default="postgres")
ap.add_argument("--superuser-password", default=os.environ.get("PGPASSWORD", ""))
ap.add_argument(
    "--natquery-password", default=os.environ.get("NATQUERY_PW", "natquery-demo-pw")
)
ap.add_argument(
    "--erp-app-password", default=os.environ.get("ERP_APP_PW", "erp-app-demo-pw")
)
a = ap.parse_args()


def conn(user, pw):
    c = psycopg2.connect(
        host=a.host, port=a.port, dbname=a.dbname, user=user, password=pw
    )
    c.autocommit = False
    return c


su, nq, app = (
    conn(a.superuser, a.superuser_password),
    conn("natquery", a.natquery_password),
    conn("erp_app", a.erp_app_password),
)
results = []


def check(name, ok, detail=""):
    results.append({"check": name, "ok": bool(ok), "detail": str(detail)})
    print(
        ("PASS " if ok else "FAIL ")
        + name
        + (f"  [{detail}]" if detail and not ok else "")
    )


def one(c, sql, args=None):
    cur = c.cursor()
    cur.execute(sql, args)
    r = cur.fetchone()
    c.commit()
    return r[0] if r else None


def as_role(role_ctx: dict, sql, args=None, db_role="erp_ro"):
    """Run sql the way NatQuery does: login as natquery, SET LOCAL ROLE erp_ro, set the request context."""
    cur = nq.cursor()
    try:
        for k, v in role_ctx.items():
            cur.execute("SELECT set_config(%s, %s, true)", (f"erp.{k}", str(v)))
        if db_role:
            cur.execute(f"SET LOCAL ROLE {db_role}")
        cur.execute(sql, args)
        return cur.fetchall()
    finally:
        nq.rollback()


def denied(sql, ctx=None, db_role="erp_ro"):
    try:
        as_role(ctx or {}, sql, db_role=db_role)
        return False
    except psycopg2.errors.InsufficientPrivilege:
        return True
    except psycopg2.Error:
        return False


# ---------------------------------------------------------------- pick test subjects
stu = su.cursor()
stu.execute(
    """SELECT s.student_id, s.person_id, s.roll_no, s.department_id, s.hall_id FROM people.student s
   WHERE s.current_status='ON ROLL' AND s.admission_year=2023 AND s.hall_id IS NOT NULL ORDER BY s.student_id LIMIT 1"""
)
S = stu.fetchone()
su.rollback()
sid, spid, roll, dept, hall = S
other = one(
    su,
    "SELECT student_id FROM people.student WHERE student_id <> %s AND department_id <> %s LIMIT 1",
    (sid, dept),
)
fac = su.cursor()
fac.execute(
    """SELECT e.employee_id, e.person_id, e.department_id FROM people.employee e JOIN auth.account_role ar ON ar.account_id=(SELECT account_id FROM auth.account WHERE person_id=e.person_id AND account_type='STAFF' LIMIT 1)
   JOIN auth.role r USING(role_id) WHERE r.role_code='faculty' AND e.department_id=%s ORDER BY e.employee_id LIMIT 1""",
    (dept,),
)
F = fac.fetchone()
su.rollback()
eid, epid, edept = F

# ---------------------------------------------------------------- authentication
login = one(su, "SELECT login_id FROM auth.account WHERE login_id=%s", (roll,))
pw = f"Erp@{roll}"
r = one(su, "SELECT count(*) FROM auth.login(%s, %s)", (roll, pw))
su.commit()
check("login with the right password returns a session", r == 1)
row = su.cursor()
row.execute("SELECT session_token, roles FROM auth.login(%s, %s)", (roll, pw))
tok, roles = row.fetchone()
su.commit()
check("session carries the student role", roles == ["student"], roles)
check(
    "stored session is a SHA-256 hash, not the token",
    one(su, "SELECT count(*) FROM auth.session WHERE token_hash = %s", (tok,)) == 0
    and one(
        su,
        "SELECT count(*) FROM auth.session WHERE token_hash = encode(digest(%s,'sha256'),'hex')",
        (tok,),
    )
    == 1,
)
check(
    "verify_session accepts the token",
    one(su, "SELECT count(*) FROM auth.verify_session(%s)", (tok,)) == 1,
)
check(
    "verify_session rejects a forged token",
    one(su, "SELECT count(*) FROM auth.verify_session(%s)", ("0" * 64,)) == 0,
)
check(
    "logout kills the session",
    one(su, "SELECT auth.logout(%s)", (tok,)) is True
    and one(su, "SELECT count(*) FROM auth.verify_session(%s)", (tok,)) == 0,
)
su.commit()
check(
    "wrong password and unknown user both return nothing",
    one(su, "SELECT count(*) FROM auth.login(%s,'nope')", (roll,)) == 0
    and one(su, "SELECT count(*) FROM auth.login('nobody','nope')") == 0,
)
su.commit()
su.cursor().execute(
    "UPDATE auth.account SET failed_attempts=0, locked_until=NULL WHERE login_id=%s",
    (roll,),
)
su.commit()
for _ in range(5):
    one(su, "SELECT count(*) FROM auth.login(%s,'bad')", (roll,))
    su.commit()
check(
    "five failures lock the account",
    one(su, "SELECT locked_until > now() FROM auth.account WHERE login_id=%s", (roll,))
    is True,
)
check(
    "a locked account refuses even the right password",
    one(su, "SELECT count(*) FROM auth.login(%s,%s)", (roll, pw)) == 0,
)
su.commit()
su.cursor().execute(
    "UPDATE auth.account SET failed_attempts=0, locked_until=NULL WHERE login_id=%s",
    (roll,),
)
su.commit()
check(
    "every attempt is audited",
    one(
        su,
        "SELECT count(*) FROM auth.login_audit WHERE login_id_tried=%s AND event IN ('LOGIN_FAILED','LOGIN_BLOCKED')",
        (roll,),
    )
    >= 6,
)
code = one(su, "SELECT auth.issue_otp(%s,'EMAIL_RESET')", (roll,))
su.commit()
check(
    "OTP is stored hashed",
    one(su, "SELECT count(*) FROM auth.otp WHERE otp_hash = %s", (code,)) == 0,
)
check(
    "wrong OTP rejected, right OTP accepted once",
    one(su, "SELECT auth.verify_otp(%s,'EMAIL_RESET','000000')", (roll,)) is False
    and one(su, "SELECT auth.verify_otp(%s,'EMAIL_RESET',%s)", (roll, code)) is True,
)
su.commit()
check(
    "OTP cannot be reused",
    one(su, "SELECT auth.verify_otp(%s,'EMAIL_RESET',%s)", (roll, code)) is False,
)
check(
    "weak new password refused",
    one(su, "SELECT auth.change_password(%s,%s,'short1')", (roll, pw)) is False,
)
check(
    "passwords are bcrypt",
    one(
        su,
        "SELECT password_hash LIKE '$2a$%%' FROM auth.account WHERE login_id=%s",
        (roll,),
    )
    is True,
)
check(
    "disabled account cannot log in",
    (lambda r: r == 0)(
        (lambda d: one(su, "SELECT count(*) FROM auth.login(%s,%s)", (d, f"Erp@{d}")))(
            one(su, "SELECT login_id FROM auth.account WHERE status='DISABLED' LIMIT 1")
        )
    ),
)
su.commit()
check(
    "one person can hold two logins (Switch Login)",
    one(
        su,
        "SELECT count(*) FROM (SELECT person_id FROM auth.account GROUP BY 1 HAVING count(*)>1) x",
    )
    >= 10,
)

# ---------------------------------------------------------------- privileges
check(
    "erp_ro cannot read password hashes",
    denied("SELECT password_hash FROM auth.account"),
)
check("erp_ro cannot read OTP hashes", denied("SELECT otp_hash FROM auth.otp"))
check(
    "erp_ro cannot read session tokens", denied("SELECT token_hash FROM auth.session")
)
check(
    "erp_ro can read the non-secret account columns",
    len(as_role({"role": "admin"}, "SELECT login_id, status FROM auth.account LIMIT 1"))
    == 1,
)
check(
    "erp_ro cannot write",
    denied("UPDATE people.student SET roll_no = roll_no")
    and denied("DELETE FROM acad.result")
    and denied(
        "INSERT INTO org.hall(hall_id,hall_code,name,capacity) VALUES (99,'x','x',1)"
    ),
)
check(
    "natquery login alone has no table access",
    denied("SELECT count(*) FROM people.student", db_role=None),
)
check(
    "natquery cannot become erp_owner or erp_app",
    all(denied("SELECT 1", db_role=r) for r in ("erp_owner", "erp_app")),
)
app.rollback()
try:
    cur = app.cursor()
    cur.execute("SELECT password_hash FROM auth.account LIMIT 1")
    hashed_ok = False
except psycopg2.errors.InsufficientPrivilege:
    hashed_ok = True
app.rollback()
check("erp_app has no direct access to auth.account (only the functions)", hashed_ok)
check(
    "erp_app sees all rows (its own allow-all RLS policy)",
    (
        lambda cur: (
            cur.execute("SELECT count(*) FROM people.student"),
            cur.fetchone()[0],
        )[1]
    )(app.cursor())
    > 2000,
)
app.rollback()

# ---------------------------------------------------------------- row-level security vs an oracle
total_students = one(su, "SELECT count(*) FROM people.student")


def n(ctx, sql):
    return as_role(ctx, sql)[0][0]


check(
    "no request context -> no rows at all",
    all(
        n({}, f"SELECT count(*) FROM {t}") == 0
        for t in (
            "people.student",
            "people.person",
            "acad.registration",
            "acad.result",
            "fin.payment",
            "fin.salary_slip",
            "lib.loan",
            "people.bank_account",
        )
    ),
)
ctx_s = {"role": "student", "student_id": sid, "person_id": spid}
check(
    "student sees exactly one student row (own)",
    n(ctx_s, "SELECT count(*) FROM people.student") == 1,
)
check(
    "student sees only own registrations",
    n(ctx_s, "SELECT count(*) FROM acad.registration")
    == one(su, "SELECT count(*) FROM acad.registration WHERE student_id=%s", (sid,)),
)
check(
    "student sees only own results",
    n(ctx_s, "SELECT count(*) FROM acad.result")
    == one(
        su,
        "SELECT count(*) FROM acad.result r JOIN acad.registration g USING(registration_id) WHERE g.student_id=%s",
        (sid,),
    ),
)
check(
    "student sees only own payments",
    n(ctx_s, "SELECT count(*) FROM fin.payment")
    == one(su, "SELECT count(*) FROM fin.payment WHERE student_id=%s", (sid,)),
)
check(
    "student cannot see anyone else's row by id",
    n(ctx_s, f"SELECT count(*) FROM people.student WHERE student_id={other}") == 0,
)
check(
    "student sees no salary slips",
    n(ctx_s, "SELECT count(*) FROM fin.salary_slip") == 0,
)
check(
    "student sees only own person row",
    n(ctx_s, "SELECT count(*) FROM people.person") == 1,
)
check(
    "student sees no guardians of others",
    n(ctx_s, "SELECT count(*) FROM people.guardian WHERE student_id <> %s" % sid) == 0,
)
check(
    "a forged student_id in the context only ever shows that one student",
    n(
        {"role": "student", "student_id": other, "person_id": spid},
        "SELECT count(*) FROM people.student",
    )
    == 1,
)
ctx_f = {"role": "faculty", "employee_id": eid, "person_id": epid, "dept_id": edept}
exp_f = one(
    su,
    """SELECT count(*) FROM people.student s WHERE
   EXISTS (SELECT 1 FROM people.faculty_advisor fa WHERE fa.student_id=s.student_id AND fa.employee_id=%s AND fa.to_session_id IS NULL)
   OR EXISTS (SELECT 1 FROM acad.registration r JOIN acad.offering_instructor oi USING(offering_id) WHERE r.student_id=s.student_id AND oi.employee_id=%s)""",
    (eid, eid),
)
check(
    "faculty sees advisees plus own course students",
    n(ctx_f, "SELECT count(*) FROM people.student") == exp_f
    and 0 < exp_f < total_students,
    exp_f,
)
check(
    "faculty sees only own salary",
    n(ctx_f, "SELECT count(*) FROM fin.salary_slip")
    == one(su, "SELECT count(*) FROM fin.salary_slip WHERE employee_id=%s", (eid,)),
)
check(
    "faculty cannot see payments or bank accounts of students",
    n(ctx_f, "SELECT count(*) FROM fin.payment") == 0
    and n(
        ctx_f, "SELECT count(*) FROM people.bank_account WHERE person_id <> %s" % epid
    )
    == 0,
)
ctx_h = {"role": "hod", "employee_id": eid, "person_id": epid, "dept_id": dept}
check(
    "HoD sees exactly the department's students",
    n(ctx_h, "SELECT count(*) FROM people.student")
    == one(su, "SELECT count(*) FROM people.student WHERE department_id=%s", (dept,)),
)
check(
    "HoD sees only the department's registrations",
    n(ctx_h, "SELECT count(*) FROM acad.registration")
    == one(
        su,
        "SELECT count(*) FROM acad.registration r JOIN people.student s USING(student_id) WHERE s.department_id=%s",
        (dept,),
    ),
)
check("HoD sees no fee payments", n(ctx_h, "SELECT count(*) FROM fin.payment") == 0)
ctx_a = {"role": "accounts", "employee_id": eid, "person_id": epid}
check(
    "accounts sees every payment and salary slip",
    n(ctx_a, "SELECT count(*) FROM fin.payment")
    == one(su, "SELECT count(*) FROM fin.payment")
    and n(ctx_a, "SELECT count(*) FROM fin.salary_slip")
    == one(su, "SELECT count(*) FROM fin.salary_slip"),
)
check(
    "accounts sees no academic results",
    n(ctx_a, "SELECT count(*) FROM acad.result") == 0,
)
ctx_acad = {"role": "acad_section"}
check(
    "academic section sees all students and results, no bank accounts or payments",
    n(ctx_acad, "SELECT count(*) FROM people.student") == total_students
    and n(ctx_acad, "SELECT count(*) FROM acad.result")
    == one(su, "SELECT count(*) FROM acad.result")
    and n(ctx_acad, "SELECT count(*) FROM people.bank_account") == 0
    and n(ctx_acad, "SELECT count(*) FROM fin.payment") == 0,
)
ctx_hall = {"role": "hall_office", "hall_id": hall}
check(
    "hall office sees only its residents",
    n(ctx_hall, "SELECT count(*) FROM people.student")
    == one(su, "SELECT count(*) FROM people.student WHERE hall_id=%s", (hall,)),
)
check(
    "library sees all loans but no results, payments or bank accounts",
    n({"role": "library"}, "SELECT count(*) FROM lib.loan")
    == one(su, "SELECT count(*) FROM lib.loan")
    and all(
        n({"role": "library"}, f"SELECT count(*) FROM {t}") == 0
        for t in (
            "acad.result",
            "fin.payment",
            "people.bank_account",
            "acad.registration",
        )
    ),
)
check(
    "CDC sees only students who applied for jobs",
    n({"role": "cdc"}, "SELECT count(*) FROM people.student")
    == one(su, "SELECT count(DISTINCT student_id) FROM cdc.job_application"),
)
check(
    "admin sees everything",
    n({"role": "admin"}, "SELECT count(*) FROM people.student") == total_students,
)
check(
    "an unknown role name sees nothing",
    n({"role": "wizard"}, "SELECT count(*) FROM people.student") == 0,
)
check(
    "a role name with SQL in it is just a string",
    n({"role": "admin' OR '1'='1"}, "SELECT count(*) FROM people.student") == 0,
)

(HERE / "results").mkdir(exist_ok=True)
(HERE / "results" / "erp_db_checks.json").write_text(
    json.dumps(
        {
            "passed": sum(r["ok"] for r in results),
            "total": len(results),
            "checks": results,
        },
        indent=1,
    )
)
print(f"\n{sum(r['ok'] for r in results)}/{len(results)} checks passed")
raise SystemExit(0 if all(r["ok"] for r in results) else 1)
