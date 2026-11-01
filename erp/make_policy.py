"""Generate natquery/policy.yaml and natquery/semantics.json for the ERP database.

    python make_policy.py

The policy is generated (not hand-written) because the same few filters repeat across ~60 tables per role.
Identity attributes a NatQuery user carries: person_id, student_id, employee_id, dept_id, hall_id, account_id.
The database's own row-level security (sql/04_roles_rls.sql) mirrors these row scopes as a second line of defence.
"""

import json
import pathlib

import yaml
from gen_ddl import parse

HERE = pathlib.Path(__file__).resolve().parent
T = parse()

# ------------------------------------------------------------------ reusable row scopes
OWN_STUDENT = "student_id = :student_id"
OWN_PERSON = "person_id = :person_id"
OWN_ACCOUNT = "account_id = :account_id"
OWN_REG = "registration_id IN (SELECT registration_id FROM acad.registration WHERE student_id = :student_id)"
FAC_STUDENTS = (
    "SELECT student_id FROM people.faculty_advisor WHERE employee_id = :employee_id AND to_session_id IS NULL "
    "UNION SELECT r.student_id FROM acad.registration r JOIN acad.offering_instructor oi ON oi.offering_id = r.offering_id "
    "WHERE oi.employee_id = :employee_id"
)
OWN_OFFERINGS = (
    "SELECT offering_id FROM acad.offering_instructor WHERE employee_id = :employee_id"
)
DEPT_STUDENTS = "SELECT student_id FROM people.student WHERE department_id = :dept_id"
HALL_STUDENTS = "SELECT student_id FROM people.student WHERE hall_id = :hall_id"
CDC_STUDENTS = "SELECT student_id FROM cdc.job_application"

STU_LIMITED = [
    "student_id",
    "roll_no",
    "person_id",
    "program_id",
    "department_id",
    "admission_year",
    "current_semester",
    "current_status",
    "hall_id",
]
PERSON_LIMITED = ["person_id", "full_name", "gender", "institute_email"]
ACCOUNT_SAFE = [
    "account_id",
    "login_id",
    "person_id",
    "account_type",
    "status",
    "failed_attempts",
    "locked_until",
    "last_login_at",
    "created_at",
]
EMP_DIRECTORY = [
    "employee_id",
    "emp_code",
    "display_name",
    "department_id",
    "designation",
    "employee_type",
    "official_email",
    "is_active",
]


def g(columns="*", row_filter=None, masked=None, deny=None):
    d = {"columns": columns}
    if row_filter:
        d["row_filter"] = row_filter
    if masked:
        d["masked"] = masked
    if deny:
        d["deny_columns"] = deny
    return d


def via_reg(scope_sql):
    return f"registration_id IN (SELECT registration_id FROM acad.registration WHERE student_id IN ({scope_sql}))"


# ------------------------------------------------------------------ roles
base = {  # reference data every signed-in user may read
    "org.*": g(),
    "sys.*": g(),
    "acad.curriculum": g(),
    "acad.course": g(),
    "acad.curriculum_course": g(),
    "acad.course_offering": g(),
    "acad.offering_instructor": g(),
    "acad.timetable_slot": g(),
    "acad.exam_schedule": g(),
    "fin.fee_head": g(),
    "fin.fee_structure": g(),
    "fin.scholarship_scheme": g(),
    "lib.book": g(),
    "lib.book_copy": g(),
    "svc.application_type": g(),
    "cdc.company": g(),
    "cdc.job_posting": g(),
    "people.employee": g(EMP_DIRECTORY),
    "auth.account": g(
        [
            "account_id",
            "login_id",
            "account_type",
            "status",
            "last_login_at",
            "created_at",
        ],
        OWN_ACCOUNT,
    ),
    "auth.login_audit": g("*", OWN_ACCOUNT),
}
student = {
    "people.person": g("*", OWN_PERSON),
    "people.person_identity": g("*", OWN_PERSON),
    "people.bank_account": g("*", OWN_PERSON),
    "people.address": g("*", OWN_PERSON),
    "people.emergency_contact": g("*", OWN_PERSON),
    "people.student": g("*", OWN_STUDENT),
    "acad.registration": g("*", OWN_STUDENT),
    "acad.result": g("*", OWN_REG),
    "acad.assignment_mark": g("*", OWN_REG),
    "acad.class_attendance": g("*", OWN_REG),
    "acad.course_feedback": g("*", OWN_REG),
    "fin.fee_demand_item": g(
        "*",
        "demand_id IN (SELECT demand_id FROM fin.fee_demand WHERE student_id = :student_id)",
    ),
    "lib.loan": g("*", OWN_PERSON),
    "lib.library_due": g("*", OWN_PERSON),
    "svc.paperless_application": g("*", "applicant_person_id = :person_id"),
    "svc.application_step": g(
        "*",
        "application_id IN (SELECT application_id FROM svc.paperless_application WHERE applicant_person_id = :person_id)",
    ),
    "svc.complaint": g("*", OWN_ACCOUNT),
    "svc.grievance": g("*", OWN_ACCOUNT),
    "svc.mail_recipient": g("*", OWN_ACCOUNT),
    "svc.mail_message": g(
        "*",
        "message_id IN (SELECT message_id FROM svc.mail_recipient WHERE account_id = :account_id) OR from_account_id = :account_id",
    ),
    "svc.notice": g("*", "audience IN ('ALL', 'STUDENT')"),
    "cdc.offer": g(
        "*",
        "job_application_id IN (SELECT job_application_id FROM cdc.job_application WHERE student_id = :student_id)",
    ),
}
for t in (
    "people.guardian",
    "people.student_status_history",
    "people.faculty_advisor",
    "acad.registration_card",
    "acad.semester_performance",
    "acad.admit_card",
    "acad.special_program_application",
    "acad.program_switch_application",
    "acad.breadth_choice",
    "acad.convocation_registration",
    "acad.graduation_clearance",
    "acad.award_prize",
    "acad.student_activity",
    "acad.medical_leave",
    "acad.student_document",
    "acad.rep_nomination",
    "acad.data_sharing_consent",
    "acad.exam_attendance",
    "fin.fee_demand",
    "fin.payment",
    "fin.family_income_declaration",
    "fin.scholarship_award",
    "fin.mediclaim_claim",
    "svc.counselling_session",
    "cdc.job_application",
):
    student[t] = g("*", OWN_STUDENT)

staff_common = {  # what every employee role may do with their own records
    "fin.salary_slip": g("*", "employee_id = :employee_id"),
    "lib.loan": g("*", OWN_PERSON),
    "lib.library_due": g("*", OWN_PERSON),
    "svc.paperless_application": g("*", "applicant_person_id = :person_id"),
    "svc.application_step": g(
        "*",
        "application_id IN (SELECT application_id FROM svc.paperless_application WHERE applicant_person_id = :person_id)",
    ),
    "svc.mail_recipient": g("*", OWN_ACCOUNT),
    "svc.mail_message": g(
        "*",
        "message_id IN (SELECT message_id FROM svc.mail_recipient WHERE account_id = :account_id) OR from_account_id = :account_id",
    ),
    "svc.notice": g("*", "audience IN ('ALL', 'STAFF')"),
    "gh.guesthouse": g(),
    "gh.gh_room": g(),
    "gh.gh_booking": g("*", "booked_by_account_id = :account_id"),
}
faculty = {
    "people.student": g(STU_LIMITED + ["category"], f"student_id IN ({FAC_STUDENTS})"),
    "people.person": g(
        PERSON_LIMITED,
        f"person_id = :person_id OR person_id IN (SELECT person_id FROM people.student WHERE student_id IN ({FAC_STUDENTS}))",
    ),
    "people.faculty_advisor": g("*", "employee_id = :employee_id"),
    "acad.registration": g("*", f"student_id IN ({FAC_STUDENTS})"),
    "acad.result": g("*", via_reg(FAC_STUDENTS)),
    "acad.semester_performance": g("*", f"student_id IN ({FAC_STUDENTS})"),
    "acad.class_attendance": g(
        "*",
        f"registration_id IN (SELECT registration_id FROM acad.registration WHERE offering_id IN ({OWN_OFFERINGS}))",
    ),
    "acad.assignment_mark": g(
        "*",
        f"registration_id IN (SELECT registration_id FROM acad.registration WHERE offering_id IN ({OWN_OFFERINGS}))",
    ),
    "acad.course_feedback": g(
        "*",
        f"registration_id IN (SELECT registration_id FROM acad.registration WHERE offering_id IN ({OWN_OFFERINGS}))",
    ),
    **staff_common,
}
hod = {
    "people.student": g(
        STU_LIMITED + ["category", "nature_of_admission", "native_state"],
        "department_id = :dept_id",
    ),
    "people.person": g(
        PERSON_LIMITED + ["date_of_birth"],
        "person_id = :person_id OR person_id IN (SELECT person_id FROM people.student WHERE department_id = :dept_id) "
        "OR person_id IN (SELECT person_id FROM people.employee WHERE department_id = :dept_id)",
    ),
    "people.faculty_advisor": g("*", f"student_id IN ({DEPT_STUDENTS})"),
    "acad.registration": g("*", f"student_id IN ({DEPT_STUDENTS})"),
    "acad.result": g("*", via_reg(DEPT_STUDENTS)),
    "acad.semester_performance": g("*", f"student_id IN ({DEPT_STUDENTS})"),
    "acad.class_attendance": g("*", via_reg(DEPT_STUDENTS)),
    "acad.assignment_mark": g("*", via_reg(DEPT_STUDENTS)),
    "acad.course_feedback": g("*", via_reg(DEPT_STUDENTS)),
    "acad.medical_leave": g(
        "*", f"student_id IN ({DEPT_STUDENTS})", masked={"reason": "redacted"}
    ),
    "acad.program_switch_application": g("*", f"student_id IN ({DEPT_STUDENTS})"),
    "acad.special_program_application": g("*", f"student_id IN ({DEPT_STUDENTS})"),
}
acad_section = {
    "acad.*": g(),
    "acad.medical_leave": g("*", masked={"reason": "redacted"}),
    "people.student": g("*", deny=["pwd_status"]),
    "people.person": g(
        PERSON_LIMITED + ["date_of_birth", "nationality", "mother_tongue"]
    ),
    "people.guardian": g(
        ["guardian_id", "student_id", "relation", "name", "mobile_no", "email"],
        masked={"mobile_no": "last4", "email": "email"},
    ),
    "people.faculty_advisor": g(),
    "people.student_status_history": g(),
    "svc.paperless_application": g(),
    "svc.application_step": g(),
    "svc.mail_recipient": g("*", OWN_ACCOUNT),
    **{
        k: v
        for k, v in staff_common.items()
        if k.startswith(("svc.notice", "svc.mail_message", "fin.salary", "lib."))
    },
}
accounts = {
    "fin.*": g(),
    "people.student": g(STU_LIMITED),
    "people.person": g(PERSON_LIMITED),
    "people.bank_account": g(
        [
            "bank_account_id",
            "person_id",
            "bank_name",
            "branch_name",
            "ifsc",
            "is_primary",
        ]
    ),
    "gh.gh_booking": g(),
    "gh.guesthouse": g(),
    "gh.gh_room": g(),
    **{
        k: v
        for k, v in staff_common.items()
        if k.startswith(("svc.notice", "svc.mail", "lib."))
    },
}
library = {
    "lib.*": g(),
    "people.student": g(
        ["student_id", "roll_no", "person_id", "department_id", "current_status"]
    ),
    "people.person": g(PERSON_LIMITED),
    **{
        k: v
        for k, v in staff_common.items()
        if k.startswith(("svc.notice", "svc.mail", "fin.salary"))
    },
}
hall_office = {
    "people.student": g(
        STU_LIMITED + ["room_id", "hall_attached"], "hall_id = :hall_id"
    ),
    "people.person": g(
        PERSON_LIMITED,
        "person_id IN (SELECT person_id FROM people.student WHERE hall_id = :hall_id)",
    ),
    **{
        k: v
        for k, v in staff_common.items()
        if k.startswith(("svc.notice", "svc.mail", "fin.salary", "lib."))
    },
}
cdc = {
    "cdc.*": g(),
    "people.student": g(STU_LIMITED, f"student_id IN ({CDC_STUDENTS})"),
    "people.person": g(
        PERSON_LIMITED,
        f"person_id IN (SELECT person_id FROM people.student WHERE student_id IN ({CDC_STUDENTS}))",
    ),
    "acad.semester_performance": g("*", f"student_id IN ({CDC_STUDENTS})"),
    **{
        k: v
        for k, v in staff_common.items()
        if k.startswith(("svc.notice", "svc.mail", "fin.salary", "lib."))
    },
}

HIDDEN_FROM_ALL = {
    "tables": ["auth.session", "auth.otp"],
    "columns": [
        "*.password_hash",
        "*.passphrase_hash",
        "*.campus_net_password_hash",
        "*.token_hash",
        "*.otp_hash",
        "people.person_identity.aadhaar_no",
        "people.bank_account.account_no",
    ],
}
staff_perm = {"can_view_sql": False}
roles = {
    "erp_base": {
        "description": "reference data (abstract: nobody is given this role directly)",
        "db_role": "erp_ro",
        "priority": 0,
        "tables": base,
    },
    "student": {
        "inherits": ["erp_base"],
        "priority": 10,
        "tables": student,
        "description": "students: own records only",
    },
    "faculty": {
        "inherits": ["erp_base"],
        "priority": 20,
        "tables": faculty,
        "description": "teachers: own courses, advisees, own salary",
    },
    "hod": {
        "inherits": ["faculty"],
        "priority": 30,
        "tables": hod,
        "description": "head of department: whole department",
    },
    "dept_office": {
        "inherits": ["hod"],
        "priority": 25,
        "description": "department office: same scope as the HoD",
    },
    "acad_section": {
        "inherits": ["erp_base"],
        "priority": 40,
        "tables": acad_section,
        "description": "academic section: all academic records",
    },
    "accounts": {
        "inherits": ["erp_base"],
        "priority": 40,
        "tables": accounts,
        "description": "accounts section: all finance",
    },
    "library": {
        "inherits": ["erp_base"],
        "priority": 25,
        "tables": library,
        "description": "library staff",
    },
    "hall_office": {
        "inherits": ["erp_base"],
        "priority": 25,
        "tables": hall_office,
        "description": "hall office: residents of one hall",
    },
    "cdc": {
        "inherits": ["erp_base"],
        "priority": 25,
        "tables": cdc,
        "description": "Career Development Cell",
    },
    "admin": {
        "inherits": ["erp_base"],
        "priority": 100,
        "tables": {"*": g()},
        "description": "system administrator",
        "permissions": {
            "can_view_sql": True,
            "can_view_audit": True,
            "can_manage_users": True,
            "can_refresh_schema": True,
        },
        "limits": {"max_rows": 1000},
    },
}
policy = {
    "version": 1,
    "defaults": {"max_rows": 200, "statement_timeout_ms": 8000, "max_joins": 8},
    "global_deny": HIDDEN_FROM_ALL,
    "db_session_vars": {
        "erp.role": "@role",
        "erp.person_id": "person_id",
        "erp.student_id": "student_id",
        "erp.employee_id": "employee_id",
        "erp.dept_id": "dept_id",
        "erp.hall_id": "hall_id",
    },
    "roles": roles,
}

# ------------------------------------------------------------------ semantics for the LLM prompt
SYN = {
    "people.student": ["students", "roll number", "enrolment", "batch"],
    "people.person": ["name", "date of birth", "email", "contact"],
    "people.employee": ["faculty", "teacher", "professor", "staff", "advisor name"],
    "acad.registration": ["enrolled courses", "course registration"],
    "acad.result": ["grade", "marks", "backlog"],
    "acad.semester_performance": ["sgpa", "cgpa", "gpa", "performance"],
    "fin.fee_demand": ["fees due", "dues", "fee"],
    "fin.payment": ["fee payment", "transaction", "receipt"],
    "fin.salary_slip": ["salary", "pay", "payslip"],
    "lib.loan": ["issued books", "borrowed books"],
    "lib.library_due": ["library fine", "overdue"],
    "org.department": ["branch", "department"],
    "cdc.job_application": ["placement", "applied companies"],
    "cdc.offer": ["placement offer", "package", "ctc"],
    "acad.class_attendance": ["attendance"],
    "svc.complaint": ["complaints", "issues"],
    "acad.course_offering": ["courses offered", "section"],
    "acad.course": ["subject", "course catalogue"],
}
sem = {
    "tables": {
        k: {"description": t["purpose"], **({"synonyms": SYN[k]} if k in SYN else {})}
        for k, t in T.items()
    }
}

(HERE / "natquery").mkdir(exist_ok=True)
(HERE / "natquery" / "policy.yaml").write_text(
    "# GENERATED by make_policy.py. Edit the script, not this file.\n"
    + yaml.safe_dump(policy, sort_keys=False, width=200)
)
(HERE / "natquery" / "semantics.json").write_text(json.dumps(sem, indent=1))
print("wrote natquery/policy.yaml and natquery/semantics.json:", len(roles), "roles")
