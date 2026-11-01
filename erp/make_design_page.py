import html
import os

HERE = os.path.dirname(os.path.abspath(__file__))
from gen_ddl import *

T = parse()
SCH = [
    ("auth", "Authentication", "Logins, roles, sessions, OTPs, login audit"),
    ("org", "Organisation", "Departments, programs, halls, academic sessions"),
    ("people", "People", "One person row; student, employee, guardian, bank, address"),
    (
        "acad",
        "Academics",
        "Curriculum, registration, results, attendance, exams, applications, records",
    ),
    ("fin", "Finance", "Fees, payments, scholarships, mediclaim, salary"),
    ("lib", "Library", "Books, copies, loans, dues"),
    ("svc", "Services", "Paperless forms, complaints, mail, notices, e-office files"),
    ("gh", "Guest house", "Guest houses, rooms, bookings"),
    ("cdc", "Career Development Cell", "Companies, postings, applications, offers"),
    ("sys", "System", "Menus, menu access per role, audit events"),
]
# access matrix: schema -> role -> cell
ROLES = [
    "student",
    "faculty",
    "hod",
    "dept_office",
    "acad_section",
    "accounts",
    "library",
    "hall_office",
    "cdc",
    "admin",
]
M = {
    "auth": dict(
        student="own login",
        faculty="own login",
        hod="own login",
        dept_office="own login",
        acad_section="own login",
        accounts="own login",
        library="own login",
        hall_office="own login",
        cdc="own login",
        admin="manage",
    ),
    "org": dict(
        student="R",
        faculty="R",
        hod="R",
        dept_office="R",
        acad_section="R/W",
        accounts="R",
        library="R",
        hall_office="R/W halls",
        cdc="R",
        admin="R/W",
    ),
    "people": dict(
        student="own",
        faculty="own + advisees",
        hod="dept",
        dept_office="dept",
        acad_section="all (no bank)",
        accounts="bank + id",
        library="name only",
        hall_office="hall residents",
        cdc="registered students",
        admin="all",
    ),
    "acad": dict(
        student="own",
        faculty="own courses + advisees",
        hod="dept",
        dept_office="dept R/W",
        acad_section="all R/W",
        accounts="none",
        library="none",
        hall_office="none",
        cdc="grades of registered",
        admin="all",
    ),
    "fin": dict(
        student="own",
        faculty="own salary",
        hod="own salary",
        dept_office="none",
        acad_section="none",
        accounts="all R/W",
        library="none",
        hall_office="hall dues",
        cdc="none",
        admin="all",
    ),
    "lib": dict(
        student="own loans",
        faculty="own loans",
        hod="own loans",
        dept_office="none",
        acad_section="none",
        accounts="none",
        library="all R/W",
        hall_office="none",
        cdc="none",
        admin="all",
    ),
    "svc": dict(
        student="own",
        faculty="own + assigned",
        hod="own + dept queue",
        dept_office="dept queue",
        acad_section="queue",
        accounts="queue",
        library="queue",
        hall_office="hall queue",
        cdc="own",
        admin="all",
    ),
    "gh": dict(
        student="none",
        faculty="own bookings",
        hod="own bookings",
        dept_office="none",
        acad_section="none",
        accounts="R",
        library="none",
        hall_office="none",
        cdc="none",
        admin="all",
    ),
    "cdc": dict(
        student="postings + own apps",
        faculty="R",
        hod="R",
        dept_office="none",
        acad_section="none",
        accounts="none",
        library="none",
        hall_office="none",
        cdc="all R/W",
        admin="all",
    ),
    "sys": dict(
        student="own menu",
        faculty="own menu",
        hod="own menu",
        dept_office="own menu",
        acad_section="own menu",
        accounts="own menu",
        library="own menu",
        hall_office="own menu",
        cdc="own menu",
        admin="all",
    ),
}


def cell(v):
    cls = (
        "no"
        if v == "none"
        else ("rw" if v in ("all R/W", "manage", "all", "R/W") else "sc")
    )
    return f'<td class="{cls}">{html.escape(v)}</td>'


e = html.escape
nT = len(T)
nC = sum(len(t["cols"]) for t in T.values())
nS = sum(c["sens"] for t in T.values() for c in t["cols"])
nF = sum(1 for t in T.values() for c in t["cols"] if c["fk"])
out = []
out.append(
    f"""<!doctype html><html lang=en><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1">
<title>ERP database design</title><style>
:root{{--bg:#fff;--fg:#1d2433;--mut:#5b6577;--line:#dfe3ea;--acc:#3b5bdb;--soft:#f4f6fb;--bad:#c92a2a;--ok:#2b8a3e}}
@media(prefers-color-scheme:dark){{:root{{--bg:#12151c;--fg:#e6e9f0;--mut:#9aa3b5;--line:#2a3040;--acc:#8ea2ff;--soft:#1a1f2a}}}}
body{{margin:0;background:var(--bg);color:var(--fg);font:15px/1.55 system-ui,sans-serif}}
main{{max-width:1100px;margin:0 auto;padding:24px 16px 80px}}
h1{{font-size:28px;margin:.2em 0}}h2{{margin-top:2.2em;border-bottom:1px solid var(--line);padding-bottom:6px}}h3{{margin-top:1.6em}}
.stats{{display:flex;flex-wrap:wrap;gap:10px;margin:14px 0}}.stats div{{background:var(--soft);padding:10px 16px;border-radius:8px}}.stats b{{font-size:22px;display:block}}
table{{border-collapse:collapse;width:100%;font-size:13px}}th,td{{border:1px solid var(--line);padding:5px 8px;text-align:left;vertical-align:top}}th{{background:var(--soft)}}
.wrap{{overflow:auto}}td.no{{color:var(--mut)}}td.rw{{color:var(--acc);font-weight:600}}td.sc{{}}
.dia{{border:1px solid var(--line);border-radius:8px;overflow:auto;max-height:640px;background:#fff;padding:8px}}.dia svg{{max-width:none;height:auto}}
.zoom{{font-size:12px;color:var(--mut)}}details{{margin:6px 0;border:1px solid var(--line);border-radius:6px;padding:4px 10px}}summary{{cursor:pointer;font-weight:600}}
.sens{{color:var(--bad);font-weight:600}}code{{background:var(--soft);padding:1px 5px;border-radius:4px}}pre{{background:var(--soft);padding:12px;border-radius:8px;overflow:auto;font-size:12.5px}}
.pill{{display:inline-block;background:var(--soft);border-radius:10px;padding:0 8px;font-size:12px;margin-right:4px}}
</style></head><body><main>
<h1>ERP database design</h1>
<p>A synthetic ERP database modelled on the field names of the student login and extended to the teacher and administrative scopes. All data is generated; nothing comes from a real ERP. Built by <code>build_erp.py</code> from <code>erp_schema.dsl</code>.</p>
<div class=stats><div><b>{len(SCH)}</b>schemas</div><div><b>{nT}</b>tables</div><div><b>{nC}</b>columns</div><div><b>{nF}</b>foreign keys</div><div><b class=sens>{nS}</b>sensitive columns</div></div>
<h2>1. Schemas at a glance</h2><div class=wrap><table><tr><th>Schema<th>Purpose<th>Tables</tr>"""
)
for s, n, d in SCH:
    ts = [t["name"] for t in T.values() if t["schema"] == s]
    out.append(
        f"<tr><td><code>{s}</code> {n}<td>{e(d)}<td>{len(ts)}: "
        + ", ".join(ts)
        + "</tr>"
    )
out.append("</table></div>")
out.append(
    "<h2>2. Overview (every table, relationships only)</h2><p class=zoom>Crow's-foot notation: <code>||</code> exactly one, <code>o|</code> zero or one, <code>o{</code> zero or many. Scroll the box to pan.</p><div class=dia>"
    + open(os.path.join(HERE, "diagrams", "overview.svg")).read()
    + "</div>"
)
out.append("<h2>3. Schemas in detail</h2>")
for s, n, d in SCH:
    out.append(
        f"<h3><code>{s}</code> {n}</h3><p>{e(d)}. Tables from other schemas appear as stubs showing only their key.</p><div class=dia>"
        + open(os.path.join(HERE, "diagrams", f"{s}.svg")).read()
        + "</div>"
    )
    for t in [t for t in T.values() if t["schema"] == s]:
        rows = "".join(
            f'<tr><td>{"<b>"+c["name"]+"</b>" if c["pk"] else c["name"]}<td>{e(c["type"])}<td>{"PK " if c["pk"] else ""}{"UQ " if c["uniq"] else ""}{"null " if c["null"] else ""}{"→ "+c["fk"] if c["fk"] else ""}<td>{"<span class=sens>sensitive</span>" if c["sens"] else ""}</tr>'
            for c in t["cols"]
        )
        out.append(
            f'<details><summary>{s}.{t["name"]} <span class=pill>{len(t["cols"])} cols</span> <span style="font-weight:400;color:var(--mut)">{e(t["purpose"])}</span></summary><div class=wrap><table><tr><th>Column<th>Type<th>Key / null / FK<th>Flag</tr>{rows}</table></div></details>'
        )
out.append(
    "<h2>4. Who can see what (the access matrix)</h2><p>Ten roles. Cells say how much of a schema the role may read. <b>own</b> = rows tied to the caller's own person/account, <b>dept</b> = the caller's department, <b>R/W</b> = write allowed through the ERP app (NatQuery is always read-only). Password hashes, OTP and token hashes are hidden from every role including admin.</p><div class=wrap><table><tr><th>Schema"
    + "".join(f"<th>{r}" for r in ROLES)
    + "</tr>"
)
for s, n, d in SCH:
    out.append(
        f"<tr><td><code>{s}</code>" + "".join(cell(M[s][r]) for r in ROLES) + "</tr>"
    )
out.append("</table></div>")
sens = [
    (t["schema"] + "." + t["name"], c["name"])
    for t in T.values()
    for c in t["cols"]
    if c["sens"]
]
out.append(
    "<h3>Sensitive columns (hidden or masked by policy)</h3><div class=wrap><table><tr><th>Table<th>Column</tr>"
    + "".join(f"<tr><td>{a}<td>{b}</tr>" for a, b in sens)
    + "</table></div>"
)
out.append(
    """<h2>5. Authentication design (all in PostgreSQL)</h2>
<ul><li><b>Passwords</b>: <code>pgcrypto</code> <code>crypt(pw, gen_salt('bf', 12))</code> in <code>auth.account.password_hash</code>. Separate hashes for the passphrase and the campus-net password, as the login page has them.</li>
<li><b>Login</b>: <code>auth.login(login_id, password, ip, user_agent)</code> returns a session token (random 32 bytes; only its SHA-256 is stored in <code>auth.session</code>). Wrong user and wrong password give the same error.</li>
<li><b>Lockout</b>: 5 failures set <code>locked_until</code> to 15 minutes ahead; success resets the counter. Every attempt is written to <code>auth.login_audit</code>.</li>
<li><b>OTP</b>: <code>auth.issue_otp</code> / <code>auth.verify_otp</code> store a hashed 6-digit code with expiry and single use (for the "email OTP" login path).</li>
<li><b>Sessions</b>: idle expiry (30 min), <code>auth.logout</code>, <code>auth.verify_session(token)</code> returns account, person and roles.</li>
<li><b>Switch login</b>: one person can have a student account and a staff account (<code>auth.account.person_id</code> is not unique).</li>
<li><b>Roles</b>: <code>auth.account_role</code> grants a role optionally scoped to a department, hall or program (HoD of CSE, hall office of RK Hall).</li></ul>
<h2>6. Database roles and defence in depth</h2>
<div class=wrap><table><tr><th>PostgreSQL role<th>Used by<th>Rights</tr>
<tr><td><code>erp_owner</code><td>migrations only<td>owns all objects (NOLOGIN)</tr>
<tr><td><code>erp_app</code><td>the ERP application<td>DML on business tables, EXECUTE on the auth functions; no direct access to password hashes, tokens or OTPs</tr>
<tr><td><code>erp_ro</code><td>NatQuery, after <code>SET LOCAL ROLE</code><td>SELECT only; no secret columns; <b>row-level security applies</b></tr>
<tr><td><code>natquery</code><td>the login NatQuery connects with<td>no table privileges at all; can only become <code>erp_ro</code></tr></table></div>
<p>Row-level security is enabled on 20 high-risk tables (students, persons, identities, bank accounts, guardians, registrations, results, performance, medical leave, counselling, fee demands, payments, income declarations, mediclaim, scholarships, salary slips, library loans and dues). NatQuery sends <code>erp.role</code>, <code>erp.student_id</code>, <code>erp.person_id</code>, <code>erp.employee_id</code>, <code>erp.dept_id</code> and <code>erp.hall_id</code> with every query; a request with no context sees no rows. Columns are limited by NatQuery only (the database role holds SELECT on the non-secret columns).</p>
<h2>7. Synthetic data</h2>
<p>Generated by <code>seed.py</code> with a fixed seed, so every build is identical: about 2,500 students (six admission years, B.Tech, M.Tech, M.Sc., Ph.D.), 240 staff, 8 halls, 190 courses, 11 academic sessions with registrations, results, attendance and performance, fee demands and payments, 9,000 library loans, 1,500 paperless applications, placements, guest-house bookings and menus: roughly 368,000 rows. Deliberate traps are included: a student with the same name as a faculty member, duplicate names, ten people holding both a student and a staff login, graduates with expired roles and disabled accounts. Contact numbers start with 55 and e-mails use <code>.example</code> domains, so none can belong to a real person.</p>
</main></body></html>"""
)
open(os.path.join(HERE, "ERP_Database_Design.html"), "w").write("".join(out))
