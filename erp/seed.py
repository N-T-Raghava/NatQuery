"""Synthetic data for the ERP database. Deterministic (seeded), all names/numbers are invented.

    python seed.py --dsn "dbname=erp user=postgres host=127.0.0.1 port=54330"

Every person, roll number, e-mail, phone, Aadhaar, bank account and password is generated; nothing comes
from the real ERP screenshots. Contact numbers start with 55 and e-mails use *.example domains, so they
cannot belong to a real person.
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import date, datetime, time, timedelta

import psycopg2
from psycopg2.extras import execute_values

SEED = 20261030
TODAY = date(2026, 10, 30)
rng = random.Random(SEED)

# ------------------------------------------------------------------ reference data
DEPTS = [  # code, name, ug-title list (8 titles each; PG titles are derived)
    (
        "CS",
        "Computer Science and Engineering",
        [
            "Algorithms",
            "Operating Systems",
            "Database Management Systems",
            "Computer Networks",
            "Compilers",
            "Machine Learning",
            "Computer Architecture",
            "Theory of Computation",
        ],
    ),
    (
        "EE",
        "Electrical Engineering",
        [
            "Signals and Systems",
            "Control Systems",
            "Power Systems",
            "Digital Signal Processing",
            "Electrical Machines",
            "Power Electronics",
            "Analog Circuits",
            "Electromagnetic Theory",
        ],
    ),
    (
        "EC",
        "Electronics and Electrical Communication Engineering",
        [
            "Digital Communication",
            "VLSI Design",
            "Microwave Engineering",
            "Information Theory",
            "Embedded Systems",
            "Antennas",
            "Wireless Communication",
            "Semiconductor Devices",
        ],
    ),
    (
        "ME",
        "Mechanical Engineering",
        [
            "Thermodynamics",
            "Fluid Mechanics",
            "Heat Transfer",
            "Machine Design",
            "Manufacturing Processes",
            "Dynamics of Machines",
            "Refrigeration",
            "Mechatronics",
        ],
    ),
    (
        "CE",
        "Civil Engineering",
        [
            "Structural Analysis",
            "Geotechnical Engineering",
            "Hydrology",
            "Transportation Engineering",
            "Concrete Technology",
            "Environmental Engineering",
            "Surveying",
            "Steel Structures",
        ],
    ),
    (
        "CH",
        "Chemical Engineering",
        [
            "Mass Transfer",
            "Chemical Reaction Engineering",
            "Process Control",
            "Transport Phenomena",
            "Process Design",
            "Polymer Engineering",
            "Petroleum Refining",
            "Biochemical Engineering",
        ],
    ),
    (
        "MA",
        "Mathematics",
        [
            "Real Analysis",
            "Linear Algebra",
            "Probability Theory",
            "Numerical Analysis",
            "Abstract Algebra",
            "Differential Equations",
            "Topology",
            "Optimization",
        ],
    ),
    (
        "PH",
        "Physics",
        [
            "Quantum Mechanics",
            "Classical Electrodynamics",
            "Statistical Mechanics",
            "Solid State Physics",
            "Optics",
            "Nuclear Physics",
            "Atomic Physics",
            "Mathematical Methods",
        ],
    ),
    (
        "CY",
        "Chemistry",
        [
            "Organic Chemistry",
            "Physical Chemistry",
            "Inorganic Chemistry",
            "Spectroscopy",
            "Chemical Kinetics",
            "Quantum Chemistry",
            "Polymer Chemistry",
            "Electrochemistry",
        ],
    ),
    (
        "MT",
        "Metallurgical and Materials Engineering",
        [
            "Physical Metallurgy",
            "Mechanical Behaviour of Materials",
            "Phase Transformations",
            "Corrosion Engineering",
            "Ceramics",
            "Extractive Metallurgy",
            "Welding Technology",
            "Nanomaterials",
        ],
    ),
    (
        "AE",
        "Aerospace Engineering",
        [
            "Aerodynamics",
            "Flight Mechanics",
            "Propulsion",
            "Aircraft Structures",
            "Space Dynamics",
            "Avionics",
            "Gas Dynamics",
            "Composite Materials",
        ],
    ),
    (
        "BT",
        "Biotechnology",
        [
            "Molecular Biology",
            "Genetics",
            "Bioprocess Engineering",
            "Immunology",
            "Bioinformatics",
            "Enzymology",
            "Cell Biology",
            "Genomics",
        ],
    ),
]
ADMIN_DEPT = ("AD", "Administration")
COMMON = [  # code, title, dept(owner), semester, credits
    ("MA11001", "Mathematics I", "MA", 1, 4),
    ("PH11001", "Physics of Waves and Optics", "PH", 1, 4),
    ("CS11001", "Programming and Data Structures", "CS", 1, 4),
    ("HS11001", "English for Communication", "MA", 1, 2),
    ("EE11001", "Electrical Technology", "EE", 1, 3),
    ("MA11002", "Mathematics II", "MA", 2, 4),
    ("CY11001", "Chemistry", "CY", 2, 4),
    ("ME11002", "Workshop Practice", "ME", 2, 2),
    ("BT11001", "Introduction to Biology", "BT", 2, 3),
    ("HS11002", "Professional Ethics", "MA", 2, 2),
]
BREADTH = [
    "Introduction to Economics",
    "Cognitive Psychology",
    "Indian Philosophy",
    "Entrepreneurship",
    "Environmental Science",
    "Public Policy",
    "Sociology of Technology",
    "Creative Writing",
    "Music Appreciation",
    "Financial Accounting",
    "Organisational Behaviour",
    "History of Science",
]
FIRST = [
    "Aarav",
    "Vivaan",
    "Aditya",
    "Arjun",
    "Sai",
    "Reyansh",
    "Krishna",
    "Ishaan",
    "Rohan",
    "Karthik",
    "Siddharth",
    "Rahul",
    "Aman",
    "Nikhil",
    "Varun",
    "Harsh",
    "Pranav",
    "Manish",
    "Suresh",
    "Ramesh",
    "Anil",
    "Vijay",
    "Deepak",
    "Gaurav",
    "Tarun",
    "Yash",
    "Kunal",
    "Mohit",
    "Ananya",
    "Diya",
    "Aadhya",
    "Saanvi",
    "Pooja",
    "Neha",
    "Priya",
    "Sneha",
    "Kavya",
    "Isha",
    "Meera",
    "Riya",
    "Shreya",
    "Divya",
    "Lakshmi",
    "Swati",
    "Nisha",
    "Anjali",
    "Tanvi",
    "Sruthi",
    "Bhavana",
    "Harini",
    "Aishwarya",
    "Rekha",
    "Sunita",
    "Radha",
    "Geeta",
    "Mamta",
]
LAST = [
    "Sharma",
    "Verma",
    "Reddy",
    "Naidu",
    "Rao",
    "Patel",
    "Singh",
    "Kumar",
    "Gupta",
    "Mishra",
    "Das",
    "Banerjee",
    "Chatterjee",
    "Mukherjee",
    "Iyer",
    "Nair",
    "Menon",
    "Pillai",
    "Joshi",
    "Kulkarni",
    "Deshmukh",
    "Patil",
    "Shah",
    "Mehta",
    "Agarwal",
    "Bose",
    "Sen",
    "Ghosh",
    "Yadav",
    "Chauhan",
    "Thakur",
    "Pandey",
    "Tiwari",
    "Dubey",
    "Srinivasan",
    "Krishnan",
    "Raman",
    "Subramanian",
    "Murthy",
    "Hegde",
    "Shetty",
    "Gowda",
]
STATES = [
    ("Andhra Pradesh", ["Visakhapatnam", "Vijayawada", "Guntur", "Tirupati"]),
    ("West Bengal", ["Kolkata", "Howrah", "Siliguri", "Durgapur"]),
    ("Maharashtra", ["Pune", "Mumbai", "Nagpur"]),
    ("Uttar Pradesh", ["Lucknow", "Kanpur", "Varanasi"]),
    ("Tamil Nadu", ["Chennai", "Madurai", "Coimbatore"]),
    ("Karnataka", ["Bengaluru", "Mysuru"]),
    ("Bihar", ["Patna", "Gaya"]),
    ("Odisha", ["Bhubaneswar", "Cuttack"]),
    ("Rajasthan", ["Jaipur", "Kota"]),
    ("Telangana", ["Hyderabad", "Warangal"]),
    ("Delhi", ["New Delhi"]),
    ("Gujarat", ["Ahmedabad", "Surat"]),
]
HALLS = [
    "Azad Hall",
    "Patel Hall",
    "Nehru Hall",
    "Gokhale Hall",
    "Tagore Hall",
    "Sarojini Hall",
    "Rani Laxmibai Hall",
    "Vidyasagar Hall",
]
GRADES = [
    (90, "EX", 10),
    (80, "A", 9),
    (70, "B", 8),
    (60, "C", 7),
    (50, "D", 6),
    (40, "P", 5),
    (0, "F", 0),
]

ROLES = [
    ("student", 10),
    ("faculty", 20),
    ("hod", 30),
    ("dept_office", 25),
    ("acad_section", 40),
    ("accounts", 40),
    ("library", 25),
    ("hall_office", 25),
    ("cdc", 25),
    ("admin", 100),
]


def pick(seq):
    return seq[rng.randrange(len(seq))]


def phone():
    return "55" + "".join(str(rng.randrange(10)) for _ in range(8))


def ts(d: date, h=None) -> datetime:
    return datetime.combine(
        d,
        time(
            h if h is not None else rng.randrange(8, 20),
            rng.randrange(60),
            rng.randrange(60),
        ),
    )


def rdate(a: date, b: date) -> date:
    return a + timedelta(days=rng.randrange(max((b - a).days, 1)))


class DB:
    def __init__(self, dsn):
        self.conn = psycopg2.connect(dsn)
        self.cur = self.conn.cursor()
        self.counts: dict[str, int] = {}
        self.cur.execute(
            "SELECT table_schema||'.'||table_name, column_name, character_maximum_length FROM information_schema.columns "
            "WHERE character_maximum_length IS NOT NULL AND table_schema IN ('auth','org','people','acad','fin','lib','svc','gh','cdc','sys')"
        )
        self.limits = {(t, c): n for t, c, n in self.cur.fetchall()}
        self.too_long: set[tuple[str, str, str]] = set()

    def ins(self, table, cols, rows):
        rows = list(rows)
        if not rows:
            return
        lim = [self.limits.get((table, c)) for c in cols]
        if any(
            lim
        ):  # report every over-long value at once instead of failing one table at a time
            fixed = []
            for r in rows:
                r = list(r)
                for i, (v, n, c) in enumerate(zip(r, lim, cols)):
                    if n and isinstance(v, str) and len(v) > n:
                        self.too_long.add((table, c, v))
                        r[i] = v[:n]
                fixed.append(tuple(r))
            rows = fixed
        execute_values(
            self.cur,
            f"INSERT INTO {table} ({','.join(cols)}) VALUES %s",
            rows,
            page_size=5000,
        )
        self.counts[table] = self.counts.get(table, 0) + len(rows)

    def run(self, sql, args=None):
        self.cur.execute(sql, args)


def seed(dsn: str, verbose=True):
    db = DB(dsn)
    log = print if verbose else (lambda *a, **k: None)

    # ================================================================== org
    depts = [(i + 1, c, n) for i, (c, n, _) in enumerate(DEPTS)] + [
        (len(DEPTS) + 1, *ADMIN_DEPT)
    ]
    DID = {c: i for i, c, _ in depts}
    db.ins(
        "org.department",
        ["department_id", "dept_code", "name", "is_active"],
        [(i, c, n, True) for i, c, n in depts],
    )

    programs = []  # id, code, name, level, years, dept
    pid = 0
    PROG = {}  # (dept, level) -> id
    for c, n, _ in DEPTS:
        pid += 1
        PROG[(c, "UG")] = pid
        programs.append((pid, f"BT-{c}", f"B.Tech. {n}", "UG", 4, DID[c]))
        pid += 1
        PROG[(c, "PG")] = pid
        programs.append((pid, f"MT-{c}", f"M.Tech. {n}", "PG", 2, DID[c]))
        pid += 1
        PROG[(c, "PHD")] = pid
        programs.append((pid, f"PH-{c}", f"Ph.D. {n}", "PHD", 5, DID[c]))
    for c in ("MA", "PH", "CY"):
        pid += 1
        PROG[(c, "MSC")] = pid
        programs.append(
            (
                pid,
                f"MS-{c}",
                f"M.Sc. {dict((a, b) for a, b, _ in DEPTS)[c]}",
                "MSC",
                2,
                DID[c],
            )
        )
    db.ins(
        "org.program",
        [
            "program_id",
            "program_code",
            "name",
            "level",
            "duration_years",
            "department_id",
        ],
        programs,
    )

    sessions, SID = [], {}
    sid = 0
    for y in range(2021, 2027):
        for term in ("Autumn", "Spring"):
            if y == 2026 and term == "Spring":
                continue
            sid += 1
            if term == "Autumn":
                a, b = date(y, 7, 25), date(y, 12, 5)
            else:
                a, b = date(y + 1, 1, 5), date(y + 1, 5, 15)
            sessions.append(
                (
                    sid,
                    f"{y}-{str(y + 1)[2:]}",
                    term,
                    a,
                    b,
                    term == "Autumn" and y == 2026,
                )
            )
            SID[(y, term)] = sid
    db.ins(
        "org.academic_session",
        ["session_id", "acad_year", "term", "start_date", "end_date", "is_current"],
        sessions,
    )
    CUR = SID[(2026, "Autumn")]
    ordinal = {s[0]: s[0] for s in sessions}  # 1..11, Autumn odd / Spring even

    halls = []
    for i, n in enumerate(HALLS, 1):
        halls.append((i, f"H{i}", n, 600 + 40 * (i % 3)))
    db.ins("org.hall", ["hall_id", "hall_code", "name", "capacity"], halls)
    rooms, hall_rooms = [], {h: [] for h in range(1, len(HALLS) + 1)}
    rid = 0
    for h in range(1, len(HALLS) + 1):
        for f in range(1, 5):
            for r in range(1, 41):
                rid += 1
                rooms.append((rid, h, f"{'ABCD'[f - 1]}-{f}{r:02d}", 2 if r % 5 else 1))
                hall_rooms[h].append(rid)
    db.ins("org.hall_room", ["room_id", "hall_id", "room_no", "capacity"], rooms)

    # ================================================================== people: names
    used_names = set()

    def new_name(allow_dup=False):
        for _ in range(100):
            n = f"{pick(FIRST)} {pick(LAST)}"
            if allow_dup or n not in used_names:
                used_names.add(n)
                return n
        n = f"{pick(FIRST)} {pick(LAST)} {rng.randrange(10, 99)}"
        used_names.add(n)
        return n

    persons, identities, bank, addr, emerg = [], [], [], [], []
    person_ctr = [0]
    email_ctr: dict[str, int] = {}

    def make_person(kind: str, dob_year: int, name: str | None = None, hindi=True):
        person_ctr[0] += 1
        p = person_ctr[0]
        name = name or new_name()
        g = "F" if name.split()[0] in FIRST[FIRST.index("Ananya") :] else "M"
        base = name.lower().replace(" ", ".")
        email_ctr[base] = email_ctr.get(base, 0) + 1
        sfx = "" if email_ctr[base] == 1 else str(email_ctr[base])
        persons.append(
            (
                p,
                name,
                name if hindi and rng.random() < 0.7 else None,
                rng.random() < 0.5,
                g,
                date(dob_year, rng.randrange(1, 13), rng.randrange(1, 28)),
                pick(["A+", "B+", "O+", "AB+", "A-", "O-", None]),
                pick(
                    [
                        "Hindu",
                        "Hindu",
                        "Hindu",
                        "Muslim",
                        "Christian",
                        "Sikh",
                        "Jain",
                        None,
                    ]
                ),
                "Indian",
                pick(
                    [
                        "Hindi",
                        "Telugu",
                        "Bengali",
                        "Tamil",
                        "Marathi",
                        "Kannada",
                        "Odia",
                        None,
                    ]
                ),
                "Single" if kind == "S" else pick(["Single", "Married"]),
                None,
                f"{base.replace('.', '')}{rng.randrange(10, 99)}@mail.example",
                f"{base}{sfx}@erp-demo.example",
                phone(),
            )
        )
        identities.append(
            (
                p,
                "".join(str(rng.randrange(10)) for _ in range(12)),
                (
                    "".join(str(rng.randrange(10)) for _ in range(12))
                    if rng.random() < 0.8
                    else None
                ),
                (
                    "".join(str(rng.randrange(10)) for _ in range(14))
                    if rng.random() < 0.5
                    else None
                ),
            )
        )
        bk = pick(
            [
                "State Bank of India",
                "HDFC Bank",
                "Canara Bank",
                "Punjab National Bank",
                "Bank of Baroda",
                "ICICI Bank",
            ]
        )
        bank.append(
            (
                p,
                bk,
                f"{pick(STATES)[1][0]} Main Branch",
                f"{bk[:4].upper().replace(' ', 'X')}0{rng.randrange(100000, 999999)}",
                "".join(str(rng.randrange(10)) for _ in range(rng.randrange(11, 17))),
                rng.random() < 0.8,
                True,
            )
        )
        st, cities = pick(STATES)
        city = pick(cities)
        addr.append(
            (
                p,
                "PERMANENT",
                f"{rng.randrange(1, 300)}, {pick(['Gandhi', 'Nehru', 'Station', 'Temple', 'Lake'])} Road",
                city,
                f"{city} PS" if rng.random() < 0.7 else None,
                str(rng.randrange(500000, 799999)),
                city,
                st,
                "India",
            )
        )
        addr.append(
            (
                p,
                "MAILING",
                f"Room {rng.randrange(1, 400)}, Campus",
                "Campus Town",
                None,
                "721302",
                "Paschim Medinipur",
                "West Bengal",
                "India",
            )
        )
        emerg.append(
            (
                p,
                new_name(True),
                pick(["Father", "Mother", "Spouse", "Brother", "Sister"]),
                phone(),
                None,
            )
        )
        return p, name, st

    # ================================================================== employees
    emps, emp_rows = [], []  # (employee_id, person_id, dept_code, kind, function)
    eid = 0
    FUNCS = [("faculty", d[0]) for d in DEPTS for _ in range(15)]
    staff_plan = (
        [("dept_office", d[0]) for d in DEPTS]
        + [("acad_section", "AD")] * 8
        + [("accounts", "AD")] * 8
        + [("library", "AD")] * 6
        + [("hall_office", "AD")] * len(HALLS)
        + [("cdc", "AD")] * 5
        + [("admin", "AD")] * 4
        + [("registry", "AD")] * 6
    )
    for func, d in FUNCS + staff_plan:
        eid += 1
        yr = (
            rng.randrange(1962, 1992)
            if func == "faculty"
            else rng.randrange(1970, 2000)
        )
        p, name, st = make_person("E", yr)
        desig = (
            pick(
                [
                    "Professor",
                    "Professor",
                    "Associate Professor",
                    "Associate Professor",
                    "Assistant Professor",
                    "Assistant Professor",
                    "Assistant Professor",
                ]
            )
            if func == "faculty"
            else {
                "dept_office": "Office Superintendent",
                "acad_section": "Assistant Registrar",
                "accounts": "Accounts Officer",
                "library": "Library Assistant",
                "hall_office": "Hall Manager",
                "cdc": "Placement Officer",
                "admin": "System Administrator",
                "registry": "Junior Superintendent",
            }[func]
        )
        emps.append(dict(id=eid, person=p, dept=d, func=func, name=name))
        emp_rows.append(
            (
                eid,
                f"E{eid:04d}",
                p,
                DID[d],
                desig,
                "FACULTY" if func == "faculty" else "STAFF",
                name,
                persons[p - 1][13],
                rdate(date(1995, 1, 1), date(2025, 6, 30)),
                True,
            )
        )
    faculty_by_dept = {
        d[0]: [e for e in emps if e["func"] == "faculty" and e["dept"] == d[0]]
        for d in DEPTS
    }

    # ================================================================== students
    COHORTS = []  # (year, level, count)
    for y, n in (
        (2021, 230),
        (2022, 250),
        (2023, 300),
        (2024, 320),
        (2025, 330),
        (2026, 340),
    ):
        COHORTS.append((y, "UG", n))
    for y, n in ((2024, 120), (2025, 150), (2026, 160)):
        COHORTS.append((y, "PG", n))
    for y, n in ((2025, 60), (2026, 60)):
        COHORTS.append((y, "MSC", n))
    for y, n in (
        (2021, 25),
        (2022, 25),
        (2023, 30),
        (2024, 30),
        (2025, 25),
        (2026, 25),
    ):
        COHORTS.append((y, "PHD", n))
    DUR = {"UG": 4, "PG": 2, "MSC": 2, "PHD": 5}
    students = []
    sidn = 0
    roll_ctr = {}
    for y, lvl, n in COHORTS:
        dlist = (
            [d[0] for d in DEPTS] if lvl in ("UG", "PG", "PHD") else ["MA", "PH", "CY"]
        )
        wts = {
            "CS": 3,
            "EE": 2,
            "EC": 2,
            "ME": 2,
            "CE": 1.5,
            "CH": 1,
            "MA": 1,
            "PH": 1,
            "CY": 1,
            "MT": 1,
            "AE": 1,
            "BT": 1,
        }
        for _ in range(n):
            dpt = rng.choices(dlist, [wts[x] for x in dlist])[0]
            sidn += 1
            dob = (
                y
                - (18 if lvl == "UG" else 22 if lvl in ("PG", "MSC") else 24)
                - rng.randrange(0, 3)
            )
            p, name, st = make_person("S", dob)
            k = (y, dpt, lvl)
            roll_ctr[k] = roll_ctr.get(k, 0) + 1
            code = {"UG": "", "PG": "M", "MSC": "S", "PHD": "R"}[lvl]
            roll = f"{str(y)[2:]}{dpt}{code}{10000 + roll_ctr[k]}"[:12]
            end_year = y + DUR[lvl]
            graduated = (lvl in ("UG", "PG", "MSC") and end_year <= 2026) or False
            dereg = (not graduated) and rng.random() < 0.012
            status = (
                "GRADUATED" if graduated else ("DEREGISTERED" if dereg else "ON ROLL")
            )
            sem = None if graduated else min((2026 - y) * 2 + 1, DUR[lvl] * 2 + 2)
            students.append(
                dict(
                    id=sidn,
                    person=p,
                    name=name,
                    dept=dpt,
                    level=lvl,
                    year=y,
                    roll=roll,
                    state=st,
                    status=status,
                    sem=sem,
                    graduated=graduated,
                    cat=rng.choices(
                        ["GEN", "OBC", "SC", "ST", "EWS"], [45, 27, 15, 7, 6]
                    )[0],
                    hall=None,
                    room=None,
                    start_ord=SID[(y, "Autumn")] if SID.get((y, "Autumn")) else 1,
                )
            )
    # a few deliberate traps: a student sharing a faculty member's exact name; two students with one name
    fac = pick(emps[:20])
    for s, nm in zip(
        students[300:303], [fac["name"], fac["name"], students[500]["name"]]
    ):
        s["name"] = nm
    # halls: ON ROLL students not graduated, UG/PG/MSC/PHD get halls; ~85%
    occupied: dict[int, set] = {h: set() for h in hall_rooms}
    for s in students:
        if s["status"] != "ON ROLL" or rng.random() < 0.15:
            continue
        h = rng.randrange(1, len(HALLS) + 1)
        free = [r for r in hall_rooms[h] if r not in occupied[h]]
        if not free:
            continue
        r = pick(free)
        occupied[h].add(r)
        s["hall"], s["room"] = h, r

    # curricula
    curricula, CURR = [], {}
    cid = 0
    for prog in programs:
        cid += 1
        CURR[prog[0]] = cid
        curricula.append((cid, f"{prog[2]} Curriculum 2020", prog[0], 2020, True))
    db.ins(
        "people.person",
        [
            "person_id",
            "full_name",
            "name_hindi",
            "hindi_name_verified",
            "gender",
            "date_of_birth",
            "blood_group",
            "religion",
            "nationality",
            "mother_tongue",
            "marital_status",
            "photo_path",
            "personal_email",
            "institute_email",
            "mobile_no",
        ],
        persons,
    )
    db.ins(
        "people.person_identity",
        ["person_id", "aadhaar_no", "abc_id", "nad_id"],
        identities,
    )
    db.ins(
        "people.bank_account",
        [
            "person_id",
            "bank_name",
            "branch_name",
            "ifsc",
            "account_no",
            "aadhaar_linked",
            "is_primary",
        ],
        bank,
    )
    db.ins(
        "people.address",
        [
            "person_id",
            "address_type",
            "address_line",
            "village_town_city",
            "police_station",
            "pin_code",
            "district",
            "state",
            "country",
        ],
        addr,
    )
    db.ins(
        "people.emergency_contact",
        ["person_id", "contact_name", "relationship", "contact_no", "address"],
        emerg,
    )
    db.ins(
        "acad.curriculum",
        ["curriculum_id", "name", "program_id", "effective_from_year", "is_active"],
        curricula,
    )
    db.ins(
        "people.employee",
        [
            "employee_id",
            "emp_code",
            "person_id",
            "department_id",
            "designation",
            "employee_type",
            "display_name",
            "official_email",
            "joined_on",
            "is_active",
        ],
        emp_rows,
    )
    # HoDs and wardens
    for d, c, _ in depts[:-1]:
        h = faculty_by_dept[c][0]
        db.run(
            "UPDATE org.department SET hod_employee_id = %s WHERE department_id = %s",
            (h["id"], d),
        )
        h["hod"] = d
    for i, h in enumerate(range(1, len(HALLS) + 1)):
        db.run(
            "UPDATE org.hall SET warden_employee_id = %s WHERE hall_id = %s",
            (pick(emps[:180])["id"], h),
        )

    stu_rows, hist_rows, guard_rows, adv_rows = [], [], [], []
    emp_of_func = lambda f: [e for e in emps if e["func"] == f]
    for s in students:
        prog = PROG[(s["dept"], s["level"])]
        stu_rows.append(
            (
                s["id"],
                s["roll"],
                s["person"],
                f"G{s['id']:07d}",
                prog,
                DID[s["dept"]],
                CURR[prog],
                s["year"],
                {"UG": "JEE", "PG": "GATE", "MSC": "JAM", "PHD": "GATE"}[s["level"]],
                s["cat"],
                None,
                rng.choice(
                    [None, None, None, None, None, "Locomotor 40%", "Visual 45%"]
                ),
                s["state"],
                s["hall"],
                s["room"],
                s["hall"] is not None,
                None,
                s["sem"] if s["sem"] else DUR[s["level"]] * 2,
                s["status"],
                rdate(date(s["year"], 8, 1), date(s["year"], 9, 30)),
                min(s["year"] + DUR[s["level"]], 2026) if s["graduated"] else None,
            )
        )
        hist_rows.append(
            (
                s["id"],
                "ON ROLL",
                date(s["year"], 7, 28),
                date(s["year"] + DUR[s["level"]], 6, 1) if s["graduated"] else None,
                "Admitted",
            )
        )
        if s["graduated"]:
            hist_rows.append(
                (
                    s["id"],
                    "GRADUATED",
                    date(s["year"] + DUR[s["level"]], 6, 1),
                    None,
                    "Degree awarded",
                )
            )
        elif s["status"] == "DEREGISTERED":
            hist_rows.append(
                (
                    s["id"],
                    "DEREGISTERED",
                    rdate(date(2025, 1, 1), TODAY),
                    None,
                    "Did not register for the semester",
                )
            )
        for rel, nm in (
            ("FATHER", f"{pick(FIRST[:28])} {s['name'].split()[-1]}"),
            ("MOTHER", f"{pick(FIRST[28:])} {s['name'].split()[-1]}"),
        ):
            guard_rows.append(
                (
                    s["id"],
                    rel,
                    nm,
                    pick(
                        [
                            "Farmer",
                            "Teacher",
                            "Engineer",
                            "Doctor",
                            "Businessperson",
                            "Government Service",
                            "Homemaker",
                            None,
                        ]
                    ),
                    pick(["Graduate", "Post Graduate", "Secondary", None]),
                    phone() if rng.random() < 0.9 else None,
                    None,
                    (
                        f"{nm.lower().replace(' ', '.')}@mail.example"
                        if rng.random() < 0.5
                        else None
                    ),
                )
            )
        if not s["graduated"]:
            adv = pick(faculty_by_dept[s["dept"]])
            adv_rows.append(
                (
                    s["id"],
                    adv["id"],
                    SID[(s["year"], "Autumn")] if s["year"] >= 2021 else 1,
                    None,
                )
            )
    db.ins(
        "people.student",
        [
            "student_id",
            "roll_no",
            "person_id",
            "guid",
            "program_id",
            "department_id",
            "curriculum_id",
            "admission_year",
            "nature_of_admission",
            "category",
            "sub_category",
            "pwd_status",
            "native_state",
            "hall_id",
            "room_id",
            "hall_attached",
            "allotted_eaa",
            "current_semester",
            "current_status",
            "smartcard_issued_on",
            "year_of_pass",
        ],
        stu_rows,
    )
    db.ins(
        "people.student_status_history",
        ["student_id", "status", "active_from", "active_to", "remark"],
        hist_rows,
    )
    db.ins(
        "people.guardian",
        [
            "student_id",
            "relation",
            "name",
            "profession",
            "education",
            "mobile_no",
            "landline_no",
            "email",
        ],
        guard_rows,
    )
    db.ins(
        "people.faculty_advisor",
        ["student_id", "employee_id", "from_session_id", "to_session_id"],
        adv_rows,
    )
    log(
        "people:",
        len(persons),
        "persons,",
        len(students),
        "students,",
        len(emps),
        "employees",
    )

    # ================================================================== curriculum and courses
    courses, course_id = [], {}
    cc = 0

    def add_course(code, title, credits, dept, lh=3, th=0, ph=0):
        nonlocal cc
        cc += 1
        courses.append((cc, code, title, credits, lh, th, ph, DID[dept]))
        course_id[code] = cc
        return cc

    for code, title, d, sem, cr in COMMON:
        add_course(code, title, cr, d, 3, 1 if cr >= 4 else 0, 0)
    for i, t in enumerate(BREADTH):
        add_course(f"HS{20001 + i}", t, 3, "MA")
    ug_courses, pg_courses = {}, {}
    for dcode, _, titles in DEPTS:
        ug_courses[dcode], pg_courses[dcode] = [], []
        for i, t in enumerate(titles):
            ug_courses[dcode].append(
                add_course(
                    f"{dcode}{21001 + i}",
                    t,
                    4 if i % 3 == 0 else 3,
                    dcode,
                    3,
                    1,
                    2 if i % 4 == 0 else 0,
                )
            )
        for i, t in enumerate(titles[:6]):
            pg_courses[dcode].append(
                add_course(f"{dcode}{61001 + i}", f"Advanced {t}", 3, dcode)
            )
    db.ins(
        "acad.course",
        [
            "course_id",
            "course_code",
            "title",
            "credits",
            "lecture_hours",
            "tutorial_hours",
            "practical_hours",
            "department_id",
        ],
        courses,
    )

    cur_courses = []  # curriculum_course
    ccid = 0
    breadth_ids = [course_id[f"HS{20001 + i}"] for i in range(len(BREADTH))]
    SEMCOURSES: dict[tuple[int, int], list[int]] = (
        {}
    )  # (curriculum, semester) -> course ids
    for prog in programs:
        prog_id, _, _, lvl, years, dept_id = prog
        dcode = next(c for c, i in DID.items() if i == dept_id)
        cur = CURR[prog_id]
        plan: dict[int, list[tuple[int, str]]] = {}
        if lvl == "UG":
            for code, _, _, sem, _ in COMMON:
                plan.setdefault(sem, []).append((course_id[code], "CORE"))
            for sem, idxs in {
                3: [0, 1],
                4: [2, 3],
                5: [4],
                6: [5],
                7: [6],
                8: [7],
            }.items():
                for i in idxs:
                    plan.setdefault(sem, []).append(
                        (ug_courses[dcode][i], "CORE" if sem < 7 else "ELECTIVE")
                    )
            for sem in (3, 4, 5, 6, 7, 8):
                plan[sem].append(
                    (breadth_ids[(sem + dept_id) % len(breadth_ids)], "BREADTH")
                )
        elif lvl in ("PG", "MSC"):
            for sem, idxs in {1: [0, 1], 2: [2, 3], 3: [4], 4: [5]}.items():
                for i in idxs:
                    plan.setdefault(sem, []).append(
                        (pg_courses[dcode][i], "CORE" if sem < 3 else "ELECTIVE")
                    )
        for sem, items in plan.items():
            SEMCOURSES[(cur, sem)] = [c for c, _ in items]
            for c, cat in items:
                ccid += 1
                cur_courses.append((ccid, cur, c, sem, cat))
    db.ins(
        "acad.curriculum_course",
        [
            "curriculum_course_id",
            "curriculum_id",
            "course_id",
            "semester_no",
            "category",
        ],
        cur_courses,
    )

    # ================================================================== offerings
    offerings, offer_id = [], {}
    oid = 0
    course_dept = {c[0]: c[7] for c in courses}
    needed = {}  # (course, session) offerings needed
    sems_by_term = {"Autumn": (1, 3, 5, 7), "Spring": (2, 4, 6, 8)}
    for sess in sessions:
        sidx, _, term = sess[0], sess[1], sess[2]
        for (cur, sem), cl in SEMCOURSES.items():
            if (sem % 2 == 1) == (term == "Autumn"):
                for c in cl:
                    needed[(c, sidx)] = True
    for c, sidx in sorted(needed):
        oid += 1
        offer_id[(c, sidx)] = oid
        offerings.append(
            (
                oid,
                c,
                sidx,
                "A",
                120 if c in {course_id[x[0]] for x in COMMON} else 60,
                "OFFERED",
            )
        )
    db.ins(
        "acad.course_offering",
        ["offering_id", "course_id", "session_id", "section", "seats", "status"],
        offerings,
    )
    cdept_code = {v: k for k, v in DID.items()}
    instr_rows, slot_rows = [], []
    for o in offerings:
        dcode = cdept_code[course_dept[o[1]]]
        fl = faculty_by_dept.get(dcode) or faculty_by_dept["MA"]
        instr_rows.append((o[0], pick(fl)["id"], "INSTRUCTOR"))
        if rng.random() < 0.3:
            instr_rows.append((o[0], pick(fl)["id"], "INSTRUCTOR"))
        for _ in range(2):
            h = rng.randrange(8, 17)
            slot_rows.append(
                (
                    o[0],
                    rng.randrange(1, 6),
                    pick(["A1", "B2", "C3", "D4", "E5", "F1", "G2"]),
                    time(h),
                    time(h + 1),
                    f"{pick(['NR', 'NC', 'V1', 'F116'])}-{rng.randrange(101, 420)}",
                    pick(["Nalanda", "Vikramshila", "Takshashila", None]),
                )
            )
    db.ins(
        "acad.offering_instructor", ["offering_id", "employee_id", "role"], instr_rows
    )
    db.ins(
        "acad.timetable_slot",
        [
            "offering_id",
            "day_of_week",
            "slot_code",
            "start_time",
            "end_time",
            "room_no",
            "building",
        ],
        slot_rows,
    )

    # ================================================================== registrations, results, performance
    reg_rows, res_rows, perf_rows, card_rows, att_rows, asg_rows, fb_rows = (
        [],
        [],
        [],
        [],
        [],
        [],
        [],
    )
    regn = 0
    credits = {c[0]: c[3] for c in courses}
    cur_instr = {}
    for r in instr_rows:
        cur_instr.setdefault(r[0], r[1])
    emp_ids = [e["id"] for e in emps if e["func"] == "faculty"]
    recent = {CUR, CUR - 1}
    grad_year_map = {}
    for s in students:
        if s["level"] == "PHD":
            continue
        prog = PROG[(s["dept"], s["level"])]
        cur = CURR[prog]
        first_ord = SID[(s["year"], "Autumn")]
        last_ord = (
            CUR
            if not s["graduated"]
            else SID[(s["year"] + DUR[s["level"]] - 1, "Spring")]
        )
        if s["status"] == "DEREGISTERED":
            last_ord = max(first_ord, CUR - 2)
        if s["level"] == "UG" and s["graduated"] and s["year"] < 2023:
            # older graduates: only the performance summary is kept in this extract
            cg_pts = cg_cr = 0
            for sidx in range(max(first_ord, 1), last_ord + 1):
                sgpa = round(min(10, max(4.5, rng.gauss(7.6, 1.0))), 2)
                cr = rng.randrange(20, 26)
                cg_pts += sgpa * cr
                cg_cr += cr
                perf_rows.append((s["id"], sidx, sgpa, round(cg_pts / cg_cr, 2), cr))
            continue
        cg_pts = cg_cr = 0
        for sidx in range(first_ord, last_ord + 1):
            sem = sidx - first_ord + 1
            if sem > DUR[s["level"]] * 2:
                break
            cl = SEMCOURSES.get((cur, sem), [])
            card_rows.append(
                (
                    s["id"],
                    sidx,
                    "REG",
                    ts(sessions[sidx - 1][3] - timedelta(days=rng.randrange(1, 10))),
                )
            )
            pts = cr_sum = 0
            ability = rng.gauss(66, 11)
            for c in cl:
                o = offer_id.get((c, sidx))
                if not o:
                    continue
                regn += 1
                rtype = rng.choices(["REGULAR", "LATE", "SUPPLEMENTARY"], [93, 5, 2])[0]
                dropped = rng.random() < 0.015
                reg_rows.append(
                    (
                        regn,
                        s["id"],
                        o,
                        rtype,
                        "DROPPED" if dropped else "REGISTERED",
                        ts(
                            sessions[sidx - 1][3] + timedelta(days=rng.randrange(0, 12))
                        ),
                    )
                )
                if dropped:
                    continue
                if sidx < CUR:
                    marks = round(max(0, min(100, rng.gauss(ability, 12))), 2)
                    g = next(x for x in GRADES if marks >= x[0])
                    res_rows.append(
                        (
                            regn,
                            marks,
                            g[1],
                            g[2],
                            g[1] == "F",
                            ts(
                                sessions[sidx - 1][4]
                                + timedelta(days=rng.randrange(5, 25))
                            ),
                        )
                    )
                    pts += g[2] * credits[c]
                    cr_sum += credits[c] if g[2] else 0
                    fb_rows.append(
                        (
                            regn,
                            rng.choices([1, 2, 3, 4, 5], [3, 7, 20, 40, 30])[0],
                            pick(
                                [
                                    None,
                                    None,
                                    "Clear explanations",
                                    "Too fast-paced",
                                    "Good tutorials",
                                    "Need more examples",
                                ]
                            ),
                            ts(sessions[sidx - 1][4]),
                        )
                    )
                else:
                    res_rows.append((regn, None, None, None, False, None))
                if sidx in recent:
                    start = sessions[sidx - 1][3] + timedelta(days=14)
                    for k in range(12):
                        att_rows.append(
                            (
                                regn,
                                start + timedelta(days=k * 3 + rng.randrange(0, 2)),
                                rng.choices(["P", "A", "L"], [84, 12, 4])[0],
                                cur_instr.get(o, emp_ids[0]),
                            )
                        )
                    for t in ("Assignment 1", "Assignment 2"):
                        asg_rows.append(
                            (
                                regn,
                                t,
                                20,
                                (
                                    round(rng.uniform(8, 20), 1)
                                    if sidx < CUR or t == "Assignment 1"
                                    else None
                                ),
                                cur_instr.get(o, emp_ids[0]),
                                ts(start + timedelta(days=20)),
                            )
                        )
            if sidx < CUR and cl:
                tot_cr = sum(credits[c] for c in cl if (c, sidx) in offer_id) or 1
                sgpa = round(min(10, pts / tot_cr), 2)
                cg_pts += pts
                cg_cr += tot_cr
                perf_rows.append(
                    (s["id"], sidx, sgpa, round(min(10, cg_pts / cg_cr), 2), cr_sum)
                )
    CHUNK = 20000
    db.ins(
        "acad.registration",
        [
            "registration_id",
            "student_id",
            "offering_id",
            "reg_type",
            "status",
            "registered_at",
        ],
        reg_rows,
    )
    db.ins(
        "acad.registration_card",
        ["student_id", "session_id", "card_type", "generated_at"],
        card_rows,
    )
    db.ins(
        "acad.result",
        [
            "registration_id",
            "marks",
            "grade",
            "grade_points",
            "is_backlog",
            "declared_at",
        ],
        res_rows,
    )
    db.ins(
        "acad.semester_performance",
        ["student_id", "session_id", "sgpa", "cgpa", "credits_earned"],
        perf_rows,
    )
    db.ins(
        "acad.class_attendance",
        ["registration_id", "attended_on", "status", "marked_by_employee_id"],
        att_rows,
    )
    db.ins(
        "acad.assignment_mark",
        [
            "registration_id",
            "title",
            "max_marks",
            "marks_obtained",
            "entered_by_employee_id",
            "entered_at",
        ],
        asg_rows,
    )
    db.ins(
        "acad.course_feedback",
        ["registration_id", "rating", "comments", "submitted_at"],
        fb_rows,
    )
    log(
        "acad: registrations",
        len(reg_rows),
        "results",
        len(res_rows),
        "attendance",
        len(att_rows),
    )

    # exams
    ex_rows, exa_rows, admit_rows = [], [], []
    exn = 0
    cur_offers = [o for o in offerings if o[2] in (CUR - 1, CUR)]
    exam_ids = {}
    for o in cur_offers:
        for et, off in (("MIDSEM", 60), ("ENDSEM", 110)):
            exn += 1
            exam_ids[(o[0], et)] = exn
            base = sessions[o[2] - 1][3]
            ex_rows.append(
                (
                    exn,
                    o[0],
                    et,
                    "CENTRAL" if o[1] in range(1, 11) else "DEPARTMENTAL",
                    base + timedelta(days=off + rng.randrange(0, 10)),
                    time(rng.choice([9, 14])),
                    f"{pick(['NR', 'NC', 'V1'])}-{rng.randrange(101, 420)}",
                )
            )
    db.ins(
        "acad.exam_schedule",
        [
            "exam_id",
            "offering_id",
            "exam_type",
            "scope",
            "exam_date",
            "start_time",
            "venue",
        ],
        ex_rows,
    )
    reg_by_off = {}
    for r in reg_rows:
        if r[4] == "REGISTERED":
            reg_by_off.setdefault(r[2], []).append(r[1])
    for (o, et), e in exam_ids.items():
        if et != "MIDSEM":
            continue
        for st in reg_by_off.get(o, []):
            if offerings[o - 1][2] in (CUR - 1, CUR):
                exa_rows.append(
                    (
                        e,
                        st,
                        f"S{rng.randrange(1, 400)}",
                        rng.choices(["P", "A"], [95, 5])[0],
                    )
                )
    db.ins(
        "acad.exam_attendance", ["exam_id", "student_id", "seat_no", "status"], exa_rows
    )
    for s in students:
        if s["status"] == "ON ROLL" and s["level"] != "PHD":
            for et in ("MIDSEM", "ENDSEM"):
                if et == "MIDSEM" or rng.random() < 0.2:
                    admit_rows.append(
                        (
                            s["id"],
                            CUR if et == "MIDSEM" else CUR - 1,
                            et,
                            ts(date(2026, 9, rng.randrange(1, 28))),
                            ts(date(2026, 9, 28)) if rng.random() < 0.8 else None,
                        )
                    )
    db.ins(
        "acad.admit_card",
        ["student_id", "session_id", "exam_type", "generated_at", "downloaded_at"],
        admit_rows,
    )

    # applications and student records
    active = [s for s in students if s["status"] == "ON ROLL"]
    ug_active = [s for s in active if s["level"] == "UG"]
    spa, psw, brd, conv, gcl, awd, act, med, doc, nom, cons = ([] for _ in range(11))
    for s in rng.sample(ug_active, 220):
        spa.append(
            (
                s["id"],
                pick(["MINOR", "MICRO_CREDIT", "SAIP", "SAPP", "VERTICAL"]),
                DID[pick(DEPTS)[0]],
                pick(["APPLIED", "APPROVED", "APPROVED", "REJECTED"]),
                ts(date(2026, rng.randrange(1, 10), rng.randrange(1, 28))),
                None,
                None,
            )
        )
    for s in rng.sample(ug_active, 90):
        a = DID[s["dept"]]
        b = pick([d for d in DID.values() if d != a and d != DID["AD"]])
        psw.append(
            (
                s["id"],
                pick(["BRANCH_CHANGE", "BRANCH_CHANGE", "DUAL_DEGREE", "INTERDISC"]),
                a,
                b,
                round(rng.uniform(7.0, 9.8), 2),
                pick(["APPLIED", "APPROVED", "REJECTED"]),
                ts(date(2026, rng.randrange(1, 10), rng.randrange(1, 28))),
            )
        )
    bc_students = [s for s in ug_active if s["year"] <= 2025]
    for s in rng.sample(bc_students, 200):
        for pref in (1, 2, 3):
            brd.append(
                (
                    s["id"],
                    CUR,
                    pick(breadth_ids),
                    pref,
                    pref == 1 and rng.random() < 0.7,
                )
            )
    for s in students:
        if (
            s["year"] == 2022
            and s["level"] == "UG"
            or s["year"] == 2024
            and s["level"] == "PG"
        ):
            conv.append(
                (
                    s["id"],
                    2026,
                    pick(["REGISTERED", "REGISTERED", "PENDING"]),
                    ts(date(2026, rng.randrange(5, 9), rng.randrange(1, 28))),
                )
            )
        if s["graduated"] and s["year"] >= 2022:
            clr = rng.random() < 0.9
            gcl.append(
                (
                    s["id"],
                    clr or rng.random() < 0.5,
                    clr or rng.random() < 0.7,
                    clr or rng.random() < 0.8,
                    clr or rng.random() < 0.6,
                    "CLEARED" if clr else "PENDING",
                    ts(date(2026, rng.randrange(4, 9), rng.randrange(1, 28))),
                )
            )
    for s in rng.sample(students, 180):
        awd.append(
            (
                s["id"],
                pick(
                    [
                        "Institute Silver Medal",
                        "Dean's List",
                        "Best Project Award",
                        "Sports Colours",
                        "Cultural Excellence Prize",
                        "Alumni Merit Award",
                    ]
                ),
                rng.randrange(2022, 2027),
                None,
            )
        )
    for s in rng.sample(active, 600):
        act.append(
            (
                s["id"],
                CUR - rng.randrange(0, 3),
                pick(["NSS", "NCC", "SPORTS", "CULTURAL", "TECHNICAL"]),
                pick(
                    [
                        "NSS Camp",
                        "NCC Parade",
                        "Inter-IIT Hockey",
                        "Spring Fest Volunteering",
                        "Robotics Club",
                        "Coding Club",
                        "Dramatics Society",
                    ]
                ),
                rng.randrange(10, 120) if rng.random() < 0.8 else None,
                pick(emp_ids) if rng.random() < 0.6 else None,
            )
        )
    for s in rng.sample(active, 260):
        a = rdate(date(2026, 1, 1), TODAY)
        med.append(
            (
                s["id"],
                a,
                a + timedelta(days=rng.randrange(2, 12)),
                pick(
                    [
                        "Viral fever",
                        "Dengue",
                        "Fracture",
                        "Typhoid",
                        "Surgery recovery",
                        "Jaundice",
                        "Chikungunya",
                    ]
                ),
                pick(["APPROVED", "APPROVED", "PENDING", "REJECTED"]),
                pick(emp_ids) if rng.random() < 0.8 else None,
            )
        )
    for s in rng.sample(active, 500):
        doc.append(
            (
                s["id"],
                pick(["GATE_SCORECARD", "CASTE_CERT", "MARKSHEET", "INCOME_CERT"]),
                f"/docs/{s['roll']}/{rng.randrange(1000, 9999)}.pdf",
                rng.randrange(300, 900) if s["level"] == "PG" else None,
                ts(rdate(date(2025, 6, 1), TODAY)),
                rng.random() < 0.85,
            )
        )
    for s in rng.sample(active, 60):
        nom.append(
            (
                s["id"],
                CUR,
                pick(
                    [
                        "Hall Representative",
                        "Department Representative",
                        "Senate Member",
                        "Sports Secretary",
                    ]
                ),
                pick(["NOMINATED", "ELECTED", "REJECTED"]),
            )
        )
    for s in students:
        if s["status"] == "ON ROLL":
            cons.append(
                (s["id"], rng.random() < 0.7, ts(rdate(date(2025, 8, 1), TODAY)))
            )
    db.ins(
        "acad.special_program_application",
        [
            "student_id",
            "program_type",
            "target_department_id",
            "status",
            "applied_at",
            "decided_at",
            "remarks",
        ],
        spa,
    )
    db.ins(
        "acad.program_switch_application",
        [
            "student_id",
            "switch_type",
            "from_department_id",
            "to_department_id",
            "cgpa_at_application",
            "status",
            "applied_at",
        ],
        psw,
    )
    db.ins(
        "acad.breadth_choice",
        ["student_id", "session_id", "course_id", "preference_no", "allotted"],
        brd,
    )
    db.ins(
        "acad.convocation_registration",
        ["student_id", "convocation_year", "status", "registered_at"],
        conv,
    )
    db.ins(
        "acad.graduation_clearance",
        [
            "student_id",
            "department_clear",
            "library_clear",
            "hall_clear",
            "accounts_clear",
            "status",
            "updated_at",
        ],
        gcl,
    )
    db.ins(
        "acad.award_prize", ["student_id", "award_name", "awarded_year", "details"], awd
    )
    db.ins(
        "acad.student_activity",
        [
            "student_id",
            "session_id",
            "activity_type",
            "activity_name",
            "hours",
            "verified_by_employee_id",
        ],
        act,
    )
    db.ins(
        "acad.medical_leave",
        [
            "student_id",
            "from_date",
            "to_date",
            "reason",
            "status",
            "approved_by_employee_id",
        ],
        med,
    )
    db.ins(
        "acad.student_document",
        [
            "student_id",
            "doc_type",
            "file_path",
            "gate_score",
            "uploaded_at",
            "verified",
        ],
        doc,
    )
    db.ins("acad.rep_nomination", ["student_id", "session_id", "post", "status"], nom)
    db.ins("acad.data_sharing_consent", ["student_id", "consented", "decided_at"], cons)

    # ================================================================== finance
    heads = [
        ("Tuition Fee", "ACADEMIC"),
        ("Institute Fee", "ACADEMIC"),
        ("Examination Fee", "ACADEMIC"),
        ("Hall Rent", "HALL"),
        ("Mess Advance", "HALL"),
        ("Medical Fee", "OTHER"),
        ("Convocation Fee", "OTHER"),
        ("Students' Gymkhana Fee", "OTHER"),
    ]
    db.ins(
        "fin.fee_head",
        ["head_id", "name", "category"],
        [(i + 1, n, c) for i, (n, c) in enumerate(heads)],
    )
    base_amt = {1: 62500, 2: 8500, 3: 1500, 4: 3000, 5: 6000, 6: 1000, 7: 2000, 8: 900}
    fs, fsid = [], {}
    k = 0
    for prog in programs:
        mult = {"UG": 1.0, "PG": 0.4, "MSC": 0.4, "PHD": 0.1}[prog[3]]
        for ay in ("2023-24", "2024-25", "2025-26", "2026-27"):
            for h in range(1, 9):
                if h == 7 and prog[3] == "PHD":
                    continue
                k += 1
                fsid[(prog[0], ay, h)] = k
                fs.append(
                    (
                        k,
                        prog[0],
                        ay,
                        h,
                        round(
                            base_amt[h]
                            * (mult if h <= 3 else 1.0)
                            * (1.0 + 0.04 * (int(ay[:4]) - 2023)),
                            2,
                        ),
                    )
                )
    fsamt = {(r[1], r[2], r[3]): r[4] for r in fs}
    db.ins(
        "fin.fee_structure",
        ["fee_structure_id", "program_id", "acad_year", "head_id", "amount"],
        fs,
    )
    dm, di, pay = [], [], []
    dn = di_n = pn = 0
    for s in students:
        if s["level"] == "PHD" and rng.random() < 0.5:
            continue
        prog = PROG[(s["dept"], s["level"])]
        first = SID[(s["year"], "Autumn")]
        last = (
            CUR
            if not s["graduated"]
            else SID[(s["year"] + DUR[s["level"]] - 1, "Spring")]
        )
        for sidx in range(max(first, 5), last + 1):
            ay = sessions[sidx - 1][1]
            if (prog, ay, 1) not in fsid:
                continue
            hs = [1, 2, 3, 6, 8] + ([4, 5] if s["hall"] else [])
            amts = {h: fsamt[(prog, ay, h)] for h in hs}
            tot = round(sum(amts.values()), 2)
            dn += 1
            due = sessions[sidx - 1][3] + timedelta(days=15)
            paid = rng.random() < (0.995 if sidx < CUR else 0.82)
            partial = (not paid) and rng.random() < 0.3
            status = (
                "PAID"
                if paid
                else ("PARTIAL" if partial else ("OVERDUE" if due < TODAY else "DUE"))
            )
            dm.append((dn, s["id"], sidx, tot, due, status))
            for h, a in amts.items():
                di_n += 1
                di.append((dn, h, a))
            if paid or partial:
                pn += 1
                amt = tot if paid else round(tot * 0.5, 2)
                pay.append(
                    (
                        dn,
                        s["id"],
                        amt,
                        pick(["UPI", "NETBANK", "CARD", "DD"]),
                        f"TXN{SEED % 1000}{pn:08d}",
                        "SUCCESS",
                        ts(due - timedelta(days=rng.randrange(0, 14))),
                        f"RCP{pn:010d}",
                    )
                )
                if rng.random() < 0.04:
                    pn += 1
                    pay.append(
                        (
                            dn,
                            s["id"],
                            amt,
                            "UPI",
                            f"TXN{SEED % 1000}{pn:08d}",
                            "FAILED",
                            ts(due - timedelta(days=rng.randrange(14, 20))),
                            f"RCP{pn:010d}",
                        )
                    )
    db.ins(
        "fin.fee_demand",
        ["demand_id", "student_id", "session_id", "total_amount", "due_date", "status"],
        dm,
    )
    db.ins("fin.fee_demand_item", ["demand_id", "head_id", "amount"], di)
    db.ins(
        "fin.payment",
        [
            "demand_id",
            "student_id",
            "amount",
            "mode",
            "gateway_txn_id",
            "status",
            "paid_at",
            "receipt_no",
        ],
        pay,
    )
    fam = []
    income = {}
    for s in students:
        if s["status"] == "ON ROLL":
            income[s["id"]] = round(rng.lognormvariate(13.2, 0.9) / 1000) * 1000
            fam.append(
                (
                    s["id"],
                    "2025-26",
                    float(income[s["id"]]),
                    pick(
                        [
                            "Salaried",
                            "Business",
                            "Agriculture",
                            "Pension",
                            "Self-employed",
                        ]
                    ),
                    pick(emp_ids) if rng.random() < 0.7 else None,
                    ts(rdate(date(2025, 8, 1), date(2025, 10, 30))),
                )
            )
    db.ins(
        "fin.family_income_declaration",
        [
            "student_id",
            "acad_year",
            "annual_income",
            "income_source",
            "verified_by_employee_id",
            "declared_at",
        ],
        fam,
    )
    sch = [
        ("Merit-cum-Means", "Institute", 8.0, 500000),
        ("Institute Free Studentship", "Institute", None, 100000),
        ("Central Sector Scholarship", "Government of India", 7.5, 450000),
        ("Alumni Scholarship", "Alumni Association", 8.5, None),
        ("Post Matric (SC/ST)", "State Government", None, 250000),
        ("Dr. Rao Memorial Fellowship", "Endowment", 9.0, None),
    ]
    db.ins(
        "fin.scholarship_scheme",
        ["scheme_id", "name", "provider", "min_cgpa", "max_family_income"],
        [(i + 1, *x) for i, x in enumerate(sch)],
    )
    sa = []
    for s in rng.sample([s for s in active if s["level"] in ("UG", "PG")], 420):
        sa.append(
            (
                s["id"],
                rng.randrange(1, 7),
                CUR - rng.randrange(0, 4),
                float(rng.choice([10000, 20000, 31250, 62500])),
                pick(["AWARDED", "DISBURSED", "DISBURSED"]),
                ts(rdate(date(2025, 8, 1), TODAY)) if rng.random() < 0.7 else None,
            )
        )
    db.ins(
        "fin.scholarship_award",
        ["student_id", "scheme_id", "session_id", "amount", "status", "disbursed_at"],
        sa,
    )
    mc = []
    for s in rng.sample(active, 300):
        b = round(rng.uniform(800, 90000), 2)
        sub = ts(rdate(date(2025, 8, 1), TODAY))
        stt = pick(["SUBMITTED", "SETTLED", "SETTLED", "REJECTED"])
        mc.append(
            (
                s["id"],
                pick(
                    [
                        "Kalyani General Hospital",
                        "Apollo Clinic",
                        "Midnapore Medical College",
                        "Fortis Hospital",
                        "City Care Nursing Home",
                    ]
                ),
                b,
                round(b * rng.uniform(0.6, 1), 2),
                stt,
                sub,
                (
                    sub + timedelta(days=rng.randrange(10, 60))
                    if stt == "SETTLED"
                    else None
                ),
            )
        )
    db.ins(
        "fin.mediclaim_claim",
        [
            "student_id",
            "hospital",
            "bill_amount",
            "claimed_amount",
            "status",
            "submitted_at",
            "settled_at",
        ],
        mc,
    )
    sl = []
    for e in emps:
        base = {
            "Professor": 210000,
            "Associate Professor": 165000,
            "Assistant Professor": 125000,
        }.get(emp_rows[e["id"] - 1][4], 58000 + rng.randrange(0, 30000))
        base += rng.randrange(-5000, 5000)
        for m in range(12):
            mo = (
                date(2025, 11, 1)
                if m == 0
                else date(2025 + (10 + m) // 12, (10 + m) % 12 + 1, 1)
            )
            gross = float(base)
            ded = round(gross * 0.18, 2)
            sl.append((e["id"], mo, gross, ded, round(gross - ded, 2)))
    db.ins(
        "fin.salary_slip",
        ["employee_id", "pay_month", "gross", "deductions", "net"],
        sl,
    )
    log("fin: demands", len(dm), "payments", len(pay), "salary slips", len(sl))

    # ================================================================== library
    cats = ["Engineering", "Science", "Humanities", "Management", "Fiction"]
    bk, cp, ln, dues = [], [], [], []
    for i in range(1, 1201):
        d = pick(DEPTS)
        t = pick(d[2])
        bk.append(
            (
                i,
                f"978{rng.randrange(10**9, 10**10 - 1)}",
                (
                    f"{t}: Principles and Practice, Vol {rng.randrange(1, 4)}"
                    if i % 3
                    else f"Fundamentals of {t}"
                ),
                f"{pick(FIRST)} {pick(LAST)}",
                pick(
                    [
                        "Pearson",
                        "McGraw-Hill",
                        "Wiley",
                        "Oxford University Press",
                        "Springer",
                        None,
                    ]
                ),
                pick(cats[:2] + cats[:2] + cats[2:]),
            )
        )
    db.ins(
        "lib.book", ["book_id", "isbn", "title", "author", "publisher", "category"], bk
    )
    cpid = 0
    for b in range(1, 1201):
        for _ in range(rng.randrange(1, 5)):
            cpid += 1
            cp.append((cpid, b, f"ACC{cpid:08d}", "AVAILABLE"))
    people_pool = [s["person"] for s in active] + [
        e["person"] for e in emps if e["func"] == "faculty"
    ]
    ln_id = 0
    copy_status = {c[0]: "AVAILABLE" for c in cp}
    for _ in range(9000):
        ln_id += 1
        c = rng.randrange(1, cpid + 1)
        issued = rdate(date(2024, 8, 1), TODAY)
        due = issued + timedelta(days=21)
        returned = (
            None
            if (issued > TODAY - timedelta(days=25) and rng.random() < 0.7)
            or rng.random() < 0.02
            else issued + timedelta(days=rng.randrange(3, 45))
        )
        if returned and returned > TODAY:
            returned = TODAY
        p = pick(people_pool)
        ln.append((ln_id, c, p, issued, due, returned))
        if returned is None:
            copy_status[c] = "ISSUED"
        if returned and returned > due:
            dues.append(
                (
                    ln_id,
                    p,
                    round((returned - due).days * 2.0, 2),
                    "LATE",
                    pick(["PAID", "PAID", "UNPAID"]),
                )
            )
        elif returned is None and due < TODAY and rng.random() < 0.8:
            dues.append(
                (ln_id, p, round((TODAY - due).days * 2.0, 2), "LATE", "UNPAID")
            )
    cp = [
        (a, b, c, copy_status[a] if rng.random() > 0.004 else "LOST")
        for (a, b, c, _) in cp
    ]
    db.ins("lib.book_copy", ["copy_id", "book_id", "accession_no", "status"], cp)
    db.ins(
        "lib.loan",
        ["loan_id", "copy_id", "person_id", "issued_on", "due_on", "returned_on"],
        ln,
    )
    db.ins(
        "lib.library_due", ["loan_id", "person_id", "amount", "reason", "status"], dues
    )

    # ================================================================== auth (accounts, roles)
    acc_rows, ar_rows = [], []
    ROLEID = {r: i + 1 for i, (r, _) in enumerate(ROLES)}
    db.ins(
        "auth.role",
        ["role_id", "role_code", "description", "priority"],
        [
            (i + 1, r, f"{r.replace('_', ' ')} role", p)
            for i, (r, p) in enumerate(ROLES)
        ],
    )
    aid = 0
    ACC = {}  # login -> (account_id, kind)
    for s in students:
        aid += 1
        ACC[s["roll"]] = aid
        st = (
            "ACTIVE"
            if s["status"] == "ON ROLL"
            else ("DISABLED" if s["status"] == "DEREGISTERED" else "ACTIVE")
        )
        acc_rows.append(
            (
                aid,
                s["roll"],
                s["person"],
                "STUDENT",
                "x",
                None,
                None,
                st,
                0,
                None,
                None,
                ts(date(s["year"], 8, 1)),
            )
        )
        ar_rows.append(
            (
                aid,
                ROLEID["student"],
                "GLOBAL",
                None,
                date(s["year"], 8, 1),
                date(s["year"] + DUR[s["level"]], 7, 31) if s["graduated"] else None,
            )
        )
    for e in emps:
        aid += 1
        ACC[f"E{e['id']:04d}"] = aid
        e["account"] = aid
        acc_rows.append(
            (
                aid,
                f"E{e['id']:04d}",
                e["person"],
                "STAFF",
                "x",
                None,
                None,
                "ACTIVE",
                0,
                None,
                None,
                ts(date(2020, 1, 1)),
            )
        )
        role = e["func"] if e["func"] in ROLEID else "dept_office"
        if e["func"] == "registry":
            role = "acad_section"
        scope, sid_ = (
            ("DEPARTMENT", DID[e["dept"]])
            if role in ("faculty", "dept_office")
            else ("GLOBAL", None)
        )
        if role == "hall_office":
            scope = "HALL"
            sid_ = ((e["id"] - 1) % len(HALLS)) + 1
        ar_rows.append((aid, ROLEID[role], scope, sid_, date(2020, 1, 1), None))
        if e.get("hod"):
            ar_rows.append(
                (aid, ROLEID["hod"], "DEPARTMENT", e["hod"], date(2023, 1, 1), None)
            )
    # "Switch Login": ten PhD students also hold a staff (teaching-assistant) account on the same person
    dual = [s for s in students if s["level"] == "PHD" and s["status"] == "ON ROLL"][
        :10
    ]
    eid2 = len(emps)
    extra_emps = []
    for s in dual:
        eid2 += 1
        extra_emps.append(
            (
                eid2,
                f"E{eid2:04d}",
                s["person"],
                DID[s["dept"]],
                "Teaching Assistant",
                "STAFF",
                s["name"],
                persons[s["person"] - 1][13],
                date(s["year"], 8, 15),
                True,
            )
        )
        aid += 1
        ACC[f"E{eid2:04d}"] = aid
        acc_rows.append(
            (
                aid,
                f"E{eid2:04d}",
                s["person"],
                "STAFF",
                "x",
                None,
                None,
                "ACTIVE",
                0,
                None,
                None,
                ts(date(s["year"], 8, 15)),
            )
        )
        ar_rows.append(
            (
                aid,
                ROLEID["faculty"],
                "DEPARTMENT",
                DID[s["dept"]],
                date(s["year"], 8, 15),
                None,
            )
        )
        s["dual_emp"] = eid2
    db.ins(
        "people.employee",
        [
            "employee_id",
            "emp_code",
            "person_id",
            "department_id",
            "designation",
            "employee_type",
            "display_name",
            "official_email",
            "joined_on",
            "is_active",
        ],
        extra_emps,
    )
    db.ins(
        "auth.account",
        [
            "account_id",
            "login_id",
            "person_id",
            "account_type",
            "password_hash",
            "passphrase_hash",
            "campus_net_password_hash",
            "status",
            "failed_attempts",
            "locked_until",
            "last_login_at",
            "created_at",
        ],
        acc_rows,
    )
    db.ins(
        "auth.account_role",
        ["account_id", "role_id", "scope_type", "scope_id", "valid_from", "valid_to"],
        ar_rows,
    )
    # passwords: bcrypt (cost 8) of 'Erp@' || login_id, for every synthetic account
    db.run(
        "UPDATE auth.account SET password_hash = crypt('Erp@' || login_id, gen_salt('bf', 8)), "
        "passphrase_hash = crypt('Pass@' || login_id, gen_salt('bf', 8)) WHERE account_type = 'STUDENT' AND random() < 0.4"
    )
    db.run(
        "UPDATE auth.account SET password_hash = crypt('Erp@' || login_id, gen_salt('bf', 8)) WHERE password_hash = 'x'"
    )
    # login history
    la = []
    ids = list(range(1, aid + 1))
    for _ in range(6000):
        a = pick(ids)
        ev = rng.choices(["LOGIN_OK", "LOGIN_FAILED", "LOGOUT"], [70, 15, 15])[0]
        la.append(
            (
                a,
                None,
                ev,
                f"10.{rng.randrange(0, 255)}.{rng.randrange(0, 255)}.{rng.randrange(1, 255)}",
                "Mozilla/5.0",
                ts(rdate(date(2026, 6, 1), TODAY)),
            )
        )
    for _ in range(40):
        la.append(
            (
                None,
                f"x{rng.randrange(100, 999)}",
                "LOGIN_FAILED",
                f"10.9.{rng.randrange(0, 255)}.{rng.randrange(1, 255)}",
                "curl/8",
                ts(rdate(date(2026, 9, 1), TODAY)),
            )
        )
    db.ins(
        "auth.login_audit",
        ["account_id", "login_id_tried", "event", "ip_addr", "user_agent", "at"],
        [(a, (login or ""), ev, ip, ua, at) for (a, login, ev, ip, ua, at) in la],
    )
    db.run(
        "UPDATE auth.login_audit l SET login_id_tried = a.login_id FROM auth.account a WHERE l.account_id = a.account_id AND l.login_id_tried = ''"
    )
    db.run(
        "UPDATE auth.account a SET last_login_at = x.m FROM (SELECT account_id, max(at) m FROM auth.login_audit WHERE event = 'LOGIN_OK' GROUP BY 1) x WHERE a.account_id = x.account_id"
    )
    ses = []
    for a in rng.sample(ids, 30):
        ses.append(
            (
                a,
                "".join(rng.choice("0123456789abcdef") for _ in range(64)),
                datetime.now() - timedelta(minutes=5),
                datetime.now() + timedelta(minutes=25),
                "10.1.2.3",
            )
        )
    db.ins(
        "auth.session",
        ["account_id", "token_hash", "created_at", "expires_at", "ip_addr"],
        ses,
    )

    # ================================================================== services
    types = [
        ("FEE_CERTIFICATE", "Fee Certificate", "CERTIFICATE"),
        ("BONAFIDE", "Bonafide Certificate", "CERTIFICATE"),
        ("CRECHE", "Creche Admission", "FACILITY"),
        ("INTERNSHIP_NOC", "Internship NOC", "ACADEMIC"),
        ("HALL_CHANGE", "Hall Room Change", "FACILITY"),
        ("TRANSCRIPT", "Official Transcript", "CERTIFICATE"),
        ("LEAVE_OF_ABSENCE", "Leave of Absence", "ACADEMIC"),
        ("NO_DUES", "No Dues Certificate", "CERTIFICATE"),
        ("ID_CARD_REISSUE", "ID Card Re-issue", "FACILITY"),
        ("SEMESTER_DROP", "Semester Drop", "ACADEMIC"),
        ("BACKLOG_EXAM", "Backlog Examination", "ACADEMIC"),
        ("BANK_CHANGE", "Bank Details Change", "FACILITY"),
    ]
    db.ins(
        "svc.application_type",
        ["type_id", "code", "name", "category"],
        [(i + 1, *t) for i, t in enumerate(types)],
    )
    apps, steps = [], []
    approvers = [
        e["account"]
        for e in emps
        if e["func"] in ("acad_section", "dept_office", "accounts", "hall_office")
    ]
    for i in range(1, 1501):
        s = pick(active)
        stt = rng.choices(
            ["SUBMITTED", "APPROVED", "REJECTED", "PENDING"], [15, 55, 8, 22]
        )[0]
        t = rng.randrange(1, 13)
        apps.append(
            (
                i,
                t,
                s["person"],
                s["id"],
                ts(rdate(date(2026, 1, 1), TODAY)),
                stt,
                json.dumps(
                    {
                        "reason": pick(
                            ["Visa", "Scholarship", "Job", "Bank", "Higher studies"]
                        ),
                        "copies": rng.randrange(1, 4),
                    }
                ),
            )
        )
        for n in range(1, rng.randrange(2, 4)):
            steps.append(
                (
                    i,
                    n,
                    pick(approvers),
                    (
                        "APPROVED"
                        if stt == "APPROVED" or (stt != "REJECTED" and n == 1)
                        else ("REJECTED" if stt == "REJECTED" and n == 2 else "PENDING")
                    ),
                    pick([None, "Verified", "Please resubmit with documents"]),
                    ts(rdate(date(2026, 1, 1), TODAY)) if stt != "PENDING" else None,
                )
            )
    db.ins(
        "svc.paperless_application",
        [
            "application_id",
            "type_id",
            "applicant_person_id",
            "student_id",
            "submitted_at",
            "status",
            "payload",
        ],
        apps,
    )
    db.ins(
        "svc.application_step",
        [
            "application_id",
            "step_no",
            "approver_account_id",
            "action",
            "remarks",
            "acted_at",
        ],
        steps,
    )
    stu_acc = [ACC[s["roll"]] for s in active]
    staff_acc = [e["account"] for e in emps]
    admin_acc = [
        e["account"]
        for e in emps
        if e["func"] in ("admin", "acad_section", "hall_office")
    ]
    cmp_ = [
        (
            pick(stu_acc),
            pick(["HOSTEL", "ACADEMIC", "IT", "MESS", "OTHER"]),
            pick(
                [
                    "Wi-Fi not working",
                    "Water cooler broken",
                    "Marks not updated",
                    "Mess quality",
                    "Room repair",
                ]
            ),
            "Details of the issue as described by the complainant.",
            pick(["OPEN", "RESOLVED", "ONGOING"]),
            ts(rdate(date(2026, 3, 1), TODAY)),
            None,
        )
        for _ in range(400)
    ]
    cmp_ = [
        (
            a,
            b,
            c,
            d,
            e,
            f,
            f + timedelta(days=rng.randrange(1, 15)) if e == "RESOLVED" else None,
        )
        for (a, b, c, d, e, f, g) in cmp_
    ]
    db.ins(
        "svc.complaint",
        [
            "account_id",
            "category",
            "subject",
            "description",
            "status",
            "created_at",
            "resolved_at",
        ],
        cmp_,
    )
    db.ins(
        "svc.grievance",
        [
            "account_id",
            "category",
            "description",
            "status",
            "assigned_to_account_id",
            "created_at",
        ],
        [
            (
                pick(stu_acc),
                pick(["ACADEMIC", "HOSTEL", "HARASSMENT", "FINANCIAL"]),
                "Grievance description text.",
                pick(["OPEN", "RESOLVED", "ONGOING"]),
                pick(admin_acc) if rng.random() < 0.7 else None,
                ts(rdate(date(2026, 1, 1), TODAY)),
            )
            for _ in range(150)
        ],
    )
    mails, mrec = [], []
    for i in range(1, 2001):
        sender = pick(staff_acc + stu_acc[:200])
        sent = ts(rdate(date(2026, 6, 1), TODAY))
        mails.append(
            (
                i,
                sender,
                pick(
                    [
                        "Registration reminder",
                        "Fee due notice",
                        "Seminar announcement",
                        "Exam schedule",
                        "Hall maintenance",
                        "Placement drive",
                    ]
                ),
                "Body of the internal mail.",
                sent,
            )
        )
        for r in rng.sample(stu_acc, rng.randrange(1, 4)):
            mrec.append(
                (
                    i,
                    r,
                    (
                        sent + timedelta(hours=rng.randrange(1, 100))
                        if rng.random() < 0.7
                        else None
                    ),
                )
            )
    db.ins(
        "svc.mail_message",
        ["message_id", "from_account_id", "subject", "body", "sent_at"],
        mails,
    )
    db.ins("svc.mail_recipient", ["message_id", "account_id", "read_at"], mrec)
    db.ins(
        "svc.notice",
        [
            "title",
            "body",
            "posted_by_account_id",
            "audience",
            "posted_at",
            "expires_at",
        ],
        [
            (
                pick(
                    [
                        "Mid-semester examination schedule",
                        "Hall allotment results",
                        "Library closed on Sunday",
                        "Convocation registration open",
                        "Campus placement drive",
                        "Fee payment deadline",
                    ]
                ),
                "Notice body.",
                pick(admin_acc),
                pick(["ALL", "STUDENT", "STAFF"]),
                ts(rdate(date(2026, 6, 1), TODAY)),
                ts(TODAY + timedelta(days=rng.randrange(-30, 60))),
            )
            for _ in range(80)
        ],
    )
    counsellors = [e["id"] for e in emps if e["func"] == "faculty"][:6]
    db.ins(
        "svc.counselling_session",
        ["student_id", "counsellor_employee_id", "scheduled_at", "status", "notes"],
        [
            (
                s["id"],
                pick(counsellors),
                ts(rdate(date(2026, 1, 1), date(2026, 11, 15))),
                pick(["SCHEDULED", "DONE", "DONE", "MISSED"]),
                pick(
                    [
                        "Academic stress discussed.",
                        "Follow-up recommended.",
                        "No concerns.",
                        None,
                    ]
                ),
            )
            for s in rng.sample(active, 220)
        ],
    )
    ef_cat, ef, efm = [], [], []
    ecn = 0
    for d, c, _ in depts:
        for nm in ("Establishment", "Purchase", "Academic Matters"):
            ecn += 1
            ef_cat.append((ecn, d, nm))
    db.ins("svc.efile_category", ["category_id", "department_id", "name"], ef_cat)
    for i in range(1, 601):
        cat = pick(ef_cat)
        owner = pick(staff_acc)
        ef.append(
            (
                i,
                cat[0],
                cat[1],
                pick(
                    [
                        "Purchase of lab equipment",
                        "Faculty recruitment 2026",
                        "Course revision proposal",
                        "Annual maintenance contract",
                        "Travel grant approval",
                    ]
                ),
                owner,
                pick(["OPEN", "OPEN", "CLOSED"]),
                ts(rdate(date(2025, 6, 1), TODAY)),
            )
        )
        for _ in range(rng.randrange(1, 5)):
            s_ = ts(rdate(date(2025, 6, 1), TODAY))
            efm.append(
                (
                    i,
                    owner,
                    pick(staff_acc),
                    s_,
                    (
                        s_ + timedelta(hours=rng.randrange(1, 70))
                        if rng.random() < 0.8
                        else None
                    ),
                    pick([None, "Please examine", "Approved", "For comments"]),
                )
            )
    db.ins(
        "svc.efile",
        [
            "efile_id",
            "category_id",
            "department_id",
            "subject",
            "created_by_account_id",
            "status",
            "created_at",
        ],
        ef,
    )
    db.ins(
        "svc.efile_movement",
        [
            "efile_id",
            "from_account_id",
            "to_account_id",
            "sent_at",
            "received_at",
            "remarks",
        ],
        efm,
    )
    disp = [
        (
            i,
            DID[pick(DEPTS)[0]],
            pick(["INWARD", "OUTWARD"]),
            pick(["FILE", "LETTER", "PARCEL"]),
            pick(
                [
                    "Sealed tender documents",
                    "Equipment invoice",
                    "Letter from sponsor",
                    "Examination papers",
                ]
            ),
            pick(
                ["M/s Sharma Traders", "Ministry of Education", "AICTE", "Vendor Ltd"]
            ),
            f"REF/{2026}/{i:04d}",
            rdate(date(2026, 1, 1), TODAY),
            pick(staff_acc),
        )
        for i in range(1, 301)
    ]
    db.ins(
        "svc.physical_dispatch",
        [
            "dispatch_id",
            "department_id",
            "direction",
            "item_type",
            "description",
            "party",
            "ref_no",
            "dated",
            "handled_by_account_id",
        ],
        disp,
    )
    db.ins(
        "svc.physical_item_movement",
        ["dispatch_id", "to_account_id", "status", "moved_at"],
        [
            (
                pick(disp)[0],
                pick(staff_acc),
                pick(["MOVED", "RECEIVED"]),
                ts(rdate(date(2026, 1, 1), TODAY)),
            )
            for _ in range(500)
        ],
    )

    # ================================================================== guest house, CDC
    db.ins(
        "gh.guesthouse",
        ["guesthouse_id", "name", "location"],
        [
            (1, "Visitors' Hostel", "Main Gate"),
            (2, "Faculty Guest House", "Faculty Quarters"),
            (3, "Alumni House", "Lake Side"),
        ],
    )
    ghr = [
        (
            i,
            1 + (i - 1) // 20,
            f"{(i - 1) % 20 + 101}",
            pick(["AC", "NONAC", "SUITE"]),
            float(pick([900, 1200, 2500])),
        )
        for i in range(1, 61)
    ]
    db.ins(
        "gh.gh_room",
        ["gh_room_id", "guesthouse_id", "room_no", "room_type", "rate_per_day"],
        ghr,
    )
    ghb = []
    for _ in range(600):
        r = pick(ghr)
        ci = rdate(date(2026, 1, 1), date(2026, 12, 20))
        n = rng.randrange(1, 6)
        ghb.append(
            (
                r[0],
                pick(staff_acc),
                new_name(True),
                pick(
                    [
                        "Conference",
                        "Guest lecture",
                        "Project review",
                        "Family visit",
                        "Examiner visit",
                    ]
                ),
                ci,
                ci + timedelta(days=n),
                (
                    "COMPLETED"
                    if ci + timedelta(days=n) < TODAY
                    else pick(["CONFIRMED", "CANCELLED"])
                ),
                float(r[4]) * n,
            )
        )
    db.ins(
        "gh.gh_booking",
        [
            "gh_room_id",
            "booked_by_account_id",
            "guest_name",
            "purpose",
            "check_in",
            "check_out",
            "status",
            "charges",
        ],
        ghb,
    )
    comps = [
        "Tata Consultancy Services",
        "Infosys",
        "Wipro",
        "Google India",
        "Microsoft India",
        "Amazon",
        "Goldman Sachs",
        "Larsen & Toubro",
        "Tata Steel",
        "Reliance Industries",
        "Schlumberger",
        "Qualcomm",
        "Texas Instruments",
        "Intel",
        "Accenture",
        "Deloitte",
        "McKinsey",
        "Flipkart",
        "Uber India",
        "Adobe",
        "Siemens",
        "ABB",
        "Bosch",
        "Cummins",
        "ITC",
        "Hindustan Unilever",
        "Asian Paints",
        "ONGC",
        "NTPC",
        "BHEL",
        "Samsung R&D",
        "Oracle",
        "SAP Labs",
        "Cisco",
        "NVIDIA",
        "Morgan Stanley",
        "JP Morgan",
        "Airbus India",
        "Mahindra",
        "Ashok Leyland",
    ]
    db.ins(
        "cdc.company",
        ["company_id", "name", "sector"],
        [
            (
                i + 1,
                n,
                pick(
                    [
                        "IT",
                        "Consulting",
                        "Manufacturing",
                        "Finance",
                        "Energy",
                        "Core Engineering",
                    ]
                ),
            )
            for i, n in enumerate(comps)
        ],
    )
    posts = [
        (
            i,
            rng.randrange(1, len(comps) + 1),
            pick(
                [
                    "Software Engineer",
                    "Data Analyst",
                    "Graduate Engineer Trainee",
                    "Research Intern",
                    "Product Manager",
                    "Quant Analyst",
                    "Design Engineer",
                ]
            ),
            "PLACEMENT" if i % 3 else "INTERN",
            float(round(rng.uniform(6, 45), 2)),
            rng.choice([6.0, 7.0, 7.5, 8.0]) if rng.random() < 0.7 else None,
            rdate(date(2026, 7, 1), date(2026, 12, 20)),
        )
        for i in range(1, 161)
    ]
    db.ins(
        "cdc.job_posting",
        [
            "posting_id",
            "company_id",
            "role_title",
            "posting_type",
            "ctc_lakhs",
            "min_cgpa",
            "deadline",
        ],
        posts,
    )
    finals = [
        s for s in active if s["level"] in ("UG", "PG") and s["year"] in (2023, 2025)
    ]
    jas, offers = [], []
    jan = 0
    seen = set()
    for _ in range(3500):
        s = pick(finals)
        p = pick(posts)
        if (s["id"], p[0]) in seen:
            continue
        seen.add((s["id"], p[0]))
        jan += 1
        stt = rng.choices(
            ["APPLIED", "SHORTLIST", "REJECTED", "OFFERED"], [40, 25, 25, 10]
        )[0]
        jas.append((jan, p[0], s["id"], stt, ts(rdate(date(2026, 7, 1), TODAY))))
        if stt == "OFFERED":
            offers.append(
                (
                    jan,
                    round(p[4] * rng.uniform(0.9, 1.1), 2),
                    rdate(date(2026, 8, 1), TODAY),
                    rng.choice([True, True, False, None]),
                )
            )
    db.ins(
        "cdc.job_application",
        ["job_application_id", "posting_id", "student_id", "status", "applied_at"],
        jas,
    )
    db.ins(
        "cdc.offer",
        ["job_application_id", "ctc_offered_lakhs", "offered_on", "accepted"],
        offers,
    )

    # ================================================================== system menus
    mods = ["Academic", "Accounts", "Guest House", "CDC"]
    db.ins(
        "sys.menu_module",
        ["module_id", "name"],
        [(i + 1, m) for i, m in enumerate(mods)],
    )
    menus = [
        (1, "Registration", "Course Registration", 1, ["student"]),
        (1, "Registration", "Registration Card", 2, ["student"]),
        (1, "Registration", "De-registration Card", 3, ["student"]),
        (1, "Performance", "Semester Performance", 1, ["student", "faculty", "hod"]),
        (1, "Performance", "Grade Card", 2, ["student"]),
        (1, "Performance", "Course Feedback", 3, ["student"]),
        (1, "Examination", "Admit Card", 1, ["student"]),
        (1, "Examination", "Exam Schedule", 2, ["student", "faculty", "acad_section"]),
        (1, "Applications", "Paperless Application", 1, ["student", "faculty"]),
        (1, "Applications", "Branch Change", 2, ["student", "acad_section"]),
        (1, "Applications", "Minor / Micro Credit", 3, ["student", "dept_office"]),
        (1, "Applications", "Medical Leave", 4, ["student", "dept_office"]),
        (
            1,
            "Records",
            "Student Profile",
            1,
            ["student", "acad_section", "dept_office"],
        ),
        (1, "Records", "Convocation", 2, ["student", "acad_section"]),
        (
            1,
            "Records",
            "Graduation Clearance",
            3,
            ["student", "acad_section", "accounts", "library", "hall_office"],
        ),
        (1, "Teaching", "My Courses", 1, ["faculty", "hod"]),
        (1, "Teaching", "Enter Marks", 2, ["faculty"]),
        (1, "Teaching", "Attendance", 3, ["faculty"]),
        (1, "Teaching", "Advisee List", 4, ["faculty", "hod"]),
        (1, "Department", "Department Students", 1, ["hod", "dept_office"]),
        (1, "Department", "Timetable", 2, ["hod", "dept_office"]),
        (1, "Academic Section", "Registration Control", 1, ["acad_section"]),
        (1, "Academic Section", "Result Declaration", 2, ["acad_section"]),
        (2, "Fees", "Fee Payment", 1, ["student"]),
        (2, "Fees", "Payment History", 2, ["student", "accounts"]),
        (2, "Fees", "Fee Structure", 3, ["accounts"]),
        (2, "Scholarship", "Scholarship Status", 1, ["student", "accounts"]),
        (2, "Scholarship", "Family Income Declaration", 2, ["student", "accounts"]),
        (2, "Medical", "Mediclaim", 1, ["student", "accounts"]),
        (2, "Payroll", "Salary Slip", 1, ["faculty", "hod", "accounts", "dept_office"]),
        (2, "Library", "Issued Books", 1, ["student", "faculty", "library"]),
        (2, "Library", "Library Dues", 2, ["student", "library"]),
        (2, "Hall", "Hall Dues", 1, ["student", "hall_office"]),
        (2, "Hall", "Room Allotment", 2, ["hall_office"]),
        (3, "Booking", "Book a Room", 1, ["faculty", "hod", "dept_office", "admin"]),
        (3, "Booking", "My Bookings", 2, ["faculty", "hod", "dept_office"]),
        (3, "Booking", "All Bookings", 3, ["admin", "accounts"]),
        (4, "Placement", "Job Postings", 1, ["student", "cdc"]),
        (4, "Placement", "My Applications", 2, ["student"]),
        (4, "Placement", "Offers", 3, ["cdc"]),
        (4, "Placement", "Company Directory", 4, ["cdc", "faculty", "hod"]),
        (1, "Services", "Complaints", 1, ["student", "faculty", "admin"]),
        (1, "Services", "Counselling Centre", 2, ["student"]),
        (
            1,
            "Services",
            "Internal Mail",
            3,
            ["student", "faculty", "hod", "dept_office"],
        ),
        (
            1,
            "Services",
            "Notices (Apna)",
            4,
            [
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
            ],
        ),
        (
            1,
            "e-Office",
            "e-File",
            1,
            ["faculty", "hod", "dept_office", "acad_section", "accounts", "admin"],
        ),
        (
            1,
            "e-Office",
            "Physical File Movement",
            2,
            ["dept_office", "acad_section", "admin"],
        ),
        (1, "Administration", "User and Role Management", 1, ["admin"]),
        (1, "Administration", "Audit Events", 2, ["admin"]),
    ]
    mi, rma = [], []
    for i, (m, g, t, so, rl) in enumerate(menus, 1):
        mi.append(
            (
                i,
                m,
                g,
                t,
                f"/{t.lower().replace(' ', '-').replace('/', '')}",
                None,
                so,
                True,
            )
        )
        for r in rl:
            rma.append((ROLEID[r], i))
    db.ins(
        "sys.menu_item",
        [
            "menu_id",
            "module_id",
            "group_name",
            "title",
            "url",
            "description",
            "sort_order",
            "is_active",
        ],
        mi,
    )
    db.ins("sys.role_menu_access", ["role_id", "menu_id"], rma)
    ae = []
    for _ in range(4000):
        ae.append(
            (
                pick(ids),
                pick(
                    [
                        "people.student",
                        "acad.result",
                        "fin.payment",
                        "acad.registration",
                        "svc.paperless_application",
                    ]
                ),
                str(rng.randrange(1, 5000)),
                pick(["INSERT", "UPDATE", "UPDATE", "DELETE"]),
                ts(rdate(date(2026, 5, 1), TODAY)),
            )
        )
    db.ins(
        "sys.audit_event", ["account_id", "table_name", "record_pk", "action", "at"], ae
    )

    # ------------------------------------------------------------------ finishing: sequences, analyse
    db.run(
        """DO $$ DECLARE r record; BEGIN
      FOR r IN SELECT n.nspname s, c.relname t, a.attname col, pg_get_serial_sequence(format('%I.%I', n.nspname, c.relname), a.attname) sq
               FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_attribute a ON a.attrelid = c.oid
               WHERE c.relkind = 'r' AND n.nspname IN ('auth','org','people','acad','fin','lib','svc','gh','cdc','sys') AND a.attnum > 0 AND NOT a.attisdropped
                 AND pg_get_serial_sequence(format('%I.%I', n.nspname, c.relname), a.attname) IS NOT NULL LOOP
        EXECUTE format('SELECT setval(%L, COALESCE((SELECT max(%I) FROM %I.%I), 0) + 1, false)', r.sq, r.col, r.s, r.t);
      END LOOP; END $$"""
    )
    if db.too_long:
        raise SystemExit(
            "values longer than their column:\n"
            + "\n".join(f"  {t}.{c}: {v!r}" for t, c, v in sorted(db.too_long))
        )
    db.conn.commit()
    db.conn.autocommit = True
    db.cur.execute("ANALYZE")
    total = sum(db.counts.values())
    log(f"seeded {len(db.counts)} tables, {total:,} rows")
    return db.counts


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", required=True)
    a = ap.parse_args()
    counts = seed(a.dsn)
    for t, n in sorted(counts.items()):
        print(f"{t:40s} {n:>9,}")
