# ERP test bed

A synthetic university ERP in PostgreSQL, built to test NatQuery on a database shaped like the real one: ten schemas, 87 tables,
549 columns, 130 foreign keys, about 368,000 rows, ten roles, logins in SQL, and row-level security. Everything is generated;
nothing comes from a real ERP. The field names follow the student-login screens and are extended to the teacher and
administrative sides.

| Item | Result |
|---|---|
| Database checks (`test_erp.py`): logins, lockout, OTP, sessions, grants, row-level security against an independent oracle | **51 of 51** |
| NatQuery dry run (`bench/erp_bench.py gold`): every ground-truth query and every attack sent through the full pipeline with no LLM | **41 of 41**: all 35 answerable questions return exactly the expected rows; all 6 attack queries return nothing forbidden |
| LLM run (`bench/erp_bench.py nl`, gpt-oss-120b on Groq free tier) | **28 of 40 correct (70%)**: easy 9/11, medium 11/17, hard 8/12; 23 of 35 answerable questions; 5 of 5 security questions safe, 0 leaks. Results in `results/` |

## Build

```bash
pip install -e ".[dev]" psycopg2-binary pyyaml
python erp/build_erp.py --host 127.0.0.1 --port 5432 --user postgres --password ...   # about 70 seconds
python erp/test_erp.py  --port 5432 --superuser-password ...                           # 51 database checks
```

`build_erp.py` needs a PostgreSQL superuser. It creates the roles `erp_owner`, `erp_app`, `erp_ro` and the login `natquery`
(passwords from `ERP_APP_PW` and `NATQUERY_PW`, demo values otherwise), the database `erp`, the tables, the auth functions, the
data, the grants and the row-level security. Re-running it rebuilds from scratch with identical data.

## Files

| File | What it is |
|---|---|
| `erp_schema.dsl` | the model: one line per table and column; the single source of the DDL, the diagrams and the policy |
| `gen_ddl.py` | checks the model (keys, types, foreign keys) and writes `sql/01_ddl.sql` and the Mermaid diagrams |
| `sql/02_auth.sql` | `auth.login`, `verify_session`, `logout`, `issue_otp`, `verify_otp`, `change_password` (bcrypt, 5-failure lockout for 15 minutes, hashed session tokens and OTPs, full audit) |
| `sql/04_roles_rls.sql` | ownership, least-privilege grants, row-level security on 20 tables, the `natquery` login |
| `seed.py` | the synthetic data (fixed seed) |
| `build_erp.py`, `test_erp.py` | build and database checks |
| `make_policy.py` | writes `natquery/policy.yaml` and `natquery/semantics.json` (11 roles, about 60 tables each, table descriptions for the prompt) |
| `bench/` | `questions.py` (40 questions with ground truth), `erp_bench.py` (runner), `erp_lib.py` (builds NatQuery on the ERP, one user per role) |
| `ERP_Benchmark_Questions.md` | the question list on its own |
| `ERP_Database_Design.html` and `diagrams/` | crow's-foot diagrams per schema, every column, the access matrix |
| `results/` | `erp_db_checks.json`, `erp_gold_dryrun.json` |

## How the layers fit

1. **NatQuery** reads all ten schemas (`db_schemas`), names tables `schema.table`, shows each role only its tables and columns,
   masks sensitive columns, adds row filters bound to the user (`student_id`, `employee_id`, `dept_id`, `hall_id`, ...) and checks every query.
2. **The database role.** NatQuery connects as `natquery`, which owns nothing and can read nothing, and runs each query after
   `SET LOCAL ROLE erp_ro`. `erp_ro` is SELECT-only and has no grant on password hashes, session tokens or OTP hashes.
3. **Row-level security** on 20 tables repeats the row scopes in SQL. NatQuery sends the role and identity with each query
   (`db_session_vars` in the policy). With no context a role sees no rows; `test_erp.py` proves this by querying as `erp_ro`
   directly, without NatQuery, for every role.

Roles: `student`, `faculty`, `hod`, `dept_office`, `acad_section`, `accounts`, `library`, `hall_office`, `cdc`, `admin`
(`erp_base` is the shared reference-data block that every role inherits, never assigned to a person).

## Test accounts

Every synthetic account has the password `Erp@<login id>` (for example `Erp@25CS10024`). Students log in with their roll number,
staff with `E` plus a four-digit code. Graduates keep an ACTIVE account whose student role has expired, and de-registered
students have DISABLED accounts. Ten people hold both a student and a staff login ("Switch Login").

## Running the LLM benchmark

```bash
export PGPORT=5432 PGPASSWORD=<superuser pw> NATQUERY_PW=natquery-demo-pw
export GROQ_API_KEY=<your key>          # or GROQ_API_KEYS=key1,key2 to rotate; never write keys to a file
cd erp/bench
python erp_bench.py gold                # no LLM, about 30 seconds: the access layer on its own
python erp_bench.py nl --gap 22         # 40 questions, resumable; about 45 minutes on a free Groq key (8,000 tokens per minute)
python erp_bench.py report              # results/erp_summary.md and .json
```

The prompt for a question is about 2,000 to 3,000 tokens (the schema is ranked and pruned because every role sees more than
250 columns; a wider retry runs when the model answers "unanswerable"), so 40 questions still fit within one free Groq key's daily allowance. `--only E01,H03` reruns single questions.

## What to expect and what it cannot show

- Answers are scoped silently: a student asking for "the department average" gets only their own rows (question H01), and a HoD's
  registration counts cover department students only (H06). Nothing leaks, but the answer can mislead. This is the known limit 10.
- Faculty and students are named only through `people.person`, whose rows are scoped, so teachers see names of their own students
  and advisees only; students see faculty names through the open `people.employee` directory.
- The data is synthetic and the questions were written by the people who wrote the policy. The real ERP has its own quirks, so
  this shows the engine handles a database of this size and shape, not that the real policy is right.
- RLS covers rows only. Column hiding and masking are the enforcer's job (limit 12 in ARCHITECTURE.md).


## Where the LLM still misses (final run)

* "My" questions on tables without a row filter (E07, M06, H06): the prompt tells the model not to filter on the user's own id, and `offering_instructor` has no row filter, so the answer covers every instructor instead of the asker.
* Refusals on departmental questions (E08, M08, M09) and one permission refusal (H05).
* Multi-step ranking and ratio questions (M10, H10, H11) and a threshold question (M03).

Value hints (`make_value_hints.py`, run after `make_policy.py`) list the distinct values of categorical columns for the model; the table picker weighs table names above column names and adds small lookup tables.
