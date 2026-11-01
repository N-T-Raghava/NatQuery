# NatQuery - architecture, security model and known limits

## Request path
```
question ──► session check (identity re-read from provider every request)
          ──► role chosen (highest priority, or the one the user selected; never a union)
          ──► role-filtered schema view  ──► LLM (sees ONLY this view)
          ──► SQL enforcer (parse → allow-list → qualify against the view → rewrite → re-serialise)
          ──► audit record "authorized"  (if this write fails, nothing executes)
          ──► read-only, time- and row-bounded execution (pooled, always rolled back)
          ──► result guard (pattern redaction, blob/size caps) ──► audit "completed" ──► answer
```
The text the LLM wrote is never executed; only SQL regenerated from the validated syntax tree is.

## Where each concern lives (and how to extend it)
| Concern | Module | To extend |
|---|---|---|
| Who are you? | `auth/` – `AuthProvider` + registry; built-in `local` (scrypt, lockout) | implement `AuthProvider`, `register_provider(...)` or ship an entry point in group `natquery.auth_providers` (LDAP, OIDC/SSO, ERP session...) |
| What may a role see? | `access/policy.py` + `policy.yaml` | add a role = add a block; no code. Roles are plain strings |
| Which rows? | `row_filter` with `:attr` placeholders bound from `Identity.attributes` | providers decide what attributes exist (roll_no, emp_id, dept ...) |
| Which columns / masking? | `columns`, `deny_columns`, `masked` (named templates), `global_deny` | add templates under `masking:` |
| SQL safety | `access/enforcer.py`, `access/functions.py` | extra functions via `allowed_functions:` |
| Database | `backends/` – `QueryBackend` ABC; Postgres implemented | new class + sqlglot dialect name (e.g. Oracle) |
| Several schemas | `db_schemas` in config (list); table keys become `schema.table`; grants accept `schema.*` | see "Multi-schema mode" below |
| Database-side row security | `db_session_vars` in the policy; per-role `db_role` | see "Row-level security" below |
| LLM | `llm/base.py` – `LLMClient`; Groq implemented | implement `complete`, `register_llm` |
| Input screening / rate limits / sessions | `security/input_guard.py`, `security/rate_limit.py`, `auth/session.py` | swap ABC implementations (Redis etc.) |
| Evidence | `audit/log.py` – hash-chained JSONL (`NATQUERY_AUDIT_KEY` makes it an HMAC) | ship the file off-box |

## Guarantees the code enforces
* Hidden tables/columns are absent from the LLM prompt AND fail to resolve in the enforcer.
* Row filters wrap every table reference (joins, subqueries, CTEs, UNION) as subqueries; attribute values are validated and bound as literals.
* Masked columns can only be selected; any filter/sort/group/join/aggregate on them is refused (no oracle attacks).
* Only SELECT/set-operations; no INTO, locks, recursion, LATERAL, table functions, bind params, catalog casts; functions are allow-listed.
* Execution: read-only transaction, `statement_timeout`, row cap enforced in SQL and at fetch, never commit, optional `SET LOCAL ROLE`.
* Users see only generic messages + an opaque reference id; details go to the audit log. DB errors are sanitised before reaching the LLM.
* Secrets are redacted from every log; config/audit/user files are 0600 in 0700 directories (POSIX).

## Multi-schema mode
Set `db_schemas: ["people", "acad", ...]` in the connection config (default `["public"]`). With one schema nothing changes. With
several, every table is named `schema.table` everywhere: in the prompt, in `policy.yaml` (`people.student`, or `fin.*` for a whole
schema), in `global_deny` (`auth.*`) and in the enforcer. A bare table name that exists in exactly one visible schema is
completed automatically; one that exists in several is sent back to the LLM as fixable ("write schema.table"). A table from a
schema that is not listed is blocked as before. Schema extraction reads `pg_catalog`, so the connecting login does not need
any table privileges to be introspected.

## Row-level security (second line of defence)
`db_session_vars` maps database settings to identity attributes, for example
`erp.role: "@role"`, `erp.student_id: student_id`. They are sent with every query as transaction-local settings
(`set_config(name, value, true)`) before `SET LOCAL ROLE <db_role>`, so PostgreSQL policies can read `current_setting('erp.student_id')`.
The enforcer's row filters and the database's policies then say the same thing twice; if the enforcer ever had a bug, the
database still limits the rows. Column scope is not duplicated in the database: the DB role holds SELECT on the columns a
role-group may ever need, and the enforcer narrows it. See `erp/sql/04_roles_rls.sql` for a complete example.

## Known limits - read before connecting to real data
1. **The policy is only as good as its author.** Row filters and column grants must be written against the real ERP schema; run `natquery policy check` and review each role's view.
2. **Real IIT KGP ERP database**: unknown here. If it is not PostgreSQL a new `QueryBackend` is needed. Always use a dedicated SELECT-only DB account; also consider DB-side row-level security as a second layer.
3. **Data leaves the machine in two ways**: the question text and the role-visible *schema* (table/column names, descriptions) go to the LLM provider. Row data does not. Check institutional policy; the provider is pluggable (on-prem model).
4. **Inference/aggregation**: a role allowed to aggregate rows can learn statistics about them; there is no minimum-group-size rule yet.
5. **Prompt-injection screening is heuristic** (speed bump + audit signal). The enforcer is the boundary.
6. **Per-username lockout** can be abused to lock someone out. Add per-IP/device throttling when moving to a network API.
7. Sessions and rate limits are in-memory (single process). The audit chain is tamper-evident, not tamper-proof.
8. On Windows, `chmod` does not restrict access; rely on the user-profile ACLs or use env vars for secrets.
9. One role is active per query (no union semantics), by design: simpler to reason about and audit.
10. **Answers are scoped silently.** A row filter changes what a query returns without telling the user: a sales rep asking for "the company's total revenue" gets their own total, and an employee asking "how many employees work here" gets 1. Nothing leaks, but the answer can mislead. The LLM cannot see row filters, so questions phrased as "my ..." are sometimes judged unanswerable. See SECURITY_REPORT.md.
11. Multi-schema mode needs `schema.table` names in the policy; a table name that exists in several schemas must be written in full by the LLM, which costs repair attempts on large ERP schemas.
12. Row-level security in the database covers rows only. Column hiding and masking exist in the enforcer alone, except for the secret columns that the database role is never granted.


## Testing and release checks

* `pytest` runs ~850 tests (unit and, with a database, integration) with a 90% branch-coverage gate (currently ~98%, nothing omitted).
* `pytest -m integration` (needs `NATQUERY_TEST_PG_*`) re-runs the enforcer corpus and the full pipeline against a real
  PostgreSQL, including write attempts that bypass the enforcer, statement timeouts, `SET LOCAL ROLE` and 40-thread isolation.
* `tests/unit/test_enforcer_properties.py` fuzzes the enforcer with Hypothesis; its invariants are documented in the file.
* sqlglot is a security-relevant dependency (the enforcer keys on its node classes). The declared range
  (`>=25.34,<31`) is the range the full suite was run against; widen it only after re-running the suite.
* Manual mutation testing (sabotaging one security rule at a time) was used to prove the suite goes red when a rule is
  removed: 96 of 97 mutants were killed; the survivor is an equivalent mutant (a defensive duplicate of sqlglot's own
  column validation).
