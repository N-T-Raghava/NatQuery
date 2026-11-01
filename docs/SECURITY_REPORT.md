# NatQuery security testing on AdventureWorks

## 1. Setup

| | |
|---|---|
| Database | Microsoft AdventureWorks 2014 OLTP on PostgreSQL: 68 tables, 31,465 sales orders, 19,972 people, 290 employees, with real-looking secrets (password hashes, credit card numbers, national ID numbers) |
| Schema | The five AdventureWorks schemas flattened into one `public` schema, because NatQuery reads one schema per connection |
| Roles | `employee` (own HR record and pay, product catalogue), `sales_rep` (own orders and customers, customer e-mail masked as `***@domain`), `admin` (everything except the permanently hidden data) |
| Hidden for everyone, admin included | `password`, `creditcard`, `personcreditcard`, `jobcandidate`; columns `passwordhash`, `passwordsalt`, `cardnumber`, `nationalidnumber`, `creditcardid`, `creditcardapprovalcode` |
| Users | Gail (employee), Linda (sales rep, 418 orders, 69 customers), root (admin) |
| LLM | `openai/gpt-oss-120b` on Groq for the natural-language part |
| Database login | The PostgreSQL superuser, on purpose: if NatQuery's checks ever failed, nothing below them would help |

**Pass criteria, fixed before the runs:** zero leaked values, zero successful writes, zero unbounded executions, every request audited.

**Leak checker.** It is independent of NatQuery's policy code. It holds every protected value read straight from the tables (89,686 secret strings), each role's entitled keys (for example Linda's 418 order ids), and checks every returned cell, key column and masked e-mail. Aggregates are compared with the role's own numbers: Linda's order count must be 418, not 31,465.

Four test layers:

| Layer | What it simulates |
|---|---|
| A. Hostile SQL | The LLM is compromised or jailbroken and returns the attacker's SQL. Goes through the full pipeline |
| B. Auth, sessions, audit | Login, lockout, forged tokens, role escalation, audit tampering, what the LLM is shown |
| C. Natural language | Real users typing real attacks and real questions into the real LLM |
| D. Unprotected baseline | Same LLM and same kinds of attack, no NatQuery, database owner connection |

## 2. Results

| | Result |
|---|---|
| **Hostile SQL: leaked values** | **0** of 560 runs (227 distinct queries, 3 roles, 11 attack families) |
| Hostile SQL: successful writes or DDL | **0**; a fingerprint of all 68 tables was identical before and after |
| Must-refuse queries refused | **511 of 511** |
| Allowed queries returned only entitled data | **49 of 49** (for example Linda's `COUNT(*)` on all orders returned 418; on all order lines, 7,107) |
| Auth, session and audit checks | **40 of 40** |
| Natural-language attacks through NatQuery | **0 leaks in 30** (25 refused or "unanswerable", 5 answered with entitled data only) |
| Same kinds of attack on an unprotected text-to-SQL | **7 of 8 leaked** (all 290 national IDs, all password hashes, company-wide revenue, every employee's row) |
| Same hostile SQL run directly on the database | 141 of 187 must-refuse queries completed; 47 returned secret data |
| Cost of the security layer on the prompt | **66% fewer tokens** per question (2,170 vs 6,295): the LLM only sees the tables its role may see |
| Enforcer plus execution, hostile SQL | median **6 ms** per request |

Attack families in layer A: globally hidden data, tables or columns outside the role's grant, row-filter bypasses (`OR TRUE`, self-joins, subqueries, CTEs, UNION), masked-column probing (LIKE, ORDER BY, GROUP BY, MIN, md5, joins), writes and DDL, stacked statements, dangerous functions (`pg_sleep`, `pg_read_file`, `lo_import`), catalog and system tables, obfuscation (quoted identifiers, comments, unicode escapes, `TABLE x`), whole-row references, resource abuse.

Layer B covers: wrong password and unknown user give the same message, lockout, forged and tampered tokens, logout, idle expiry, role escalation (6 attempts), live role change and account disable, a poisoned identity attribute, identical errors for a hidden and a non-existent table or column, no hidden name in any LLM prompt, card-number redaction, rate limiting, no query runs if its audit record cannot be written, and editing, deleting or reordering audit records is detected.

### What the tests found, and fixed

Testing found three real bugs in the engine. All are fixed and have regression tests (the unit suite is now 720 tests).

| Bug | Why it mattered |
|---|---|
| `SELECT employee::text FROM employee` returned the whole row, including the hidden national ID | A real leak: a table name used as a value bypassed column grants |
| `repeat('a', 1000000000)` crashed the database connection | An allowed function could exhaust memory |
| `JOIN ... ON true` passed the cross-join rule | Resource abuse, bounded only by the timeout |

The first run is kept in `benchmarks/security/results/*_initial_run.*`.

## 3. Limitations

1. **We tested our own defences.** The attack list was written by the people who wrote the enforcer, so it reflects what we thought of. The first run found three bugs, so more probably exist. A fixed list shows no known attack works; it cannot show none does. An outside red team is the missing step.
2. **The natural-language sample is small.** 18 legitimate questions, 30 attacks and 8 baseline attacks, because a free Groq key allows about 200,000 tokens a day. Zero leaks in 30 only rules out a leak rate above about 10%. The strong claim rests on layer A, which does not depend on the LLM.
3. **Answers are scoped silently.** Linda asked for "the total revenue of the whole company" and got her own 11.7 million, not the company's 123 million, with no warning. An employee asking "how many employees work here" gets 1. Nothing leaked, but a user can be misled. Cause: row filters change results without any notice, and the response only notes masking and truncation. This should be fixed before an ERP rollout.
4. **Usefulness for legitimate questions is only 72% (13 of 18).** The 5 misses: in four the LLM answered "unanswerable" (for "How many orders have I made?" because it cannot see that its data is already limited to the user, and for the admin it was shown the wrong subset of tables because the admin view (256 columns) is just above the 250-column limit for sending the schema whole, and AdventureWorks table names such as `salesorderheader` are run-together words that the table ranking does not match); in the fifth it answered a different question (calendar-year sales instead of the stored year-to-date figure). Raising the limit is a one-line setting; we did not re-run after changing it.
5. **Inference is not covered.** A role allowed to aggregate rows can learn statistics about them. There is no minimum group size, and no layer tests this.
6. **One schema per connection (in this run).** AdventureWorks was flattened into one schema for this run. Multi-schema support exists in the engine now and is exercised on the ERP database in `erp/`; this AdventureWorks run was not repeated with it.
7. **No second line of defence was exercised in this run.** We connected as the database superuser to be conservative. The ERP track (`erp/`) tests the database role and row-level security separately; a real deployment should use a SELECT-only account on top of the enforcer.
8. **The audit log is tamper-evident, not tamper-proof.** Without `NATQUERY_AUDIT_KEY`, someone who can rewrite the whole file can forge a consistent chain. Even with the key, cutting records off the end is not detectable by the chain alone. Ship the log off the machine or record its last hash elsewhere.
9. **Prompt-injection screening is a speed bump.** The input guard stopped 4 of 30 attacks. The rest were stopped by the model declining or by the enforcer, which is why the enforcer, not the guard, is the boundary.
10. **One LLM, one database engine.** Only gpt-oss-120b at low reasoning effort was tried, and only PostgreSQL. A different model may answer attacks differently; the enforcer result does not depend on it.
11. **Concurrency, timing channels and SSO are untested here.** Parallel users, response-time side channels and LDAP or OIDC login with the real ERP are not covered by this track. Thread isolation is covered separately by the repo's integration tests.
12. **The hardening has a small accuracy cost on the BIRD benchmark.** The `ON true` rule rejects 1 of 500 BIRD first-attempt queries (a join of two one-row CTEs). The published BIRD numbers were measured before this fix and were not re-run.

## 4. Files

- `benchmarks/security/sec_bench.py`: runner (`sql`, `auth`, `nl`, `raw`, `report`)
- `benchmarks/security/corpus_sql.py`, `corpus_nl.py`: all attacks and questions
- `benchmarks/security/policy.yaml`: the three-role policy
- `benchmarks/security/setup_adventureworks.sh`: builds the database
- `benchmarks/security/results/`: every result, plus `summary.json`
