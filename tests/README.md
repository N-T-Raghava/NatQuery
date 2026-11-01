# NatQuery test suite

```bash
pip install -e ".[dev]"
pytest                                   # unit tests + coverage gate (>= 90%, branch coverage)

# real-PostgreSQL tests (create and drop a throw-away database; need a superuser connection)
NATQUERY_TEST_PG_HOST=127.0.0.1 NATQUERY_TEST_PG_PORT=5432 \
NATQUERY_TEST_PG_USER=postgres  NATQUERY_TEST_PG_PASSWORD=... \
pytest -m integration

NATQUERY_FUZZ_EXAMPLES=5000 pytest tests/unit/test_enforcer_properties.py   # deeper fuzzing
```

| File | What it protects |
|---|---|
| `unit/test_enforcer_corpus.py`, `corpus.py` | ~130 accepted queries and ~50 attacks against the SQL enforcer (shared with the Postgres tests) |
| `unit/test_enforcer_edges.py` | scoping, limits, aliasing, function allow-list, defence in depth |
| `unit/test_enforcer_properties.py` | Hypothesis: for *any* input the enforcer refuses or returns safe, filtered, capped SQL |
| `unit/test_service_pipeline.py` | login -> role -> LLM -> enforcer -> audit -> backend, repair loop, no-leak prompts, fail-closed auditing |
| `unit/test_auth.py`, `test_audit.py` | hashing, lockout, no user enumeration, sessions, tamper-evident log, concurrency |
| `unit/test_policy_loading.py`, `test_schema_view.py` | policy parsing, inheritance, role selection, per-role schema view |
| `unit/test_postgres_backend.py` | read-only/rollback/timeout/row-cap protocol, pool back-pressure (fake psycopg2 pool) |
| `unit/test_cli_behaviour.py` | admin commands, shell, `main()` dispatch with scripted terminal input |
| `unit/test_packaging.py` | pyproject contract (version, deps, package data, sqlglot range) |
| `integration/` | everything above again on a real database, including 40-thread isolation |

The original suites (`test_sql_and_llm.py`, `test_security_and_auth.py`, `test_policy_and_schema.py`,
`test_cli_and_shell.py`, `integration/test_pipeline.py`) are kept as they were, apart from formatting
and a few blind `pytest.raises(Exception)` that now name the exception they expect.

Password hashing uses a cheap work factor inside tests (see `conftest.py`) so the suite stays fast.
