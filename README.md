# NatQuery

[![PyPI Version](https://img.shields.io/pypi/v/natquery)](https://pypi.org/project/natquery/)
[![PyPI Downloads](https://img.shields.io/pepy/dt/natquery)](https://pypi.org/project/natquery/)
[![CI](https://github.com/N-T-Raghava/NatQuery/actions/workflows/ci.yml/badge.svg)](https://github.com/N-T-Raghava/NatQuery/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/N-T-Raghava/natquery/graph/badge.svg?token=KZIEXD9ALC)](https://codecov.io/gh/N-T-Raghava/natquery)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-000000)](https://github.com/psf/black)
[![Lint: Ruff](https://img.shields.io/badge/lint-ruff-261230)](https://github.com/astral-sh/ruff)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Ask PostgreSQL questions in plain English. NatQuery turns the question into SQL with an LLM, then **never runs the LLM's SQL**: the SQL is parsed, checked against the caller's role, rewritten with row filters, masks and limits, regenerated, and only then executed.

- **Authentication and roles** (student / teacher / admin or your own), defined in a YAML policy with inheritance, column grants, masks, and row filters bound to the caller's identity.
- **Leak-resistant by construction**: hidden tables and columns are absent from the LLM prompt and fail to resolve in the enforcer.
- **Tamper-evident audit log** (hash-chained, optional HMAC).
- **Several schemas** in one connection (`schema.table`, `fin.*` grants) and **database row-level security** driven by the caller's identity.
- **Pluggable**: auth provider, database backend, LLM, rate limiter, session store, input guard.
- **Bounded repair loop**: if the enforcer rejects a fixable query, the LLM gets the (sanitised) reason and tries again, at most twice.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the security model and known limits.

## Benchmark

On BIRD Mini-Dev (PostgreSQL, 500 questions) with the open `gpt-oss-120b` model, NatQuery scores **50.6% execution accuracy** with the security layer on, at about 1.1 s per question and $0.0004 per question. Full results and limitations: [docs/BIRD_REPORT.md](docs/BIRD_REPORT.md). Reproduction: [benchmarks/bird](benchmarks/bird/README.md).

## Security testing

On AdventureWorks with three roles, 227 hostile SQL queries (560 role-by-query runs), 40 auth/audit checks and 30 natural-language attacks produced **zero leaks**; an unprotected text-to-SQL leaked on 7 of 8 comparable attacks. Details and limitations: [docs/SECURITY_REPORT.md](docs/SECURITY_REPORT.md). Reproduction: [benchmarks/security](benchmarks/security/README.md).

## ERP test bed

`erp/` holds a synthetic university ERP (10 schemas, 87 tables, about 368,000 rows, 10 roles, logins in SQL, row-level security) and a 40-question benchmark in three difficulty levels. It exercises multi-schema support (`db_schemas`, `schema.table`, `fin.*` grants) and database-side row-level security (`db_session_vars`). Database checks: 51 of 51; no-LLM dry run through the full pipeline: 41 of 41. Details: [erp/README.md](erp/README.md), questions: [erp/ERP_Benchmark_Questions.md](erp/ERP_Benchmark_Questions.md).

## Install and test

```bash
pip install -e ".[dev]"
pytest                      # ~850 tests, 90% coverage gate
natquery --help
```

## Licence

MIT. BIRD benchmark data under `benchmarks/bird/results` is CC BY-SA 4.0.

## Quick start

```bash
pip install natquery
natquery connect                 # database, schema and LLM key (kept in your workspace, never in the repo)
natquery policy init             # writes an example policy.yaml and semantics.json to edit
natquery user add alice --roles student     # see `natquery user add --help`
natquery                         # interactive shell: log in, then ask questions in English
```

## Repository layout

| Path | Content |
|---|---|
| `src/natquery/` | The library (auth, access control, enforcer, audit, backends, LLM, CLI) |
| `tests/` | Unit and integration tests |
| `benchmarks/bird/` | BIRD Mini-Dev accuracy and efficiency benchmark |
| `benchmarks/security/` | AdventureWorks security benchmark (hostile SQL, auth, natural-language attacks) |
| `benchmarks/benchmarker.py`, `benchmarks/results/` | Earlier v1 query-optimisation benchmark and its results |
| `erp/` | Synthetic university ERP test bed (87 tables, row-level security) and 40-question benchmark |
| `docs/` | Architecture and security model, BIRD report, security report |
| `report/` | LaTeX source of the B.Tech project report |
