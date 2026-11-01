# Changelog

All notable changes to NatQuery are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [2.0.0] - 2026-11-01

NatQuery 2.0.0 turns the 1.x prototype into a governed, auditable natural-language
interface for PostgreSQL. The LLM only ever *proposes* SQL; a deterministic enforcement
layer decides whether it runs.

### Added
- **SQL enforcer** (sqlglot AST based): single read-only `SELECT`, allow-listed tables and
  columns, blocked functions and system catalogs, forced `LIMIT`, statement timeout.
- **Role policy engine** with YAML/JSON policies: per-role table/column access, row filters,
  column masks and abstract (inheritable) roles; `natquery policy init` scaffolds examples.
- **Multi-schema mode** and **PostgreSQL Row-Level Security** integration via
  `db_session_vars` and `SET LOCAL ROLE`, so the database enforces what the policy declares.
- **Hash-chained audit log** (tamper-evident, `verify()`), secret scrubbing in logs,
  private (0600/0700) config and workspace files written atomically.
- **Pluggable authentication** (local user store with hashed passwords and strength
  checks; extra providers through the `natquery.auth_providers` entry-point group).
- **Schema intelligence**: weighted table ranking, wider-schema retry, value hints
  (`natquery schema hints`) and per-column semantics merged into the prompt.
- **Benchmarks**: BIRD dev evaluation harness (`benchmarks/bird`), 5-case security suite
  (`benchmarks/security`) and a 40-question ERP benchmark (`erp/`), with results.
- **ERP test bed**: 87 tables / 549 columns / 10 roles / ~368k rows reproducible generator.
- **Documentation**: `docs/ARCHITECTURE.md`, `docs/BIRD_REPORT.md`, `docs/SECURITY_REPORT.md`
  and the full project report in LaTeX (`report/`).
- 855 tests (unit + PostgreSQL integration), >= 90 % coverage gate.

### Changed
- Project layout, README and packaging rewritten; the wheel now ships only `natquery`
  (benchmarks and ERP assets live in the repository, not on PyPI).
- CI and release workflows aligned: same lint/format/test gates, tag-vs-version check,
  release notes generated from this changelog.
- Code formatted with black 25.1.0 and linted with ruff 0.15.6 across the whole repository.

### Results
- BIRD dev (500 questions): official EX 45.6 %, tolerant scorer 50.6 %.
- ERP 40-question benchmark: 28/40 (70 %), 0 data leaks across 5 adversarial prompts.

### Notes
- Version `2.0.0` is the first release with the governance layer; configuration written by
  1.x remains readable, but policies are new and must be created with `natquery policy init`.

---

---

## [1.0.1] - 2026-04-14

### Added
- Integration tests for query pipeline
- Improved validation coverage

### Internal
- Minor bug fixes in the testing and stability improvements

---


## [1.0.0] - 2026-04-13

### Core Features
- Natural language to SQL conversion via LLM
- Interactive CLI for querying databases
- PostgreSQL query execution engine
- End-to-end query pipeline

### Intelligence Layer
- Dynamic schema extraction
- Structured prompt builder for LLM
- Self-correction mechanism for failed SQL queries

### Security & Validation
- SQL validation (SELECT-only enforcement)
- LIMIT enforcement for safe query execution
- Protection against unsafe query patterns

### Observability & Logging
- Structured logging system
- Query tracking with execution time and results

### Performance & Optimization
- EXPLAIN ANALYZE integration
- Cost analyzer for PostgreSQL execution plans
- Index recommender for query optimization
- Performance API for tracking and comparing query runs
