# Changelog
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