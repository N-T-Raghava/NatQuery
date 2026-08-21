# NatQuery

[![PyPI Version](https://img.shields.io/pypi/v/natquery)](https://pypi.org/project/natquery/)
[![PyPI Downloads](https://img.shields.io/pepy/dt/natquery)](https://pypi.org/project/natquery/)
[![codecov](https://codecov.io/gh/N-T-Raghava/natquery/graph/badge.svg?token=KZIEXD9ALC)](https://codecov.io/gh/N-T-Raghava/natquery)
[![Type Checked](https://img.shields.io/badge/mypy-checked-blue)](https://github.com/python/mypy)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-000000)](https://github.com/psf/black)
[![license](https://img.shields.io/github/license/mashape/apistatus.svg)]((https://img.shields.io/github/license/mashape/apistatus.svg))

natquery is a Python package that enables users to interact with relational databases using natural language via a secure, performance-aware command-line interface along with database driven machine learning analytics.

```
natquery/
│
├── pyproject.toml
├── README.md
├── requirements.txt
│── src/
├   ├── natquery/
│       ├── __init__.py
│
│       # =========================
│       # 1. CLI LAYER
│       # =========================
│       ├── cli/
│       │   ├── __init__.py
│       │   ├── main.py              # Entry point
│       │   ├── shell.py             # Interactive REPL
│       │   ├── commands.py          # CLI command registry
│
│       # =========================
│       # 2. CONFIG LAYER
│       # =========================
│       ├── config/
│       │   ├── __init__.py
│       │   ├── settings.py          # API keys and DB config
│       │   ├── connection.py        # PostgreSQL connection manager
│
│       # =========================
│       # 3. ORCHESTRATION LAYER
│       # =========================
│       ├── orchestration/
│       │   ├── __init__.py
│       │   ├── pipeline.py          # End-to-end coordinator
│       │   ├── error_handler.py
│
│       # =========================
│       # 4. SCHEMA MODULE
│       # =========================
│       ├── schema/
│       │   ├── __init__.py
│       │   ├── extractor.py         # Dynamic schema extraction
│       │   ├── formatter.py         # Convert schema to prompt-friendly text
│
│       # =========================
│       # 5. PROMPT BUILDER
│       # =========================
│       ├── prompt/
│       │   ├── __init__.py
│       │   ├── builder.py           # Structured prompt construction
│
│       # =========================
│       # 6. LLM CLIENT
│       # =========================
│       ├── llm/
│       │   ├── __init__.py
│       │   ├── client.py            # LLM API calls
│       │   ├── self_corrector.py    # Retry & correction mechanism
│
│       # =========================
│       # 7. SECURITY + VALIDATION
│       # =========================
│       ├── security/
│       │   ├── __init__.py
│       │   ├── validator.py         # SELECT-only enforcement
│       │   ├── limiter.py           # LIMIT enforcement
│
│       # =========================
│       # 8. EXECUTION ENGINE
│       # =========================
│       ├── execution/
│       │   ├── __init__.py
│       │   ├── engine.py            # SQL execution
│       │   ├── explain.py           # EXPLAIN / ANALYZE
│
│       # =========================
│       # 9. MACHINE LEARNING
│       # =========================
│       ├── ml/
│       │   ├── __init__.py
│       │   ├── preprocessing.py
│       │   ├── trainer.py
│       │   ├── inference.py
│       │
│       # =========================
│       # 10. PERFORMANCE + LOGGING
│       # =========================
│       ├── observability/
│          ├── __init__.py
│          ├── logger.py
│          ├── cost_analyzer.py
│          ├── index_recommender.py
│
│
├── database/
│   ├── schema.sql
│   ├── seed.py
│   ├── synthetic_data.py
│
├── tests/
│   ├── test_pipeline.py
│   ├── test_security.py
│   ├── test_ml.py
│
└── benchmarks/
    ├── nl_queries.json
