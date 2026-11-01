#!/usr/bin/env python3
"""Add example values of categorical columns to natquery/semantics.json (run after make_policy.py).

    PGPORT=... PGUSER=... python make_value_hints.py

Generic: uses natquery.schema.value_hints over the live schema, no question-specific input.
Secret columns (password hashes, tokens, OTPs ...) are skipped by name inside the library.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "bench"))
import erp_lib as L  # noqa: E402  (also puts natquery on sys.path)

from natquery.backends.postgres import PostgresBackend  # noqa: E402
from natquery.schema.value_hints import collect_value_hints  # noqa: E402

path = L.ERP / "natquery" / "semantics.json"
sem = json.loads(path.read_text())
schema = PostgresBackend(
    {"type": "params", **L.NQ, "schemas": L.SCHEMAS}, max_conn=2
).extract_schema()
hints = collect_value_hints(lambda sql: L.sq(sql), schema)
for t in sem["tables"].values():
    t.pop("value_hints", None)
for t, h in hints.items():
    sem["tables"].setdefault(t, {})["value_hints"] = h
path.write_text(json.dumps(sem, indent=1))
print(sum(len(h) for h in hints.values()), "columns in", len(hints), "tables")
