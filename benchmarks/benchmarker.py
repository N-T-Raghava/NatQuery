"""Run all 50 NatQuery benchmark questions against PostgreSQL."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import statistics
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor

    _PSYCOPG3 = False
except ImportError:
    import psycopg
    from psycopg.rows import dict_row

    _PSYCOPG3 = True


ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent))
sys.path.insert(0, str(ROOT.parent / "src"))
QUESTIONS_PATH = ROOT / "questions" / "benchmark.json"
DEFAULT_OUTPUT = ROOT / "results"
REQUIRED_TABLES = {
    "customers",
    "categories",
    "suppliers",
    "products",
    "orders",
    "order_items",
    "payments",
    "warehouses",
    "shipments",
}


def load_questions() -> list[dict[str, Any]]:
    with QUESTIONS_PATH.open(encoding="utf-8") as handle:
        questions = json.load(handle)
    expected = {f"Q{number:03d}" for number in range(1, 51)}
    if len(questions) != 50 or {q["id"] for q in questions} != expected:
        raise ValueError("benchmark.json must contain exactly Q001 through Q050")
    return questions


def connect(dsn: str, timeout: int):
    if _PSYCOPG3:
        return psycopg.connect(dsn, connect_timeout=timeout)
    return psycopg2.connect(dsn, connect_timeout=timeout)


def check_schema(dsn: str, timeout: int) -> list[str]:
    with connect(dsn, timeout) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = ANY(%s)",
                (list(REQUIRED_TABLES),),
            )
            present = {row[0] for row in cursor.fetchall()}
    return sorted(REQUIRED_TABLES - present)


def prepare_natquery_schema(dsn: str, timeout: int) -> None:
    """Point NatQuery at this DSN and create its schema cache from PostgreSQL."""
    parsed = urlparse(dsn)
    database_name = parsed.path.lstrip("/") or "benchmark"
    with connect(dsn, timeout) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' ORDER BY table_name"
            )
            table_names = [row[0] for row in cursor.fetchall()]
            schema = {"tables": {}}
            for table_name in table_names:
                cursor.execute(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = %s "
                    "ORDER BY ordinal_position",
                    (table_name,),
                )
                columns = {row[0]: row[1] for row in cursor.fetchall()}
                schema["tables"][table_name] = {
                    "columns": columns,
                    "primary_key": [],
                    "foreign_keys": [],
                }
    schema_path = Path.home() / ".natquery" / database_name / "schema.json"
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    schema_path.write_text(json.dumps(schema, indent=2), encoding="utf-8")

    from natquery.config.settings import Settings

    Settings.get_db_config = classmethod(
        lambda cls: {
            "type": "dsn",
            "dsn": dsn,
            "dbname": database_name,
        }
    )


def execute(connection, sql: str, timeout: int) -> tuple[list[dict[str, Any]], float]:
    cursor_args = (
        {"row_factory": dict_row} if _PSYCOPG3 else {"cursor_factory": RealDictCursor}
    )
    with connection.cursor(**cursor_args) as cursor:
        cursor.execute("SET LOCAL statement_timeout = %s", (timeout * 1000,))
        started = time.perf_counter()
        cursor.execute(sql)
        elapsed = (time.perf_counter() - started) * 1000
        rows = [dict(row) for row in cursor.fetchall()] if cursor.description else []
    return rows, elapsed


def normalize(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "as_tuple"):
        return float(value)
    return value


def analyze_sql(sql: str) -> dict[str, Any]:
    try:
        from sqlglot import exp, parse_one

        statement = parse_one(sql, read="postgres")
        tables = []
        for table in statement.find_all(exp.Table):
            name = table.name
            if name and name not in tables:
                tables.append(name)
        joins = list(statement.find_all(exp.Join))
        return {
            "tables": tables,
            "table_count": len(tables),
            "join_count": len(joins),
        }
    except Exception:
        return {"tables": [], "table_count": 0, "join_count": 0}


def generate_sql_with_timeout(question: str, timeout: int) -> str:
    from natquery.llm.client import generate_sql

    result: list[str] = []
    failure: list[BaseException] = []

    def generate() -> None:
        try:
            result.append(generate_sql(question))
        except BaseException as error:
            failure.append(error)

    worker = threading.Thread(target=generate, daemon=True)
    worker.start()
    worker.join(timeout)
    if worker.is_alive():
        raise TimeoutError(f"LLM generation exceeded {timeout} seconds")
    if failure:
        raise failure[0]
    if not result or not result[0].strip():
        raise ValueError("Empty SQL generated by NatQuery")
    return result[0]


def run_question(
    question: dict[str, Any], dsn: str, repetitions: int, timeout: int
) -> dict[str, Any]:
    started = time.perf_counter()
    record = {
        "benchmark_id": question["id"],
        "question": question["natural_language"],
        "category": question.get("category"),
        "success": False,
        "generated_sql": None,
        "generated_tables": [],
        "table_count": None,
        "join_count": None,
        "rows_returned": None,
        "execution_times_ms": [],
        "median_execution_time_ms": None,
        "error": None,
    }
    try:
        prepare_natquery_schema(dsn, timeout)
        sql = generate_sql_with_timeout(question["natural_language"], timeout)
        metrics = analyze_sql(sql)
        record.update(
            generated_sql=sql,
            generated_tables=metrics["tables"],
            table_count=metrics["table_count"],
            join_count=metrics["join_count"],
        )
        with connect(dsn, timeout) as connection:
            rows = []
            for _ in range(max(1, repetitions)):
                rows, elapsed = execute(connection, sql, timeout)
                record["execution_times_ms"].append(elapsed)
        record.update(
            success=True,
            rows_returned=len(rows),
            median_execution_time_ms=statistics.median(record["execution_times_ms"]),
        )
    except Exception as error:
        record["error"] = f"{type(error).__name__}: {error}"
    record["pipeline_latency_ms"] = (time.perf_counter() - started) * 1000
    return record


def write_outputs(
    output_root: Path, records: list[dict[str, Any]], blocked: str | None
) -> Path:
    run_dir = output_root / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    run_dir.mkdir(parents=True, exist_ok=False)
    summary = {
        "total_questions": len(records),
        "successful_executions": sum(r["success"] for r in records),
        "blocked": blocked,
    }
    (run_dir / "results.json").write_text(
        json.dumps(records, indent=2, default=str), encoding="utf-8"
    )
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    fields = sorted({key for record in records for key in record})
    with (run_dir / "results.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
    report = (
        "\n".join(
            [
                "# NatQuery Benchmark Results",
                "",
                f"- Questions: {summary['total_questions']}",
                f"- Successful executions: {summary['successful_executions']}",
                "- Correctness: manual review required",
                f"- Blocked: {blocked or 'no'}",
                "",
                "Results are in `results.json` and `results.csv`.",
            ]
        )
        + "\n"
    )
    (run_dir / "report.md").write_text(report, encoding="utf-8")
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run all 50 NatQuery benchmark questions"
    )
    parser.add_argument(
        "--dsn", default=os.getenv("NATQUERY_BENCHMARK_DSN"), help="PostgreSQL DSN"
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    if not args.dsn:
        parser.error("Provide --dsn or set NATQUERY_BENCHMARK_DSN")
    questions = load_questions()
    try:
        missing = check_schema(args.dsn, args.timeout)
    except Exception as error:
        blocked = f"Database connection failed: {type(error).__name__}: {error}"
        records = [
            {
                "benchmark_id": q["id"],
                "question": q["natural_language"],
                "success": False,
                "error": blocked,
            }
            for q in questions
        ]
        print(write_outputs(args.output, records, blocked))
        return 1
    if missing:
        blocked = "Missing benchmark tables: " + ", ".join(missing)
        records = [
            {
                "benchmark_id": q["id"],
                "question": q["natural_language"],
                "success": False,
                "error": blocked,
            }
            for q in questions
        ]
        print(write_outputs(args.output, records, blocked))
        return 1
    records = []
    for number, question in enumerate(questions, 1):
        print(f"[{number:02d}/50] {question['id']}", flush=True)
        records.append(run_question(question, args.dsn, args.repetitions, args.timeout))
    run_dir = write_outputs(args.output, records, None)
    print(f"Saved results to {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
