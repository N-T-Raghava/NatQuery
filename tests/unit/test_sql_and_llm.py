from natquery.execution.optimizer import evaluate_query_plan, should_rewrite
from natquery.execution.query_planner import generate_rewrite_candidates
from natquery.llm.generator import SQLGenerator, is_unanswerable, normalize_sql
from natquery.observability.cost_analyzer import (
    analyze_cost,
    extract_columns_from_condition,
)
from natquery.observability.index_recommender import suggest_indexes
from natquery.orchestration.error_classifier import classify_error
from natquery.prompt.builder import build_prompt, build_repair_prompt


def test_prompt_builder_sql_generator_and_error_classifier():
    class FakeLLM:
        def __init__(self):
            self.calls = []

        def complete(self, messages, *, temperature=0.0, max_tokens=800):
            self.calls.append(messages)
            return "```sql\nSELECT id, name FROM students;\n```"

    llm = FakeLLM()
    messages = build_prompt("list students", "TABLE students ( id INT )")
    assert messages[0]["role"] == "system"
    assert "DATABASE SCHEMA" in messages[0]["content"]
    assert "Question: list students" in messages[1]["content"]

    repair = build_repair_prompt(
        "list students", "SELECT * FROM students", "syntax error", "TABLE students"
    )
    assert "Return corrected SQL" in repair[1]["content"]

    gen = SQLGenerator(llm)
    sql = gen.generate("list students", "TABLE students ( id INT )")
    assert sql == "SELECT id, name FROM students;"
    assert not is_unanswerable(sql)

    assert normalize_sql("```sql\n-- UNANSWERABLE\n```") == "-- UNANSWERABLE"
    assert is_unanswerable("-- UNANSWERABLE")

    assert classify_error("syntax error at or near SELECT") == "SYNTAX_ERROR"
    assert classify_error("column students.name does not exist") == "UNDEFINED_COLUMN"
    assert classify_error("table foo does not exist") == "UNDEFINED_TABLE"


def test_audit_logger_cost_analysis_and_optimizer():
    import os
    from pathlib import Path

    from natquery.audit.log import AuditLog
    from natquery.config.paths import base_dir
    from natquery.observability.logger import NatQueryLogger

    audit = AuditLog(path=Path(os.environ["NATQUERY_HOME"]) / "audit.log")
    ref = audit.record("login", actor={"username": "alice"}, outcome="ok")
    ok, count, problem = audit.verify()
    assert ok and count >= 1 and problem is None
    assert audit.tail(1)[0]["id"] == ref

    NatQueryLogger.log_event(
        "INFO",
        "test_event",
        db_name="demo",
        user="alice",
        details={"api_key": "secret"},
    )
    text = (base_dir() / "system.log").read_text(encoding="utf-8")
    assert "secret" not in text

    plan = {
        "Plan": {
            "Node Type": "Seq Scan",
            "Relation Name": "students",
            "Total Cost": 1000.0,
            "Plan Rows": 10,
            "Actual Rows": 50,
            "Actual Total Time": 3.5,
            "Filter": "((dept_id = 42) AND (status = 'active'))",
            "Plans": [],
        },
        "Execution Time": 10,
        "Planning Time": 5,
    }
    analysis = analyze_cost(plan)
    assert analysis["summary"]["total_cost"] == 1000.0
    assert analysis["nodes"][0]["relation"] == "students"
    assert set(
        extract_columns_from_condition("(dept_id = 42) AND (status = 'active')")
    ) == {"dept_id", "status"}

    metrics = evaluate_query_plan(analysis)
    assert metrics["sequential_scans"] >= 1
    assert should_rewrite(metrics, cost_threshold=100) is True

    suggestions = suggest_indexes(analysis)
    assert suggestions
    assert any(item["type"] in {"index", "composite_index"} for item in suggestions)

    cand = generate_rewrite_candidates(
        "SELECT * FROM students WHERE EXTRACT(YEAR FROM joined_at) = 2024"
    )
    assert cand and "DATE '2024-01-01'" in cand[0]
