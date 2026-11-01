import json

import pytest

from natquery.execution.optimizer import evaluate_query_plan, should_rewrite
from natquery.execution.query_planner import generate_rewrite_candidates
from natquery.observability import cost_analyzer as ca
from natquery.observability.index_recommender import suggest_indexes
from natquery.observability.logger import NatQueryLogger
from natquery.schema.formatter import format_schema
from natquery.schema.knowledge_base import build_schema_index, search_schema_memory
from natquery.schema.retriever import retrieve_relevant_schema
from natquery.schema.store import (
    apply_semantics,
    load_schema,
    save_schema,
    schema_path,
    semantics_path,
)

SCHEMA = {
    "tables": {
        "students": {
            "columns": {"roll_no": "text", "name": "text", "cgpa": "numeric"},
            "primary_key": ["roll_no"],
            "foreign_keys": [],
            "description": "Enrolled students",
        },
        "enrollments": {
            "columns": {"roll_no": "text", "course_id": "text", "grade": "text"},
            "primary_key": [],
            "foreign_keys": [
                {
                    "column": "roll_no",
                    "references": {"table": "students", "column": "roll_no"},
                }
            ],
        },
        "courses": {
            "columns": {"course_id": "text", "title": "text"},
            "primary_key": ["course_id"],
        },
        "fees": {
            "columns": {"amount_due": "numeric"},
            "primary_key": [],
            "synonyms": ["tuition", "payments"],
        },
        "salaries": {"columns": {"gross": "numeric"}, "primary_key": []},
    }
}


# ---------------------------------------------------------------- formatter
def test_format_schema_renders_types_keys_masks_relationships():
    s = {
        "tables": {
            "t": {
                "columns": {"id": "int", "e": "text"},
                "primary_key": ["id"],
                "masked_columns": ["e"],
                "description": "desc",
                "foreign_keys": [
                    {"column": "id", "references": {"table": "u", "column": "id"}}
                ],
            }
        }
    }
    out = format_schema(s)
    assert "TABLE t  -- desc (" in out and "id INT PRIMARY KEY" in out
    assert "e TEXT  -- DISPLAY-ONLY" in out and "RELATIONSHIPS:\nt.id -> u.id" in out


@pytest.mark.parametrize("s", [None, {}, {"x": 1}])
def test_format_schema_empty(s):
    assert format_schema(s) == ""


def test_format_schema_is_deterministic_and_sorted():
    out = format_schema(SCHEMA)
    assert out == format_schema(SCHEMA)
    assert out.index("TABLE courses") < out.index("TABLE students")


# ---------------------------------------------------------------- knowledge base / retriever
def test_search_ranks_by_overlap_with_stemming():
    ranked = search_schema_memory("show student grades for each course", SCHEMA)
    assert ranked[0] == "enrollments" or "students" in ranked[:2]
    assert "salaries" not in ranked


def test_search_uses_synonyms_and_descriptions():
    assert search_schema_memory("outstanding tuition", SCHEMA)[0] == "fees"
    assert "students" in search_schema_memory("who is enrolled", SCHEMA)


def test_search_no_match_returns_empty():
    assert search_schema_memory("zzz qqq", SCHEMA) == []
    assert search_schema_memory("", SCHEMA) == []


def test_index_includes_semantic_tags():
    idx = build_schema_index(
        {"tables": {"t": {"columns": {}, "semantic_tags": ["Pupil Roster"]}}}
    )
    assert {"pupil", "roster"} <= idx[0].terms


def test_retriever_adds_fk_neighbours_and_bounds():
    r = retrieve_relevant_schema("students named x", SCHEMA)
    assert (
        "students" in r["tables"] and "enrollments" in r["tables"]
    )  # neighbour via FK
    assert "salaries" not in r["tables"]


def test_retriever_falls_back_to_first_tables_and_respects_max():
    r = retrieve_relevant_schema("zzz", SCHEMA, max_tables=2)
    assert list(r["tables"]) == ["courses", "enrollments"]


def test_retriever_cannot_surface_tables_outside_input():
    view = {"tables": {"courses": SCHEMA["tables"]["courses"]}}
    assert set(
        retrieve_relevant_schema("salaries gross fees students", view)["tables"]
    ) <= {"courses"}


def test_retriever_does_not_mutate_input_and_handles_dangling_fk():
    s = {
        "tables": {
            "a": {
                "columns": {"x": "int"},
                "foreign_keys": [
                    {"column": "x", "references": {"table": "ghost", "column": "id"}}
                ],
            }
        }
    }
    r = retrieve_relevant_schema("a x", s)
    assert set(r["tables"]) == {"a"} and "ghost" not in s["tables"]


def test_retriever_top_k_limits_core():
    many = {"tables": {f"t{i}": {"columns": {"common": "int"}} for i in range(30)}}
    assert (
        len(retrieve_relevant_schema("common", many, top_k=3, max_tables=20)["tables"])
        == 3
    )


# ---------------------------------------------------------------- store
def test_schema_roundtrip_and_semantics_merge():
    p = save_schema("erp", {"tables": {"t": {"columns": {"a": "int"}}}})
    assert p == schema_path("erp") and oct(p.stat().st_mode & 0o777) == "0o600"
    semantics_path("erp").write_text(
        json.dumps(
            {
                "tables": {
                    "t": {
                        "description": "D",
                        "synonyms": ["s"],
                        "semantic_tags": ["x"],
                        "ignored": 1,
                    },
                    "ghost": {"description": "nope"},
                }
            }
        )
    )
    s = load_schema("erp")
    assert s["tables"]["t"]["description"] == "D" and s["tables"]["t"]["synonyms"] == [
        "s"
    ]
    assert "ignored" not in s["tables"]["t"] and "ghost" not in s["tables"]


def test_load_schema_missing_is_empty():
    assert load_schema("never-saved") == {}


def test_apply_semantics_tolerates_empty():
    s = {"tables": {"t": {"columns": {}}}}
    assert apply_semantics(s, {}) == s and apply_semantics(s, {"tables": None}) == s


def test_store_rejects_traversal():
    with pytest.raises(ValueError):
        schema_path("../x")


# ---------------------------------------------------------------- cost analyzer
PLAN = [
    {
        "Planning Time": 0.5,
        "Execution Time": 2.0,
        "Plan": {
            "Node Type": "Nested Loop",
            "Total Cost": 250.0,
            "Plan Rows": 10,
            "Join Type": "Inner",
            "Plans": [
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "students",
                    "Filter": "((cgpa > 5) AND (status = 'x'))",
                    "Total Cost": 50,
                    "Plan Rows": 5,
                },
                {
                    "Node Type": "Index Scan",
                    "Relation Name": "enrollments",
                    "Index Cond": "(roll_no = s.roll_no)",
                },
            ],
        },
    }
]


def test_extract_summary_and_root_handling():
    a = ca.analyze_cost(PLAN)
    assert a["summary"] == {
        "execution_time_ms": 2.0,
        "planning_time_ms": 0.5,
        "total_cost": 250.0,
        "plan_rows": 10,
    }
    assert ca.analyze_cost(PLAN[0]) == a  # list or dict


def test_flatten_plan_walks_children_and_extracts_columns():
    nodes = ca.analyze_cost(PLAN)["nodes"]
    assert [n["node_type"] for n in nodes] == ["Nested Loop", "Seq Scan", "Index Scan"]
    assert set(nodes[1]["columns"]) == {"cgpa", "status"} and nodes[2]["columns"] == [
        "roll_no"
    ]


@pytest.mark.parametrize(
    "cond,cols",
    [(None, []), ("", []), ("(a = 1)", ["a"]), ("x >= 3 AND y < 2", ["x", "y"])],
)
def test_extract_columns(cond, cols):
    assert sorted(ca.extract_columns_from_condition(cond)) == cols


def test_empty_plan_is_survivable():
    assert ca.analyze_cost({})["nodes"][0]["node_type"] is None


# ---------------------------------------------------------------- recommender
def test_index_suggestions():
    sugg = suggest_indexes(ca.analyze_cost(PLAN))
    kinds = [s["type"] for s in sugg]
    assert "composite_index" in kinds and "join" in kinds
    comp = next(s for s in sugg if s["type"] == "composite_index")
    assert comp["table"] == "students" and comp["sql"].startswith(
        "CREATE INDEX idx_students_"
    )


def test_single_column_and_no_filter_scans():
    one = suggest_indexes(
        {"nodes": [{"node_type": "Seq Scan", "relation": "t", "columns": ["a"]}]}
    )
    assert one[0]["type"] == "index" and "ON t(a)" in one[0]["sql"]
    none = suggest_indexes(
        {"nodes": [{"node_type": "Seq Scan", "relation": "t", "columns": []}]}
    )
    assert none[0]["type"] == "seq_scan_warning"
    assert (
        suggest_indexes({"nodes": [{"node_type": "Index Scan", "relation": "t"}]}) == []
    )
    assert suggest_indexes({}) == []


def test_suggestions_never_execute_anything():
    """They are text for a human DBA; make sure they are only ever CREATE INDEX statements."""
    for s in suggest_indexes(ca.analyze_cost(PLAN)):
        assert s.get("sql", "CREATE INDEX").startswith("CREATE INDEX")


# ---------------------------------------------------------------- optimizer
def test_evaluate_and_threshold():
    m = evaluate_query_plan(ca.analyze_cost(PLAN))
    assert (
        m["sequential_scans"] == 1
        and m["nested_loops"] == 1
        and m["score"] == 250 + 100 + 50
    )
    assert should_rewrite(m) is False and should_rewrite({"score": 1001}) is True
    assert should_rewrite({}) is False
    assert evaluate_query_plan({})["score"] == 0


def test_row_amplification_scores():
    m = evaluate_query_plan({"nodes": [{"actual_rows": 5000, "plan_rows": 1000}]})
    assert m["row_amplification"] == 4000 and m["score"] == 4.0


def test_year_filter_rewrite_is_sargable():
    c = generate_rewrite_candidates(
        "SELECT * FROM t WHERE EXTRACT(YEAR FROM d) = 2024 AND x = 1"
    )
    assert c == [
        "SELECT * FROM t WHERE d >= DATE '2024-01-01' AND d < DATE '2025-01-01' AND x = 1"
    ]
    assert generate_rewrite_candidates("SELECT 1") == []
    assert generate_rewrite_candidates(
        "SELECT * FROM t WHERE extract(year from t.d)=1999"
    )


# ---------------------------------------------------------------- logger
def test_system_log_scrubs_secrets_and_is_private(tmp_path):
    NatQueryLogger.log_event(
        "INFO", "evt", "erp", details={"password": "p", "msg": "postgres://u:pw@h/d"}
    )
    f = tmp_path / ".natquery" / "system.log"
    rec = json.loads(f.read_text().splitlines()[0])
    assert rec["details"]["password"] == "[REDACTED]" and "pw@" not in f.read_text()
    assert oct(f.stat().st_mode & 0o777) == "0o600"


def test_performance_log(tmp_path):
    NatQueryLogger.log_performance(
        "erp", "ref1", "SELECT 1", {"total_cost": 3}, [{"type": "index"}], user="alice"
    )
    f = tmp_path / ".natquery" / "erp" / "logs" / "performance.jsonl"
    rec = json.loads(f.read_text())
    assert (
        rec["user"] == "alice"
        and rec["total_cost"] == 3
        and len(rec["query_hash"]) == 16
    )


def test_retriever_caps_neighbours_keeping_best_ranked_first():
    tables = {
        f"t{i:02d}": {
            "columns": {"hub": "int"},
            "foreign_keys": [
                {"column": "hub", "references": {"table": "t00", "column": "hub"}}
            ],
        }
        for i in range(1, 30)
    }
    tables["t00"] = {"columns": {"hub": "int", "name": "text"}, "foreign_keys": []}
    r = retrieve_relevant_schema("name", {"tables": tables}, top_k=1, max_tables=5)
    assert len(r["tables"]) == 5 and "t00" in r["tables"]


# ------------------------------------------- retrieval fixes (found by the BIRD benchmark)
def _t(*cols, fks=()):
    return {
        "columns": {c: "text" for c in cols},
        "foreign_keys": [
            {"column": c, "references": {"table": t, "column": rc}} for c, t, rc in fks
        ],
    }


def test_small_schema_is_sent_whole_when_enabled():
    tables = {
        "customers": _t("segment"),
        "gasstations": _t("country"),
        "transactions": _t("price"),
    }
    # keyword retrieval alone drops tables that share no word with the question
    assert (
        "gasstations"
        not in retrieve_relevant_schema("what segment paid price", {"tables": tables})[
            "tables"
        ]
    )
    full = retrieve_relevant_schema(
        "what segment paid price", {"tables": tables}, full_if_columns=10
    )
    assert set(full["tables"]) == set(tables)


def test_full_schema_shortcut_respects_column_and_table_limits():
    wide = {"tables": {"a": _t(*[f"c{i}" for i in range(50)]), "b": _t("x")}}
    r = retrieve_relevant_schema("x", wide, full_if_columns=10)
    assert set(r["tables"]) == {"b"}  # too many columns: normal pruning applies
    many = {"tables": {f"t{i}": _t("common") for i in range(30)}}
    r = retrieve_relevant_schema(
        "common", many, top_k=3, max_tables=20, full_if_columns=1000
    )
    assert len(r["tables"]) == 3  # too many tables: normal pruning applies


def test_full_schema_shortcut_is_off_by_default_and_never_mutates_input():
    s = {"tables": {"a": _t("x"), "b": _t("y")}}
    before = json.dumps(s, sort_keys=True)
    assert set(retrieve_relevant_schema("x", s)["tables"]) == {"a"}
    retrieve_relevant_schema("x", s, full_if_columns=10)
    assert json.dumps(s, sort_keys=True) == before


def test_rare_words_outweigh_words_found_in_every_table():
    tables = {f"t{i}": _t("id", "name") for i in range(8)}
    tables["fees"] = _t("id", "name", "tuition")
    ranked = search_schema_memory("id name tuition", {"tables": tables})
    assert ranked[0] == "fees"


def test_bridge_table_between_two_chosen_tables_is_included():
    tables = {
        "customers": _t("segment", "cid"),
        "orders": _t("oid", "cid", fks=[("cid", "customers", "cid")]),
        "items": _t("sku", "oid", fks=[("oid", "orders", "oid")]),
        "unrelated": _t("zzz"),
    }
    r = retrieve_relevant_schema("segment sku", {"tables": tables}, top_k=2)
    assert {"customers", "items", "orders"} <= set(r["tables"])
    assert "unrelated" not in r["tables"]


def test_pipeline_sends_a_small_schema_whole(tmp_path):
    from fakes import ERP_SCHEMA, FakeLLM, login, make_service

    llm = FakeLLM("SELECT 1")
    svc, _ = make_service(tmp_path, llm=llm)
    session = login(svc, "root")
    svc.ask(session.token, "how many departments")
    for table in ERP_SCHEMA["tables"]:
        if table != "erp_users":  # globally denied: never shown
            assert f"TABLE {table}" in llm.prompts
