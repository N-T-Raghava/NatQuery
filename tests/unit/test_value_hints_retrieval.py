from natquery.access.schema_view import build_view  # noqa: F401
from natquery.schema.formatter import format_schema
from natquery.schema.knowledge_base import search_schema_memory
from natquery.schema.retriever import retrieve_relevant_schema
from natquery.schema.store import apply_semantics
from natquery.schema.value_hints import collect_value_hints


def _schema():
    return {
        "tables": {
            "pay": {
                "columns": {
                    "id": "integer",
                    "status": "varchar(10)",
                    "memo": "text",
                    "pay_name": "varchar(9)",
                    "note": "text",
                    "dept_id": "integer",
                    "kind": "varchar(5)",
                },
                "primary_key": ["id"],
                "foreign_keys": [
                    {
                        "column": "dept_id",
                        "references": {"table": "dept", "column": "id"},
                    }
                ],
            },
            "dept": {
                "columns": {"id": "integer", "name": "text"},
                "primary_key": ["id"],
                "foreign_keys": [],
            },
            "tiny": {
                "columns": {"id": "integer", "state": "varchar(5)"},
                "primary_key": ["id"],
                "foreign_keys": [],
            },
        }
    }


def test_collect_value_hints_filters():
    rows = {"count": {"pay": 100, "dept": 100, "tiny": 3}}

    def run(sql):
        if "count(*)" in sql:
            return [(rows["count"]["tiny" if "tiny" in sql else "pay"],)]
        col = sql.split('"')[1]
        if col == "status":
            return [("PAID",), ("FAILED",)]
        return [(f"v{i}",) for i in range(20)]  # too many distinct values

    hints = collect_value_hints(run, _schema())
    assert hints == {
        "pay": {"status": ["FAILED", "PAID"]}
    }  # ids, personal-looking names, tiny tables, high cardinality skipped


def test_hints_formatted_and_quoted():
    sch = apply_semantics(
        _schema(),
        {
            "tables": {
                "pay": {"value_hints": {"status": ["O'K", "PAID"], "ghost": ["x"]}}
            }
        },
    )
    text = format_schema(sch)
    assert "-- values: 'O''K', 'PAID'" in text and "ghost" not in text


def test_masked_columns_never_get_hints():
    sch = _schema()
    sch["tables"]["pay"]["masked_columns"] = ["status"]
    sch["tables"]["pay"]["value_hints"] = {"status": ["SECRET"]}
    assert "SECRET" not in format_schema(sch)


def test_name_match_outweighs_column_match():
    sch = _schema()
    sch["tables"]["pay"]["columns"]["dept_name_text"] = "text"
    assert search_schema_memory("show dept", sch)[0] == "dept"


def test_lookup_tables_follow_foreign_keys():
    sch = _schema()
    for i in range(30):  # make the schema too big to return whole
        sch["tables"][f"t{i}"] = {
            "columns": {"id": "integer"},
            "primary_key": ["id"],
            "foreign_keys": [],
        }
    out = retrieve_relevant_schema("payment status pay", sch, top_k=1, max_tables=3)
    assert {"pay", "dept"} <= set(out["tables"])
