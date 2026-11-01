import pytest

from natquery.access.policy import parse_policy
from natquery.access.schema_view import build_view


def view_for(schema, **role):
    p = parse_policy({"global_deny": role.pop("global_deny", {}), "roles": {"r": role}})
    return build_view(schema, p, p.resolve("r"))


SCHEMA = {
    "tables": {
        "a": {
            "columns": {"id": "int", "secret": "text", "name": "text"},
            "primary_key": ["id", "secret"],
            "foreign_keys": [
                {"column": "id", "references": {"table": "b", "column": "id"}},
                {"column": "secret", "references": {"table": "b", "column": "id"}},
            ],
            "row_count": 99,
            "description": "A table",
            "semantic_tags": ["x"],
            "synonyms": ["s"],
        },
        "b": {"columns": {"id": "int"}, "primary_key": ["id"], "foreign_keys": []},
        "hidden": {"columns": {"id": "int"}},
    }
}


def test_only_granted_tables_and_columns_visible():
    v = view_for(SCHEMA, tables={"a": {"columns": ["id", "name"]}})
    assert list(v.tables) == ["a"] and set(v.tables["a"]) == {"id", "name"}


def test_no_grants_means_empty_view():
    assert view_for(SCHEMA).is_empty()


def test_star_with_deny_columns():
    v = view_for(SCHEMA, tables={"a": {"columns": "*", "deny_columns": ["secret"]}})
    assert set(v.tables["a"]) == {"id", "name"}


def test_table_with_no_visible_columns_vanishes():
    v = view_for(SCHEMA, tables={"a": {"columns": ["nonexistent"]}})
    assert "a" not in v.tables


def test_prompt_schema_hides_everything_filtered():
    v = view_for(
        SCHEMA, tables={"a": {"columns": ["id", "name"]}, "b": {"columns": "*"}}
    )
    entry = v.prompt_schema["tables"]["a"]
    assert "row_count" not in entry and "secret" not in str(v.prompt_schema)
    assert entry["primary_key"] == ["id"]  # PK column 'secret' dropped
    assert entry["foreign_keys"] == [
        {"column": "id", "references": {"table": "b", "column": "id"}}
    ]
    assert entry["description"] == "A table" and entry["semantic_tags"] == ["x"]


def test_foreign_keys_to_invisible_tables_are_dropped():
    v = view_for(SCHEMA, tables={"a": {"columns": ["id"]}})
    assert v.prompt_schema["tables"]["a"]["foreign_keys"] == []


def test_masked_and_row_filter_recorded():
    v = view_for(
        SCHEMA,
        tables={
            "a": {
                "columns": ["id", "name"],
                "masked": {"name": "redacted"},
                "row_filter": "id = :x",
            }
        },
    )
    assert v.masked == {"a": {"name": "redacted"}} and v.row_filters == {"a": "id = :x"}
    assert v.prompt_schema["tables"]["a"]["masked_columns"] == ["name"]


def test_masked_column_not_visible_is_not_recorded():
    v = view_for(
        SCHEMA, tables={"a": {"columns": ["id"], "masked": {"name": "redacted"}}}
    )
    assert v.masked == {}


def test_global_deny_beats_grants():
    v = view_for(
        SCHEMA,
        global_deny={"tables": ["b"], "columns": ["a.secret"]},
        tables={"*": {"columns": "*"}},
    )
    assert (
        "b" not in v.tables and "hidden" in v.tables and "secret" not in v.tables["a"]
    )


def test_wildcard_grant_covers_unlisted_tables_but_explicit_wins():
    v = view_for(SCHEMA, tables={"*": {"columns": "*"}, "a": {"columns": ["id"]}})
    assert set(v.tables["a"]) == {"id"} and set(v.tables) == {"a", "b", "hidden"}


def test_sqlglot_schema_is_a_copy():
    v = view_for(SCHEMA, tables={"a": {"columns": ["id"]}})
    v.sqlglot_schema()["a"]["injected"] = "x"
    assert "injected" not in v.tables["a"]


def test_source_schema_not_mutated():
    import copy

    before = copy.deepcopy(SCHEMA)
    view_for(SCHEMA, tables={"*": {"columns": "*"}})
    assert before == SCHEMA


@pytest.mark.parametrize("schema", [{}, {"tables": {}}])
def test_empty_schema(schema):
    assert view_for(schema, tables={"*": {"columns": "*"}}).is_empty()
