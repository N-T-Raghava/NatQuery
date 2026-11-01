"""Where the extracted schema and optional semantic hints live, and how they merge."""

import json
from pathlib import Path

from natquery.config import paths


def schema_path(db_name: str) -> Path:
    return paths.workspace_dir(db_name) / "schema.json"


def semantics_path(db_name: str) -> Path:
    return paths.workspace_dir(db_name) / "semantics.json"


def save_schema(db_name: str, schema: dict) -> Path:
    path = schema_path(db_name)
    paths.write_private(path, json.dumps(schema, indent=2))
    return path


def apply_semantics(schema: dict, semantics: dict) -> dict:
    """Merge {tables: {name: {description, synonyms, semantic_tags, value_hints}}} into the schema."""
    for table, hints in (semantics.get("tables") or {}).items():
        if table in schema.get("tables", {}):
            for key in ("description", "synonyms", "semantic_tags", "value_hints"):
                if key in hints:
                    schema["tables"][table][key] = hints[key]
    return schema


def load_schema(db_name: str) -> dict:
    """Full (unfiltered) schema. Admin-level artefact: never hand it to an LLM directly;
    always go through access.schema_view.build_view first."""
    path = schema_path(db_name)
    if not path.exists():
        return {}
    schema = json.loads(path.read_text(encoding="utf-8"))
    sem = semantics_path(db_name)
    if sem.exists():
        apply_semantics(schema, json.loads(sem.read_text(encoding="utf-8")))
    return schema
