def format_schema(schema: dict) -> str:
    """Convert a (role-filtered) schema dict into compact SQL-like text for the LLM."""
    if not schema or "tables" not in schema:
        return ""

    tables = schema["tables"]
    blocks = []
    for table_name in sorted(tables):
        table = tables[table_name]
        columns = table.get("columns", {})
        pk = set(table.get("primary_key", []))
        masked = set(table.get("masked_columns", []))

        hints = table.get("value_hints", {})
        col_lines = []
        for col_name, col_type in columns.items():
            line = f"  {col_name} {col_type.upper()}"
            if col_name in pk:
                line += " PRIMARY KEY"
            if col_name in masked:
                line += (
                    "  -- DISPLAY-ONLY: select directly, never filter/sort/aggregate"
                )
            if col_name in hints and col_name not in masked:
                line += "  -- values: " + ", ".join(
                    "'" + v.replace("'", "''") + "'" for v in hints[col_name]
                )
            col_lines.append(line)

        header = f"TABLE {table_name}"
        if table.get("description"):
            header += f"  -- {table['description']}"
        blocks.append(header + " (\n" + ",\n".join(col_lines) + "\n)")

    relationships = [
        f"{t}.{fk['column']} -> {fk['references']['table']}.{fk['references']['column']}"
        for t in sorted(tables)
        for fk in tables[t].get("foreign_keys", [])
    ]
    if relationships:
        blocks.append("RELATIONSHIPS:\n" + "\n".join(relationships))
    return "\n\n".join(blocks)
