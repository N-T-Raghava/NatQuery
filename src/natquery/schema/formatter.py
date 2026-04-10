def format_schema(schema: dict) -> str:
    """
    Convert schema JSON → compact SQL-like text for LLM.

    RAG-ready:
    - Works with full schema OR filtered schema
    """

    if not schema or "tables" not in schema:
        return ""

    lines = []

    tables = schema["tables"]

    # Ensure deterministic ordering
    for table_name in sorted(tables.keys()):
        table = tables[table_name]

        columns = table.get("columns", {})
        pk = set(table.get("primary_key", []))

        col_lines = []

        for col_name in columns:
            col_type = columns[col_name].upper()

            if col_name in pk:
                col_lines.append(f"  {col_name} {col_type} PRIMARY KEY")
            else:
                col_lines.append(f"  {col_name} {col_type}")

        table_block = f"TABLE {table_name} (\n" + ",\n".join(col_lines) + "\n)"
        lines.append(table_block)

    # Relationships (FKs)
    relationships = []

    for table_name in sorted(tables.keys()):
        fks = tables[table_name].get("foreign_keys", [])

        for fk in fks:
            relationships.append(
                f"{table_name}.{fk['column']} → "
                f"{fk['references']['table']}.{fk['references']['column']}"
            )

    if relationships:
        lines.append("\nRELATIONSHIPS:")
        lines.extend(relationships)

    return "\n\n".join(lines)
