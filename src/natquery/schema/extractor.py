def extract_schema(conn):
    """
    Extract full schema:
    - tables
    - columns + types
    - primary keys
    - foreign keys
    """

    cursor = conn.cursor()

    schema = {"tables": {}}

    # Get tables
    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';
    """
    )

    tables = [row[0] for row in cursor.fetchall()]

    for table in tables:
        schema["tables"][table] = {"columns": {}, "primary_key": [], "foreign_keys": []}

        # Columns and types
        cursor.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s;
        """,
            (table,),
        )

        for col, dtype in cursor.fetchall():
            schema["tables"][table]["columns"][col] = dtype

        # Primary keys
        cursor.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = %s
              AND tc.constraint_type = 'PRIMARY KEY';
        """,
            (table,),
        )

        schema["tables"][table]["primary_key"] = [row[0] for row in cursor.fetchall()]

        # Foreign keys
        cursor.execute(
            """
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table,
                ccu.column_name AS foreign_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_name = %s;
        """,
            (table,),
        )

        for col, ref_table, ref_col in cursor.fetchall():
            schema["tables"][table]["foreign_keys"].append(
                {"column": col, "references": {"table": ref_table, "column": ref_col}}
            )

    cursor.close()
    return schema
