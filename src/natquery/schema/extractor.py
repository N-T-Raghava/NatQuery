def extract_schema(conn):
    """
    Extract table names and column names from public schema.
    """

    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';
    """
    )

    tables = cursor.fetchall()

    schema = {}

    for (table_name,) in tables:
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
        """,
            (table_name,),
        )

        columns = cursor.fetchall()
        schema[table_name] = [col[0] for col in columns]

    return schema
