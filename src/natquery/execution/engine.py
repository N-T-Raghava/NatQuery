from natquery.config.connection import get_connection, close_connection, get_cursor


def execute_sql(sql: str):
    conn = get_connection()

    try:
        cursor = get_cursor(conn)
        cursor.execute(sql)

        if cursor.description:
            rows = cursor.fetchall()
            # RealDictCursor rows are already dict-like, convert to plain dict
            result = [
                (
                    dict(row)
                    if hasattr(row, "keys")
                    else dict(zip([desc[0] for desc in cursor.description], row))
                )
                for row in rows
            ]
        else:
            result = []

        conn.commit()
        return result

    finally:
        close_connection(conn)
