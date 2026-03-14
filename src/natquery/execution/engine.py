from natquery.config.connection import get_connection, close_connection


def execute_sql(sql: str):
    conn = get_connection()

    try:
        cursor = conn.cursor()
        cursor.execute(sql)

        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            result = [
                dict(zip(columns, row))
                for row in rows
            ]
        else:
            result = []

        conn.commit()
        return result

    finally:
        close_connection(conn)