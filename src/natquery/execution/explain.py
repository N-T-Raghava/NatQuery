from natquery.config.connection import get_connection, close_connection


def run_explain(query: str):
    """
    Runs EXPLAIN (FORMAT JSON) on a query.
    Returns the plan as JSON.
    """
    conn = get_connection()

    try:
        cursor = conn.cursor()

        explain_query = f"EXPLAIN (FORMAT JSON) {query}"
        cursor.execute(explain_query)

        result = cursor.fetchone()[0]  # JSON result

        # PostgreSQL returns list with single JSON object
        if isinstance(result, list):
            return result[0]

        return result

    finally:
        close_connection(conn)


def run_explain_analyze(query: str):
    """
    Runs EXPLAIN ANALYZE (FORMAT JSON) for actual execution metrics.
    Returns the plan with actual execution stats as JSON.
    """
    conn = get_connection()

    try:
        cursor = conn.cursor()

        explain_query = f"EXPLAIN (ANALYZE, FORMAT JSON) {query}"
        cursor.execute(explain_query)

        result = cursor.fetchone()[0]  # JSON result

        # PostgreSQL returns list with single JSON object
        if isinstance(result, list):
            return result[0]

        return result

    finally:
        close_connection(conn)
