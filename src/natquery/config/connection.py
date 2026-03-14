import psycopg2
from psycopg2.extensions import connection as PGConnection
from psycopg2.extras import RealDictCursor
from .settings import Settings


def get_connection() -> PGConnection:
    """
    Creates and returns a PostgreSQL connection.
    """
    db_config = Settings.get_db_config()

    try:
        conn = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
        )
        conn.autocommit = False
        return conn

    except psycopg2.Error as e:
        raise ConnectionError(
            f"Database connection failed: {e}"
        ) from e


def get_cursor(conn: PGConnection):
    """
    Returns a dictionary-style cursor.
    """
    return conn.cursor(cursor_factory=RealDictCursor)


def close_connection(conn: PGConnection) -> None:
    """
    Safely closes a PostgreSQL connection.
    """
    if conn:
        try:
            conn.close()
        except Exception:
            pass