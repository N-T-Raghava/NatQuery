import psycopg2
from psycopg2.extensions import connection as PGConnection
from psycopg2.extras import RealDictCursor
from .settings import Settings


def get_connection() -> PGConnection:
    """
    Creates and returns a PostgreSQL connection.
    Supports:
        - Locally hosted databases
        - Remote databases
        - SSH Tunneled databases
        - Cloud-managed (SSL) (optional)
    """

    db_config = Settings.get_db_config()

    connect_args = {
        "host": db_config["host"],
        "port": db_config["port"],
        "dbname": db_config["dbname"],
        "user": db_config["user"],
        "password": db_config["password"],
        "connect_timeout": 5,
    }

    # Optional SSL connectivity
    sslmode = db_config.get("sslmode")
    if sslmode:
        connect_args["sslmode"] = sslmode

    try:
        conn = psycopg2.connect(**connect_args)
        conn.autocommit = False
        return conn

    except psycopg2.OperationalError as e:
        raise ConnectionError(
            f"Unable to connect to PostgreSQL server.\nDetails: {e}"
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