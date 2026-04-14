import psycopg2
from psycopg2.extensions import connection as PGConnection
from psycopg2.extras import RealDictCursor
from natquery.config.settings import Settings
from natquery.observability.logger import NatQueryLogger


def get_connection() -> PGConnection:
    """
    Creates and returns a PostgreSQL connection.
    Supports:
        - Standard host/port configuration
        - DSN (connection string)
        - SSL (optional)
    """

    db_config = Settings.get_db_config()

    # Determine DB name safely (for logging)
    db_name = db_config.get("dbname", "unknown_db")

    # Log connection attempt
    NatQueryLogger.log_event(
        level="INFO",
        event="db_connection_attempt",
        db_name=db_name,
        conv_id=None,
        details=db_config,
    )

    try:

        # ---- DSN MODE ----
        if db_config["type"] == "dsn":

            conn = psycopg2.connect(
                db_config["dsn"],
                connect_timeout=5,
            )

        # ---- STANDARD MODE ----
        else:

            connect_args = {
                "host": db_config["host"],
                "port": db_config["port"],
                "dbname": db_config["dbname"],
                "user": db_config["user"],
                "password": db_config["password"],
                "connect_timeout": 5,
            }

            # Optional SSL
            sslmode = db_config.get("sslmode")
            if sslmode:
                connect_args["sslmode"] = sslmode

            conn = psycopg2.connect(**connect_args)

        conn.autocommit = False

        # Log success
        NatQueryLogger.log_event(
            level="INFO",
            event="db_connection_success",
            db_name=db_name,
            conv_id=None,
        )

        return conn

    except psycopg2.OperationalError as e:

        # Log failure
        NatQueryLogger.log_event(
            level="ERROR",
            event="db_connection_failed",
            db_name=db_name,
            conv_id=None,
            details={"error": str(e)},
        )

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
            # Safely extract db_name; use fallback if connection is partially closed
            try:
                db_name = conn.get_dsn_parameters().get("dbname")
            except (AttributeError, psycopg2.Error):
                db_name = "unknown_db"

            conn.close()

            NatQueryLogger.log_event(
                level="INFO",
                event="db_connection_closed",
                db_name=db_name,
                conv_id=None,
            )

        except Exception as e:
            NatQueryLogger.log_event(
                level="ERROR",
                event="db_connection_close_failed",
                db_name=None,
                conv_id=None,
                details={"error": str(e)},
            )
