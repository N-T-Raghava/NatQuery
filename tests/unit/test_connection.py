import pytest
from unittest.mock import patch, MagicMock
from psycopg2 import OperationalError
from psycopg2.extras import RealDictCursor
from natquery.config.connection import get_connection, get_cursor, close_connection


class TestGetConnection:
    """Test get_connection() function."""

    @patch("natquery.config.connection.Settings.get_db_config")
    @patch("natquery.config.connection.NatQueryLogger.log_event")
    @patch("psycopg2.connect")
    def test_get_connection_success(
        self, mock_psycopg_connect, mock_log_event, mock_get_db_config
    ):
        """Test successful database connection."""
        mock_get_db_config.return_value = {
            "host": "localhost",
            "port": 5432,
            "dbname": "testdb",
            "user": "testuser",
            "password": "testpass",
            "sslmode": None,
        }
        mock_conn = MagicMock()
        mock_psycopg_connect.return_value = mock_conn

        conn = get_connection()

        assert conn == mock_conn
        mock_psycopg_connect.assert_called_once_with(
            host="localhost",
            port=5432,
            dbname="testdb",
            user="testuser",
            password="testpass",
            connect_timeout=5,
        )
        assert mock_conn.autocommit is False
        # Check logging calls
        assert mock_log_event.call_count == 2
        mock_log_event.assert_any_call(
            level="INFO",
            event="db_connection_attempt",
            db_name="testdb",
            conv_id=None,
            details={"host": "localhost", "port": 5432},
        )
        mock_log_event.assert_any_call(
            level="INFO", event="db_connection_success", db_name="testdb", conv_id=None
        )

    @patch("natquery.config.connection.Settings.get_db_config")
    @patch("natquery.config.connection.NatQueryLogger.log_event")
    @patch("psycopg2.connect")
    def test_get_connection_with_ssl(
        self, mock_psycopg_connect, mock_log_event, mock_get_db_config
    ):
        """Test connection with SSL mode."""
        mock_get_db_config.return_value = {
            "host": "localhost",
            "port": 5432,
            "dbname": "testdb",
            "user": "testuser",
            "password": "testpass",
            "sslmode": "require",
        }
        mock_conn = MagicMock()
        mock_psycopg_connect.return_value = mock_conn

        get_connection()

        mock_psycopg_connect.assert_called_once_with(
            host="localhost",
            port=5432,
            dbname="testdb",
            user="testuser",
            password="testpass",
            connect_timeout=5,
            sslmode="require",
        )

    @patch("natquery.config.connection.Settings.get_db_config")
    @patch("natquery.config.connection.NatQueryLogger.log_event")
    @patch("psycopg2.connect")
    def test_get_connection_operational_error(
        self, mock_psycopg_connect, mock_log_event, mock_get_db_config
    ):
        """Test connection failure due to OperationalError."""
        mock_get_db_config.return_value = {
            "host": "localhost",
            "port": 5432,
            "dbname": "testdb",
            "user": "testuser",
            "password": "testpass",
        }
        mock_psycopg_connect.side_effect = OperationalError("Connection failed")

        with pytest.raises(
            ConnectionError, match="Unable to connect to PostgreSQL server"
        ):
            get_connection()

        # Check error logging
        mock_log_event.assert_any_call(
            level="ERROR",
            event="db_connection_failed",
            db_name="testdb",
            conv_id=None,
            details={"error": "Connection failed"},
        )


class TestGetCursor:
    """Test get_cursor() function."""

    def test_get_cursor_returns_real_dict_cursor(self, mock_db_connection):
        """Test that get_cursor returns a RealDictCursor."""
        cursor = get_cursor(mock_db_connection)
        mock_db_connection.cursor.assert_called_once_with(cursor_factory=RealDictCursor)
        assert cursor == mock_db_connection.cursor.return_value


class TestCloseConnection:
    """Test close_connection() function."""

    @patch("natquery.config.connection.NatQueryLogger.log_event")
    def test_close_connection_success(self, mock_log_event, mock_db_connection):
        """Test successful connection closure."""
        mock_db_connection.get_dsn_parameters.return_value = {"dbname": "testdb"}

        close_connection(mock_db_connection)

        mock_db_connection.close.assert_called_once()
        mock_log_event.assert_called_once_with(
            level="INFO", event="db_connection_closed", db_name="testdb", conv_id=None
        )

    @patch("natquery.config.connection.NatQueryLogger.log_event")
    def test_close_connection_with_exception(self, mock_log_event, mock_db_connection):
        """Test connection closure with exception."""
        mock_db_connection.close.side_effect = Exception("Close failed")
        mock_db_connection.get_dsn_parameters.return_value = {"dbname": "testdb"}

        close_connection(mock_db_connection)

        mock_log_event.assert_called_once_with(
            level="ERROR",
            event="db_connection_close_failed",
            db_name=None,
            conv_id=None,
            details={"error": "Close failed"},
        )

    @patch("natquery.config.connection.NatQueryLogger.log_event")
    def test_close_connection_none_conn(self, mock_log_event):
        """Test close_connection with None connection."""
        close_connection(None)
        mock_log_event.assert_not_called()
