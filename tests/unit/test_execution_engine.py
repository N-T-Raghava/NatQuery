import pytest
from unittest.mock import patch, MagicMock
from natquery.execution.engine import execute_sql


class TestExecuteSql:
    """Test execute_sql() function."""

    @patch("natquery.execution.engine.close_connection")
    @patch("natquery.execution.engine.get_cursor")
    @patch("natquery.execution.engine.get_connection")
    def test_execute_sql_select_query(
        self, mock_get_connection, mock_get_cursor, mock_close_connection
    ):
        """Test execution of SELECT query."""
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        mock_cursor = MagicMock()
        mock_get_cursor.return_value = mock_cursor

        # Mock description for SELECT
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]

        result = execute_sql("SELECT id, name FROM users")

        expected = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        assert result == expected
        mock_cursor.execute.assert_called_once_with("SELECT id, name FROM users")
        mock_conn.commit.assert_called_once()
        mock_close_connection.assert_called_once_with(mock_conn)

    @patch("natquery.execution.engine.close_connection")
    @patch("natquery.execution.engine.get_cursor")
    @patch("natquery.execution.engine.get_connection")
    def test_execute_sql_insert_query(
        self, mock_get_connection, mock_get_cursor, mock_close_connection
    ):
        """Test execution of INSERT query (no result)."""
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        mock_cursor = MagicMock()
        mock_get_cursor.return_value = mock_cursor

        # Mock no description for INSERT
        mock_cursor.description = None

        result = execute_sql("INSERT INTO users (name) VALUES ('Charlie')")

        assert result == []
        mock_cursor.execute.assert_called_once_with(
            "INSERT INTO users (name) VALUES ('Charlie')"
        )
        mock_conn.commit.assert_called_once()
        mock_close_connection.assert_called_once_with(mock_conn)

    @patch("natquery.execution.engine.close_connection")
    @patch("natquery.execution.engine.get_cursor")
    @patch("natquery.execution.engine.get_connection")
    def test_execute_sql_exception_handling(
        self, mock_get_connection, mock_get_cursor, mock_close_connection
    ):
        """Test that connection is closed even if exception occurs."""
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        mock_cursor = MagicMock()
        mock_get_cursor.return_value = mock_cursor

        mock_cursor.execute.side_effect = Exception("SQL Error")

        with pytest.raises(Exception, match="SQL Error"):
            execute_sql("INVALID SQL")

        mock_close_connection.assert_called_once_with(mock_conn)
        mock_conn.commit.assert_not_called()  # Should not commit on error

    @patch("natquery.execution.engine.close_connection")
    @patch("natquery.execution.engine.get_cursor")
    @patch("natquery.execution.engine.get_connection")
    def test_execute_sql_empty_result(
        self, mock_get_connection, mock_get_cursor, mock_close_connection
    ):
        """Test SELECT query with no results."""
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        mock_cursor = MagicMock()
        mock_get_cursor.return_value = mock_cursor

        mock_cursor.description = [("id",)]
        mock_cursor.fetchall.return_value = []

        result = execute_sql("SELECT id FROM users WHERE false")

        assert result == []
        mock_conn.commit.assert_called_once()
        mock_close_connection.assert_called_once_with(mock_conn)
