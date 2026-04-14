import pytest
from unittest.mock import patch, MagicMock
from natquery.execution.explain import run_explain, run_explain_analyze


class TestRunExplain:
    """Test run_explain() function."""

    @patch("natquery.execution.explain.close_connection")
    @patch("natquery.execution.explain.get_connection")
    def test_run_explain_basic_query(self, mock_get_connection, mock_close_connection):
        """Test EXPLAIN on a basic SELECT query."""
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Mock EXPLAIN JSON output
        explain_result = {
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "users",
                "Total Cost": 45.00,
                "Plan Rows": 100,
            },
            "Planning Time": 0.234,
            "Execution Time": 1.234,
        }

        mock_cursor.fetchone.return_value = [explain_result]

        result = run_explain("SELECT * FROM users")

        assert result == explain_result
        mock_cursor.execute.assert_called_once_with(
            "EXPLAIN (FORMAT JSON) SELECT * FROM users"
        )
        mock_close_connection.assert_called_once_with(mock_conn)

    @patch("natquery.execution.explain.close_connection")
    @patch("natquery.execution.explain.get_connection")
    def test_run_explain_with_filter(self, mock_get_connection, mock_close_connection):
        """Test EXPLAIN on a query with WHERE clause."""
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        explain_result = {
            "Plan": {
                "Node Type": "Index Scan",
                "Relation Name": "users",
                "Index Name": "idx_users_id",
                "Total Cost": 4.50,
                "Plan Rows": 1,
                "Filter": "(id = 1)",
            },
            "Planning Time": 0.123,
            "Execution Time": 0.234,
        }

        mock_cursor.fetchone.return_value = [explain_result]

        result = run_explain("SELECT * FROM users WHERE id = 1")

        assert result == explain_result
        assert result["Plan"]["Node Type"] == "Index Scan"

    @patch("natquery.execution.explain.close_connection")
    @patch("natquery.execution.explain.get_connection")
    def test_run_explain_handles_list_result(
        self, mock_get_connection, mock_close_connection
    ):
        """Test EXPLAIN handles PostgreSQL list-wrapped result."""
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        explain_result = {
            "Plan": {"Node Type": "Seq Scan", "Total Cost": 45.00},
            "Planning Time": 0.234,
        }

        # PostgreSQL sometimes returns result wrapped in list
        mock_cursor.fetchone.return_value = [[explain_result]]

        result = run_explain("SELECT * FROM users")

        assert result == explain_result

    @patch("natquery.execution.explain.close_connection")
    @patch("natquery.execution.explain.get_connection")
    def test_run_explain_connection_closed_on_error(
        self, mock_get_connection, mock_close_connection
    ):
        """Test that connection is closed even if EXPLAIN fails."""
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("Syntax Error")

        with pytest.raises(Exception, match="Syntax Error"):
            run_explain("INVALID QUERY")

        mock_close_connection.assert_called_once_with(mock_conn)


class TestRunExplainAnalyze:
    """Test run_explain_analyze() function."""

    @patch("natquery.execution.explain.close_connection")
    @patch("natquery.execution.explain.get_connection")
    def test_run_explain_analyze_basic_query(
        self, mock_get_connection, mock_close_connection
    ):
        """Test EXPLAIN ANALYZE on a SELECT query."""
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        explain_result = {
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "users",
                "Total Cost": 45.00,
                "Plan Rows": 100,
                "Actual Rows": 100,
                "Actual Total Time": 2.345,
            },
            "Planning Time": 0.234,
            "Execution Time": 2.569,
        }

        mock_cursor.fetchone.return_value = [explain_result]

        result = run_explain_analyze("SELECT * FROM users")

        assert result == explain_result
        mock_cursor.execute.assert_called_once_with(
            "EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM users"
        )
        mock_close_connection.assert_called_once_with(mock_conn)

    @patch("natquery.execution.explain.close_connection")
    @patch("natquery.execution.explain.get_connection")
    def test_run_explain_analyze_with_actual_metrics(
        self, mock_get_connection, mock_close_connection
    ):
        """Test EXPLAIN ANALYZE includes actual execution metrics."""
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        explain_result = {
            "Plan": {
                "Node Type": "Index Scan",
                "Relation Name": "users",
                "Index Name": "idx_users_status",
                "Total Cost": 12.50,
                "Startup Cost": 0.42,
                "Plan Rows": 50,
                "Actual Rows": 48,  # Actual differs from plan
                "Actual Total Time": 1.234,
            },
            "Planning Time": 0.123,
            "Execution Time": 1.357,
        }

        mock_cursor.fetchone.return_value = [explain_result]

        result = run_explain_analyze("SELECT * FROM users WHERE status = 'active'")

        assert result["Plan"]["Actual Rows"] == 48
        assert result["Execution Time"] == 1.357

    @patch("natquery.execution.explain.close_connection")
    @patch("natquery.execution.explain.get_connection")
    def test_run_explain_analyze_nested_plans(
        self, mock_get_connection, mock_close_connection
    ):
        """Test EXPLAIN ANALYZE with nested plan nodes (joins, subqueries)."""
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        explain_result = {
            "Plan": {
                "Node Type": "Hash Join",
                "Join Type": "Inner",
                "Total Cost": 78.50,
                "Plans": [
                    {
                        "Node Type": "Seq Scan",
                        "Relation Name": "users",
                        "Total Cost": 35.00,
                        "Actual Rows": 100,
                    },
                    {
                        "Node Type": "Hash",
                        "Total Cost": 25.00,
                        "Plans": [
                            {
                                "Node Type": "Seq Scan",
                                "Relation Name": "orders",
                                "Total Cost": 15.00,
                                "Actual Rows": 500,
                            }
                        ],
                    },
                ],
                "Actual Rows": 500,
            },
            "Planning Time": 0.456,
            "Execution Time": 5.678,
        }

        mock_cursor.fetchone.return_value = [explain_result]

        result = run_explain_analyze(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
        )

        assert result["Plan"]["Node Type"] == "Hash Join"
        assert len(result["Plan"]["Plans"]) == 2

    @patch("natquery.execution.explain.close_connection")
    @patch("natquery.execution.explain.get_connection")
    def test_run_explain_analyze_handles_list_result(
        self, mock_get_connection, mock_close_connection
    ):
        """Test EXPLAIN ANALYZE unwraps list-wrapped result."""
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        explain_result = {
            "Plan": {"Node Type": "Seq Scan"},
            "Execution Time": 1.234,
        }

        # PostgreSQL returns result wrapped in list
        mock_cursor.fetchone.return_value = [[explain_result]]

        result = run_explain_analyze("SELECT * FROM users")

        assert result == explain_result
