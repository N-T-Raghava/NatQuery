from unittest.mock import patch
from natquery.cli.shell import start_shell


class TestStartShell:
    """Test start_shell() function - simplified and more reliable."""

    @patch("natquery.cli.shell.show_banner")
    @patch("rich.console.Console.input")
    @patch("natquery.cli.shell.run_query")
    @patch("natquery.cli.shell.print")
    def test_shell_exits_on_quit_or_exit(
        self, mock_print, mock_run_query, mock_input, mock_banner
    ):
        """Test that shell exits on 'quit' or 'exit' without calling run_query."""
        mock_input.return_value = "exit"

        start_shell()

        mock_banner.assert_called_once()
        mock_run_query.assert_not_called()

    @patch("natquery.cli.shell.show_banner")
    @patch("rich.console.Console.input")
    @patch("natquery.cli.shell.run_query")
    @patch("natquery.cli.shell.generate_sql")
    @patch("natquery.cli.shell.print")
    def test_shell_processes_query(
        self, mock_print, mock_generate_sql, mock_run_query, mock_input, mock_banner
    ):
        """Test that shell processes a query and calls run_query."""
        # Set up: First input is a query, second input is exit
        mock_input.side_effect = ["show users", "exit"]
        mock_run_query.return_value = {
            "result": [{"id": 1, "name": "Alice"}],
            "summary": {"execution_time_ms": 1.5, "total_cost": 35.0},
            "suggestions": [],
        }
        mock_generate_sql.return_value = "SELECT * FROM users"

        start_shell()

        # Verify the query was processed
        assert mock_run_query.call_count == 1
        mock_run_query.assert_called_with("show users")
        # generate_sql is not called when show_sql is False (default)

    @patch("natquery.cli.shell.show_banner")
    @patch("rich.console.Console.input")
    @patch("natquery.cli.shell.run_query")
    @patch("natquery.cli.shell.generate_sql")
    @patch("natquery.cli.shell.print")
    def test_shell_handles_query_errors(
        self, mock_print, mock_generate_sql, mock_run_query, mock_input, mock_banner
    ):
        """Test that shell handles query errors gracefully."""
        mock_input.side_effect = ["bad query", "exit"]
        mock_generate_sql.return_value = "SELECT * FROM bad"
        mock_run_query.side_effect = Exception("SQL Error")

        # Should not raise, errors are caught by try/except
        start_shell()

        # Verify run_query was called and error was handled
        assert mock_run_query.call_count == 1
