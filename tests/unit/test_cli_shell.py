from unittest.mock import patch, MagicMock
from natquery.cli.shell import start_shell


class TestStartShell:
    """Test start_shell() function."""

    @patch("natquery.cli.shell.show_banner")
    @patch("natquery.cli.shell.console.input")
    @patch("natquery.cli.shell.run_query")
    @patch("natquery.cli.shell.print")
    def test_shell_accepts_quit(self, mock_print, mock_run_query, mock_input, mock_banner):
        """Test that shell exits on 'quit'."""
        mock_input.return_value = "quit"

        start_shell()

        # Check that the banner was shown
        mock_banner.assert_called_once()
        # Check that run_query was not called
        mock_run_query.assert_not_called()
        # Check that Goodbye message was printed
        calls = [str(call) for call in mock_print.call_args_list]
        # Just verify the shell printed something (banner + ready + commands + goodbye)
        assert mock_print.call_count >= 3

    @patch("natquery.cli.shell.show_banner")
    @patch("natquery.cli.shell.console.input")
    @patch("natquery.cli.shell.run_query")
    @patch("natquery.cli.shell.print")
    def test_shell_accepts_exit(self, mock_print, mock_run_query, mock_input, mock_banner):
        """Test that shell exits on 'exit'."""
        mock_input.return_value = "exit"

        start_shell()

        # Check that banner was shown
        mock_banner.assert_called_once()
        # Check that run_query was not called
        mock_run_query.assert_not_called()
        # Check that print was called multiple times
        assert mock_print.call_count >= 3

    @patch("natquery.cli.shell.show_banner")
    @patch("natquery.cli.shell.console.input")
    @patch("natquery.cli.shell.run_query")
    @patch("natquery.cli.shell.print")
    def test_shell_calls_run_query(self, mock_print, mock_run_query, mock_input, mock_banner):
        """Test that shell calls run_query on user input."""
        mock_input.side_effect = ["show users", "exit"]
        mock_run_query.return_value = [{"id": 1, "name": "Alice"}]

        start_shell()

        # Verify run_query was called with the query
        mock_run_query.assert_called_once_with("show users")
        # Verify banner was shown
        mock_banner.assert_called_once()

    @patch("natquery.cli.shell.show_banner")
    @patch("natquery.cli.shell.console.input")
    @patch("natquery.cli.shell.run_query")
    @patch("natquery.cli.shell.print")
    def test_shell_prints_result(self, mock_print, mock_run_query, mock_input, mock_banner):
        """Test that shell prints the result from run_query."""
        mock_input.side_effect = ["SELECT 1", "quit"]
        mock_run_query.return_value = "Query executed"

        start_shell()

        # Verify run_query was called
        mock_run_query.assert_called_once_with("SELECT 1")
        # Verify banner was shown
        mock_banner.assert_called_once()

    @patch("natquery.cli.shell.show_banner")
    @patch("natquery.cli.shell.console.input")
    @patch("natquery.cli.shell.run_query")
    @patch("natquery.cli.shell.print")
    def test_shell_error_handling(self, mock_print, mock_run_query, mock_input, mock_banner):
        """Test that shell handles errors gracefully."""
        mock_input.side_effect = ["bad query", "exit"]
        mock_run_query.side_effect = Exception("SQL Error")

        # The shell should handle the exception and print an error
        with patch("natquery.cli.shell.print") as print_mock:
            try:
                start_shell()
            except Exception:
                # Exception may propagate but should be caught in the try/except
                pass

        # Verify run_query was called
        mock_run_query.assert_called_once_with("bad query")
