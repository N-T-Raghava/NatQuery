from unittest.mock import patch
from natquery.config.settings import Settings


class TestMain:
    """Test main() entry point."""

    @patch("sys.argv", ["natquery"])
    @patch.object(Settings, "exists", return_value=False)
    def test_main_no_args_not_configured(self, mock_settings):
        """Test main() when no args and not configured."""
        from natquery.cli.main import main

        with patch("natquery.cli.main.print") as mock_print:
            main()
            # Should print not configured message
            assert any(
                "configured" in str(call).lower() for call in mock_print.call_args_list
            )

    @patch("sys.argv", ["natquery"])
    @patch.object(Settings, "exists", return_value=True)
    def test_main_no_args_configured(self, mock_settings):
        """Test main() when no args and configured."""
        from natquery.cli.main import main

        with patch("natquery.cli.main.start_shell") as mock_shell:
            with patch("natquery.cli.main.initialize_workspace") as mock_init:
                main()
                # Should initialize workspace and start shell
                mock_init.assert_called_once()
                mock_shell.assert_called_once()

    @patch("sys.argv", ["natquery", "connect"])
    def test_main_connect_command_no_dsn(self):
        """Test natquery connect command without DSN."""
        from natquery.cli.main import main

        with patch("natquery.cli.main.connect_command") as mock_connect:
            main()
            mock_connect.assert_called_once_with(None)

    @patch("sys.argv", ["natquery", "connect", "postgresql://localhost/testdb"])
    def test_main_connect_command_with_dsn(self):
        """Test natquery connect command with DSN."""
        from natquery.cli.main import main

        with patch("natquery.cli.main.connect_command") as mock_connect:
            main()
            dsn = "postgresql://localhost/testdb"
            mock_connect.assert_called_once_with(dsn)

    @patch("sys.argv", ["natquery", "reset"])
    def test_main_reset_command(self):
        """Test natquery reset command."""
        from natquery.cli.main import main

        with patch("natquery.cli.main.reset_command") as mock_reset:
            main()
            mock_reset.assert_called_once()

    @patch("sys.argv", ["natquery", "config"])
    def test_main_config_command(self):
        """Test natquery config command."""
        from natquery.cli.main import main

        with patch("natquery.cli.main.show_config_command") as mock_config:
            main()
            mock_config.assert_called_once()

    @patch("sys.argv", ["natquery", "select"])
    def test_main_select_command(self):
        """Test natquery select command."""
        from natquery.cli.main import main

        with patch("natquery.cli.main.select_database_command") as mock_select:
            main()
            mock_select.assert_called_once()

    @patch("sys.argv", ["natquery", "unknown"])
    def test_main_unknown_command(self):
        """Test unknown command."""
        from natquery.cli.main import main

        with patch("natquery.cli.main.print") as mock_print:
            main()
            # Should print unknown command error
            assert any(
                "unknown" in str(call).lower() for call in mock_print.call_args_list
            )
