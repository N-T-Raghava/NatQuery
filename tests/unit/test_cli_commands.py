import json
from pathlib import Path
from unittest.mock import patch, call
import natquery.cli.commands as commands
from natquery.cli.commands import connect_command, reset_command, show_config_command


class TestConnectCommand:
    """Test connect_command() function."""

    @patch("natquery.cli.commands.input")
    @patch("natquery.cli.commands.getpass")
    @patch("builtins.print")
    def test_connect_command_creates_config(
        self, mock_print, mock_getpass, mock_input, temp_dir
    ):
        """Test that connect_command creates database-specific config."""
        # Mock user inputs - db_name is the first input
        mock_input.side_effect = [
            "testdb",  # db_name
            "no",  # use_ssl
            "localhost",  # db_host
            "5432",  # db_port
            "testuser",  # db_user
            "groq",  # llm_provider
            "llama3-8b-8192",  # llm_model
        ]
        mock_getpass.side_effect = [
            "testpass",  # db_password
            "test_key",  # llm_api_key
        ]

        base_dir = Path(temp_dir) / ".natquery"
        with patch.object(commands, "BASE_DIR", base_dir):
            connect_command()

            # Check database-specific config was created
            config_file = base_dir / "testdb" / "config.json"
            assert config_file.exists()

            # Check current_db file was set
            assert (base_dir / "current_db").read_text().strip() == "testdb"

            # Check content
            with open(config_file, "r") as f:
                config = json.load(f)

            expected = {
                "db_host": "localhost",
                "db_port": "5432",
                "db_name": "testdb",
                "db_user": "testuser",
                "db_password": "testpass",
                "llm_provider": "groq",
                "llm_api_key": "test_key",
                "llm_model": "llama3-8b-8192",
            }
            assert config == expected


class TestResetCommand:
    """Test reset_command() function."""

    @patch("builtins.print")
    def test_reset_command_deletes_active_db_config(
        self, mock_print, temp_natquery_dir
    ):
        """Test that reset_command deletes active database config."""
        with patch.object(commands.Settings, "BASE_DIR", temp_natquery_dir):
            reset_command()

            # Config should be deleted
            config_file = temp_natquery_dir / "testdb" / "config.json"
            assert not config_file.exists()
            mock_print.assert_called_with("Configuration removed for active database.")

    @patch("builtins.print")
    def test_reset_command_no_active_db(self, mock_print, temp_dir):
        """Test reset_command when no active database is set."""
        base_dir = Path(temp_dir) / ".natquery"
        base_dir.mkdir(exist_ok=True)

        with patch.object(commands.Settings, "BASE_DIR", base_dir):
            reset_command()

            mock_print.assert_called_with("No active database configured.")


class TestShowConfigCommand:
    """Test show_config_command() function."""

    @patch("builtins.print")
    def test_show_config_command_masks_secrets(self, mock_print, temp_natquery_dir):
        """Test that show_config_command masks sensitive fields."""
        with patch.object(commands.Settings, "BASE_DIR", temp_natquery_dir):
            show_config_command()

            # Get the printed JSON
            call_args = mock_print.call_args[0][0]
            displayed_config = json.loads(call_args)

            assert displayed_config["db_password"] == "********"
            assert displayed_config["llm_api_key"] == "********"
            # Other fields should be unchanged
            assert displayed_config["db_host"] == "localhost"
            assert displayed_config["llm_model"] == "llama3-8b-8192"

    @patch("builtins.print")
    def test_show_config_command_no_config(self, mock_print, temp_dir):
        """Test show_config_command when no active database is set."""
        base_dir = Path(temp_dir) / ".natquery"
        base_dir.mkdir(exist_ok=True)

        with patch.object(commands.Settings, "BASE_DIR", base_dir):
            show_config_command()

            mock_print.assert_has_calls(
                [call("NatQuery not configured."), call("Run: natquery connect")]
            )
