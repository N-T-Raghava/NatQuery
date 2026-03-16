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
        """Test that connect_command creates config file with correct structure."""
        # Mock user inputs
        mock_input.side_effect = [
            "localhost",  # db_host
            "5432",  # db_port
            "testdb",  # db_name
            "testuser",  # db_user
            "groq",  # llm_provider
            "llama3-8b-8192",  # llm_model
        ]
        mock_getpass.side_effect = [
            "testpass",  # db_password
            "test_key",  # llm_api_key
        ]

        config_file = Path(temp_dir) / ".natquery" / "config.json"
        with patch.object(commands, "BASE_DIR", Path(temp_dir) / ".natquery"):
            with patch.object(commands, "CONFIG_FILE", config_file):
                connect_command()

                # Check file was created
                assert config_file.exists()

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

                # Check success message printed
                assert mock_print.call_count >= 2
                printed = [c[0][0] if c[0] else "" for c in mock_print.call_args_list]
                assert "\nConfiguration saved successfully." in printed
                assert "You can now run: natquery, to enter thr CLI.\n" in printed


class TestResetCommand:
    """Test reset_command() function."""

    @patch("builtins.print")
    def test_reset_command_deletes_config(self, mock_print, temp_dir):
        """Test that reset_command deletes existing config file."""
        config_file = Path(temp_dir) / ".natquery" / "config.json"
        config_file.parent.mkdir(parents=True, exist_ok=True)
        config_file.touch()

        with patch("natquery.cli.commands.CONFIG_FILE", config_file):
            reset_command()

            assert not config_file.exists()
            mock_print.assert_called_with("Configuration removed.")

    @patch("builtins.print")
    def test_reset_command_no_config(self, mock_print):
        """Test reset_command when no config file exists."""
        with patch.object(commands, "CONFIG_FILE", Path("/nonexistent/config.json")):
            reset_command()

            mock_print.assert_called_with("No configuration found.")


class TestShowConfigCommand:
    """Test show_config_command() function."""

    @patch("builtins.print")
    def test_show_config_command_masks_secrets(
        self, mock_print, sample_config, temp_config_file
    ):
        """Test that show_config_command masks sensitive fields."""
        with patch("natquery.cli.commands.CONFIG_FILE", Path(temp_config_file)):
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
    def test_show_config_command_no_config(self, mock_print):
        """Test show_config_command when config file does not exist."""
        with patch.object(commands, "CONFIG_FILE", Path("/nonexistent/config.json")):
            show_config_command()

            mock_print.assert_has_calls(
                [call("NatQuery not configured."), call("Run: natquery connect")]
            )
