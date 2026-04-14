import json
import tempfile
from pathlib import Path
from unittest.mock import patch
from natquery.cli.commands import (
    extract_db_name_from_dsn,
    connect_command,
    reset_command,
    show_config_command,
    select_database_command,
    performance_summary_command,
    performance_compare_command,
    performance_slowest_command,
)


class TestExtractDbNameFromDsn:
    """Test extract_db_name_from_dsn() function."""

    def test_extract_url_format(self):
        """Test extracting DB name from URL format DSN."""
        dsn = "postgresql://user:pass@localhost:5432/mydb"
        result = extract_db_name_from_dsn(dsn)
        assert result == "mydb"

    def test_extract_url_with_query_params(self):
        """Test extracting DB name from URL with query parameters."""
        dsn = "postgresql://localhost/mydb?sslmode=require"
        result = extract_db_name_from_dsn(dsn)
        assert result == "mydb"

    def test_extract_keyvalue_format(self):
        """Test extracting DB name from key-value format DSN."""
        dsn = "dbname=mydb user=postgres host=localhost"
        result = extract_db_name_from_dsn(dsn)
        assert result == "mydb"

    def test_extract_invalid_dsn(self):
        """Test extracting from invalid DSN."""
        dsn = "invalid dsn without dbname"
        result = extract_db_name_from_dsn(dsn)
        assert result == "unknown_db"

    def test_extract_empty_url_path(self):
        """Test extracting from URL with empty path."""
        dsn = "postgresql://localhost/"
        result = extract_db_name_from_dsn(dsn)
        assert result == "unknown_db"


class TestConnectCommand:
    """Test connect_command() function."""

    def test_connect_with_dsn(self):
        """Test connect_command with DSN argument."""
        dsn = "postgresql://localhost/testdb"
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("natquery.cli.commands.BASE_DIR", Path(tmpdir)):
                with patch("builtins.print"):
                    with patch("builtins.input") as mock_input:
                        with patch(
                            "natquery.cli.commands.getpass", return_value="api_key"
                        ):
                            mock_input.side_effect = [
                                "groq",  # llm_provider
                                "llama-3",  # llm_model
                            ]
                            connect_command(dsn_arg=dsn)

                            # Check config file was created
                            config_file = Path(tmpdir) / "testdb" / "config.json"
                            assert config_file.exists()

                            with open(config_file) as f:
                                config = json.load(f)
                            assert config["connection_type"] == "dsn"
                            assert config["db_dsn"] == dsn

    def test_connect_interactive_standard(self):
        """Test connect_command with interactive standard connection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("natquery.cli.commands.BASE_DIR", Path(tmpdir)):
                with patch("builtins.input") as mock_input:
                    with patch("natquery.cli.commands.getpass") as mock_getpass:
                        with patch("builtins.print"):
                            mock_input.side_effect = [
                                "mydb",  # db_name
                                "no",  # use_ssl
                                "localhost",  # host
                                "5432",  # port
                                "user",  # user
                                "groq",  # llm_provider
                                "llama-3",  # llm_model
                            ]
                            mock_getpass.side_effect = ["pass", "api_key"]

                            connect_command()

                            # Verify config created
                            config_file = Path(tmpdir) / "mydb" / "config.json"
                            assert config_file.exists()


class TestResetCommand:
    """Test reset_command() function."""

    def test_reset_command_success(self):
        """Test reset_command removes configuration."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_dir = Path(tmpdir) / "testdb"
            db_dir.mkdir(parents=True)
            config_file = db_dir / "config.json"
            config_file.write_text(json.dumps({"test": "data"}))

            with patch(
                "natquery.config.settings.Settings._get_config_path",
                return_value=config_file,
            ):
                with patch("builtins.print"):
                    reset_command()
                    assert not config_file.exists()

    def test_reset_command_not_configured(self):
        """Test reset_command when not configured."""
        with patch(
            "natquery.config.settings.Settings._get_config_path",
            side_effect=RuntimeError("No config"),
        ):
            with patch("builtins.print") as mock_print:
                reset_command()
                assert any(
                    "configured" in str(call) for call in mock_print.call_args_list
                )


class TestShowConfigCommand:
    """Test show_config_command() function."""

    def test_show_config_success(self):
        """Test show_config_command displays config."""
        test_config = {
            "db_name": "testdb",
            "db_password": "secret",
            "llm_api_key": "secret_key",
        }

        with patch(
            "natquery.config.settings.Settings.load_config", return_value=test_config
        ):
            with patch("builtins.print") as mock_print:
                show_config_command()
                # Check that secrets are masked
                output = str(mock_print.call_args_list)
                assert "********" in output

    def test_show_config_not_configured(self):
        """Test show_config_command when not configured."""
        with patch(
            "natquery.config.settings.Settings.load_config",
            side_effect=RuntimeError("No config"),
        ):
            with patch("builtins.print") as mock_print:
                show_config_command()
                assert any(
                    "not configured" in str(call).lower()
                    for call in mock_print.call_args_list
                )


class TestSelectDatabaseCommand:
    """Test select_database_command() function."""

    def test_select_database_no_databases(self):
        """Test select_database_command with no configured databases."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("natquery.cli.commands.BASE_DIR", Path(tmpdir)):
                with patch("builtins.print") as mock_print:
                    select_database_command()
                    assert any(
                        "no configured databases" in str(call).lower()
                        for call in mock_print.call_args_list
                    )

    def test_select_database_success(self):
        """Test select_database_command successfully switches database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)

            # Create test databases
            for db_name in ["db1", "db2", "db3"]:
                db_dir = base / db_name
                db_dir.mkdir()
                (db_dir / "config.json").write_text(json.dumps({"name": db_name}))

            with patch("natquery.cli.commands.BASE_DIR", base):
                with patch("builtins.input", return_value="2"):  # Select db2
                    with patch("builtins.print"):
                        select_database_command()

                        # Check current_db was updated
                        current = (base / "current_db").read_text().strip()
                        assert current == "db2"

    def test_select_database_invalid_input(self):
        """Test select_database_command with invalid input then valid."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)

            db_dir = base / "db1"
            db_dir.mkdir(parents=True)
            (db_dir / "config.json").write_text(json.dumps({"name": "db1"}))

            with patch("natquery.cli.commands.BASE_DIR", base):
                with patch("builtins.input", side_effect=["invalid", "1"]):
                    with patch("builtins.print"):
                        select_database_command()

                        current = (base / "current_db").read_text().strip()
                        assert current == "db1"


class TestPerformanceCommands:
    """Test performance summary/compare/slowest commands."""

    def test_performance_summary_no_data(self):
        """Test performance_summary_command with no data."""
        with patch(
            "natquery.config.settings.Settings.get_db_config",
            return_value={"dbname": "testdb"},
        ):
            with patch("natquery.cli.commands.get_last_run", return_value=None):
                with patch("builtins.print") as mock_print:
                    performance_summary_command()
                    assert any(
                        "no performance data" in str(call).lower()
                        for call in mock_print.call_args_list
                    )

    def test_performance_summary_with_data(self):
        """Test performance_summary_command with data."""
        test_data = {
            "execution_time_ms": 100,
            "total_cost": 50.0,
            "planning_time_ms": 10,
            "suggestions": ["Use index on id", "Avoid full table scan"],
        }

        with patch(
            "natquery.config.settings.Settings.get_db_config",
            return_value={"dbname": "testdb"},
        ):
            with patch("natquery.cli.commands.get_last_run", return_value=test_data):
                with patch("builtins.print") as mock_print:
                    performance_summary_command()
                    output = str(mock_print.call_args_list)
                    assert "100" in output  # execution time

    def test_performance_compare_no_data(self):
        """Test performance_compare_command with no data."""
        with patch(
            "natquery.config.settings.Settings.get_db_config",
            return_value={"dbname": "testdb"},
        ):
            with patch(
                "natquery.cli.commands.compare_last_two_runs", return_value=None
            ):
                with patch("builtins.print") as mock_print:
                    performance_compare_command()
                    assert any(
                        "not enough data" in str(call).lower()
                        for call in mock_print.call_args_list
                    )

    def test_performance_compare_improvement(self):
        """Test performance_compare_command showing improvement."""
        test_data = {
            "execution_time_diff": -50,  # Negative = improvement
            "cost_diff": -10.0,
            "last": {},
            "previous": {},
        }

        with patch(
            "natquery.config.settings.Settings.get_db_config",
            return_value={"dbname": "testdb"},
        ):
            with patch(
                "natquery.cli.commands.compare_last_two_runs", return_value=test_data
            ):
                with patch("builtins.print") as mock_print:
                    performance_compare_command()
                    output = str(mock_print.call_args_list)
                    assert "improved" in output.lower()

    def test_performance_slowest_queries(self):
        """Test performance_slowest_command."""
        test_queries = [
            {"execution_time_ms": 300, "sql": "SELECT * FROM big_table"},
            {"execution_time_ms": 200, "sql": "SELECT * FROM medium_table"},
        ]

        with patch(
            "natquery.config.settings.Settings.get_db_config",
            return_value={"dbname": "testdb"},
        ):
            with patch(
                "natquery.cli.commands.get_slowest_queries", return_value=test_queries
            ):
                with patch("builtins.print") as mock_print:
                    performance_slowest_command()
                    output = str(mock_print.call_args_list)
                    assert "300" in output
