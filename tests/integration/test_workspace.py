import pytest
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
from natquery.orchestration.workspace import initialize_workspace


@pytest.mark.integration
class TestInitializeWorkspace:
    """Integration tests for initialize_workspace() function."""

    @patch("natquery.orchestration.workspace.NatQueryLogger.log_event")
    @patch("natquery.orchestration.workspace.close_connection")
    @patch("natquery.orchestration.workspace.extract_schema")
    @patch("natquery.orchestration.workspace.get_connection")
    @patch("natquery.orchestration.workspace.Settings.get_db_config")
    @patch("builtins.print")
    def test_initialize_workspace_creates_dirs(
        self,
        mock_print,
        mock_get_db_config,
        mock_get_connection,
        mock_extract_schema,
        mock_close_connection,
        mock_log_event,
        temp_dir,
    ):
        """Test that workspace creates necessary directories."""
        base_dir = Path(temp_dir) / ".natquery"
        base_dir.mkdir(parents=True)
        config_file = base_dir / "config.json"
        config_file.touch()

        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn
        mock_extract_schema.return_value = {"users": ["id", "name"]}

        with patch(
            "natquery.orchestration.workspace.Path.home", return_value=Path(temp_dir)
        ):
            initialize_workspace()

        # Verify the db_config was retrieved
        mock_get_db_config.assert_called()

        # Verify schema file was created
        schema_file = base_dir / "testdb" / "schema.json"
        assert schema_file.exists()

        with open(schema_file, "r") as f:
            schema = json.load(f)
            assert schema == {"users": ["id", "name"]}

        # Verify logging occurred
        mock_log_event.assert_called_once()
        call_kwargs = mock_log_event.call_args[1]
        assert call_kwargs["level"] == "INFO"
        assert call_kwargs["event"] == "schema_extracted"
        assert call_kwargs["db_name"] == "testdb"

    @patch("natquery.orchestration.workspace.NatQueryLogger.log_event")
    @patch("natquery.orchestration.workspace.close_connection")
    @patch("natquery.orchestration.workspace.extract_schema")
    @patch("natquery.orchestration.workspace.get_connection")
    @patch("natquery.orchestration.workspace.Settings.get_db_config")
    def test_initialize_workspace_no_config(
        self,
        mock_get_db_config,
        mock_get_connection,
        mock_extract_schema,
        mock_close_connection,
        mock_log_event,
        temp_dir,
    ):
        """Test that initialize_workspace handles missing dbname in config."""
        # Simulate config without dbname
        mock_get_db_config.return_value = {}

        try:
            initialize_workspace()
        except KeyError:
            # Expected - dbname key is missing
            pass

    @patch("natquery.orchestration.workspace.NatQueryLogger.log_event")
    @patch("natquery.orchestration.workspace.close_connection")
    @patch("natquery.orchestration.workspace.extract_schema")
    @patch("natquery.orchestration.workspace.get_connection")
    @patch("natquery.orchestration.workspace.Settings.get_db_config")
    def test_initialize_workspace_caches_schema(
        self,
        mock_get_db_config,
        mock_get_connection,
        mock_extract_schema,
        mock_close_connection,
        mock_log_event,
        temp_dir,
    ):
        """Test that schema is not re-extracted if already exists."""
        base_dir = Path(temp_dir) / ".natquery"
        base_dir.mkdir(parents=True)
        config_file = base_dir / "config.json"
        config_file.touch()

        # Create existing schema
        schema_file = base_dir / "testdb" / "schema.json"
        schema_file.parent.mkdir(parents=True)
        with open(schema_file, "w") as f:
            json.dump({"users": ["id"]}, f)

        mock_get_db_config.return_value = {"dbname": "testdb"}

        with patch(
            "natquery.orchestration.workspace.Path.home", return_value=Path(temp_dir)
        ):
            initialize_workspace()

        # Should not call connection/extraction since schema exists
        mock_get_connection.assert_not_called()
        mock_extract_schema.assert_not_called()
        mock_close_connection.assert_not_called()
        mock_log_event.assert_not_called()

    @patch("natquery.orchestration.workspace.NatQueryLogger.log_event")
    @patch("natquery.orchestration.workspace.close_connection")
    @patch("natquery.orchestration.workspace.extract_schema")
    @patch("natquery.orchestration.workspace.get_connection")
    @patch("natquery.orchestration.workspace.Settings.get_db_config")
    def test_initialize_workspace_idempotent(
        self,
        mock_get_db_config,
        mock_get_connection,
        mock_extract_schema,
        mock_close_connection,
        mock_log_event,
        temp_dir,
    ):
        """Test that running initialize_workspace multiple times is safe."""
        base_dir = Path(temp_dir) / ".natquery"
        base_dir.mkdir(parents=True)
        config_file = base_dir / "config.json"
        config_file.touch()

        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn
        mock_extract_schema.return_value = {"users": ["id"]}

        with patch(
            "natquery.orchestration.workspace.Path.home", return_value=Path(temp_dir)
        ):
            # First run - should extract schema
            initialize_workspace()
            assert mock_get_connection.call_count == 1

            # Second run - should skip (schema exists)
            initialize_workspace()
            assert mock_get_connection.call_count == 1  # Still 1, not 2
