"""Unit tests for orchestration/workspace.py"""

import json
from pathlib import Path
from unittest.mock import patch, MagicMock
from natquery.orchestration.workspace import initialize_workspace


class TestInitializeWorkspace:
    """Test initialize_workspace() function."""

    @patch("natquery.orchestration.workspace.NatQueryLogger")
    @patch("natquery.orchestration.workspace.close_connection")
    @patch("natquery.orchestration.workspace.extract_schema")
    @patch("natquery.orchestration.workspace.get_connection")
    @patch("natquery.orchestration.workspace.Settings.get_db_config")
    def test_initialize_workspace_creates_directory(
        self,
        mock_get_db_config,
        mock_get_conn,
        mock_extract,
        mock_close,
        mock_logger,
        temp_dir,
    ):
        """Test that workspace directory is created."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_extract.return_value = {
            "tables": {
                "users": {
                    "columns": {"id": "integer"},
                    "primary_key": ["id"],
                    "foreign_keys": [],
                }
            }
        }

        with patch.object(Path, "home", return_value=Path(temp_dir)):
            with patch("builtins.print"):
                initialize_workspace()

        expected_dir = Path(temp_dir) / ".natquery" / "testdb"
        assert expected_dir.exists()

    @patch("natquery.orchestration.workspace.NatQueryLogger")
    @patch("natquery.orchestration.workspace.close_connection")
    @patch("natquery.orchestration.workspace.extract_schema")
    @patch("natquery.orchestration.workspace.get_connection")
    @patch("natquery.orchestration.workspace.Settings.get_db_config")
    def test_initialize_workspace_saves_schema(
        self,
        mock_get_db_config,
        mock_get_conn,
        mock_extract,
        mock_close,
        mock_logger,
        temp_dir,
    ):
        """Test that schema is saved to JSON file."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        schema_data = {
            "tables": {
                "users": {
                    "columns": {"id": "integer", "name": "varchar"},
                    "primary_key": ["id"],
                    "foreign_keys": [],
                }
            }
        }
        mock_extract.return_value = schema_data

        with patch.object(Path, "home", return_value=Path(temp_dir)):
            with patch("builtins.print"):
                initialize_workspace()

        schema_file = Path(temp_dir) / ".natquery" / "testdb" / "schema.json"
        assert schema_file.exists()

        loaded_schema = json.loads(schema_file.read_text())
        assert loaded_schema == schema_data

    @patch("natquery.orchestration.workspace.NatQueryLogger")
    @patch("natquery.orchestration.workspace.close_connection")
    @patch("natquery.orchestration.workspace.extract_schema")
    @patch("natquery.orchestration.workspace.get_connection")
    @patch("natquery.orchestration.workspace.Settings.get_db_config")
    def test_initialize_workspace_skips_if_exists(
        self,
        mock_get_db_config,
        mock_get_conn,
        mock_extract,
        mock_close,
        mock_logger,
        temp_dir,
    ):
        """Test that initialization skips if schema already exists."""
        mock_get_db_config.return_value = {"dbname": "testdb"}

        # Pre-create schema file
        schema_dir = Path(temp_dir) / ".natquery" / "testdb"
        schema_dir.mkdir(parents=True, exist_ok=True)
        schema_file = schema_dir / "schema.json"
        existing_schema = {"tables": {"old": {}}}
        schema_file.write_text(json.dumps(existing_schema))

        with patch.object(Path, "home", return_value=Path(temp_dir)):
            with patch("builtins.print"):
                initialize_workspace()

        # Should not call extract_schema or get_connection
        mock_get_conn.assert_not_called()
        mock_extract.assert_not_called()

        # Verify original schema unchanged
        loaded = json.loads(schema_file.read_text())
        assert loaded == existing_schema

    @patch("natquery.orchestration.workspace.NatQueryLogger")
    @patch("natquery.orchestration.workspace.close_connection")
    @patch("natquery.orchestration.workspace.extract_schema")
    @patch("natquery.orchestration.workspace.get_connection")
    @patch("natquery.orchestration.workspace.Settings.get_db_config")
    def test_initialize_workspace_closes_connection(
        self,
        mock_get_db_config,
        mock_get_conn,
        mock_extract,
        mock_close,
        mock_logger,
        temp_dir,
    ):
        """Test that database connection is closed."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_extract.return_value = {"tables": {}}

        with patch.object(Path, "home", return_value=Path(temp_dir)):
            with patch("builtins.print"):
                initialize_workspace()

        mock_close.assert_called_once_with(mock_conn)

    @patch("natquery.orchestration.workspace.NatQueryLogger")
    @patch("natquery.orchestration.workspace.close_connection")
    @patch("natquery.orchestration.workspace.extract_schema")
    @patch("natquery.orchestration.workspace.get_connection")
    @patch("natquery.orchestration.workspace.Settings.get_db_config")
    def test_initialize_workspace_calls_get_connection(
        self,
        mock_get_db_config,
        mock_get_conn,
        mock_extract,
        mock_close,
        mock_logger,
        temp_dir,
    ):
        """Test that get_connection is called."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_extract.return_value = {"tables": {}}

        with patch.object(Path, "home", return_value=Path(temp_dir)):
            with patch("builtins.print"):
                initialize_workspace()

        mock_get_conn.assert_called()

    @patch("natquery.orchestration.workspace.NatQueryLogger")
    @patch("natquery.orchestration.workspace.close_connection")
    @patch("natquery.orchestration.workspace.extract_schema")
    @patch("natquery.orchestration.workspace.get_connection")
    @patch("natquery.orchestration.workspace.Settings.get_db_config")
    def test_initialize_workspace_calls_extract_schema(
        self,
        mock_get_db_config,
        mock_get_conn,
        mock_extract,
        mock_close,
        mock_logger,
        temp_dir,
    ):
        """Test that extract_schema is called with connection."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_extract.return_value = {"tables": {}}

        with patch.object(Path, "home", return_value=Path(temp_dir)):
            with patch("builtins.print"):
                initialize_workspace()

        mock_extract.assert_called_once_with(mock_conn)

    @patch("natquery.orchestration.workspace.NatQueryLogger")
    @patch("natquery.orchestration.workspace.close_connection")
    @patch("natquery.orchestration.workspace.extract_schema")
    @patch("natquery.orchestration.workspace.get_connection")
    @patch("natquery.orchestration.workspace.Settings.get_db_config")
    def test_initialize_workspace_logs_event(
        self,
        mock_get_db_config,
        mock_get_conn,
        mock_extract,
        mock_close,
        mock_logger,
        temp_dir,
    ):
        """Test that schema extraction event is logged."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_extract.return_value = {"tables": {}}

        with patch.object(Path, "home", return_value=Path(temp_dir)):
            with patch("builtins.print"):
                initialize_workspace()

        # Verify logging was called
        assert mock_logger.log_event.called
        call_kwargs = mock_logger.log_event.call_args[1]
        assert call_kwargs["event"] == "schema_extracted"
        assert call_kwargs["db_name"] == "testdb"

    @patch("natquery.orchestration.workspace.NatQueryLogger")
    @patch("natquery.orchestration.workspace.close_connection")
    @patch("natquery.orchestration.workspace.extract_schema")
    @patch("natquery.orchestration.workspace.get_connection")
    @patch("natquery.orchestration.workspace.Settings.get_db_config")
    def test_initialize_workspace_with_complex_schema(
        self,
        mock_get_db_config,
        mock_get_conn,
        mock_extract,
        mock_close,
        mock_logger,
        temp_dir,
    ):
        """Test initialization with complex schema."""
        mock_get_db_config.return_value = {"dbname": "proddb"}

        complex_schema = {
            "tables": {
                "users": {
                    "columns": {"id": "integer", "email": "varchar"},
                    "primary_key": ["id"],
                    "foreign_keys": [],
                },
                "posts": {
                    "columns": {"id": "integer", "user_id": "integer"},
                    "primary_key": ["id"],
                    "foreign_keys": [
                        {
                            "column": "user_id",
                            "references": {"table": "users", "column": "id"},
                        }
                    ],
                },
            }
        }
        mock_extract.return_value = complex_schema
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        with patch.object(Path, "home", return_value=Path(temp_dir)):
            with patch("builtins.print"):
                initialize_workspace()

        schema_file = Path(temp_dir) / ".natquery" / "proddb" / "schema.json"
        loaded = json.loads(schema_file.read_text())
        assert len(loaded["tables"]) == 2
        assert "users" in loaded["tables"]
        assert "posts" in loaded["tables"]

    @patch("natquery.orchestration.workspace.NatQueryLogger")
    @patch("natquery.orchestration.workspace.close_connection")
    @patch("natquery.orchestration.workspace.extract_schema")
    @patch("natquery.orchestration.workspace.get_connection")
    @patch("natquery.orchestration.workspace.Settings.get_db_config")
    def test_initialize_workspace_json_formatting(
        self,
        mock_get_db_config,
        mock_get_conn,
        mock_extract,
        mock_close,
        mock_logger,
        temp_dir,
    ):
        """Test that schema JSON is saved with proper indentation."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        schema = {"tables": {"t1": {}}}
        mock_extract.return_value = schema
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        with patch.object(Path, "home", return_value=Path(temp_dir)):
            with patch("builtins.print"):
                initialize_workspace()

        schema_file = Path(temp_dir) / ".natquery" / "testdb" / "schema.json"
        content = schema_file.read_text()

        # Should be readable with indentation
        assert "  " in content  # Check for indentation
        json.loads(content)  # Should parse successfully
