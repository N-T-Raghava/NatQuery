import json
import uuid
from pathlib import Path
from unittest.mock import patch, MagicMock
from natquery.observability.logger import NatQueryLogger


class TestGenerateConvId:
    """Test generate_conv_id() method."""

    def test_generate_conv_id_unique(self):
        """Test that generated conversation IDs are unique."""
        id1 = NatQueryLogger.generate_conv_id()
        id2 = NatQueryLogger.generate_conv_id()

        assert id1 != id2
        # Verify it's a valid UUID
        uuid.UUID(id1)
        uuid.UUID(id2)


class TestLogEvent:
    """Test log_event() method."""

    @patch("natquery.observability.logger.datetime")
    @patch("builtins.open", create=True)
    @patch("pathlib.Path.mkdir")
    def test_log_event_file_created(
        self, mock_mkdir, mock_open, mock_datetime, temp_dir
    ):
        """Test that log_event creates file and writes JSONL entry."""
        mock_datetime.now.return_value = MagicMock()
        mock_datetime.now.return_value.isoformat.return_value = "2023-01-01T12:00:00"

        with patch.object(NatQueryLogger, "BASE_DIR", Path(temp_dir) / ".natquery"):
            with patch.object(
                NatQueryLogger,
                "SYSTEM_LOG",
                Path(temp_dir) / ".natquery" / "system.log",
            ):
                NatQueryLogger.log_event(
                    level="INFO",
                    event="test_event",
                    db_name="testdb",
                    conv_id="conv-123",
                    details={"key": "value"},
                )

                mock_open.assert_called_once_with(
                    Path(temp_dir) / ".natquery" / "system.log", "a"
                )
                written_content = (
                    mock_open.return_value.__enter__.return_value.write.call_args[0][0]
                )

                log_entry = json.loads(written_content.strip())
                assert log_entry["timestamp"] == "2023-01-01T12:00:00Z"
                assert log_entry["level"] == "INFO"
                assert log_entry["event"] == "test_event"
                assert log_entry["db_name"] == "testdb"
                assert log_entry["conv_id"] == "conv-123"
                assert log_entry["details"] == {"key": "value"}

    @patch("builtins.open", create=True)
    @patch("pathlib.Path.mkdir")
    def test_log_event_jsonl_format(self, mock_mkdir, mock_open):
        """Test that log entries are written in JSONL format."""
        NatQueryLogger.log_event(level="ERROR", event="error_event")

        written_content = mock_open.return_value.__enter__.return_value.write.call_args[
            0
        ][0]
        assert written_content.endswith("\n")

        # Should be valid JSON
        json.loads(written_content.strip())

    @patch("natquery.observability.logger.datetime")
    @patch("builtins.open", create=True)
    @patch("pathlib.Path.mkdir")
    def test_log_event_timestamp_format(self, mock_mkdir, mock_open, mock_datetime):
        """Test timestamp format in log entries."""
        mock_datetime.now.return_value = MagicMock()
        mock_datetime.now.return_value.isoformat.return_value = "2023-01-01T12:00:00"

        NatQueryLogger.log_event(level="INFO", event="test")

        written_content = mock_open.return_value.__enter__.return_value.write.call_args[
            0
        ][0]
        log_entry = json.loads(written_content.strip())
        assert log_entry["timestamp"] == "2023-01-01T12:00:00Z"

    @patch("builtins.open", create=True)
    @patch("pathlib.Path.mkdir")
    def test_log_event_missing_details(self, mock_mkdir, mock_open):
        """Test log_event with None details."""
        NatQueryLogger.log_event(level="INFO", event="test", details=None)

        written_content = mock_open.return_value.__enter__.return_value.write.call_args[
            0
        ][0]
        log_entry = json.loads(written_content.strip())
        assert log_entry["details"] == {}


class TestLogConversation:
    """Test log_conversation() method."""

    @patch("natquery.observability.logger.datetime")
    @patch("builtins.open", create=True)
    @patch("pathlib.Path.mkdir")
    def test_log_conversation_file_structure(
        self, mock_mkdir, mock_open, mock_datetime, temp_dir
    ):
        """Test that conversation logs are written to correct file structure."""
        mock_datetime.now.return_value = MagicMock()
        mock_datetime.now.return_value.isoformat.return_value = "2023-01-01T12:00:00"

        with patch.object(NatQueryLogger, "BASE_DIR", Path(temp_dir) / ".natquery"):
            NatQueryLogger.log_conversation(
                db_name="testdb",
                conv_id="conv-123",
                user_query="show users",
                generated_sql="SELECT * FROM users",
                rows_returned=5,
                execution_time_ms=150.5,
            )

            expected_path = (
                Path(temp_dir) / ".natquery" / "testdb" / "logs" / "conversations.jsonl"
            )
            mock_open.assert_called_once_with(expected_path, "a")

            written_content = (
                mock_open.return_value.__enter__.return_value.write.call_args[0][0]
            )
            conv_entry = json.loads(written_content.strip())

            assert conv_entry["timestamp"] == "2023-01-01T12:00:00Z"
            assert conv_entry["conv_id"] == "conv-123"
            assert conv_entry["user_query"] == "show users"
            assert conv_entry["generated_sql"] == "SELECT * FROM users"
            assert conv_entry["rows_returned"] == 5
            assert conv_entry["execution_time_ms"] == 150.5
