import pytest
from unittest.mock import patch
from natquery.orchestration.pipeline import run_query


@pytest.mark.integration
class TestRunQuery:
    """Integration tests for run_query() function."""

    @patch("natquery.orchestration.pipeline.NatQueryLogger.log_conversation")
    @patch("natquery.orchestration.pipeline.NatQueryLogger.log_event")
    @patch("natquery.orchestration.pipeline.execute_sql")
    @patch("natquery.orchestration.pipeline.generate_sql")
    @patch("natquery.orchestration.pipeline.Settings.get_db_config")
    @patch("natquery.orchestration.pipeline.NatQueryLogger.generate_conv_id")
    @patch("time.time")
    def test_run_query_full_flow(
        self,
        mock_time,
        mock_generate_conv_id,
        mock_get_db_config,
        mock_generate_sql,
        mock_execute_sql,
        mock_log_event,
        mock_log_conversation,
    ):
        """Test complete query execution flow."""
        # Mock time
        mock_time.side_effect = [1000.0, 1000.150]  # 150ms execution

        # Mock conv_id
        mock_generate_conv_id.return_value = "conv-123"

        # Mock db config
        mock_get_db_config.return_value = {"dbname": "testdb"}

        # Mock SQL generation
        mock_generate_sql.return_value = "SELECT * FROM users"

        # Mock execution
        mock_execute_sql.return_value = [{"id": 1, "name": "Alice"}]

        result = run_query("show all users")

        assert result == [{"id": 1, "name": "Alice"}]

        # Verify conv_id generated
        mock_generate_conv_id.assert_called_once()

        # Verify logging calls
        expected_log_calls = [
            # Query received
            {
                "level": "INFO",
                "event": "query_received",
                "db_name": "testdb",
                "conv_id": "conv-123",
                "details": {"user_query": "show all users"},
            },
            # SQL generated
            {
                "level": "INFO",
                "event": "llm_sql_generated",
                "db_name": "testdb",
                "conv_id": "conv-123",
                "details": {"generated_sql": "SELECT * FROM users"},
            },
            # Execution completed
            {
                "level": "INFO",
                "event": "db_execution_completed",
                "db_name": "testdb",
                "conv_id": "conv-123",
                "details": {"rows_returned": 1, "execution_time_ms": 150.0},
            },
        ]

        assert mock_log_event.call_count == 3
        for call, expected in zip(mock_log_event.call_args_list, expected_log_calls):
            # Compare all fields except execution_time_ms (which has float precision issues)
            call_kwargs = call[1].copy()
            if (
                "details" in call_kwargs
                and "execution_time_ms" in call_kwargs["details"]
            ):
                # Check execution time is approximately correct (within 1ms)
                assert call_kwargs["details"]["execution_time_ms"] == pytest.approx(
                    expected["details"]["execution_time_ms"], abs=1.0
                )
                # Remove it for the rest of the comparison
                call_kwargs["details"] = {
                    k: v
                    for k, v in call_kwargs["details"].items()
                    if k != "execution_time_ms"
                }
                expected_copy = expected.copy()
                expected_copy["details"] = {
                    k: v
                    for k, v in expected_copy["details"].items()
                    if k != "execution_time_ms"
                }
                assert call_kwargs == expected_copy
            else:
                assert call_kwargs == expected

        # Verify conversation logging with approximate execution time
        mock_log_conversation.assert_called_once()
        call_kwargs = mock_log_conversation.call_args[1]
        assert call_kwargs["db_name"] == "testdb"
        assert call_kwargs["conv_id"] == "conv-123"
        assert call_kwargs["user_query"] == "show all users"
        assert call_kwargs["generated_sql"] == "SELECT * FROM users"
        assert call_kwargs["rows_returned"] == 1
        assert call_kwargs["execution_time_ms"] == pytest.approx(150.0, abs=1.0)

    @patch("natquery.orchestration.pipeline.NatQueryLogger.log_event")
    @patch("natquery.orchestration.pipeline.execute_sql")
    @patch("natquery.orchestration.pipeline.generate_sql")
    @patch("natquery.orchestration.pipeline.Settings.get_db_config")
    @patch("natquery.orchestration.pipeline.NatQueryLogger.generate_conv_id")
    def test_run_query_error_propagation(
        self,
        mock_generate_conv_id,
        mock_get_db_config,
        mock_generate_sql,
        mock_execute_sql,
        mock_log_event,
    ):
        """Test that errors during execution are logged and re-raised."""
        mock_generate_conv_id.return_value = "conv-123"
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_generate_sql.return_value = "INVALID SQL"
        mock_execute_sql.side_effect = Exception("SQL syntax error")

        with pytest.raises(Exception, match="SQL syntax error"):
            run_query("bad query")

        # Verify error logging - check that error event was logged with details
        error_logged = False
        for call in mock_log_event.call_args_list:
            if (
                call[1].get("level") == "ERROR"
                and call[1].get("event") == "db_execution_failed"
                and call[1].get("db_name") == "testdb"
                and call[1].get("conv_id") == "conv-123"
                and "error" in call[1].get("details", {})
            ):
                error_logged = True
                break
        assert error_logged, "Error event not properly logged"
