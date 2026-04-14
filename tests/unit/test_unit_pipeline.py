"""Unit tests for orchestration/pipeline.py"""

import pytest
from unittest.mock import patch
from natquery.orchestration.pipeline import run_query


class TestRunQuery:
    """Test run_query() function."""

    @patch("natquery.orchestration.pipeline.NatQueryLogger")
    @patch("natquery.orchestration.pipeline.handle_query_error")
    @patch("natquery.orchestration.pipeline.execute_sql")
    @patch("natquery.orchestration.pipeline.validate_sql")
    @patch("natquery.orchestration.pipeline.generate_sql")
    @patch("natquery.orchestration.pipeline.Settings.get_db_config")
    def test_run_query_successful_execution(
        self,
        mock_get_db_config,
        mock_generate_sql,
        mock_validate_sql,
        mock_execute_sql,
        mock_handle_error,
        mock_logger,
    ):
        """Test successful query execution."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_generate_sql.return_value = "SELECT * FROM users;"
        mock_validate_sql.return_value = True
        mock_execute_sql.return_value = [{"id": 1, "name": "John"}]
        mock_logger.generate_conv_id.return_value = "conv-123"

        result = run_query("show users")

        assert result["result"] == [{"id": 1, "name": "John"}]
        assert "summary" in result
        assert "suggestions" in result

    @patch("natquery.orchestration.pipeline.NatQueryLogger")
    @patch("natquery.orchestration.pipeline.handle_query_error")
    @patch("natquery.orchestration.pipeline.execute_sql")
    @patch("natquery.orchestration.pipeline.validate_sql")
    @patch("natquery.orchestration.pipeline.generate_sql")
    @patch("natquery.orchestration.pipeline.Settings.get_db_config")
    def test_run_query_empty_sql_generation(
        self,
        mock_get_db_config,
        mock_generate_sql,
        mock_validate_sql,
        mock_execute_sql,
        mock_handle_error,
        mock_logger,
    ):
        """Test that empty SQL from generator is caught."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_generate_sql.return_value = ""
        mock_logger.generate_conv_id.return_value = "conv-123"

        with pytest.raises(ValueError) as exc_info:
            run_query("query")

        assert "Empty SQL generated" in str(exc_info.value)

    @patch("natquery.orchestration.pipeline.NatQueryLogger")
    @patch("natquery.orchestration.pipeline.handle_query_error")
    @patch("natquery.orchestration.pipeline.execute_sql")
    @patch("natquery.orchestration.pipeline.validate_sql")
    @patch("natquery.orchestration.pipeline.generate_sql")
    @patch("natquery.orchestration.pipeline.Settings.get_db_config")
    def test_run_query_validation_failure_triggers_correction(
        self,
        mock_get_db_config,
        mock_generate_sql,
        mock_validate_sql,
        mock_execute_sql,
        mock_handle_error,
        mock_logger,
    ):
        """Test that validation failure triggers error handling."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_generate_sql.return_value = "BAD SQL"
        mock_validate_sql.side_effect = ValueError("Invalid SQL")
        mock_execute_sql.return_value = []
        mock_handle_error.return_value = "SELECT * FROM users;"
        mock_logger.generate_conv_id.return_value = "conv-123"

        run_query("query")

        # After correction, should execute corrected SQL
        assert mock_handle_error.called

    @patch("natquery.orchestration.pipeline.NatQueryLogger")
    @patch("natquery.orchestration.pipeline.handle_query_error")
    @patch("natquery.orchestration.pipeline.execute_sql")
    @patch("natquery.orchestration.pipeline.validate_sql")
    @patch("natquery.orchestration.pipeline.generate_sql")
    @patch("natquery.orchestration.pipeline.Settings.get_db_config")
    def test_run_query_execution_failure_triggers_correction(
        self,
        mock_get_db_config,
        mock_generate_sql,
        mock_validate_sql,
        mock_execute_sql,
        mock_handle_error,
        mock_logger,
    ):
        """Test that execution failure triggers error handling and retry."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_generate_sql.return_value = "SELECT * FROM bad_table;"
        mock_validate_sql.return_value = True
        mock_execute_sql.side_effect = [
            Exception("table does not exist"),  # First execution fails
            [{"id": 1}],  # Second execution (after correction) succeeds
        ]
        mock_handle_error.return_value = "SELECT * FROM users;"
        mock_logger.generate_conv_id.return_value = "conv-123"

        result = run_query("get data")

        assert result["result"] == [{"id": 1}]
        mock_handle_error.assert_called_once()

    @patch("natquery.orchestration.pipeline.NatQueryLogger")
    @patch("natquery.orchestration.pipeline.handle_query_error")
    @patch("natquery.orchestration.pipeline.execute_sql")
    @patch("natquery.orchestration.pipeline.validate_sql")
    @patch("natquery.orchestration.pipeline.generate_sql")
    @patch("natquery.orchestration.pipeline.Settings.get_db_config")
    def test_run_query_logs_events(
        self,
        mock_get_db_config,
        mock_generate_sql,
        mock_validate_sql,
        mock_execute_sql,
        mock_handle_error,
        mock_logger,
    ):
        """Test that query execution is logged."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_generate_sql.return_value = "SELECT * FROM users;"
        mock_validate_sql.return_value = True
        mock_execute_sql.return_value = [{"id": 1}]
        mock_logger.generate_conv_id.return_value = "conv-000"

        run_query("test query")

        # Check that events were logged
        log_calls = mock_logger.log_event.call_args_list
        assert len(log_calls) > 0

        # Should log query_received and other events
        event_names = [call[1]["event"] for call in log_calls]
        assert "query_received" in event_names or any(
            "received" in name for name in event_names
        )

    @patch("natquery.orchestration.pipeline.NatQueryLogger")
    @patch("natquery.orchestration.pipeline.handle_query_error")
    @patch("natquery.orchestration.pipeline.execute_sql")
    @patch("natquery.orchestration.pipeline.validate_sql")
    @patch("natquery.orchestration.pipeline.generate_sql")
    @patch("natquery.orchestration.pipeline.Settings.get_db_config")
    def test_run_query_logs_conversation(
        self,
        mock_get_db_config,
        mock_generate_sql,
        mock_validate_sql,
        mock_execute_sql,
        mock_handle_error,
        mock_logger,
    ):
        """Test that conversation is logged at end."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_generate_sql.return_value = "SELECT id FROM users;"
        mock_validate_sql.return_value = True
        mock_execute_sql.return_value = [{"id": 1}, {"id": 2}]
        mock_logger.generate_conv_id.return_value = "conv-111"

        run_query("get user ids")

        # Check log_conversation was called
        mock_logger.log_conversation.assert_called_once()
        call_kwargs = mock_logger.log_conversation.call_args[1]
        assert call_kwargs["db_name"] == "testdb"
        assert call_kwargs["conv_id"] == "conv-111"
        assert call_kwargs["rows_returned"] == 2

    @patch("natquery.orchestration.pipeline.NatQueryLogger")
    @patch("natquery.orchestration.pipeline.handle_query_error")
    @patch("natquery.orchestration.pipeline.execute_sql")
    @patch("natquery.orchestration.pipeline.validate_sql")
    @patch("natquery.orchestration.pipeline.generate_sql")
    @patch("natquery.orchestration.pipeline.Settings.get_db_config")
    def test_run_query_empty_result_handled(
        self,
        mock_get_db_config,
        mock_generate_sql,
        mock_validate_sql,
        mock_execute_sql,
        mock_handle_error,
        mock_logger,
    ):
        """Test handling of empty query results."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_generate_sql.return_value = "SELECT * FROM empty_table;"
        mock_validate_sql.return_value = True
        mock_execute_sql.return_value = []
        mock_logger.generate_conv_id.return_value = "conv-222"

        result = run_query("get data from empty table")

        assert result["result"] == []
        # Should still log with 0 rows
        call_kwargs = mock_logger.log_conversation.call_args[1]
        assert call_kwargs["rows_returned"] == 0

    @patch("natquery.orchestration.pipeline.NatQueryLogger")
    @patch("natquery.orchestration.pipeline.handle_query_error")
    @patch("natquery.orchestration.pipeline.execute_sql")
    @patch("natquery.orchestration.pipeline.validate_sql")
    @patch("natquery.orchestration.pipeline.generate_sql")
    @patch("natquery.orchestration.pipeline.Settings.get_db_config")
    @patch("natquery.orchestration.pipeline.time.time")
    def test_run_query_measures_execution_time(
        self,
        mock_time,
        mock_get_db_config,
        mock_generate_sql,
        mock_validate_sql,
        mock_execute_sql,
        mock_handle_error,
        mock_logger,
    ):
        """Test that execution time is measured."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_generate_sql.return_value = "SELECT 1;"
        mock_validate_sql.return_value = True
        mock_execute_sql.return_value = [{"result": 1}]
        mock_logger.generate_conv_id.return_value = "conv-333"
        mock_time.side_effect = [100.0, 100.5]  # 500ms execution

        run_query("select 1")

        call_kwargs = mock_logger.log_conversation.call_args[1]
        # Should be around 500ms
        assert "execution_time_ms" in call_kwargs

    @patch("natquery.orchestration.pipeline.NatQueryLogger")
    @patch("natquery.orchestration.pipeline.handle_query_error")
    @patch("natquery.orchestration.pipeline.execute_sql")
    @patch("natquery.orchestration.pipeline.validate_sql")
    @patch("natquery.orchestration.pipeline.generate_sql")
    @patch("natquery.orchestration.pipeline.Settings.get_db_config")
    def test_run_query_calls_generate_before_validate(
        self,
        mock_get_db_config,
        mock_generate_sql,
        mock_validate_sql,
        mock_execute_sql,
        mock_handle_error,
        mock_logger,
    ):
        """Test that SQL is generated before validation."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_generate_sql.return_value = "SELECT COUNT(*) FROM users;"
        mock_validate_sql.return_value = True
        mock_execute_sql.return_value = [{"count": 42}]
        mock_logger.generate_conv_id.return_value = "conv-444"

        run_query("count users")

        # Verify generate was called first
        assert mock_generate_sql.called
        assert mock_validate_sql.called

    @patch("natquery.orchestration.pipeline.NatQueryLogger")
    @patch("natquery.orchestration.pipeline.handle_query_error")
    @patch("natquery.orchestration.pipeline.execute_sql")
    @patch("natquery.orchestration.pipeline.validate_sql")
    @patch("natquery.orchestration.pipeline.generate_sql")
    @patch("natquery.orchestration.pipeline.Settings.get_db_config")
    def test_run_query_generation_failure_logged_and_raised(
        self,
        mock_get_db_config,
        mock_generate_sql,
        mock_validate_sql,
        mock_execute_sql,
        mock_handle_error,
        mock_logger,
    ):
        """Test that LLM generation failures are logged and raised."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_generate_sql.side_effect = Exception("LLM API error")
        mock_logger.generate_conv_id.return_value = "conv-555"

        with pytest.raises(Exception) as exc_info:
            run_query("query")

        assert "LLM API error" in str(exc_info.value)

    @patch("natquery.orchestration.pipeline.NatQueryLogger")
    @patch("natquery.orchestration.pipeline.suggest_indexes")
    @patch("natquery.orchestration.pipeline.analyze_cost")
    @patch("natquery.orchestration.pipeline.run_explain_analyze")
    @patch("natquery.orchestration.pipeline.handle_query_error")
    @patch("natquery.orchestration.pipeline.execute_sql")
    @patch("natquery.orchestration.pipeline.validate_sql")
    @patch("natquery.orchestration.pipeline.generate_sql")
    @patch("natquery.orchestration.pipeline.Settings.get_db_config")
    def test_run_query_performance_analysis_on_success(
        self,
        mock_get_db_config,
        mock_generate_sql,
        mock_validate_sql,
        mock_execute_sql,
        mock_handle_error,
        mock_run_explain_analyze,
        mock_analyze_cost,
        mock_suggest_indexes,
        mock_logger,
    ):
        """Test that performance analysis runs after successful execution."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_generate_sql.return_value = "SELECT * FROM users WHERE status = 'active';"
        mock_validate_sql.return_value = True
        mock_execute_sql.return_value = [{"id": 1}]
        mock_logger.generate_conv_id.return_value = "conv-perf-1"

        # Mock explain output
        explain_result = {
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "users",
                "Total Cost": 45.00,
            },
            "Execution Time": 2.345,
        }
        mock_run_explain_analyze.return_value = explain_result

        # Mock cost analysis
        cost_analysis = {
            "summary": {
                "total_cost": 45.00,
                "execution_time_ms": 2.345,
                "planning_time_ms": 0.234,
                "plan_rows": 100,
            },
            "nodes": [
                {
                    "node_type": "Seq Scan",
                    "relation": "users",
                    "columns": ["status"],
                }
            ],
        }
        mock_analyze_cost.return_value = cost_analysis

        # Mock index suggestions
        suggestions = [
            {
                "type": "index",
                "table": "users",
                "columns": ["status"],
                "sql": "CREATE INDEX idx_users_status ON users(status);",
            }
        ]
        mock_suggest_indexes.return_value = suggestions

        result = run_query("get active users")

        assert result["result"] == [{"id": 1}]
        # Performance analysis should be called
        mock_run_explain_analyze.assert_called_once()
        mock_analyze_cost.assert_called_once_with(explain_result)
        mock_suggest_indexes.assert_called_once_with(cost_analysis)

    @patch("natquery.orchestration.pipeline.NatQueryLogger")
    @patch("natquery.orchestration.pipeline.suggest_indexes")
    @patch("natquery.orchestration.pipeline.analyze_cost")
    @patch("natquery.orchestration.pipeline.run_explain_analyze")
    @patch("natquery.orchestration.pipeline.handle_query_error")
    @patch("natquery.orchestration.pipeline.execute_sql")
    @patch("natquery.orchestration.pipeline.validate_sql")
    @patch("natquery.orchestration.pipeline.generate_sql")
    @patch("natquery.orchestration.pipeline.Settings.get_db_config")
    def test_run_query_performance_analysis_logged(
        self,
        mock_get_db_config,
        mock_generate_sql,
        mock_validate_sql,
        mock_execute_sql,
        mock_handle_error,
        mock_run_explain_analyze,
        mock_analyze_cost,
        mock_suggest_indexes,
        mock_logger,
    ):
        """Test that performance analysis results are logged."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_generate_sql.return_value = "SELECT * FROM users;"
        mock_validate_sql.return_value = True
        mock_execute_sql.return_value = []
        mock_logger.generate_conv_id.return_value = "conv-perf-2"

        explain_result = {"Plan": {"Total Cost": 50.0}, "Execution Time": 3.0}
        mock_run_explain_analyze.return_value = explain_result

        cost_analysis = {
            "summary": {"total_cost": 50.0, "execution_time_ms": 3.0},
            "nodes": [],
        }
        mock_analyze_cost.return_value = cost_analysis
        mock_suggest_indexes.return_value = []

        run_query("get users")

        # Should log performance event
        log_calls = mock_logger.log_event.call_args_list
        event_names = [call[1]["event"] for call in log_calls]
        assert "performance_analysis" in event_names

    @patch("natquery.orchestration.pipeline.NatQueryLogger")
    @patch("natquery.orchestration.pipeline.suggest_indexes")
    @patch("natquery.orchestration.pipeline.analyze_cost")
    @patch("natquery.orchestration.pipeline.run_explain_analyze")
    @patch("natquery.orchestration.pipeline.handle_query_error")
    @patch("natquery.orchestration.pipeline.execute_sql")
    @patch("natquery.orchestration.pipeline.validate_sql")
    @patch("natquery.orchestration.pipeline.generate_sql")
    @patch("natquery.orchestration.pipeline.Settings.get_db_config")
    def test_run_query_performance_analysis_failure_handled(
        self,
        mock_get_db_config,
        mock_generate_sql,
        mock_validate_sql,
        mock_execute_sql,
        mock_handle_error,
        mock_run_explain_analyze,
        mock_analyze_cost,
        mock_suggest_indexes,
        mock_logger,
    ):
        """Test that performance analysis failure doesn't crash query."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_generate_sql.return_value = "SELECT 1;"
        mock_validate_sql.return_value = True
        mock_execute_sql.return_value = [{"result": 1}]
        mock_logger.generate_conv_id.return_value = "conv-perf-3"

        # EXPLAIN fails
        mock_run_explain_analyze.side_effect = Exception("EXPLAIN not supported")

        # Should not raise - should handle gracefully
        result = run_query("select 1")

        assert result["result"] == [{"result": 1}]
        # Should log the error
        log_calls = mock_logger.log_event.call_args_list
        event_names = [call[1]["event"] for call in log_calls]
        assert "performance_analysis_failed" in event_names

    @patch("natquery.orchestration.pipeline.NatQueryLogger")
    @patch("natquery.orchestration.pipeline.suggest_indexes")
    @patch("natquery.orchestration.pipeline.analyze_cost")
    @patch("natquery.orchestration.pipeline.run_explain_analyze")
    @patch("natquery.orchestration.pipeline.handle_query_error")
    @patch("natquery.orchestration.pipeline.execute_sql")
    @patch("natquery.orchestration.pipeline.validate_sql")
    @patch("natquery.orchestration.pipeline.generate_sql")
    @patch("natquery.orchestration.pipeline.Settings.get_db_config")
    def test_run_query_logs_performance_summary(
        self,
        mock_get_db_config,
        mock_generate_sql,
        mock_validate_sql,
        mock_execute_sql,
        mock_handle_error,
        mock_run_explain_analyze,
        mock_analyze_cost,
        mock_suggest_indexes,
        mock_logger,
    ):
        """Test that performance summary is logged with all metrics."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_generate_sql.return_value = "SELECT * FROM large_table WHERE id > 1000;"
        mock_validate_sql.return_value = True
        mock_execute_sql.return_value = [{"id": 1001}]
        mock_logger.generate_conv_id.return_value = "conv-perf-4"

        explain_result = {
            "Plan": {"Total Cost": 100.0, "Plan Rows": 5000},
            "Execution Time": 10.5,
            "Planning Time": 0.5,
        }
        mock_run_explain_analyze.return_value = explain_result

        cost_analysis = {
            "summary": {
                "total_cost": 100.0,
                "plan_rows": 5000,
                "execution_time_ms": 10.5,
                "planning_time_ms": 0.5,
            },
            "nodes": [],
        }
        mock_analyze_cost.return_value = cost_analysis
        mock_suggest_indexes.return_value = []

        run_query("get large table")

        # Verify log_performance was called with correct data
        mock_logger.log_performance.assert_called_once()
        call_kwargs = mock_logger.log_performance.call_args[1]
        assert call_kwargs["summary"]["total_cost"] == 100.0
        assert call_kwargs["summary"]["plan_rows"] == 5000
        # Should have logged the error
        assert mock_logger.log_event.called

    @patch("natquery.orchestration.pipeline.NatQueryLogger")
    @patch("natquery.orchestration.pipeline.handle_query_error")
    @patch("natquery.orchestration.pipeline.execute_sql")
    @patch("natquery.orchestration.pipeline.validate_sql")
    @patch("natquery.orchestration.pipeline.generate_sql")
    @patch("natquery.orchestration.pipeline.Settings.get_db_config")
    def test_run_query_with_complex_result(
        self,
        mock_get_db_config,
        mock_generate_sql,
        mock_validate_sql,
        mock_execute_sql,
        mock_handle_error,
        mock_logger,
    ):
        """Test with complex multi-row result."""
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_generate_sql.return_value = "SELECT id, name, email FROM users;"
        mock_validate_sql.return_value = True

        complex_result = [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"},
            {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
        ]
        mock_execute_sql.return_value = complex_result
        mock_logger.generate_conv_id.return_value = "conv-666"

        result = run_query("get all user data")

        assert result["result"] == complex_result
        assert len(result) == 3
