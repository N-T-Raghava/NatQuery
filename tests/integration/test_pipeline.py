import pytest
from unittest.mock import patch
from natquery.orchestration.pipeline import run_query


@pytest.mark.integration
class TestRunQuery:
    """Integration tests for run_query() function."""

    @patch("natquery.orchestration.pipeline.suggest_indexes")
    @patch("natquery.orchestration.pipeline.analyze_cost")
    @patch("natquery.orchestration.pipeline.run_explain_analyze")
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
        mock_run_explain_analyze,
        mock_analyze_cost,
        mock_suggest_indexes,
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

        # Mock performance analysis
        mock_run_explain_analyze.return_value = {
            "Plan": {"Total Cost": 35.00},
            "Execution Time": 0.150,
        }
        mock_analyze_cost.return_value = {
            "summary": {"total_cost": 35.00, "execution_time_ms": 0.150},
            "nodes": [],
        }
        mock_suggest_indexes.return_value = []

        result = run_query("show all users")

        assert result["result"] == [{"id": 1, "name": "Alice"}]
        assert result["summary"] is not None

        # Verify conv_id generated
        mock_generate_conv_id.assert_called_once()

        # Verify logging calls include performance analysis event
        log_events = [call[1]["event"] for call in mock_log_event.call_args_list]
        assert "query_received" in log_events
        assert "llm_sql_generated" in log_events
        assert "db_execution_completed" in log_events
        assert "performance_analysis" in log_events

        # Verify conversation logging with approximate execution time
        mock_log_conversation.assert_called_once()
        call_kwargs = mock_log_conversation.call_args[1]
        assert call_kwargs["db_name"] == "testdb"
        assert call_kwargs["conv_id"] == "conv-123"
        assert call_kwargs["user_query"] == "show all users"
        assert call_kwargs["generated_sql"] == "SELECT * FROM users"
        assert call_kwargs["rows_returned"] == 1
        assert call_kwargs["execution_time_ms"] == pytest.approx(150.0, abs=1.0)

    @patch("natquery.orchestration.pipeline.handle_query_error")
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
        mock_handle_query_error,
    ):
        """Test that errors during execution are logged and re-raised."""
        mock_generate_conv_id.return_value = "conv-123"
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_generate_sql.return_value = "SELECT * FROM users"
        mock_execute_sql.side_effect = Exception("SQL syntax error")
        mock_handle_query_error.side_effect = Exception("SQL syntax error")

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

    @patch("natquery.orchestration.pipeline.NatQueryLogger.log_performance")
    @patch("natquery.orchestration.pipeline.NatQueryLogger.log_event")
    @patch("natquery.orchestration.pipeline.suggest_indexes")
    @patch("natquery.orchestration.pipeline.analyze_cost")
    @patch("natquery.orchestration.pipeline.run_explain_analyze")
    @patch("natquery.orchestration.pipeline.execute_sql")
    @patch("natquery.orchestration.pipeline.generate_sql")
    @patch("natquery.orchestration.pipeline.Settings.get_db_config")
    @patch("natquery.orchestration.pipeline.NatQueryLogger.generate_conv_id")
    @patch("time.time")
    def test_run_query_with_performance_analysis(
        self,
        mock_time,
        mock_generate_conv_id,
        mock_get_db_config,
        mock_generate_sql,
        mock_execute_sql,
        mock_run_explain_analyze,
        mock_analyze_cost,
        mock_suggest_indexes,
        mock_log_event,
        mock_log_performance,
    ):
        """Test full flow with performance analysis."""
        # Mock time
        mock_time.side_effect = [1000.0, 1000.100]  # 100ms execution

        # Setup mocks
        mock_generate_conv_id.return_value = "perf-conv-1"
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_generate_sql.return_value = "SELECT * FROM users WHERE status = 'active'"
        mock_execute_sql.return_value = [{"id": 1, "status": "active"}]

        # Mock EXPLAIN ANALYZE output
        explain_result = {
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "users",
                "Total Cost": 45.00,
                "Plan Rows": 100,
            },
            "Execution Time": 2.345,
            "Planning Time": 0.234,
        }
        mock_run_explain_analyze.return_value = explain_result

        # Mock cost analysis output
        cost_analysis = {
            "summary": {
                "execution_time_ms": 2.345,
                "planning_time_ms": 0.234,
                "total_cost": 45.00,
                "plan_rows": 100,
            },
            "nodes": [
                {
                    "node_type": "Seq Scan",
                    "relation": "users",
                    "columns": ["status"],
                    "total_cost": 45.00,
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
                "reason": "Sequential scan with filter condition",
            }
        ]
        mock_suggest_indexes.return_value = suggestions

        result = run_query("get active users")

        assert result["result"] == [{"id": 1, "status": "active"}]

        # Verify EXPLAIN was called
        mock_run_explain_analyze.assert_called_once_with(
            "SELECT * FROM users WHERE status = 'active'"
        )

        # Verify cost analysis was called
        mock_analyze_cost.assert_called_once_with(explain_result)

        # Verify index suggestions were requested
        mock_suggest_indexes.assert_called_once_with(cost_analysis)

        # Verify performance logging
        mock_log_performance.assert_called_once()
        perf_call_kwargs = mock_log_performance.call_args[1]
        assert perf_call_kwargs["db_name"] == "testdb"
        assert perf_call_kwargs["conv_id"] == "perf-conv-1"
        assert perf_call_kwargs["summary"]["total_cost"] == 45.00

        # Verify performance event was logged
        event_logged = False
        for call in mock_log_event.call_args_list:
            if call[1].get("event") == "performance_analysis":
                event_logged = True
                details = call[1].get("details", {})
                assert details["summary"]["total_cost"] == 45.00
                assert len(details["suggestions"]) == 1
                break
        assert event_logged, "Performance analysis event not logged"

    @patch("natquery.orchestration.pipeline.NatQueryLogger.log_event")
    @patch("natquery.orchestration.pipeline.suggest_indexes")
    @patch("natquery.orchestration.pipeline.analyze_cost")
    @patch("natquery.orchestration.pipeline.run_explain_analyze")
    @patch("natquery.orchestration.pipeline.execute_sql")
    @patch("natquery.orchestration.pipeline.generate_sql")
    @patch("natquery.orchestration.pipeline.Settings.get_db_config")
    @patch("natquery.orchestration.pipeline.NatQueryLogger.generate_conv_id")
    @patch("time.time")
    def test_run_query_performance_analysis_failure_doesnt_block_result(
        self,
        mock_time,
        mock_generate_conv_id,
        mock_get_db_config,
        mock_generate_sql,
        mock_execute_sql,
        mock_run_explain_analyze,
        mock_analyze_cost,
        mock_suggest_indexes,
        mock_log_event,
    ):
        """Test that performance analysis failure doesn't prevent returning results."""
        # Setup mocks
        mock_time.side_effect = [1000.0, 1000.050]
        mock_generate_conv_id.return_value = "conv-perf-fail"
        mock_get_db_config.return_value = {"dbname": "testdb"}
        mock_generate_sql.return_value = "SELECT 1"
        mock_execute_sql.return_value = [{"result": 1}]

        # Performance analysis fails
        mock_run_explain_analyze.side_effect = Exception(
            "EXPLAIN not available for this query"
        )

        # Should not raise - query results should be returned
        result = run_query("select 1")

        assert result["result"] == [{"result": 1}]

        # Should log the performance analysis failure
        error_logged = False
        for call in mock_log_event.call_args_list:
            if call[1].get("event") == "performance_analysis_failed":
                error_logged = True
                break
        assert error_logged, "Performance analysis error not logged"
