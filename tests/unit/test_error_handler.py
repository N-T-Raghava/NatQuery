"""Unit tests for orchestration/error_handler.py"""

import pytest
from unittest.mock import patch
from natquery.orchestration.error_handler import handle_query_error


class TestHandleQueryError:
    """Test handle_query_error() function."""

    @patch("natquery.orchestration.error_handler.NatQueryLogger")
    @patch("natquery.orchestration.error_handler.validate_sql")
    @patch("natquery.orchestration.error_handler.correct_sql")
    @patch("natquery.orchestration.error_handler.classify_error")
    def test_successful_correction_first_attempt(
        self,
        mock_classify,
        mock_correct_sql,
        mock_validate,
        mock_logger,
    ):
        """Test successful correction on first attempt."""
        mock_classify.return_value = "SYNTAX_ERROR"
        mock_correct_sql.return_value = "SELECT * FROM users;"
        mock_validate.return_value = True

        result = handle_query_error(
            user_query="show users",
            sql="SELEC * FROM users",
            error=Exception("syntax error"),
            conv_id="conv-123",
            db_name="testdb",
            max_retries=3,
        )

        assert result == "SELECT * FROM users;"
        mock_correct_sql.assert_called_once()
        mock_validate.assert_called_once_with("SELECT * FROM users;")
        assert mock_logger.log_event.call_count >= 1

    @patch("natquery.orchestration.error_handler.NatQueryLogger")
    @patch("natquery.orchestration.error_handler.validate_sql")
    @patch("natquery.orchestration.error_handler.correct_sql")
    @patch("natquery.orchestration.error_handler.classify_error")
    def test_multiple_correction_attempts(
        self,
        mock_classify,
        mock_correct_sql,
        mock_validate,
        mock_logger,
    ):
        """Test multiple correction attempts."""
        mock_classify.return_value = "UNDEFINED_COLUMN"
        # First attempt fails validation, second succeeds
        mock_correct_sql.side_effect = [
            "SELECT wrong_col FROM users;",
            "SELECT id, name FROM users;",
        ]
        mock_validate.side_effect = [
            ValueError("Invalid column"),
            True,
        ]

        result = handle_query_error(
            user_query="get user data",
            sql="SELECT user_id FROM users;",
            error=Exception('column "user_id" does not exist'),
            conv_id="conv-456",
            db_name="testdb",
            max_retries=3,
        )

        assert result == "SELECT id, name FROM users;"
        assert mock_correct_sql.call_count == 2
        assert mock_validate.call_count == 2

    @patch("natquery.orchestration.error_handler.NatQueryLogger")
    @patch("natquery.orchestration.error_handler.validate_sql")
    @patch("natquery.orchestration.error_handler.correct_sql")
    @patch("natquery.orchestration.error_handler.classify_error")
    def test_max_retries_exceeded(
        self,
        mock_classify,
        mock_correct_sql,
        mock_validate,
        mock_logger,
    ):
        """Test that exception is raised when max retries exceeded."""
        mock_classify.return_value = "SYNTAX_ERROR"
        mock_correct_sql.side_effect = [
            ValueError("Still invalid"),
            ValueError("Still invalid"),
            ValueError("Still invalid"),
        ]
        mock_validate.side_effect = ValueError("Invalid")

        with pytest.raises(RuntimeError) as exc_info:
            handle_query_error(
                user_query="bad query",
                sql="INVALID SQL",
                error=Exception("syntax error"),
                conv_id="conv-789",
                db_name="testdb",
                max_retries=3,
            )

        assert "Max retries reached" in str(exc_info.value)
        assert mock_correct_sql.call_count == 3

    @patch("natquery.orchestration.error_handler.NatQueryLogger")
    @patch("natquery.orchestration.error_handler.validate_sql")
    @patch("natquery.orchestration.error_handler.correct_sql")
    @patch("natquery.orchestration.error_handler.classify_error")
    def test_empty_corrected_sql_raises_error(
        self,
        mock_classify,
        mock_correct_sql,
        mock_validate,
        mock_logger,
    ):
        """Test that empty SQL from corrector raises error after max retries."""
        mock_classify.return_value = "UNDEFINED_TABLE"
        mock_correct_sql.return_value = ""  # Empty response

        # Empty SQL triggers ValueError which caught and retried
        # After max retries exhausted, raises RuntimeError
        with pytest.raises(RuntimeError) as exc_info:
            handle_query_error(
                user_query="query",
                sql="SELECT * FROM bad_table;",
                error=Exception('table "bad_table" does not exist'),
                conv_id="conv-000",
                db_name="testdb",
                max_retries=3,
            )

        assert "Max retries reached" in str(exc_info.value)

    @patch("natquery.orchestration.error_handler.NatQueryLogger")
    @patch("natquery.orchestration.error_handler.validate_sql")
    @patch("natquery.orchestration.error_handler.correct_sql")
    @patch("natquery.orchestration.error_handler.classify_error")
    def test_logging_on_failed_correction(
        self,
        mock_classify,
        mock_correct_sql,
        mock_validate,
        mock_logger,
    ):
        """Test that logging occurs on correction failure."""
        mock_classify.return_value = "JOIN_ERROR"
        mock_correct_sql.side_effect = RuntimeError("LLM error")
        mock_validate.return_value = True

        with pytest.raises(RuntimeError):
            handle_query_error(
                user_query="complex query",
                sql="SELECT * FROM users WHERE x;",
                error=Exception("missing FROM clause"),
                conv_id="conv-log",
                db_name="proddb",
                max_retries=2,
            )

        # Check that WARNING level logging was called
        warning_calls = [
            call_item
            for call_item in mock_logger.log_event.call_args_list
            if "WARNING" in str(call_item)
        ]
        assert len(warning_calls) > 0

    @patch("natquery.orchestration.error_handler.NatQueryLogger")
    @patch("natquery.orchestration.error_handler.validate_sql")
    @patch("natquery.orchestration.error_handler.correct_sql")
    @patch("natquery.orchestration.error_handler.classify_error")
    def test_corrected_sql_tracked_through_retries(
        self,
        mock_classify,
        mock_correct_sql,
        mock_validate,
        mock_logger,
    ):
        """Test that corrected SQL is tracked through retry attempts."""
        mock_classify.return_value = "TYPE_ERROR"
        # Each attempt produces different SQL
        corrections = [
            "SELECT id::text FROM users;",
            "SELECT id FROM users WHERE active = true;",
            "SELECT id, name FROM users WHERE status = 1;",
        ]
        mock_correct_sql.side_effect = corrections
        mock_validate.side_effect = [
            ValueError("Type mismatch"),
            ValueError("Unknown column"),
            True,
        ]

        result = handle_query_error(
            user_query="filter users",
            sql="SELECT id FROM users WHERE status = '1';",
            error=Exception("operator does not exist"),
            conv_id="conv-track",
            db_name="testdb",
            max_retries=3,
        )

        assert result == corrections[2]
        assert mock_correct_sql.call_count == 3

    @patch("natquery.orchestration.error_handler.NatQueryLogger")
    @patch("natquery.orchestration.error_handler.validate_sql")
    @patch("natquery.orchestration.error_handler.correct_sql")
    @patch("natquery.orchestration.error_handler.classify_error")
    def test_initialization_with_none_handled(
        self,
        mock_classify,
        mock_correct_sql,
        mock_validate,
        mock_logger,
    ):
        """Test that corrected_sql initialization doesn't cause errors."""
        mock_classify.return_value = "UNKNOWN"
        mock_correct_sql.return_value = "SELECT * FROM users;"
        mock_validate.return_value = True

        # Should not raise an error about undefined corrected_sql
        result = handle_query_error(
            user_query="query",
            sql="BAD SQL",
            error=Exception("some error"),
            conv_id="conv-init",
            db_name="testdb",
            max_retries=1,
        )

        assert result == "SELECT * FROM users;"
