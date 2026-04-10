"""Unit tests for orchestration/error_classifier.py"""

from natquery.orchestration.error_classifier import classify_error


class TestClassifyError:
    """Test classify_error() function."""

    def test_classify_syntax_error(self):
        """Test classification of syntax errors."""
        assert classify_error("syntax error at or near 'SELECT'") == "SYNTAX_ERROR"
        assert classify_error("SYNTAX ERROR in SQL statement") == "SYNTAX_ERROR"

    def test_classify_undefined_column(self):
        """Test classification of undefined column errors."""
        error_msg = 'column "user_id" does not exist'
        assert classify_error(error_msg) == "UNDEFINED_COLUMN"

    def test_classify_undefined_table(self):
        """Test classification of undefined table errors."""
        error_msg = 'relation "nonexistent_table" does not exist'
        assert classify_error(error_msg) == "UNDEFINED_TABLE"

        error_msg2 = 'table "users" does not exist'
        assert classify_error(error_msg2) == "UNDEFINED_TABLE"

    def test_classify_ambiguous_column(self):
        """Test classification of ambiguous column errors."""
        error_msg = 'column reference "id" is ambiguous'
        assert classify_error(error_msg) == "AMBIGUOUS_COLUMN"

    def test_classify_type_error(self):
        """Test classification of type mismatch errors."""
        error_msg = "operator does not exist: integer = character varying"
        assert classify_error(error_msg) == "TYPE_ERROR"

        error_msg2 = "type mismatch in expression"
        assert classify_error(error_msg2) == "TYPE_ERROR"

    def test_classify_group_by_error(self):
        """Test classification of GROUP BY errors."""
        error_msg = "column must appear in the group by clause or be used in an aggregate function"
        assert classify_error(error_msg) == "GROUP_BY_ERROR"

    def test_classify_join_error(self):
        """Test classification of JOIN/missing FROM errors."""
        error_msg = "missing FROM clause in WHERE statement"
        assert classify_error(error_msg) == "JOIN_ERROR"

    def test_classify_unknown_error(self):
        """Test classification of unknown errors."""
        assert classify_error("some random error") == "UNKNOWN"
        assert classify_error("") == "UNKNOWN"
        assert classify_error("could not find function") == "UNKNOWN"

    def test_classify_case_insensitive(self):
        """Test that classification is case insensitive."""
        assert classify_error("SYNTAX ERROR AT OR NEAR 'SELECT'") == "SYNTAX_ERROR"
        assert classify_error("Syntax Error") == "SYNTAX_ERROR"
        assert classify_error("SyntaX eRRor") == "SYNTAX_ERROR"

    def test_classify_complex_error_messages(self):
        """Test classification of complex real-world error messages."""
        error = """ERROR: column "age" does not exist
LINE 1: SELECT id, age FROM users WHERE age > 30;"""
        assert classify_error(error) == "UNDEFINED_COLUMN"

        error2 = """ERROR: syntax error at or near ";"
LINE 1: SELECT * FROM users WHERE id = 1"""
        assert classify_error(error2) == "SYNTAX_ERROR"
