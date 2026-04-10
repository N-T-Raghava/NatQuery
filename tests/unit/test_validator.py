"""Unit tests for security/validator.py"""

import pytest
from natquery.security.validator import validate_sql


class TestValidateSql:
    """Test validate_sql() function."""

    def test_valid_select_query(self):
        """Test validation of valid SELECT query."""
        assert validate_sql("SELECT * FROM users;") is True
        assert validate_sql("SELECT id, name FROM users;") is True
        assert validate_sql("SELECT COUNT(*) FROM users;") is True

    def test_select_with_where_clause(self):
        """Test SELECT with WHERE clause."""
        assert validate_sql("SELECT * FROM users WHERE id = 1;") is True
        assert validate_sql("SELECT name FROM users WHERE status = 'active';") is True

    def test_select_with_join(self):
        """Test SELECT with JOIN."""
        assert (
            validate_sql("SELECT u.id FROM users u JOIN posts p ON u.id = p.user_id;")
            is True
        )
        assert (
            validate_sql(
                "SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id;"
            )
            is True
        )

    def test_select_with_group_by(self):
        """Test SELECT with GROUP BY."""
        assert (
            validate_sql("SELECT status, COUNT(*) FROM users GROUP BY status;") is True
        )

    def test_select_with_order_by(self):
        """Test SELECT with ORDER BY."""
        assert validate_sql("SELECT * FROM users ORDER BY created_at DESC;") is True

    def test_case_insensitive_select(self):
        """Test that SELECT check is case insensitive."""
        assert validate_sql("select * from users;") is True
        assert validate_sql("SELECT * FROM users;") is True
        assert validate_sql("SeLeCt * FROM users;") is True

    def test_rejects_insert(self):
        """Test that INSERT is rejected."""
        # Queries that don't start with SELECT
        with pytest.raises(ValueError) as exc_info:
            validate_sql("INSERT INTO users (name) VALUES ('John');")
        assert "Only SELECT queries are allowed" in str(exc_info.value)

        # INSERT in SELECT should be caught by keyword check
        with pytest.raises(ValueError):
            validate_sql("SELECT * FROM users; INSERT INTO logs VALUES ('x');")

    def test_rejects_update(self):
        """Test that UPDATE is rejected."""
        with pytest.raises(ValueError):
            validate_sql("UPDATE users SET name = 'Jane' WHERE id = 1;")

        with pytest.raises(ValueError):
            validate_sql("SELECT * FROM users; Update users SET x=1;")

    def test_rejects_delete(self):
        """Test that DELETE is rejected."""
        with pytest.raises(ValueError):
            validate_sql("DELETE FROM users WHERE id = 1;")

        with pytest.raises(ValueError):
            validate_sql("SELECT * FROM users; DELETE FROM logs;")

    def test_rejects_drop(self):
        """Test that DROP is rejected."""
        with pytest.raises(ValueError):
            validate_sql("DROP TABLE users;")

        with pytest.raises(ValueError):
            validate_sql("SELECT * FROM users; DROP TABLE x;")

    def test_rejects_alter(self):
        """Test that ALTER is rejected."""
        with pytest.raises(ValueError):
            validate_sql("ALTER TABLE users ADD COLUMN age INTEGER;")

        with pytest.raises(ValueError):
            validate_sql("SELECT * FROM users; ALTER TABLE x ADD y;")

    def test_rejects_truncate(self):
        """Test that TRUNCATE is rejected."""
        with pytest.raises(ValueError):
            validate_sql("TRUNCATE TABLE users;")

        with pytest.raises(ValueError):
            validate_sql("SELECT * FROM users; TRUNCATE TABLE x;")

    def test_rejects_non_select_start(self):
        """Test that non-SELECT queries are rejected."""
        with pytest.raises(ValueError) as exc_info:
            validate_sql("INSERT INTO users SELECT * FROM archived_users;")
        assert "Only SELECT queries are allowed" in str(exc_info.value)

    def test_case_insensitive_forbidden_keywords(self):
        """Test that forbidden keywords are detected case-insensitively."""
        with pytest.raises(ValueError):
            validate_sql("SELECT * FROM users; INSERT INTO logs VALUES ('x');")

        with pytest.raises(ValueError):
            validate_sql("SELECT * FROM users; Update users SET x=1;")

        with pytest.raises(ValueError):
            validate_sql("SELECT * FROM users; DELETE FROM logs;")

    def test_whitespace_handling(self):
        """Test handling of leading/trailing whitespace."""
        assert validate_sql("   SELECT * FROM users;   ") is True
        assert validate_sql("\n\nSELECT * FROM users;\n\n") is True

    def test_multiline_select_query(self):
        """Test multiline SELECT queries."""
        query = """
        SELECT 
            u.id,
            u.name,
            COUNT(p.id) as post_count
        FROM users u
        LEFT JOIN posts p ON u.id = p.user_id
        WHERE u.status = 'active'
        GROUP BY u.id, u.name
        ORDER BY post_count DESC;
        """
        assert validate_sql(query) is True

    def test_select_with_subquery(self):
        """Test SELECT with subquery."""
        query = "SELECT * FROM (SELECT id FROM users WHERE id > 10) AS active_users;"
        assert validate_sql(query) is True

    def test_word_boundary_matching(self):
        """Test that forbidden words only match whole words."""
        # These are valid because they contain forbidden words but not as separate keywords
        assert (
            validate_sql("SELECT deleted FROM users;") is True
        )  # 'deleted' contains 'delete'
        assert (
            validate_sql("SELECT update_time FROM logs;") is True
        )  # 'update_time' contains 'update'
        assert (
            validate_sql("SELECT truncated_at FROM archive;") is True
        )  # Contains 'truncate'

    def test_insert_in_subquery_rejected(self):
        """Test that INSERT hidden in subquery is rejected."""
        with pytest.raises(ValueError):
            validate_sql("SELECT * FROM (INSERT INTO users VALUES (1, 'test')) tmp;")

    def test_union_select_allowed(self):
        """Test UNION SELECT queries are allowed."""
        query = """
        SELECT id, name FROM users
        UNION
        SELECT id, name FROM archived_users;
        """
        assert validate_sql(query) is True

    def test_common_table_expression_allowed(self):
        """Test CTE (WITH clause) SELECT queries.

        Note: CTEs are NOT allowed by this simple validator since it only
        accepts queries starting with SELECT. To support CTEs, the validator
        would need to be more sophisticated.
        """
        # This will fail because WITH doesn't start with SELECT
        query = """
        WITH active_users AS (
            SELECT * FROM users WHERE status = 'active'
        )
        SELECT * FROM active_users;
        """
        # The current validator is simple and only accepts SELECT at start
        with pytest.raises(ValueError):
            validate_sql(query)

    def test_with_statement_starting_query_invalid(self):
        """Test that WITH queries are not supported by simple validator."""
        query = "WITH x AS (SELECT 1) UPDATE users SET x=1;"
        with pytest.raises(ValueError):
            validate_sql(query)

    def test_empty_query_rejected(self):
        """Test that empty queries are rejected."""
        with pytest.raises(ValueError):
            validate_sql("   ")

        with pytest.raises(ValueError):
            validate_sql("")

    def test_comments_in_query_allowed(self):
        """Test that SQL comments within query are allowed.

        Note: Comments at the start of the query won't work because the
        validator only accepts queries starting with SELECT.
        """
        # Comments AFTER SELECT are fine
        query1 = "SELECT /* user data */ * FROM users;"
        assert validate_sql(query1) is True

        # Inline comments are fine
        query2 = "SELECT id, /* pk */ name FROM users;"
        assert validate_sql(query2) is True
