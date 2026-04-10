"""Unit tests for schema/extractor.py"""

from unittest.mock import MagicMock
from natquery.schema.extractor import extract_schema


class TestExtractSchema:
    """Test extract_schema() function."""

    def test_extract_schema_basic(self):
        """Test basic schema extraction."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Mock table names
        mock_cursor.fetchall.side_effect = [
            [("users",), ("posts",)],  # tables
            [("id", "integer"), ("name", "varchar")],  # columns for users
            [("id",)],  # primary key for users
            [],  # foreign keys for users
            [("id", "integer"), ("title", "varchar")],  # columns for posts
            [("id",)],  # primary key for posts
            [],  # foreign keys for posts
        ]

        result = extract_schema(mock_conn)

        assert "tables" in result
        assert "users" in result["tables"]
        assert "posts" in result["tables"]
        assert "id" in result["tables"]["users"]["columns"]

    def test_extract_schema_multiple_tables(self):
        """Test schema extraction with multiple tables."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.side_effect = [
            [("users",), ("posts",)],  # Tables
            [("id", "integer"), ("username", "varchar")],  # Users columns
            [("id",)],  # users pk
            [],  # users fks
            [("id", "integer"), ("title", "varchar")],  # Posts columns
            [("id",)],  # posts pk
            [],  # posts fks
        ]

        result = extract_schema(mock_conn)

        assert "users" in result["tables"]
        assert "posts" in result["tables"]
        assert len(result["tables"]["users"]["columns"]) == 2

    def test_extract_schema_with_primary_keys(self):
        """Test extraction with primary keys."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.side_effect = [
            [("users",)],  # tables
            [("id", "integer"), ("email", "varchar")],  # columns
            [("id",)],  # primary key
            [],  # foreign keys
        ]

        result = extract_schema(mock_conn)

        assert result["tables"]["users"]["primary_key"] == ["id"]

    def test_extract_schema_with_foreign_keys(self):
        """Test extraction with foreign key relationships."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.side_effect = [
            [("posts",)],  # tables
            [("id", "integer"), ("user_id", "integer")],  # columns
            [("id",)],  # primary key
            [("user_id", "users", "id")],  # foreign key
        ]

        result = extract_schema(mock_conn)

        fks = result["tables"]["posts"]["foreign_keys"]
        assert len(fks) == 1
        assert fks[0]["column"] == "user_id"
        assert fks[0]["references"]["table"] == "users"
        assert fks[0]["references"]["column"] == "id"

    def test_extract_schema_multiple_primary_keys(self):
        """Test table with composite primary key."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.side_effect = [
            [("order_items",)],  # tables
            [("order_id", "integer"), ("item_id", "integer")],  # columns
            [("order_id",), ("item_id",)],  # composite primary key
            [],  # foreign keys
        ]

        result = extract_schema(mock_conn)

        pks = result["tables"]["order_items"]["primary_key"]
        assert len(pks) == 2
        assert "order_id" in pks
        assert "item_id" in pks

    def test_extract_schema_multiple_foreign_keys(self):
        """Test table with multiple foreign keys."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.side_effect = [
            [("orders",)],  # tables
            [
                ("id", "integer"),
                ("user_id", "integer"),
                ("product_id", "integer"),
            ],  # columns
            [("id",)],  # primary key
            [  # multiple foreign keys
                ("user_id", "users", "id"),
                ("product_id", "products", "id"),
            ],
        ]

        result = extract_schema(mock_conn)

        fks = result["tables"]["orders"]["foreign_keys"]
        assert len(fks) == 2
        assert any(fk["column"] == "user_id" for fk in fks)
        assert any(fk["column"] == "product_id" for fk in fks)

    def test_extract_schema_various_data_types(self):
        """Test extraction with various PostgreSQL data types."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.side_effect = [
            [("products",)],  # tables
            [
                ("id", "integer"),
                ("name", "varchar"),
                ("price", "numeric"),
                ("description", "text"),
                ("created_at", "timestamp"),
                ("is_active", "boolean"),
                ("tags", "character varying[]"),
            ],  # columns with various types
            [],  # primary key
            [],  # foreign keys
        ]

        result = extract_schema(mock_conn)

        columns = result["tables"]["products"]["columns"]
        assert columns["id"] == "integer"
        assert columns["name"] == "varchar"
        assert columns["price"] == "numeric"
        assert columns["description"] == "text"

    def test_extract_schema_cursor_closed(self):
        """Test that cursor is properly closed after extraction."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.side_effect = [
            [("users",)],  # tables
            [("id", "integer")],  # columns
            [],  # primary key
            [],  # foreign keys
        ]

        extract_schema(mock_conn)

        # Verify cursor.close() was called
        mock_cursor.close.assert_called_once()

    def test_extract_schema_uses_parameterized_queries(self):
        """Test that parameterized queries are used (SQL injection prevention)."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.side_effect = [
            [("users",)],  # tables
            [("id", "integer")],  # columns
            [],  # primary key
            [],  # foreign keys
        ]

        extract_schema(mock_conn)

        # Check that cursor.execute was called with parameterized query
        calls = mock_cursor.execute.call_args_list

        # At least one call should use %s placeholders
        parameterized_calls = [c for c in calls if "%s" in str(c)]
        assert len(parameterized_calls) > 0

    def test_extract_schema_empty_database(self):
        """Test extraction from database with no public tables."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.return_value = []  # No tables

        result = extract_schema(mock_conn)

        assert result["tables"] == {}

    def test_extract_schema_table_without_columns(self):
        """Test table with no columns (edge case)."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.side_effect = [
            [("empty_table",)],  # tables
            [],  # no columns
            [],  # no primary key
            [],  # no foreign keys
        ]

        result = extract_schema(mock_conn)

        assert "empty_table" in result["tables"]
        assert result["tables"]["empty_table"]["columns"] == {}

    def test_extract_schema_schema_structure_valid(self):
        """Test that returned schema has correct structure."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchall.side_effect = [
            [("users",)],
            [("id", "integer")],
            [("id",)],
            [],
        ]

        result = extract_schema(mock_conn)

        # Verify structure
        assert isinstance(result, dict)
        assert "tables" in result
        assert isinstance(result["tables"], dict)

        table = result["tables"]["users"]
        assert isinstance(table, dict)
        assert "columns" in table
        assert "primary_key" in table
        assert "foreign_keys" in table
        assert isinstance(table["columns"], dict)
        assert isinstance(table["primary_key"], list)
        assert isinstance(table["foreign_keys"], list)

    def test_extract_schema_complex_scenario(self):
        """Test complex schema with multiple tables and relationships."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Simulate a blog schema
        mock_cursor.fetchall.side_effect = [
            [("users",), ("posts",), ("comments",)],  # tables
            # users columns
            [("id", "integer"), ("email", "varchar"), ("created_at", "timestamp")],
            [("id",)],  # users pk
            [],  # users fks
            # posts columns
            [("id", "integer"), ("user_id", "integer"), ("title", "varchar")],
            [("id",)],  # posts pk
            [("user_id", "users", "id")],  # posts fks
            # comments columns
            [
                ("id", "integer"),
                ("post_id", "integer"),
                ("user_id", "integer"),
                ("content", "text"),
            ],
            [("id",)],  # comments pk
            [("post_id", "posts", "id"), ("user_id", "users", "id")],  # comments fks
        ]

        result = extract_schema(mock_conn)

        # Verify all tables
        assert len(result["tables"]) == 3
        assert all(t in result["tables"] for t in ["users", "posts", "comments"])

        # Verify posts relationships
        posts_fks = result["tables"]["posts"]["foreign_keys"]
        assert len(posts_fks) == 1

        # Verify comments relationships
        comments_fks = result["tables"]["comments"]["foreign_keys"]
        assert len(comments_fks) == 2
