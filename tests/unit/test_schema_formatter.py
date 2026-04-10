"""Unit tests for schema/formatter.py"""

from natquery.schema.formatter import format_schema


class TestFormatSchema:
    """Test format_schema() function."""

    def test_format_empty_schema(self):
        """Test formatting of empty schema."""
        result = format_schema({})
        assert result == ""

    def test_format_schema_missing_tables(self):
        """Test formatting of schema without tables key."""
        result = format_schema({"other_key": "value"})
        assert result == ""

    def test_format_schema_single_table(self):
        """Test formatting of schema with single table."""
        schema = {
            "tables": {
                "users": {
                    "columns": {"id": "integer", "name": "varchar"},
                    "primary_key": ["id"],
                    "foreign_keys": [],
                }
            }
        }
        result = format_schema(schema)

        assert "TABLE users" in result
        assert "id INTEGER PRIMARY KEY" in result
        assert "name VARCHAR" in result

    def test_format_schema_with_primary_key(self):
        """Test formatting with primary key notation."""
        schema = {
            "tables": {
                "products": {
                    "columns": {
                        "product_id": "integer",
                        "title": "varchar",
                        "price": "numeric",
                    },
                    "primary_key": ["product_id"],
                    "foreign_keys": [],
                }
            }
        }
        result = format_schema(schema)

        assert "TABLE products" in result
        assert "product_id INTEGER PRIMARY KEY" in result
        assert "title VARCHAR" in result
        assert "price NUMERIC" in result

    def test_format_schema_with_foreign_keys(self):
        """Test formatting with foreign key relationships."""
        schema = {
            "tables": {
                "posts": {
                    "columns": {"post_id": "integer", "user_id": "integer"},
                    "primary_key": ["post_id"],
                    "foreign_keys": [
                        {
                            "column": "user_id",
                            "references": {"table": "users", "column": "id"},
                        }
                    ],
                },
                "users": {
                    "columns": {"id": "integer", "email": "varchar"},
                    "primary_key": ["id"],
                    "foreign_keys": [],
                },
            }
        }
        result = format_schema(schema)

        assert "RELATIONSHIPS:" in result
        assert "posts.user_id → users.id" in result

    def test_format_schema_multiple_tables(self):
        """Test formatting with multiple tables."""
        schema = {
            "tables": {
                "users": {
                    "columns": {"id": "integer", "name": "varchar"},
                    "primary_key": ["id"],
                    "foreign_keys": [],
                },
                "posts": {
                    "columns": {"id": "integer", "title": "varchar"},
                    "primary_key": ["id"],
                    "foreign_keys": [],
                },
                "comments": {
                    "columns": {"id": "integer", "text": "text"},
                    "primary_key": ["id"],
                    "foreign_keys": [],
                },
            }
        }
        result = format_schema(schema)

        # Check all tables are present
        assert "TABLE users" in result
        assert "TABLE posts" in result
        assert "TABLE comments" in result

        # Check deterministic ordering (alphabetical)
        users_idx = result.find("TABLE users")
        posts_idx = result.find("TABLE posts")
        comments_idx = result.find("TABLE comments")
        assert comments_idx < posts_idx < users_idx

    def test_format_schema_case_conversion(self):
        """Test that data types are converted to uppercase."""
        schema = {
            "tables": {
                "test_table": {
                    "columns": {
                        "col1": "varchar",
                        "col2": "integer",
                        "col3": "timestamp",
                    },
                    "primary_key": ["col1"],
                    "foreign_keys": [],
                }
            }
        }
        result = format_schema(schema)

        assert "VARCHAR PRIMARY KEY" in result
        assert "INTEGER" in result
        assert "TIMESTAMP" in result

    def test_format_schema_multiple_foreign_keys(self):
        """Test formatting with multiple foreign key relationships."""
        schema = {
            "tables": {
                "orders": {
                    "columns": {
                        "order_id": "integer",
                        "user_id": "integer",
                        "product_id": "integer",
                    },
                    "primary_key": ["order_id"],
                    "foreign_keys": [
                        {
                            "column": "user_id",
                            "references": {"table": "users", "column": "id"},
                        },
                        {
                            "column": "product_id",
                            "references": {"table": "products", "column": "id"},
                        },
                    ],
                }
            }
        }
        result = format_schema(schema)

        assert "orders.user_id → users.id" in result
        assert "orders.product_id → products.id" in result

    def test_format_schema_no_columns(self):
        """Test formatting table with no columns."""
        schema = {
            "tables": {
                "empty_table": {
                    "columns": {},
                    "primary_key": [],
                    "foreign_keys": [],
                }
            }
        }
        result = format_schema(schema)

        assert "TABLE empty_table" in result
        # Should handle gracefully

    def test_format_schema_missing_primary_key_field(self):
        """Test schema with missing primary_key field."""
        schema = {
            "tables": {
                "users": {
                    "columns": {"id": "integer", "name": "varchar"},
                    # No primary_key field
                    "foreign_keys": [],
                }
            }
        }
        result = format_schema(schema)

        assert "TABLE users" in result
        # Should not mark any columns as PRIMARY KEY

    def test_format_schema_output_structure(self):
        """Test the output structure is correct."""
        schema = {
            "tables": {
                "users": {
                    "columns": {"id": "integer"},
                    "primary_key": ["id"],
                    "foreign_keys": [],
                }
            }
        }
        result = format_schema(schema)

        lines = result.strip().split("\n")
        assert len(lines) >= 3  # TABLE header, at least one column, closing paren

    def test_format_complex_schema(self):
        """Test formatting a complex multi-table schema."""
        schema = {
            "tables": {
                "users": {
                    "columns": {
                        "id": "integer",
                        "username": "varchar",
                        "email": "varchar",
                        "created_at": "timestamp",
                    },
                    "primary_key": ["id"],
                    "foreign_keys": [],
                },
                "posts": {
                    "columns": {
                        "id": "integer",
                        "user_id": "integer",
                        "title": "varchar",
                        "content": "text",
                    },
                    "primary_key": ["id"],
                    "foreign_keys": [
                        {
                            "column": "user_id",
                            "references": {"table": "users", "column": "id"},
                        }
                    ],
                },
                "comments": {
                    "columns": {
                        "id": "integer",
                        "post_id": "integer",
                        "user_id": "integer",
                        "content": "text",
                    },
                    "primary_key": ["id"],
                    "foreign_keys": [
                        {
                            "column": "post_id",
                            "references": {"table": "posts", "column": "id"},
                        },
                        {
                            "column": "user_id",
                            "references": {"table": "users", "column": "id"},
                        },
                    ],
                },
            }
        }
        result = format_schema(schema)

        # Verify all tables
        assert "TABLE comments" in result
        assert "TABLE posts" in result
        assert "TABLE users" in result

        # Verify relationships
        assert "RELATIONSHIPS:" in result
        assert "comments.post_id → posts.id" in result
        assert "comments.user_id → users.id" in result
        assert "posts.user_id → users.id" in result
