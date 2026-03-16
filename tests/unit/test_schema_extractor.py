from natquery.schema.extractor import extract_schema


class TestExtractSchema:
    """Test extract_schema() function."""

    def test_extract_schema_basic(self, mock_db_connection):
        """Test basic schema extraction with one table."""
        mock_cursor = mock_db_connection.cursor.return_value
        # Mock tables query
        mock_cursor.fetchall.side_effect = [
            [("users",)],  # First fetchall for tables
            [("id",), ("name",), ("email",)],  # Second for columns
        ]

        schema = extract_schema(mock_db_connection)

        expected = {"users": ["id", "name", "email"]}
        assert schema == expected
        # Check execute calls
        assert mock_cursor.execute.call_count == 2
        mock_cursor.execute.assert_any_call(
            """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';
    """
        )
        mock_cursor.execute.assert_any_call(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
        """,
            ("users",),
        )

    def test_extract_schema_no_tables(self, mock_db_connection):
        """Test schema extraction when no tables exist."""
        mock_cursor = mock_db_connection.cursor.return_value
        mock_cursor.fetchall.side_effect = [
            [],  # No tables
        ]

        schema = extract_schema(mock_db_connection)

        assert schema == {}
        assert mock_cursor.execute.call_count == 1  # Only tables query

    def test_extract_schema_multiple_tables(self, mock_db_connection):
        """Test schema extraction with multiple tables."""
        mock_cursor = mock_db_connection.cursor.return_value
        mock_cursor.fetchall.side_effect = [
            [("users",), ("posts",)],  # Tables
            [("id",), ("username",)],  # Users columns
            [("id",), ("title",), ("user_id",)],  # Posts columns
        ]

        schema = extract_schema(mock_db_connection)

        expected = {"users": ["id", "username"], "posts": ["id", "title", "user_id"]}
        assert schema == expected
        assert mock_cursor.execute.call_count == 3  # Tables + 2 columns

    def test_extract_schema_special_chars_in_names(self, mock_db_connection):
        """Test schema extraction with special characters in table/column names."""
        mock_cursor = mock_db_connection.cursor.return_value
        mock_cursor.fetchall.side_effect = [
            [("user-table",)],  # Table with dash
            [("user_id",), ("user-name",)],  # Columns with underscore and dash
        ]

        schema = extract_schema(mock_db_connection)

        expected = {"user-table": ["user_id", "user-name"]}
        assert schema == expected
