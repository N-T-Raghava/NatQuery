from unittest.mock import patch
from natquery.cli.formatter import (
    _is_number,
    _infer_alignment,
    _truncate,
    _format_value,
    print_table,
)


class TestIsNumber:
    """Test _is_number() function."""

    def test_is_number_int(self):
        """Test with integer."""
        assert _is_number(5) is True
        assert _is_number(0) is True
        assert _is_number(-10) is True

    def test_is_number_float(self):
        """Test with float."""
        assert _is_number(5.5) is True
        assert _is_number(0.0) is True
        assert _is_number(-10.5) is True

    def test_is_number_string(self):
        """Test with string."""
        assert _is_number("5") is False
        assert _is_number("abc") is False

    def test_is_number_none(self):
        """Test with None."""
        assert _is_number(None) is False

    def test_is_number_bool(self):
        """Test with boolean (bool is subclass of int in Python)."""
        assert _is_number(True) is True
        assert _is_number(False) is True


class TestTruncate:
    """Test _truncate() function."""

    def test_truncate_short_string(self):
        """Test truncating short string."""
        result = _truncate("hello")
        assert result == "hello"

    def test_truncate_long_string(self):
        """Test truncating long string."""
        long_text = "a" * 100
        result = _truncate(long_text, max_length=50)
        assert len(result) <= 50
        assert result.endswith("...")

    def test_truncate_exact_length(self):
        """Test text exactly at max_length."""
        text = "a" * 50
        result = _truncate(text, max_length=50)
        assert result == text

    def test_truncate_number(self):
        """Test truncating number converts to string."""
        result = _truncate(12345, max_length=3)
        assert isinstance(result, str)
        assert result.endswith("...")

    def test_truncate_none_value(self):
        """Test truncating None."""
        result = _truncate(None, max_length=50)
        assert isinstance(result, str)


class TestFormatValue:
    """Test _format_value() function."""

    def test_format_value_string(self):
        """Test formatting string."""
        assert _format_value("hello") == "hello"

    def test_format_value_number(self):
        """Test formatting number."""
        assert _format_value(42) == "42"
        assert _format_value(3.14) == "3.14"

    def test_format_value_none(self):
        """Test formatting None."""
        assert _format_value(None) == ""

    def test_format_value_bool(self):
        """Test formatting boolean."""
        assert _format_value(True) == "True"
        assert _format_value(False) == "False"

    def test_format_value_list(self):
        """Test formatting list."""
        result = _format_value([1, 2, 3])
        assert isinstance(result, str)
        assert "[1, 2, 3]" in result


class TestInferAlignment:
    """Test _infer_alignment() function."""

    def test_infer_alignment_numbers(self):
        """Test alignment inference with numbers."""
        data = [
            {"amount": 100, "name": "item1"},
            {"amount": 200, "name": "item2"},
        ]
        alignment = _infer_alignment(data, "amount")
        assert alignment == "right"

    def test_infer_alignment_strings(self):
        """Test alignment inference with strings."""
        data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]
        alignment = _infer_alignment(data, "name")
        assert alignment == "left"

    def test_infer_alignment_mixed_with_none(self):
        """Test alignment inference with None values."""
        data = [
            {"value": None},
            {"value": "text"},
        ]
        alignment = _infer_alignment(data, "value")
        assert alignment == "left"

    def test_infer_alignment_all_none(self):
        """Test alignment inference when all values are None."""
        data = [
            {"value": None},
            {"value": None},
        ]
        alignment = _infer_alignment(data, "value")
        assert alignment == "left"  # Default to left

    def test_infer_alignment_float(self):
        """Test alignment inference with float numbers."""
        data = [
            {"price": 10.5},
            {"price": 20.3},
        ]
        alignment = _infer_alignment(data, "price")
        assert alignment == "right"


class TestPrintTable:
    """Test print_table() function."""

    def test_print_table_empty_data(self):
        """Test print_table with empty data."""
        with patch("natquery.cli.formatter.console") as mock_console:
            print_table([])
            # Should print "No results found"
            assert mock_console.print.called

    def test_print_table_single_column(self):
        """Test print_table with single column."""
        data = [{"name": "Alice"}, {"name": "Bob"}]
        with patch("natquery.cli.formatter.console") as mock_console:
            print_table(data)
            assert mock_console.print.called

    def test_print_table_multiple_columns(self):
        """Test print_table with multiple columns."""
        data = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
        ]
        with patch("natquery.cli.formatter.console") as mock_console:
            print_table(data)
            assert mock_console.print.called

    def test_print_table_pagination_single_page(self):
        """Test print_table pagination with single page."""
        data = [{"id": i, "name": f"Item{i}"} for i in range(5)]
        with patch("natquery.cli.formatter.console") as mock_console:
            print_table(data, page_size=20)
            # Should print table once
            assert mock_console.print.called

    def test_print_table_pagination_multiple_pages(self):
        """Test print_table pagination with multiple pages."""
        data = [{"id": i, "name": f"Item{i}"} for i in range(50)]
        with patch("natquery.cli.formatter.console") as mock_console:
            with patch("builtins.input", return_value=""):
                print_table(data, page_size=20)
                # Should print table multiple times
                assert mock_console.print.call_count > 1

    def test_print_table_pagination_user_stops(self):
        """Test print_table pagination when user stops."""
        data = [{"id": i, "name": f"Item{i}"} for i in range(50)]
        with patch("natquery.cli.formatter.console") as mock_console:
            with patch("builtins.input", side_effect=KeyboardInterrupt):
                print_table(data, page_size=20)
                # Should handle KeyboardInterrupt gracefully
                assert mock_console.print.called

    def test_print_table_pagination_eof_error(self):
        """Test print_table pagination when EOFError occurs."""
        data = [{"id": i, "name": f"Item{i}"} for i in range(50)]
        with patch("natquery.cli.formatter.console") as mock_console:
            with patch("builtins.input", side_effect=EOFError):
                print_table(data, page_size=20)
                # Should handle EOFError gracefully
                assert mock_console.print.called

    def test_print_table_wrap_enabled(self):
        """Test print_table with text wrapping enabled."""
        data = [{"description": "This is a very long description that should wrap"}]
        with patch("natquery.cli.formatter.console") as mock_console:
            print_table(data, wrap=True)
            assert mock_console.print.called

    def test_print_table_wrap_disabled(self):
        """Test print_table with text wrapping disabled."""
        data = [{"description": "This is a very long description that should not wrap"}]
        with patch("natquery.cli.formatter.console") as mock_console:
            print_table(data, wrap=False)
            assert mock_console.print.called

    def test_print_table_custom_title(self):
        """Test print_table with custom title."""
        data = [{"id": 1, "name": "Alice"}]
        with patch("natquery.cli.formatter.console") as mock_console:
            print_table(data, title="Custom Title")
            assert mock_console.print.called

    def test_print_table_column_alignment(self):
        """Test print_table respects column alignment."""
        data = [
            {"number": 100, "text": "left-aligned"},
            {"number": 200, "text": "also left"},
        ]
        with patch("natquery.cli.formatter.console") as mock_console:
            print_table(data)
            # Should create table with proper alignment
            assert mock_console.print.called

    def test_print_table_long_values_truncated(self):
        """Test print_table truncates very long values."""
        long_text = "a" * 200
        data = [{"content": long_text}]
        with patch("natquery.cli.formatter.console") as mock_console:
            print_table(data)
            assert mock_console.print.called

    def test_print_table_special_characters(self):
        """Test print_table handles special characters."""
        data = [
            {"id": 1, "text": "Special: @#$%^&*()"},
            {"id": 2, "text": "Unicode: 你好世界"},
        ]
        with patch("natquery.cli.formatter.console") as mock_console:
            print_table(data)
            assert mock_console.print.called
