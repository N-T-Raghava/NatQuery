import pytest
from unittest.mock import patch, MagicMock
from natquery.config.connection import get_connection


@patch("natquery.config.connection.psycopg2.connect")
def test_get_connection_success(mock_connect):
    """
    Ensure get_connection returns connection object.
    """
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    conn = get_connection()

    assert conn == mock_conn
    mock_connect.assert_called_once()


@patch("natquery.config.connection.psycopg2.connect")
def test_get_connection_failure(mock_connect):
    """
    Ensure connection error raises ConnectionError.
    """
    mock_connect.side_effect = Exception("DB down")

    with pytest.raises(ConnectionError):
        get_connection()