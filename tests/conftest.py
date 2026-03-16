import pytest
import json
import tempfile
import os
from pathlib import Path
from unittest.mock import MagicMock, patch


@pytest.fixture
def sample_config():
    """Sample valid configuration for testing."""
    return {
        "db_host": "localhost",
        "db_port": 5432,
        "db_name": "testdb",
        "db_user": "testuser",
        "db_password": "testpass",
        "llm_provider": "groq",
        "llm_api_key": "test_key",
        "llm_model": "llama3-8b-8192",
    }


@pytest.fixture
def temp_natquery_dir(sample_config):
    """Create a temporary NatQuery directory structure with config."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir) / ".natquery"
        base_dir.mkdir(exist_ok=True)

        # Create current_db file
        (base_dir / "current_db").write_text("testdb")

        # Create database-specific config
        db_dir = base_dir / "testdb"
        db_dir.mkdir(exist_ok=True)
        config_file = db_dir / "config.json"
        with open(config_file, "w") as f:
            json.dump(sample_config, f)

        yield base_dir


@pytest.fixture
def temp_config_file(sample_config):
    """Create a temporary config file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(sample_config, f)
        temp_path = f.name

    yield temp_path
    os.unlink(temp_path)


@pytest.fixture
def temp_dir():
    """Temporary directory for testing file operations."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def mock_db_connection():
    """Mock database connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [
        {"table_name": "users", "column_name": "id"},
        {"table_name": "users", "column_name": "name"},
        {"table_name": "posts", "column_name": "title"},
    ]
    return mock_conn


@pytest.fixture
def temp_llm_client():
    """Mock LLM client response."""
    with patch("groq.Groq") as mock_groq:
        mock_client = MagicMock()
        mock_groq.return_value = mock_client
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "SELECT * FROM users;"
        mock_client.chat.completions.create.return_value = mock_response
        yield mock_client


@pytest.fixture
def temp_natquery_dir_alt(sample_config, temp_dir):
    """Alternative temporary NatQuery directory with different database."""
    base_dir = Path(temp_dir) / ".natquery"
    base_dir.mkdir(exist_ok=True)

    # Create current_db file
    (base_dir / "current_db").write_text("proddb")

    # Create database-specific config
    db_dir = base_dir / "proddb"
    db_dir.mkdir(exist_ok=True)
    config_file = db_dir / "config.json"
    config = sample_config.copy()
    config["db_name"] = "proddb"
    with open(config_file, "w") as f:
        json.dump(config, f)

    return base_dir
