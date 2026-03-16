import pytest
import json
from pathlib import Path
from unittest.mock import patch
from natquery.config.settings import Settings


class TestSettingsGetActiveDb:
    """Test Settings._get_active_db_name() method."""

    def test_get_active_db_name_exists(self, temp_natquery_dir):
        """Test reading active database name from current_db file."""
        with patch.object(Settings, "BASE_DIR", temp_natquery_dir):
            db_name = Settings._get_active_db_name()
            assert db_name == "testdb"

    def test_get_active_db_name_missing_file(self, temp_dir):
        """Test RuntimeError when current_db file does not exist."""
        base_dir = Path(temp_dir) / ".natquery"
        base_dir.mkdir(exist_ok=True)

        with patch.object(Settings, "BASE_DIR", base_dir):
            with pytest.raises(RuntimeError, match="No active database configured"):
                Settings._get_active_db_name()


class TestSettingsGetConfigPath:
    """Test Settings._get_config_path() method."""

    def test_get_config_path_success(self, temp_natquery_dir):
        """Test getting config path for active database."""
        with patch.object(Settings, "BASE_DIR", temp_natquery_dir):
            path = Settings._get_config_path()
            assert path == temp_natquery_dir / "testdb" / "config.json"
            assert path.exists()


class TestSettingsExists:
    """Test Settings.exists() method."""

    def test_exists_when_file_exists(self, temp_natquery_dir):
        """Test exists returns True when config file exists."""
        with patch.object(Settings, "BASE_DIR", temp_natquery_dir):
            assert Settings.exists() is True

    def test_exists_when_file_missing(self, temp_dir):
        """Test exists returns False when config file does not exist."""
        base_dir = Path(temp_dir) / ".natquery"
        db_dir = base_dir / "testdb"
        db_dir.mkdir(parents=True, exist_ok=True)
        (base_dir / "current_db").write_text("testdb")

        with patch.object(Settings, "BASE_DIR", base_dir):
            assert Settings.exists() is False


class TestSettingsLoadConfig:
    """Test Settings.load_config() method."""

    def test_load_config_success(self, temp_natquery_dir, sample_config):
        """Test successful config loading."""
        with patch.object(Settings, "BASE_DIR", temp_natquery_dir):
            config = Settings.load_config()
            assert config == sample_config

    def test_load_config_missing_file(self, temp_dir):
        """Test RuntimeError when config file does not exist."""
        base_dir = Path(temp_dir) / ".natquery"
        db_dir = base_dir / "testdb"
        db_dir.mkdir(parents=True, exist_ok=True)
        (base_dir / "current_db").write_text("testdb")

        with patch.object(Settings, "BASE_DIR", base_dir):
            with pytest.raises(RuntimeError, match="NatQuery not configured"):
                Settings.load_config()

    def test_load_config_invalid_json(self, temp_dir):
        """Test JSONDecodeError handling for invalid JSON."""
        base_dir = Path(temp_dir) / ".natquery"
        db_dir = base_dir / "testdb"
        db_dir.mkdir(parents=True, exist_ok=True)
        (base_dir / "current_db").write_text("testdb")

        config_file = db_dir / "config.json"
        config_file.write_text("invalid json content")

        with patch.object(Settings, "BASE_DIR", base_dir):
            with pytest.raises(json.JSONDecodeError):
                Settings.load_config()


class TestSettingsGetDbConfig:
    """Test Settings.get_db_config() method."""

    def test_get_db_config_complete(self, temp_natquery_dir):
        """Test successful database config extraction."""
        with patch.object(Settings, "BASE_DIR", temp_natquery_dir):
            db_config = Settings.get_db_config()
            expected = {
                "host": "localhost",
                "port": 5432,
                "dbname": "testdb",
                "user": "testuser",
                "password": "testpass",
                "sslmode": None,
            }
            assert db_config == expected

    @pytest.mark.parametrize(
        "missing_field", ["db_host", "db_port", "db_name", "db_user", "db_password"]
    )
    def test_get_db_config_missing_field(self, temp_dir, sample_config, missing_field):
        """Test ValueError when required database field is missing."""
        base_dir = Path(temp_dir) / ".natquery"
        db_dir = base_dir / "testdb"
        db_dir.mkdir(parents=True, exist_ok=True)
        (base_dir / "current_db").write_text("testdb")

        config = sample_config.copy()
        del config[missing_field]

        config_file = db_dir / "config.json"
        with open(config_file, "w") as f:
            json.dump(config, f)

        with patch.object(Settings, "BASE_DIR", base_dir):
            with pytest.raises(
                ValueError, match=f"Missing database config field: {missing_field}"
            ):
                Settings.get_db_config()

    def test_get_db_config_empty_field(self, temp_dir, sample_config):
        """Test ValueError when required field is empty."""
        base_dir = Path(temp_dir) / ".natquery"
        db_dir = base_dir / "testdb"
        db_dir.mkdir(parents=True, exist_ok=True)
        (base_dir / "current_db").write_text("testdb")

        config = sample_config.copy()
        config["db_host"] = ""

        config_file = db_dir / "config.json"
        with open(config_file, "w") as f:
            json.dump(config, f)

        with patch.object(Settings, "BASE_DIR", base_dir):
            with pytest.raises(
                ValueError, match="Missing database config field: db_host"
            ):
                Settings.get_db_config()


class TestSettingsGetLlmConfig:
    """Test Settings.get_llm_config() method."""

    def test_get_llm_config_complete(self, temp_natquery_dir):
        """Test successful LLM config extraction."""
        with patch.object(Settings, "BASE_DIR", temp_natquery_dir):
            llm_config = Settings.get_llm_config()
            expected = {
                "provider": "groq",
                "api_key": "test_key",
                "model": "llama3-8b-8192",
            }
            assert llm_config == expected

    @pytest.mark.parametrize(
        "missing_field", ["llm_provider", "llm_api_key", "llm_model"]
    )
    def test_get_llm_config_missing_field(self, temp_dir, sample_config, missing_field):
        """Test ValueError when required LLM field is missing."""
        base_dir = Path(temp_dir) / ".natquery"
        db_dir = base_dir / "testdb"
        db_dir.mkdir(parents=True, exist_ok=True)
        (base_dir / "current_db").write_text("testdb")

        config = sample_config.copy()
        del config[missing_field]

        config_file = db_dir / "config.json"
        with open(config_file, "w") as f:
            json.dump(config, f)

        with patch.object(Settings, "BASE_DIR", base_dir):
            with pytest.raises(
                ValueError, match=f"Missing LLM config field: {missing_field}"
            ):
                Settings.get_llm_config()
