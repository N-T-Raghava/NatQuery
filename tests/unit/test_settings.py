import pytest
import json
import tempfile
import os
from pathlib import Path
from unittest.mock import patch
from natquery.config.settings import Settings


class TestSettingsExists:
    """Test Settings.exists() method."""

    def test_exists_when_file_exists(self, temp_config_file):
        """Test exists returns True when config file exists."""
        with patch.object(Settings, "CONFIG_FILE", Path(temp_config_file)):
            assert Settings.exists() is True

    def test_exists_when_file_missing(self):
        """Test exists returns False when config file does not exist."""
        with patch.object(Settings, "CONFIG_FILE", Path("/nonexistent/path.json")):
            assert Settings.exists() is False


class TestSettingsLoadConfig:
    """Test Settings.load_config() method."""

    def test_load_config_success(self, sample_config, temp_config_file):
        """Test successful config loading."""
        with patch.object(Settings, "CONFIG_FILE", Path(temp_config_file)):
            config = Settings.load_config()
            assert config == sample_config

    def test_load_config_missing_file(self):
        """Test RuntimeError when config file does not exist."""
        with patch.object(Settings, "CONFIG_FILE", Path("/nonexistent/path.json")):
            with pytest.raises(RuntimeError, match="NatQuery not configured"):
                Settings.load_config()

    def test_load_config_invalid_json(self):
        """Test JSONDecodeError handling for invalid JSON."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("invalid json content")
            temp_path = f.name

        try:
            with patch.object(Settings, "CONFIG_FILE", Path(temp_path)):
                with pytest.raises(json.JSONDecodeError):
                    Settings.load_config()
        finally:
            os.unlink(temp_path)


class TestSettingsGetDbConfig:
    """Test Settings.get_db_config() method."""

    def test_get_db_config_complete(self, sample_config, temp_config_file):
        """Test successful database config extraction."""
        with patch.object(Settings, "CONFIG_FILE", Path(temp_config_file)):
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
    def test_get_db_config_missing_field(
        self, sample_config, temp_config_file, missing_field
    ):
        """Test ValueError when required database field is missing."""
        config = sample_config.copy()
        del config[missing_field]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config, f)
            temp_path = f.name

        try:
            with patch.object(Settings, "CONFIG_FILE", Path(temp_path)):
                with pytest.raises(
                    ValueError, match=f"Missing database config field: {missing_field}"
                ):
                    Settings.get_db_config()
        finally:
            os.unlink(temp_path)

    def test_get_db_config_empty_field(self, sample_config, temp_config_file):
        """Test ValueError when required field is empty."""
        config = sample_config.copy()
        config["db_host"] = ""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config, f)
            temp_path = f.name

        try:
            with patch.object(Settings, "CONFIG_FILE", Path(temp_path)):
                with pytest.raises(
                    ValueError, match="Missing database config field: db_host"
                ):
                    Settings.get_db_config()
        finally:
            os.unlink(temp_path)


class TestSettingsGetLlmConfig:
    """Test Settings.get_llm_config() method."""

    def test_get_llm_config_complete(self, sample_config, temp_config_file):
        """Test successful LLM config extraction."""
        with patch.object(Settings, "CONFIG_FILE", Path(temp_config_file)):
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
    def test_get_llm_config_missing_field(
        self, sample_config, temp_config_file, missing_field
    ):
        """Test ValueError when required LLM field is missing."""
        config = sample_config.copy()
        del config[missing_field]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config, f)
            temp_path = f.name

        try:
            with patch.object(Settings, "CONFIG_FILE", Path(temp_path)):
                with pytest.raises(
                    ValueError, match=f"Missing LLM config field: {missing_field}"
                ):
                    Settings.get_llm_config()
        finally:
            os.unlink(temp_path)
