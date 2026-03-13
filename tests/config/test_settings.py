import os
import pytest
from natquery.config.settings import Settings


def test_get_llm_api_key(monkeypatch):
    """
    Ensure API key is correctly read from environment.
    """
    monkeypatch.setenv("NATQUERY_LLM_API_KEY", "test_key")

    assert Settings.get_llm_api_key() == "test_key"


def test_get_db_config(monkeypatch):
    """
    Ensure DB config dictionary is properly constructed.
    """
    monkeypatch.setenv("NATQUERY_DB_HOST", "localhost")
    monkeypatch.setenv("NATQUERY_DB_PORT", "5433")
    monkeypatch.setenv("NATQUERY_DB_NAME", "testdb")
    monkeypatch.setenv("NATQUERY_DB_USER", "user")
    monkeypatch.setenv("NATQUERY_DB_PASSWORD", "pass")

    config = Settings.get_db_config()

    assert config["host"] == "localhost"
    assert config["port"] == "5433"
    assert config["dbname"] == "testdb"
    assert config["user"] == "user"
    assert config["password"] == "pass"


def test_missing_db_config(monkeypatch):
    """
    Ensure error is raised when required variables are missing.
    """
    monkeypatch.delenv("NATQUERY_DB_NAME", raising=False)

    with pytest.raises(ValueError):
        Settings.get_db_config()