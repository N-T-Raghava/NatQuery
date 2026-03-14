<<<<<<< HEAD
import json
from pathlib import Path
from typing import Dict


class Settings:
    """
    Configuration manager for NatQuery.
    Reads configuration from .natquery/config.json
    """

    BASE_DIR = Path(".natquery")
    CONFIG_FILE = BASE_DIR / "config.json"

    @classmethod
    def exists(cls) -> bool:
        """Check if config file exists."""
        return cls.CONFIG_FILE.exists()

    @classmethod
    def load_config(cls) -> Dict:
        """Load configuration from config.json."""
        if not cls.exists():
            raise RuntimeError(
                "NatQuery not configured.\nRun: natquery connect"
            )

        with open(cls.CONFIG_FILE, "r") as f:
            return json.load(f)

    @classmethod
    def get_db_config(cls) -> Dict[str, str]:
        """Return database configuration."""
        config = cls.load_config()

        required = [
            "db_host",
            "db_port",
            "db_name",
            "db_user",
            "db_password",
        ]

        for key in required:
            if key not in config or not config[key]:
                raise ValueError(f"Missing database config field: {key}")

        return {
            "host": config["db_host"],
            "port": config["db_port"],
            "dbname": config["db_name"],
            "user": config["db_user"],
            "password": config["db_password"],
        }

    @classmethod
    def get_llm_config(cls) -> Dict[str, str]:
        """Return LLM configuration."""
        config = cls.load_config()

        required = [
            "llm_provider",
            "llm_api_key",
            "llm_model",
        ]

        for key in required:
            if key not in config or not config[key]:
                raise ValueError(f"Missing LLM config field: {key}")

        return {
            "provider": config["llm_provider"],
            "api_key": config["llm_api_key"],
            "model": config["llm_model"],
=======
import os
from dotenv import load_dotenv
from typing import Dict

load_dotenv()


class Settings:
    """
    Central configuration manager for NatQuery.
    """

    @staticmethod
    def load_settings() -> Dict[str, str]:
        return {
            "LLM_PROVIDER": os.getenv("NATQUERY_LLM_PROVIDER"),
            "LLM_API_KEY": os.getenv("NATQUERY_LLM_API_KEY"),
            "LLM_MODEL": os.getenv("NATQUERY_LLM_MODEL"),
            "DB_HOST": os.getenv("NATQUERY_DB_HOST"),
            "DB_PORT": os.getenv("NATQUERY_DB_PORT"),
            "DB_NAME": os.getenv("NATQUERY_DB_NAME"),
            "DB_USER": os.getenv("NATQUERY_DB_USER"),
            "DB_PASSWORD": os.getenv("NATQUERY_DB_PASSWORD"),
            "DEFAULT_LIMIT": os.getenv("NATQUERY_DEFAULT_LIMIT", "50"),
            "MAX_QUERY_COST": os.getenv("NATQUERY_MAX_QUERY_COST", "100000"),
        }

    @staticmethod
    def get_llm_api_key() -> str:
        key = os.getenv("NATQUERY_LLM_API_KEY")
        if not key:
            raise ValueError("Missing NATQUERY_LLM_API_KEY in environment.")
        return key

    @staticmethod
    def get_llm_model() -> str:
        model = os.getenv("NATQUERY_LLM_MODEL")
        if not model:
            raise ValueError("Missing NATQUERY_LLM_MODEL in environment.")
        return model

    @staticmethod
    def get_db_config() -> Dict[str, str]:
        required = [
            "NATQUERY_DB_HOST",
            "NATQUERY_DB_PORT",
            "NATQUERY_DB_NAME",
            "NATQUERY_DB_USER",
            "NATQUERY_DB_PASSWORD",
        ]

        for var in required:
            if not os.getenv(var):
                raise ValueError(f"Missing required DB environment variable: {var}")

        return {
            "host": os.getenv("NATQUERY_DB_HOST"),
            "port": os.getenv("NATQUERY_DB_PORT"),
            "dbname": os.getenv("NATQUERY_DB_NAME"),
            "user": os.getenv("NATQUERY_DB_USER"),
            "password": os.getenv("NATQUERY_DB_PASSWORD"),
>>>>>>> e3c67f6d4c7a6e402fd1d643a3c34c451c7cf566
        }