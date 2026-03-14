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
        }