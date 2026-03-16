import json
from pathlib import Path
from typing import Dict


class Settings:
    """
    Configuration manager:
    Reads configuration from:
        ~/.natquery/<active_db>/config.json
    """

    BASE_DIR = Path.home() / ".natquery"

    @classmethod
    def _get_active_db_name(cls) -> str:
        current_file = cls.BASE_DIR / "current_db"

        if not current_file.exists():
            raise RuntimeError("No active database configured.\nRun: natquery connect")

        return current_file.read_text().strip()

    @classmethod
    def _get_config_path(cls) -> Path:
        db_name = cls._get_active_db_name()
        return cls.BASE_DIR / db_name / "config.json"

    @classmethod
    def exists(cls) -> bool:
        return cls._get_config_path().exists()

    @classmethod
    def load_config(cls) -> Dict:
        config_path = cls._get_config_path()

        if not config_path.exists():
            raise RuntimeError(
                "NatQuery not configured properly.\nRun: natquery connect"
            )

        with open(config_path, "r") as f:
            return json.load(f)

    @classmethod
    def get_db_config(cls) -> Dict[str, str]:
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
            "sslmode": config.get("db_sslmode"),
        }

    @classmethod
    def get_llm_config(cls) -> Dict[str, str]:
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
