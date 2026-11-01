"""Configuration manager.

Layout (all private, 0700/0600):
    ~/.natquery/current_db                 name of the active workspace
    ~/.natquery/<db>/config.json           connection + LLM + security settings
    ~/.natquery/<db>/policy.yaml           access policy (roles -> tables/columns/rows)
    ~/.natquery/<db>/semantics.json        optional table/column descriptions + synonyms
    ~/.natquery/<db>/schema.json           extracted schema (admin-only artefact)

Secrets may be supplied through the environment instead of the file:
    NATQUERY_DB_PASSWORD, NATQUERY_DB_DSN, NATQUERY_LLM_API_KEY
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from natquery.config import paths

DEFAULT_SECURITY: dict[str, Any] = {
    "policy_file": None,  # default: <workspace>/policy.yaml
    "auth_provider": "local",
    "auth_config": {},
    "session_idle_minutes": 15,
    "session_max_hours": 8,
    "rate_limit_per_minute": 20,
    "llm_calls_per_hour": 120,
    "max_repair_attempts": 2,
    "schema_full_max_columns": 250,  # schemas up to this size go to the LLM whole, unpruned
    "enable_optimizer": False,
    "log_question_text": True,
}


class Settings:
    @classmethod
    def base_dir(cls) -> Path:
        return paths.base_dir()

    @classmethod
    def _get_active_db_name(cls) -> str:
        current_file = cls.base_dir() / "current_db"
        if not current_file.exists():
            raise RuntimeError("No active database configured.\nRun: natquery connect")
        return paths.safe_name(current_file.read_text().strip())

    @classmethod
    def workspace(cls) -> Path:
        return paths.workspace_dir(cls._get_active_db_name())

    @classmethod
    def _get_config_path(cls) -> Path:
        return cls.base_dir() / cls._get_active_db_name() / "config.json"

    @classmethod
    def exists(cls) -> bool:
        try:
            return cls._get_config_path().exists()
        except RuntimeError:
            return False

    @classmethod
    def load_config(cls) -> dict:
        config_path = cls._get_config_path()
        if not config_path.exists():
            raise RuntimeError(
                "NatQuery not configured properly.\nRun: natquery connect"
            )
        with open(config_path, encoding="utf-8") as f:
            return json.load(f)

    @classmethod
    def save_config(cls, db_name: str, config: dict) -> None:
        paths.write_private(
            paths.workspace_dir(db_name) / "config.json", json.dumps(config, indent=2)
        )

    @staticmethod
    def _schemas(config: dict) -> list[str]:
        """``db_schemas`` in config.json: a list or a comma-separated string. Default ["public"]."""
        raw = config.get("db_schemas") or ["public"]
        if isinstance(raw, str):
            raw = [s.strip() for s in raw.split(",")]
        return [s for s in raw if s]

    @classmethod
    def get_db_config(cls) -> dict:
        config = cls.load_config()
        env_dsn = os.environ.get("NATQUERY_DB_DSN")
        env_pw = os.environ.get("NATQUERY_DB_PASSWORD")

        if env_dsn or config.get("connection_type") == "dsn":
            dsn = env_dsn or config.get("db_dsn")
            if not dsn:
                raise ValueError("Missing database DSN.")
            return {
                "type": "dsn",
                "dsn": dsn,
                "dbname": config.get("db_name", "unknown_db"),
                "schemas": cls._schemas(config),
            }

        required = ["db_host", "db_port", "db_name", "db_user"]
        for key in required:
            if not config.get(key):
                raise ValueError(f"Missing database config field: {key}")
        password = env_pw or config.get("db_password")
        if not password:
            raise ValueError("Missing database config field: db_password")
        return {
            "type": "standard",
            "host": config["db_host"],
            "port": config["db_port"],
            "dbname": config["db_name"],
            "user": config["db_user"],
            "password": password,
            "sslmode": config.get("sslmode"),
            "schemas": cls._schemas(config),
        }

    @classmethod
    def get_llm_config(cls) -> dict[str, str]:
        config = cls.load_config()
        api_key = os.environ.get("NATQUERY_LLM_API_KEY") or config.get("llm_api_key")
        out = {
            "provider": config.get("llm_provider"),
            "api_key": api_key,
            "model": config.get("llm_model"),
        }
        for key, value in out.items():
            if not value:
                raise ValueError(f"Missing LLM config field: {key}")
        return out

    @classmethod
    def get_security_config(cls) -> dict[str, Any]:
        merged = dict(DEFAULT_SECURITY)
        merged.update(cls.load_config().get("security", {}))
        if not merged.get("policy_file"):
            merged["policy_file"] = str(cls.workspace() / "policy.yaml")
        return merged
