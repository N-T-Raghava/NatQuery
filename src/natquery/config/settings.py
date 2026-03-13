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
        }