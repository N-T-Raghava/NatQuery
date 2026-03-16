import json
import uuid
from datetime import datetime, timezone
from pathlib import Path


class NatQueryLogger:

    BASE_DIR = Path.home() / ".natquery"
    SYSTEM_LOG = BASE_DIR / "system.log"

    @staticmethod
    def _timestamp():
        return datetime.now(timezone.utc).isoformat() + "Z"

    @classmethod
    def generate_conv_id(cls) -> str:
        return str(uuid.uuid4())

    @classmethod
    def log_event(
        cls,
        level: str,
        event: str,
        db_name: str = None,
        conv_id: str = None,
        details: dict = None,
    ):

        cls.BASE_DIR.mkdir(exist_ok=True)

        log_entry = {
            "timestamp": cls._timestamp(),
            "level": level,
            "event": event,
            "conv_id": conv_id,
            "db_name": db_name,
            "details": details or {},
        }

        with open(cls.SYSTEM_LOG, "a") as f:
            f.write(json.dumps(log_entry) + "\n")

    @classmethod
    def log_conversation(
        cls,
        db_name: str,
        conv_id: str,
        user_query: str,
        generated_sql: str,
        rows_returned: int,
        execution_time_ms: float,
    ):

        db_log_dir = cls.BASE_DIR / db_name / "logs"
        db_log_dir.mkdir(parents=True, exist_ok=True)

        conv_file = db_log_dir / "conversations.jsonl"

        entry = {
            "timestamp": cls._timestamp(),
            "conv_id": conv_id,
            "user_query": user_query,
            "generated_sql": generated_sql,
            "rows_returned": rows_returned,
            "execution_time_ms": execution_time_ms,
        }

        with open(conv_file, "a") as f:
            f.write(json.dumps(entry) + "\n")
