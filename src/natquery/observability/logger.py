"""Operational (system) logging. Every payload is secret-scrubbed first.

Security-relevant events (logins, authorization decisions, executed queries)
go to the tamper-evident AuditLog in natquery.audit, not here.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from natquery.config import paths
from natquery.security.redaction import redact_secrets


class NatQueryLogger:
    @staticmethod
    def _timestamp() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _system_log() -> Path:
        return paths.base_dir() / "system.log"

    @staticmethod
    def _append(path: Path, entry: dict) -> None:
        with paths.open_private_append(path) as f:
            f.write(json.dumps(redact_secrets(entry), default=str) + "\n")

    @classmethod
    def log_event(
        cls, level, event, db_name=None, conv_id=None, details=None, user=None
    ):
        cls._append(
            cls._system_log(),
            {
                "timestamp": cls._timestamp(),
                "level": level,
                "event": event,
                "conv_id": conv_id,
                "db_name": db_name,
                "user": user,
                "details": details or {},
            },
        )

    @staticmethod
    def _hash_query(sql: str) -> str:
        return hashlib.sha256(sql.encode()).hexdigest()[:16]

    @classmethod
    def log_performance(cls, db_name, conv_id, sql, summary, suggestions, user=None):
        path = paths.workspace_dir(db_name) / "logs" / "performance.jsonl"
        cls._append(
            path,
            {
                "timestamp": cls._timestamp(),
                "conv_id": conv_id,
                "user": user,
                "query_hash": cls._hash_query(sql),
                "sql": sql,
                "execution_time_ms": summary.get("execution_time_ms"),
                "total_cost": summary.get("total_cost"),
                "planning_time_ms": summary.get("planning_time_ms"),
                "suggestions": suggestions,
            },
        )
