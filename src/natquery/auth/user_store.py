"""SQLite-backed user directory for the built-in password provider.

Kept separate from the ERP database on purpose: NatQuery credentials and roles
must never live in (or be writable through) the database users are querying.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from natquery.config import paths

_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE COLLATE NOCASE,
    password_hash TEXT NOT NULL,
    roles TEXT NOT NULL DEFAULT '[]',
    attributes TEXT NOT NULL DEFAULT '{}',
    active INTEGER NOT NULL DEFAULT 1,
    failed_attempts INTEGER NOT NULL DEFAULT 0,
    locked_until REAL NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    last_login REAL
);
"""


@dataclass
class UserRecord:
    user_id: str
    username: str
    password_hash: str
    roles: frozenset[str]
    attributes: dict
    active: bool
    failed_attempts: int
    locked_until: float
    created_at: float
    last_login: float | None


class UserStore:
    def __init__(self, path: Path | None = None):
        self.path = Path(path) if path else paths.base_dir() / "auth.db"
        paths.ensure_private_dir(self.path.parent)
        if not self.path.exists():  # create with 0600 before sqlite touches it
            os.close(os.open(self.path, os.O_CREAT | os.O_WRONLY, 0o600))
        with closing(self._connect()) as db:
            db.executescript(_SCHEMA)
            db.commit()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        return db

    @staticmethod
    def _row(r: sqlite3.Row) -> UserRecord:
        return UserRecord(
            user_id=r["user_id"],
            username=r["username"],
            password_hash=r["password_hash"],
            roles=frozenset(json.loads(r["roles"])),
            attributes=json.loads(r["attributes"]),
            active=bool(r["active"]),
            failed_attempts=r["failed_attempts"],
            locked_until=r["locked_until"],
            created_at=r["created_at"],
            last_login=r["last_login"],
        )

    # ---------------------------------------------------------------- queries
    def count(self) -> int:
        with closing(self._connect()) as db:
            return db.execute("SELECT COUNT(*) FROM users").fetchone()[0]

    def get(self, username: str) -> UserRecord | None:
        with closing(self._connect()) as db:
            r = db.execute(
                "SELECT * FROM users WHERE username = ?", (username,)
            ).fetchone()
        return self._row(r) if r else None

    def list_users(self) -> list[UserRecord]:
        with closing(self._connect()) as db:
            rows = db.execute("SELECT * FROM users ORDER BY username").fetchall()
        return [self._row(r) for r in rows]

    # ---------------------------------------------------------------- mutations
    def add(
        self, username: str, password_hash: str, roles, attributes=None
    ) -> UserRecord:
        user_id = uuid.uuid4().hex
        try:
            with closing(self._connect()) as db:
                db.execute(
                    "INSERT INTO users (user_id, username, password_hash, roles, attributes, created_at)"
                    " VALUES (?,?,?,?,?,?)",
                    (
                        user_id,
                        username,
                        password_hash,
                        json.dumps(sorted(roles)),
                        json.dumps(attributes or {}),
                        time.time(),
                    ),
                )
                db.commit()
        except sqlite3.IntegrityError as e:
            raise ValueError(f"User '{username}' already exists.") from e
        return self.get(username)  # type: ignore[return-value]

    def _update(self, username: str, **fields) -> None:
        cols = ", ".join(f"{k} = ?" for k in fields)  # keys are internal constants only
        with closing(self._connect()) as db:
            cur = db.execute(
                f"UPDATE users SET {cols} WHERE username = ?",
                (*fields.values(), username),
            )
            db.commit()
        if cur.rowcount == 0:
            raise ValueError(f"No such user: {username}")

    def set_roles(self, username, roles):
        self._update(username, roles=json.dumps(sorted(roles)))

    def set_attributes(self, username, attributes):
        self._update(username, attributes=json.dumps(attributes))

    def set_password_hash(self, username, password_hash):
        self._update(
            username, password_hash=password_hash, failed_attempts=0, locked_until=0
        )

    def set_active(self, username, active: bool):
        self._update(username, active=int(active))

    def record_failure(self, username: str, threshold: int, lock_seconds: int) -> None:
        with closing(self._connect()) as db:
            db.execute(
                "UPDATE users SET failed_attempts = failed_attempts + 1 WHERE username = ?",
                (username,),
            )
            db.execute(
                "UPDATE users SET locked_until = ?, failed_attempts = 0 "
                "WHERE username = ? AND failed_attempts >= ?",
                (time.time() + lock_seconds, username, threshold),
            )
            db.commit()

    def record_success(self, username: str) -> None:
        self._update(
            username, failed_attempts=0, locked_until=0, last_login=time.time()
        )
