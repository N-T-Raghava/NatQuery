"""Tamper-evident audit log.

Append-only JSON-lines file where every record embeds the hash of the previous
record (a hash chain). Editing, deleting or re-ordering any past record breaks
verification. Set NATQUERY_AUDIT_KEY to make the chain an HMAC so that someone
with file access but without the key cannot silently recompute it.

Usable from several processes: appends are serialised with an OS file lock.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from natquery.config import paths
from natquery.security.redaction import redact_secrets

try:  # POSIX
    import fcntl

    def _lock(fh):
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)

    def _unlock(fh):
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)

except ImportError:  # pragma: no cover - Windows
    import msvcrt

    def _lock(fh):
        fh.seek(0)
        msvcrt.locking(fh.fileno(), msvcrt.LK_LOCK, 1)

    def _unlock(fh):
        fh.seek(0)
        msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)


GENESIS = "0" * 64
_TAIL_BYTES = 256 * 1024


class AuditLog:
    def __init__(self, path: Path | None = None):
        self.path = Path(path) if path else paths.base_dir() / "audit.log"
        self._mutex = threading.Lock()
        key = os.environ.get("NATQUERY_AUDIT_KEY")
        self._key = key.encode() if key else None

    # ------------------------------------------------------------------ hashing
    def _digest(self, prev: str, body: str) -> str:
        data = (prev + body).encode()
        if self._key:
            return hmac.new(self._key, data, hashlib.sha256).hexdigest()
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def _canonical(entry: dict) -> str:
        return json.dumps(entry, sort_keys=True, separators=(",", ":"), default=str)

    # ------------------------------------------------------------------ writing
    @contextmanager
    def _locked_file(self):
        paths.ensure_private_dir(self.path.parent)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        fh = os.fdopen(fd, "r+b")
        with self._mutex:
            _lock(fh)
            try:
                yield fh
            finally:
                _unlock(fh)
                fh.close()

    @staticmethod
    def _last_record(fh) -> dict | None:
        fh.seek(0, os.SEEK_END)
        size = fh.tell()
        if size == 0:
            return None
        fh.seek(max(0, size - _TAIL_BYTES))
        lines = [ln for ln in fh.read().splitlines() if ln.strip()]
        return json.loads(lines[-1]) if lines else None

    def record(
        self,
        event: str,
        *,
        actor: Any = None,
        outcome: str = "ok",
        ref: str | None = None,
        **details,
    ) -> str:
        """Append one record; returns its id (shown to users as a support reference)."""
        event_id = ref or uuid.uuid4().hex[:12]
        with self._locked_file() as fh:
            last = self._last_record(fh)
            entry = {
                "seq": (last["seq"] + 1) if last else 1,
                "ts": datetime.now(timezone.utc).isoformat(),
                "id": event_id,
                "event": event,
                "outcome": outcome,
                "actor": _actor_dict(actor),
                "details": redact_secrets(details),
                "prev": last["hash"] if last else GENESIS,
            }
            entry["hash"] = self._digest(entry["prev"], self._canonical(entry))
            fh.seek(0, os.SEEK_END)
            fh.write((json.dumps(entry, default=str) + "\n").encode())
            fh.flush()
            os.fsync(fh.fileno())
        return event_id

    # ------------------------------------------------------------------ reading
    def verify(self) -> tuple[bool, int, str | None]:
        """Return (ok, records_checked, problem_description)."""
        if not self.path.exists():
            return True, 0, None
        prev, seq, count = GENESIS, 0, 0
        with open(self.path, encoding="utf-8") as f:
            for lineno, line in enumerate(f, 1):
                if not line.strip():
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    return False, count, f"line {lineno}: not valid JSON"
                stored = entry.pop("hash", None)
                if entry.get("prev") != prev:
                    return (
                        False,
                        count,
                        f"line {lineno}: chain broken (record removed or reordered)",
                    )
                if entry.get("seq") != seq + 1:
                    return False, count, f"line {lineno}: sequence gap"
                if self._digest(prev, self._canonical(entry)) != stored:
                    return False, count, f"line {lineno}: record was modified"
                prev, seq, count = stored, entry["seq"], count + 1
        return True, count, None

    def tail(self, n: int = 20) -> list[dict]:
        if not self.path.exists():
            return []
        with open(self.path, encoding="utf-8") as f:
            lines = [ln for ln in f if ln.strip()]
        return [json.loads(ln) for ln in lines[-n:]]


def _actor_dict(actor: Any) -> dict | None:
    if actor is None:
        return None
    if isinstance(actor, dict):
        return actor
    return {
        "user_id": getattr(actor, "user_id", None),
        "username": getattr(actor, "username", None),
        "roles": sorted(getattr(actor, "roles", []) or []),
    }
