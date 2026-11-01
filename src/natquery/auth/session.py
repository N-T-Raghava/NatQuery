"""Sessions: idle + absolute expiry, per-request identity refresh, pluggable storage."""

from __future__ import annotations

import hashlib
import secrets
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass

from natquery.auth.base import AuthProvider
from natquery.auth.models import Identity
from natquery.errors import SessionExpiredError


@dataclass
class Session:
    token: str
    identity: Identity
    created_at: float
    last_seen: float
    expires_at: float


class SessionStore(ABC):
    """Swap for Redis / DB storage when running more than one process."""

    @abstractmethod
    def put(self, key: str, session: Session) -> None: ...
    @abstractmethod
    def get(self, key: str) -> Session | None: ...
    @abstractmethod
    def delete(self, key: str) -> None: ...


class InMemorySessionStore(SessionStore):
    def __init__(self):
        self._d: dict[str, Session] = {}
        self._lock = threading.Lock()

    def put(self, key, session):
        with self._lock:
            self._d[key] = session

    def get(self, key):
        with self._lock:
            return self._d.get(key)

    def delete(self, key):
        with self._lock:
            self._d.pop(key, None)


class SessionManager:
    def __init__(
        self,
        provider: AuthProvider,
        idle_seconds: int = 900,
        max_seconds: int = 8 * 3600,
        store: SessionStore | None = None,
    ):
        self.provider = provider
        self.idle_seconds = idle_seconds
        self.max_seconds = max_seconds
        self.store = store or InMemorySessionStore()

    @staticmethod
    def _key(token: str) -> str:  # tokens are never stored in clear
        return hashlib.sha256(token.encode()).hexdigest()

    def create(self, identity: Identity) -> Session:
        now = time.time()
        s = Session(
            secrets.token_urlsafe(32), identity, now, now, now + self.max_seconds
        )
        self.store.put(self._key(s.token), s)
        return s

    def validate(self, token: str) -> Session:
        """Return a live session (identity refreshed from the provider) or raise."""
        key = self._key(token)
        s = self.store.get(key)
        now = time.time()
        if s is None or now > s.expires_at or now - s.last_seen > self.idle_seconds:
            self.store.delete(key)
            raise SessionExpiredError(internal="session missing or expired")
        try:
            s.identity = self.provider.refresh(s.identity)
        except Exception:
            self.store.delete(key)
            raise
        s.last_seen = now
        self.store.put(key, s)
        return s

    def revoke(self, token: str) -> None:
        self.store.delete(self._key(token))
