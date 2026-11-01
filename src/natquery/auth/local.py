"""Built-in username/password provider (scrypt hashes, lockout, generic errors)."""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any

from natquery.auth import hashing
from natquery.auth.base import AuthProvider, register_provider
from natquery.auth.models import CredentialField, Identity
from natquery.auth.user_store import UserRecord, UserStore
from natquery.errors import AccountLockedError, AuthenticationError


class LocalPasswordProvider(AuthProvider):
    name = "local"

    def __init__(
        self, config: dict[str, Any] | None = None, store: UserStore | None = None
    ):
        config = config or {}
        self.store = store or UserStore()
        self.lock_threshold = int(config.get("lock_threshold", 5))
        self.lock_seconds = int(config.get("lock_minutes", 15)) * 60

    def credential_fields(self) -> list[CredentialField]:
        return [
            CredentialField("username", "Username"),
            CredentialField("password", "Password", secret=True),
        ]

    @staticmethod
    def _identity(rec: UserRecord) -> Identity:
        return Identity(
            user_id=rec.user_id,
            username=rec.username,
            roles=rec.roles,
            attributes=dict(rec.attributes),
            auth_method="local",
        )

    def authenticate(self, credentials: Mapping[str, str]) -> Identity:
        username = (credentials.get("username") or "").strip()
        password = credentials.get("password") or ""
        rec = self.store.get(username) if username else None

        if rec is None:
            hashing.verify_password(password, hashing.DUMMY_HASH)  # equalise timing
            raise AuthenticationError(internal=f"unknown user '{username[:64]}'")
        if not rec.active:
            hashing.verify_password(password, hashing.DUMMY_HASH)
            raise AuthenticationError(internal=f"disabled account '{rec.username}'")
        if rec.locked_until > time.time():
            hashing.verify_password(password, hashing.DUMMY_HASH)
            raise AccountLockedError(internal=f"locked account '{rec.username}'")

        if not hashing.verify_password(password, rec.password_hash):
            self.store.record_failure(
                rec.username, self.lock_threshold, self.lock_seconds
            )
            raise AuthenticationError(internal=f"bad password for '{rec.username}'")

        if hashing.needs_rehash(rec.password_hash):
            self.store.set_password_hash(rec.username, hashing.hash_password(password))
        self.store.record_success(rec.username)
        return self._identity(rec)

    def refresh(self, identity: Identity) -> Identity:
        rec = self.store.get(identity.username)
        if rec is None or not rec.active or rec.user_id != identity.user_id:
            raise AuthenticationError(
                internal=f"session revoked for '{identity.username}'"
            )
        return self._identity(rec)  # picks up role / attribute changes immediately


register_provider("local", lambda config: LocalPasswordProvider(config))
