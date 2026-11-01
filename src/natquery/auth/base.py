"""Authentication provider interface + registry.

To add a new way of logging in (LDAP, OIDC/SSO token, API key, ERP session ...):

    class MyProvider(AuthProvider):
        name = "my-sso"
        def credential_fields(self): return [CredentialField("token", "SSO token", True)]
        def authenticate(self, credentials): ...   # return Identity or raise AuthenticationError

    register_provider("my-sso", lambda config: MyProvider(config))

or ship it in a separate package that exposes an entry point in the group
``natquery.auth_providers``. Select it with  security.auth_provider = "my-sso"  in config.json.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from typing import Any

from natquery.auth.models import CredentialField, Identity
from natquery.errors import ConfigError


class AuthProvider(ABC):
    name: str = "base"

    @abstractmethod
    def credential_fields(self) -> list[CredentialField]:
        """What the user must supply, in prompt order."""

    @abstractmethod
    def authenticate(self, credentials: Mapping[str, str]) -> Identity:
        """Return an Identity or raise AuthenticationError (use its generic message)."""

    def refresh(self, identity: Identity) -> Identity:
        """Called before every request of a live session.

        Re-check the account (disabled? roles changed?). Raise AuthenticationError
        to terminate the session. Default: trust the original identity.
        """
        return identity


_REGISTRY: dict[str, Callable[[dict[str, Any]], AuthProvider]] = {}


def register_provider(
    name: str, factory: Callable[[dict[str, Any]], AuthProvider]
) -> None:
    _REGISTRY[name] = factory


def create_provider(name: str, config: dict[str, Any] | None = None) -> AuthProvider:
    config = config or {}
    if name not in _REGISTRY:
        _load_entry_points()
    if name not in _REGISTRY:
        raise ConfigError(
            internal=f"Unknown auth provider '{name}'. Known: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[name](config)


def _load_entry_points() -> None:
    try:
        from importlib.metadata import entry_points

        for ep in entry_points(group="natquery.auth_providers"):
            if ep.name not in _REGISTRY:
                _REGISTRY[ep.name] = ep.load()
    except Exception:  # plugin discovery must never break core startup
        pass
