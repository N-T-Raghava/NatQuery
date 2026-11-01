"""Composition root: wires every pluggable part from the active workspace's settings."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

# importing these modules registers the built-in plugins
import natquery.auth.local  # noqa: F401
import natquery.backends.postgres  # noqa: F401
import natquery.llm.groq_provider  # noqa: F401
from natquery.access.policy import Policy, load_policy
from natquery.audit.log import AuditLog
from natquery.auth.base import AuthProvider, create_provider
from natquery.auth.session import Session, SessionManager
from natquery.backends.base import QueryBackend, create_backend
from natquery.config.settings import Settings
from natquery.errors import ConfigError
from natquery.llm.base import LLMClient, create_llm
from natquery.llm.generator import SQLGenerator
from natquery.observability.logger import NatQueryLogger
from natquery.orchestration.pipeline import QueryService
from natquery.orchestration.types import QueryResponse
from natquery.schema.store import load_schema


def load_workspace_policy(security: Mapping[str, Any]) -> Policy:
    path = Path(security["policy_file"])
    if not path.exists():
        raise ConfigError(
            internal=f"No access policy at {path}. Run: natquery policy init"
        )
    return load_policy(path)


def build_auth(security: Mapping[str, Any]) -> tuple[AuthProvider, SessionManager]:
    provider = create_provider(
        security["auth_provider"], dict(security.get("auth_config") or {})
    )
    sessions = SessionManager(
        provider,
        idle_seconds=int(security["session_idle_minutes"]) * 60,
        max_seconds=int(security["session_max_hours"]) * 3600,
    )
    return provider, sessions


class NatQueryApp:
    def __init__(
        self,
        service: QueryService,
        provider: AuthProvider,
        policy: Policy,
        audit: AuditLog,
    ):
        self.service = service
        self.provider = provider
        self.policy = policy
        self.audit = audit

    @classmethod
    def from_workspace(
        cls, *, llm: LLMClient | None = None, backend: QueryBackend | None = None
    ) -> NatQueryApp:
        """Build from the active workspace. `llm` / `backend` can be injected (tests, other providers)."""
        db_config = Settings.get_db_config()
        security = Settings.get_security_config()
        db_name = db_config["dbname"]

        policy = load_workspace_policy(security)
        schema = load_schema(db_name)
        if not schema.get("tables"):
            raise ConfigError(
                internal="No schema found for this workspace. Run: natquery schema refresh"
            )
        for warning in policy.validate_against_schema(schema):
            NatQueryLogger.log_event(
                "WARNING", "policy_warning", db_name, details={"warning": warning}
            )

        provider, sessions = build_auth(security)
        backend = backend or create_backend(db_config)
        llm = llm or create_llm(Settings.get_llm_config())
        audit = AuditLog()
        service = QueryService(
            backend=backend,
            policy=policy,
            schema=schema,
            generator=SQLGenerator(llm),
            audit=audit,
            sessions=sessions,
            security=security,
            db_name=db_name,
        )
        return cls(service, provider, policy, audit)

    # ---- thin facade used by the CLI and by library callers
    def login(self, credentials: Mapping[str, str]) -> Session:
        return self.service.login(credentials)

    def logout(self, session: Session) -> None:
        self.service.logout(session)

    def ask(
        self, session: Session, question: str, *, role: str | None = None
    ) -> QueryResponse:
        return self.service.ask(session.token, question, role=role)

    def close(self) -> None:
        self.service.close()
