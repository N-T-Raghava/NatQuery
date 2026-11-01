"""QueryService: the one place where auth, policy, LLM, enforcement and execution meet.

    login -> ask(question):
        validate session (identity re-checked)  ->  pick role  ->  role-filtered schema view
        -> input guards -> rate limit -> LLM -> ENFORCER -> audit(authorized) -> read-only execution
        -> result guard -> audit(completed) -> response

Failure policy
  * every refusal is audited with a machine-readable reason and an internal detail;
  * users only ever see `NatQueryError.user_message` plus an opaque reference id;
  * only structurally fixable problems are sent back to the LLM for repair (bounded);
    a policy denial is never retried, never explained to the LLM.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Mapping, Sequence
from typing import Any

from natquery.access.enforcer import EnforcedQuery, SQLEnforcer
from natquery.access.policy import Policy, ResolvedRole
from natquery.access.result_guard import ResultGuard
from natquery.access.schema_view import SchemaView, build_view
from natquery.audit.log import AuditLog
from natquery.auth.models import Identity
from natquery.auth.session import Session, SessionManager
from natquery.backends.base import QueryBackend
from natquery.errors import (
    AuthenticationError,
    ExecutionFailed,
    InputRejected,
    NatQueryError,
    PermissionDenied,
    QueryRejected,
    RateLimitExceeded,
)
from natquery.execution.optimizer import evaluate_query_plan, should_rewrite
from natquery.execution.query_planner import generate_rewrite_candidates
from natquery.llm.generator import SQLGenerator, is_unanswerable
from natquery.observability.cost_analyzer import analyze_cost
from natquery.observability.index_recommender import suggest_indexes
from natquery.observability.logger import NatQueryLogger
from natquery.orchestration.types import QueryResponse
from natquery.schema.formatter import format_schema
from natquery.schema.retriever import retrieve_relevant_schema
from natquery.security.input_guard import InputGuard, default_guards
from natquery.security.rate_limit import RateLimiter, SlidingWindowLimiter
from natquery.security.redaction import redact_secrets, sanitize_db_error

_DENIED_TYPES = (
    PermissionDenied,
    InputRejected,
    RateLimitExceeded,
    AuthenticationError,
)


class QueryService:
    def __init__(
        self,
        *,
        backend: QueryBackend,
        policy: Policy,
        schema: dict,
        generator: SQLGenerator,
        audit: AuditLog,
        sessions: SessionManager,
        security: Mapping[str, Any],
        db_name: str = "unknown_db",
        limiter: RateLimiter | None = None,
        guards: Sequence[InputGuard] | None = None,
    ):
        self.backend = backend
        self.policy = policy
        self.schema = schema
        self.generator = generator
        self.audit = audit
        self.sessions = sessions
        self.security = security
        self.db_name = db_name
        self.limiter = limiter or SlidingWindowLimiter()
        self.guards = list(guards) if guards is not None else default_guards()
        self.enforcer = SQLEnforcer(
            policy,
            dialect=backend.dialect,
            allowed_schema=backend.allowed_schema,
            allowed_schemas=getattr(backend, "schemas", None),
        )
        self._views: dict[str, SchemaView] = {}

    # ===================================================================== auth
    def login(self, credentials: Mapping[str, str]) -> Session:
        try:
            identity = self.sessions.provider.authenticate(credentials)
        except AuthenticationError as e:
            e.ref = self.audit.record(
                "login",
                actor={"username": str(credentials.get("username", ""))[:64]},
                outcome="denied",
                reason=e.reason,
                detail=e.internal,
            )
            raise
        session = self.sessions.create(identity)
        self.audit.record(
            "login", actor=identity, outcome="ok", method=identity.auth_method
        )
        return session

    def logout(self, session: Session) -> None:
        self.sessions.revoke(session.token)
        self.audit.record("logout", actor=session.identity)

    def role_for(self, token: str, requested: str | None = None) -> ResolvedRole:
        """Current effective role of a live session (used by the CLI for permissions/prompts)."""
        return self.policy.pick_role(self.sessions.validate(token).identity, requested)

    # ===================================================================== ask
    def ask(
        self, token: str, question: str, *, role: str | None = None
    ) -> QueryResponse:
        ref = uuid.uuid4().hex[:12]
        ctx: dict[str, Any] = {
            "identity": None,
            "role": None,
            "sql": None,
            "repairs": 0,
        }
        try:
            return self._ask(token, question, role, ref, ctx)
        except NatQueryError as e:
            e.ref = ref
            self._audit_failure(e, question, ref, ctx)
            raise
        except Exception as e:  # never let internals reach the user
            err = NatQueryError(internal=f"{type(e).__name__}: {str(e)[:300]}")
            err.ref = ref
            self._audit_failure(err, question, ref, ctx)
            NatQueryLogger.log_event(
                "ERROR",
                "unhandled_exception",
                self.db_name,
                conv_id=ref,
                details={"error": f"{type(e).__name__}: {e}"},
            )
            raise err from e

    def _ask(
        self,
        token: str,
        question: str,
        requested_role: str | None,
        ref: str,
        ctx: dict,
    ) -> QueryResponse:
        session = self.sessions.validate(token)
        identity: Identity = session.identity
        ctx["identity"] = identity

        role = self.policy.pick_role(identity, requested_role)
        ctx["role"] = role.name
        view = self._view(role)

        for guard in self.guards:
            guard.check(question)
        self.limiter.hit(
            f"q:{identity.user_id}", int(self.security["rate_limit_per_minute"]), 60
        )

        full_cols = int(self.security.get("schema_full_max_columns", 250))
        picked = retrieve_relevant_schema(
            question, view.prompt_schema, full_if_columns=full_cols
        )
        schema_text = format_schema(picked)
        self._llm_budget(identity)
        sql = self.generator.generate(question, schema_text)
        # "Unanswerable" on a pruned schema may only mean the table picker left something out:
        # retry once with a wider selection before telling the user the data is not there.
        if is_unanswerable(sql) and len(picked["tables"]) < len(
            view.prompt_schema["tables"]
        ):
            wide = retrieve_relevant_schema(
                question,
                view.prompt_schema,
                top_k=10,
                max_tables=32,
                full_if_columns=full_cols,
                lookup_max_columns=10,
            )
            if len(wide["tables"]) > len(picked["tables"]):
                schema_text = format_schema(wide)
                self._llm_budget(identity)
                sql = self.generator.generate(question, schema_text)
        max_repairs = int(self.security["max_repair_attempts"])

        while True:
            ctx["sql"] = sql
            if is_unanswerable(sql):
                raise QueryRejected(
                    "I could not find the data needed to answer that among the data you have access to.",
                    internal="LLM reported the question unanswerable from the role view",
                    reason="unanswerable",
                )
            try:
                enforced = self.enforcer.enforce(sql, identity, role, view)
                plan_info = None
                if self.security.get("enable_optimizer") and role.can("can_view_plan"):
                    enforced, plan_info = self._optimize(
                        sql, enforced, identity, role, view, ref
                    )

                # Write the audit record BEFORE touching the data: if auditing is broken, no query runs.
                self.audit.record(
                    "query_authorized",
                    actor=identity,
                    ref=ref,
                    role=role.name,
                    question=self._question_for_log(question),
                    llm_sql=sql,
                    executed_sql=enforced.sql,
                    tables=list(enforced.tables),
                    masked=list(enforced.masked_columns),
                    row_filtered=list(enforced.row_filtered_tables),
                    repairs=ctx["repairs"],
                )

                extra = {}
                session_vars = self.policy.session_vars_for(identity, role.name)
                if session_vars:  # only passed when the policy asks for it (RLS)
                    extra["session_vars"] = session_vars
                out = self.backend.execute_readonly(
                    enforced.sql,
                    max_rows=enforced.max_rows,
                    timeout_ms=enforced.statement_timeout_ms,
                    db_role=role.db_role,
                    **extra,
                )
                break
            except (QueryRejected, ExecutionFailed) as e:
                if getattr(e, "fixable", False) and ctx["repairs"] < max_repairs:
                    ctx["repairs"] += 1
                    hint = (
                        e.db_hint
                        if isinstance(e, ExecutionFailed)
                        else sanitize_db_error(e.internal)
                    )
                    self.audit.record(
                        "query_repair",
                        actor=identity,
                        ref=ref,
                        outcome="retry",
                        reason=e.reason,
                        attempt=ctx["repairs"],
                        hint=hint,
                    )
                    self._llm_budget(identity)
                    sql = self.generator.repair(question, sql, hint, schema_text)
                    continue
                raise

        rows, redactions = ResultGuard(
            self.policy.redaction_patterns, enforced.redact
        ).apply(out.rows)
        notes = []
        if out.truncated:
            notes.append(
                f"Showing the first {enforced.max_rows} rows (the limit for your role)."
            )
        if enforced.masked_columns:
            notes.append("Some columns are shown in masked form for your role.")
        if redactions:
            notes.append("Some values were redacted by data-protection rules.")

        try:
            self.audit.record(
                "query_completed",
                actor=identity,
                ref=ref,
                role=role.name,
                rows=len(rows),
                truncated=out.truncated,
                redactions=redactions,
                elapsed_ms=round(out.elapsed_ms, 1),
                repairs=ctx["repairs"],
            )
        except (
            Exception
        ) as e:  # the authorisation record already exists; don't discard good results
            NatQueryLogger.log_event(
                "ERROR",
                "audit_completion_failed",
                self.db_name,
                conv_id=ref,
                details={"error": str(e)},
            )

        return QueryResponse(
            ref=ref,
            rows=rows,
            columns=list(rows[0].keys()) if rows else [],
            rows_returned=len(rows),
            truncated=out.truncated,
            execution_time_ms=out.elapsed_ms,
            role=role.name,
            generated_sql=enforced.sql if role.can("can_view_sql") else None,
            plan_summary=(
                (plan_info or {}).get("summary") if role.can("can_view_plan") else None
            ),
            suggestions=(
                (plan_info or {}).get("suggestions", [])
                if role.can("can_view_plan")
                else []
            ),
            notes=notes,
        )

    # ================================================================= helpers
    def _view(self, role: ResolvedRole) -> SchemaView:
        if role.name not in self._views:
            self._views[role.name] = build_view(self.schema, self.policy, role)
        return self._views[role.name]

    def _llm_budget(self, identity: Identity) -> None:
        self.limiter.hit(
            f"llm:{identity.user_id}", int(self.security["llm_calls_per_hour"]), 3600
        )

    def _question_for_log(self, question: str):
        if self.security.get("log_question_text", True):
            return redact_secrets(question)
        return "sha256:" + hashlib.sha256(question.encode()).hexdigest()[:16]

    def _audit_failure(
        self, e: NatQueryError, question: str, ref: str, ctx: dict
    ) -> None:
        try:
            self.audit.record(
                "query_denied" if isinstance(e, _DENIED_TYPES) else "query_failed",
                actor=ctx.get("identity"),
                ref=ref,
                outcome="denied" if isinstance(e, _DENIED_TYPES) else "error",
                reason=e.reason,
                detail=e.internal,
                role=ctx.get("role"),
                sql=ctx.get("sql"),
                question=self._question_for_log(question),
                repairs=ctx.get("repairs", 0),
            )
        except (
            Exception
        ) as audit_err:  # the user still gets the refusal; operators get the log
            NatQueryLogger.log_event(
                "ERROR",
                "audit_write_failed",
                self.db_name,
                conv_id=ref,
                details={"error": str(audit_err), "original": e.internal},
            )

    def _optimize(
        self,
        llm_sql: str,
        enforced: EnforcedQuery,
        identity: Identity,
        role: ResolvedRole,
        view: SchemaView,
        ref: str,
    ):
        """Plan-only optimisation (never ANALYZE, never executes). Best effort: failures are ignored."""
        try:
            timeout = enforced.statement_timeout_ms
            analysis = analyze_cost(
                self.backend.explain(enforced.sql, timeout_ms=timeout)
            )
            before = evaluate_query_plan(analysis)
            suggestions = suggest_indexes(analysis)
            best, best_score = enforced, before["score"]
            if should_rewrite(before):
                for candidate in generate_rewrite_candidates(llm_sql):
                    try:
                        cand = self.enforcer.enforce(
                            candidate, identity, role, view
                        )  # re-enforced, always
                        score = evaluate_query_plan(
                            analyze_cost(
                                self.backend.explain(cand.sql, timeout_ms=timeout)
                            )
                        )["score"]
                    except Exception:
                        continue
                    if score < best_score:
                        best, best_score = cand, score
            NatQueryLogger.log_performance(
                self.db_name,
                ref,
                best.sql,
                analysis["summary"],
                suggestions,
                user=identity.username,
            )
            return best, {"summary": analysis["summary"], "suggestions": suggestions}
        except Exception as e:
            NatQueryLogger.log_event(
                "WARNING",
                "optimizer_skipped",
                self.db_name,
                conv_id=ref,
                details={"error": str(e)[:200]},
            )
            return enforced, None

    def close(self) -> None:
        self.backend.close()
