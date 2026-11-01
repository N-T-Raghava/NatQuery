"""Exception hierarchy.

Every error that can reach an end user carries a *user_message* that is safe to
display (it never contains schema names, SQL, DB error text or data values).
The *internal* detail is only ever written to the audit / system log.
"""

from __future__ import annotations


class NatQueryError(Exception):
    code = "internal_error"
    default_message = (
        "Something went wrong. Please try again or contact an administrator."
    )

    def __init__(
        self,
        user_message: str | None = None,
        *,
        internal: str | None = None,
        reason: str | None = None,
    ):
        self.user_message = user_message or self.default_message
        self.internal = internal or self.user_message
        self.reason = reason or self.code  # machine-readable cause, for audit/metrics
        self.ref: str | None = None  # audit reference id, set by the service
        super().__init__(self.internal)


class ConfigError(NatQueryError):
    code = "config_error"
    default_message = "NatQuery is not configured correctly. Contact an administrator."


class AuthenticationError(NatQueryError):
    code = "authentication_failed"
    # One message for unknown user / wrong password / locked / disabled, so the
    # response never reveals which usernames exist.
    default_message = "Invalid credentials, or the account is locked or disabled."


class AccountLockedError(AuthenticationError):
    code = "account_locked"


class SessionExpiredError(AuthenticationError):
    code = "session_expired"
    default_message = "Your session has expired. Please log in again."


class PermissionDenied(NatQueryError):
    """Policy refused the request. Never explains *which* object was hidden."""

    code = "permission_denied"
    default_message = (
        "You do not have access to the data needed to answer this question."
    )


class QueryRejected(NatQueryError):
    """SQL failed structural validation (not a permissions problem).

    ``fixable`` marks errors the LLM may be asked to repair (bad syntax, an
    unresolved name). Security violations are never fixable.
    """

    code = "query_rejected"
    default_message = "I could not turn that into a safe query. Try rephrasing."

    def __init__(
        self, user_message=None, *, internal=None, reason=None, fixable: bool = False
    ):
        super().__init__(user_message, internal=internal, reason=reason)
        self.fixable = fixable


class InputRejected(NatQueryError):
    code = "input_rejected"
    default_message = "That request was rejected by the input safety checks."


class RateLimitExceeded(NatQueryError):
    code = "rate_limited"
    default_message = "You are sending requests too quickly. Please wait and retry."


class ExecutionFailed(NatQueryError):
    code = "execution_failed"
    default_message = "The query could not be completed."

    def __init__(
        self,
        user_message=None,
        *,
        internal=None,
        reason=None,
        fixable: bool = False,
        db_hint: str = "",
    ):
        super().__init__(user_message, internal=internal, reason=reason)
        self.fixable = fixable  # SQL-level mistake the LLM may repair (PG class 42)
        self.db_hint = db_hint  # sanitised first line of the DB error, safe for the LLM


class LLMUnavailable(NatQueryError):
    code = "llm_unavailable"
    default_message = (
        "The language model is unavailable right now. Please retry shortly."
    )
