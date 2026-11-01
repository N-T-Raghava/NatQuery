from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Identity:
    """Who is asking. Produced by an AuthProvider, consumed by the policy engine.

    * roles       - free-form strings; their meaning lives ONLY in the policy file,
                    so adding a role never needs a code change.
    * attributes  - facts about the user the policy can reference in row filters,
                    e.g. {"roll_no": "21CS10001"} or {"emp_id": "F1042", "dept": "CSE"}.
    """

    user_id: str
    username: str
    roles: frozenset[str]
    attributes: Mapping[str, str] = field(default_factory=dict)
    auth_method: str = "unknown"


@dataclass(frozen=True)
class CredentialField:
    """Describes one thing a provider needs from the user (drives CLI/REST prompts)."""

    name: str
    label: str
    secret: bool = False
