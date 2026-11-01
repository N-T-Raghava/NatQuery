"""Access policy: what each role may see. Loaded from YAML or JSON.

Nothing here knows about "student", "teacher" or "admin" - those are just keys
in the file. Add a role = add a block to the file.

Concepts
--------
* tables.<name>.columns       list of visible columns, or "*"
* tables.<name>.deny_columns  removed from the above (useful with "*")
* tables.<name>.masked        {column: strategy}  -> value is replaced at SELECT time;
                              masked columns may ONLY be selected, never filtered,
                              sorted, grouped or aggregated (that would be an oracle)
* tables.<name>.row_filter    SQL condition ANDed onto every access to the table, with
                              :placeholders bound from the user's Identity.attributes
* tables."*"                  grant applying to every table not listed explicitly
* inherits                    child blocks override the parent's per-table grants
* global_deny                 tables / "table.column" / "*.column" patterns no role can see
"""

from __future__ import annotations

import fnmatch
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import sqlglot
from sqlglot import exp

from natquery.auth.models import Identity
from natquery.errors import ConfigError, PermissionDenied

ALL = "*"

DEFAULT_LIMITS: dict[str, Any] = {
    "max_rows": 500,
    "statement_timeout_ms": 5000,
    "max_joins": 6,
    "max_subquery_depth": 4,
    "max_sql_length": 6000,
    "max_ast_nodes": 3000,
    "allow_cross_join": False,
}
DEFAULT_PERMISSIONS = {
    "can_view_sql": False,
    "can_view_plan": False,
    "can_manage_users": False,
    "can_view_audit": False,
    "can_refresh_schema": False,
}

BUILTIN_MASKS = {
    "redacted": "'[REDACTED]'",
    "null": "NULL",
    "last4": "CASE WHEN {col} IS NULL THEN NULL ELSE '****' || RIGHT(CAST({col} AS TEXT), 4) END",
    "email": "CASE WHEN {col} IS NULL THEN NULL ELSE '***@' || SPLIT_PART(CAST({col} AS TEXT), '@', 2) END",
}

_PLACEHOLDER_NAME = re.compile(r"^[a-z_][a-z0-9_]*$")
_TOP_KEYS = {
    "version",
    "defaults",
    "roles",
    "global_deny",
    "masking",
    "redaction_patterns",
    "allowed_functions",
    "db_session_vars",
}
_ROLE_KEYS = {
    "inherits",
    "priority",
    "tables",
    "deny_tables",
    "permissions",
    "limits",
    "redact",
    "description",
    "db_role",
}
_TABLE_KEYS = {"columns", "deny_columns", "masked", "row_filter"}


@dataclass(frozen=True)
class TableGrant:
    columns: frozenset[str] | None  # None == every column
    deny_columns: frozenset[str] = frozenset()
    masked: Mapping[str, str] = field(default_factory=dict)
    row_filter: str | None = None


@dataclass(frozen=True)
class RoleSpec:
    name: str
    inherits: tuple[str, ...]
    priority: int
    tables: Mapping[str, TableGrant]
    deny_tables: frozenset[str]
    permissions: Mapping[str, bool]
    limits: Mapping[str, Any]
    redact: tuple[str, ...]
    db_role: str | None = None


@dataclass(frozen=True)
class ResolvedRole:
    """A role with inheritance flattened and defaults applied."""

    name: str
    priority: int
    tables: Mapping[str, TableGrant]
    deny_tables: frozenset[str]
    permissions: Mapping[str, bool]
    limits: Mapping[str, Any]
    redact: tuple[str, ...]
    db_role: str | None = (
        None  # optional DB-level role for SET LOCAL ROLE (second layer)
    )

    def can(self, permission: str) -> bool:
        return bool(self.permissions.get(permission, False))


@dataclass(frozen=True)
class Policy:
    roles: Mapping[str, RoleSpec]
    defaults: Mapping[str, Any]
    global_deny_tables: frozenset[str]
    global_deny_columns: tuple[str, ...]
    masking: Mapping[str, str]
    redaction_patterns: Mapping[str, str]
    allowed_functions: frozenset[str]
    # database setting name -> identity attribute whose value is sent with every query, so
    # row-level-security policies in the database can see who is asking (e.g. erp.person_id: person_id)
    db_session_vars: Mapping[str, str] = field(default_factory=dict)

    # ------------------------------------------------------------ role lookup
    def resolve(self, role_name: str) -> ResolvedRole:
        return _resolve(self, role_name)

    def pick_role(
        self, identity: Identity, requested: str | None = None
    ) -> ResolvedRole:
        """Choose the role a request runs under (never a union of roles).

        Highest ``priority`` wins unless the user explicitly selects one of
        their own roles. Unknown roles in the identity are ignored; no known
        role -> deny.
        """
        known = [r for r in identity.roles if r in self.roles]
        if requested is not None:
            if requested not in known:
                raise PermissionDenied(
                    internal=f"role '{requested}' not held by '{identity.username}'"
                )
            return self.resolve(requested)
        if not known:
            raise PermissionDenied(
                internal=f"no policy role for '{identity.username}' roles={sorted(identity.roles)}"
            )
        known.sort(key=lambda r: (-self.roles[r].priority, r))
        return self.resolve(known[0])

    def mask_template(self, strategy: str) -> str:
        return self.masking[strategy]

    # ------------------------------------------------------------ global deny
    def table_globally_denied(self, table: str) -> bool:
        return any(fnmatch.fnmatchcase(table, pat) for pat in self.global_deny_tables)

    def session_vars_for(
        self, identity: Identity, role_name: str = ""
    ) -> dict[str, str]:
        """Settings sent with each query. The value ``@role`` stands for the role the request runs under."""
        out = {}
        for guc, attr in self.db_session_vars.items():
            value = role_name if attr == "@role" else identity.attributes.get(attr)
            out[guc] = "" if value is None else str(value)
        return out

    def column_globally_denied(self, table: str, column: str) -> bool:
        key = f"{table}.{column}"
        return any(fnmatch.fnmatchcase(key, pat) for pat in self.global_deny_columns)

    # ------------------------------------------------------------ diagnostics
    def validate_against_schema(self, schema: dict) -> list[str]:
        """Warnings about grants naming tables/columns that don't exist."""
        tables = schema.get("tables", {})
        problems: list[str] = []
        for role in self.roles.values():
            for tname, grant in role.tables.items():
                if tname == ALL:
                    continue
                if tname.endswith(".*"):  # schema wildcard
                    if not any(t.startswith(tname[:-1]) for t in tables):
                        problems.append(
                            f"role '{role.name}': no table matches '{tname}'"
                        )
                    continue
                if tname not in tables:
                    problems.append(
                        f"role '{role.name}': table '{tname}' not in schema"
                    )
                    continue
                cols = set(tables[tname].get("columns", {}))
                named = (
                    set(grant.columns or ())
                    | set(grant.deny_columns)
                    | set(grant.masked)
                )
                for c in sorted(named - cols):
                    problems.append(
                        f"role '{role.name}': column '{tname}.{c}' not in schema"
                    )
                if grant.masked and grant.columns is not None:
                    for c in sorted(set(grant.masked) - set(grant.columns)):
                        problems.append(
                            f"role '{role.name}': masked column '{tname}.{c}' is not granted in 'columns'"
                        )
        return problems


# ====================================================================== resolving
def _resolve(
    policy: Policy, role_name: str, _seen: tuple[str, ...] = ()
) -> ResolvedRole:
    if role_name not in policy.roles:
        raise PermissionDenied(internal=f"unknown role '{role_name}'")
    if role_name in _seen:
        raise ConfigError(
            internal=f"inheritance cycle: {' -> '.join(_seen + (role_name,))}"
        )
    spec = policy.roles[role_name]

    tables: dict[str, TableGrant] = {}
    deny: frozenset[str] = frozenset()
    perms = dict(DEFAULT_PERMISSIONS)
    limits = dict(DEFAULT_LIMITS)
    limits.update({k: v for k, v in policy.defaults.items() if k in DEFAULT_LIMITS})
    redact: list[str] = []
    db_role: str | None = None

    for parent in spec.inherits:
        p = _resolve(policy, parent, _seen + (role_name,))
        tables.update(p.tables)
        deny |= p.deny_tables
        perms.update(p.permissions)
        limits.update(p.limits)
        redact.extend(p.redact)
        db_role = p.db_role or db_role

    if ALL in spec.tables:  # child wildcard supersedes anything inherited
        tables, deny = {}, frozenset()
    own_granted = set(spec.tables)
    deny = (deny - own_granted) | spec.deny_tables
    tables.update(spec.tables)
    perms.update(spec.permissions)
    limits.update(spec.limits)
    redact.extend(spec.redact)
    return ResolvedRole(
        role_name,
        spec.priority,
        tables,
        deny,
        perms,
        limits,
        tuple(dict.fromkeys(redact)),
        spec.db_role or db_role,
    )


# ====================================================================== loading
def load_policy(path: str | Path) -> Policy:
    path = Path(path)
    if not path.exists():
        raise ConfigError(internal=f"policy file not found: {path}")
    text = path.read_text(encoding="utf-8")
    try:
        if path.suffix.lower() in (".yaml", ".yml"):
            import yaml

            data = yaml.safe_load(text)
        else:
            data = json.loads(text)
    except Exception as e:
        raise ConfigError(internal=f"policy file is not valid: {e}") from e
    return parse_policy(data)


def _as_set(value, what: str) -> frozenset[str]:
    if value is None:
        return frozenset()
    if not isinstance(value, (list, tuple)) or not all(
        isinstance(v, str) for v in value
    ):
        raise ConfigError(internal=f"{what} must be a list of strings")
    return frozenset(value)


def _check_keys(block: dict, allowed: set[str], where: str) -> None:
    unknown = set(block) - allowed
    if unknown:  # typos in a security file must not be silently ignored
        raise ConfigError(internal=f"{where}: unknown key(s) {sorted(unknown)}")


def check_row_filter(template: str, where: str) -> None:
    try:
        cond = sqlglot.condition(template, dialect="postgres")
    except Exception as e:
        raise ConfigError(internal=f"{where}: row_filter does not parse: {e}") from e
    for ph in cond.find_all(exp.Placeholder):
        if not _PLACEHOLDER_NAME.fullmatch(ph.name or ""):
            raise ConfigError(
                internal=f"{where}: bad placeholder ':{ph.name}' (use :lower_snake_case)"
            )
    if list(cond.find_all(exp.Parameter)):
        raise ConfigError(internal=f"{where}: use :name placeholders only")


def _parse_session_vars(raw: Any) -> dict[str, str]:
    out = {}
    for guc, attr in (raw or {}).items():
        if not re.fullmatch(r"[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*", str(guc)):
            raise ConfigError(
                internal=f"db_session_vars: '{guc}' must look like prefix.name"
            )
        if str(attr) != "@role" and not _PLACEHOLDER_NAME.fullmatch(str(attr)):
            raise ConfigError(
                internal=f"db_session_vars.{guc}: bad attribute name '{attr}'"
            )
        out[str(guc)] = str(attr)
    return out


def parse_policy(data: Any) -> Policy:
    if not isinstance(data, dict):
        raise ConfigError(internal="policy must be a mapping")
    _check_keys(data, _TOP_KEYS, "policy")
    if data.get("version", 1) != 1:
        raise ConfigError(internal="unsupported policy version")

    masking = dict(BUILTIN_MASKS)
    for name, template in (data.get("masking") or {}).items():
        if "{col}" not in template:
            raise ConfigError(internal=f"masking.{name}: template must contain {{col}}")
        masking[name] = template

    patterns = {}
    for name, rx in (data.get("redaction_patterns") or {}).items():
        try:
            re.compile(rx)
        except re.error as e:
            raise ConfigError(internal=f"redaction_patterns.{name}: {e}") from e
        patterns[name] = rx

    defaults = dict(data.get("defaults") or {})
    _check_keys(defaults, set(DEFAULT_LIMITS), "defaults")

    gd = data.get("global_deny") or {}
    _check_keys(gd, {"tables", "columns"}, "global_deny")

    roles: dict[str, RoleSpec] = {}
    for rname, rb in (data.get("roles") or {}).items():
        rb = rb or {}
        _check_keys(rb, _ROLE_KEYS, f"roles.{rname}")
        tables: dict[str, TableGrant] = {}
        for tname, tb in (rb.get("tables") or {}).items():
            tb = tb or {}
            where = f"roles.{rname}.tables.{tname}"
            _check_keys(tb, _TABLE_KEYS, where)
            cols = tb.get("columns", ALL)
            if cols == ALL:
                columns = None
            else:
                columns = _as_set(cols, f"{where}.columns")
            masked = dict(tb.get("masked") or {})
            for col, strat in masked.items():
                if strat not in masking:
                    raise ConfigError(
                        internal=f"{where}.masked.{col}: unknown strategy '{strat}'"
                    )
            rf = tb.get("row_filter")
            if rf:
                check_row_filter(rf, where)
            tables[tname] = TableGrant(
                columns,
                _as_set(tb.get("deny_columns"), f"{where}.deny_columns"),
                masked,
                rf or None,
            )
        redact = tuple(rb.get("redact") or ())
        for r in redact:
            if r not in patterns:
                raise ConfigError(
                    internal=f"roles.{rname}.redact: unknown pattern '{r}'"
                )
        perms = dict(rb.get("permissions") or {})
        _check_keys(perms, set(DEFAULT_PERMISSIONS), f"roles.{rname}.permissions")
        limits = dict(rb.get("limits") or {})
        _check_keys(limits, set(DEFAULT_LIMITS), f"roles.{rname}.limits")
        roles[rname] = RoleSpec(
            rname,
            tuple(rb.get("inherits") or ()),
            int(rb.get("priority", 0)),
            tables,
            _as_set(rb.get("deny_tables"), f"roles.{rname}.deny_tables"),
            perms,
            limits,
            redact,
            rb.get("db_role"),
        )

    for r in roles.values():
        for parent in r.inherits:
            if parent not in roles:
                raise ConfigError(
                    internal=f"roles.{r.name}.inherits: unknown role '{parent}'"
                )

    policy = Policy(
        roles=roles,
        defaults=defaults,
        global_deny_tables=_as_set(gd.get("tables"), "global_deny.tables"),
        global_deny_columns=tuple(gd.get("columns") or ()),
        masking=masking,
        redaction_patterns=patterns,
        allowed_functions=frozenset(
            f.lower() for f in (data.get("allowed_functions") or ())
        ),
        db_session_vars=_parse_session_vars(data.get("db_session_vars")),
    )
    for rname in roles:  # surfaces inheritance cycles at load time
        policy.resolve(rname)
    return policy
