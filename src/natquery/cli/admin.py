"""Admin commands. Every one that changes state or reveals data needs a login whose
role holds the matching permission in the POLICY FILE (no hard-coded 'admin' role)."""

import json
import re
import time
from getpass import getpass

from natquery.access.policy import load_policy
from natquery.app import build_auth, load_workspace_policy
from natquery.audit.log import AuditLog
from natquery.auth import hashing
from natquery.auth.local import LocalPasswordProvider
from natquery.backends.base import create_backend
from natquery.config import paths
from natquery.config.settings import Settings
from natquery.errors import AuthenticationError, NatQueryError
from natquery.schema.store import load_schema, save_schema, semantics_path

_USERNAME = re.compile(r"^[A-Za-z0-9_.@-]{3,64}$")
_ATTR_KEY = re.compile(r"^[a-z_][a-z0-9_]*$")
_ATTR_VAL = re.compile(r"^[\w@.\- ]{1,128}$")


# ------------------------------------------------------------------ admin login
def prompt_credentials(provider) -> dict:
    creds = {}
    for f in provider.credential_fields():
        creds[f.name] = (
            getpass(f"{f.label}: ") if f.secret else input(f"{f.label}: ").strip()
        )
    return creds


def admin_login(permission: str):
    """Return (identity, provider, policy, audit) once a user holding *permission* logs in."""
    security = Settings.get_security_config()
    policy = load_workspace_policy(security)
    provider, _ = build_auth(security)
    audit = AuditLog()
    for attempt in range(1, 4):
        creds = prompt_credentials(provider)
        try:
            identity = provider.authenticate(creds)
        except AuthenticationError as e:
            audit.record(
                "admin_login",
                actor={"username": str(creds.get("username", ""))[:64]},
                outcome="denied",
                reason=e.reason,
                detail=e.internal,
                permission=permission,
            )
            print(e.user_message)
            time.sleep(attempt)
            continue
        allowed = [
            r
            for r in identity.roles
            if r in policy.roles and policy.resolve(r).can(permission)
        ]
        if not allowed:
            audit.record(
                "admin_login",
                actor=identity,
                outcome="denied",
                reason="missing_permission",
                permission=permission,
            )
            print("Your role is not permitted to do that.")
            return None
        audit.record("admin_login", actor=identity, outcome="ok", permission=permission)
        return identity, provider, policy, audit
    print("Too many failed attempts.")
    return None


def _local(provider) -> LocalPasswordProvider | None:
    if not isinstance(provider, LocalPasswordProvider):
        print(
            f"User management is only available for the built-in 'local' provider "
            f"(current: '{provider.name}'). Manage users in that identity system."
        )
        return None
    return provider


def _parse_attrs(pairs) -> dict:
    out = {}
    for pair in pairs or []:
        if "=" not in pair:
            raise ValueError(f"attribute '{pair}' must look like key=value")
        k, v = pair.split("=", 1)
        if not _ATTR_KEY.fullmatch(k) or not _ATTR_VAL.fullmatch(v):
            raise ValueError(
                f"invalid attribute '{pair}' (keys: lower_snake_case; values: letters/digits/@._- space)"
            )
        out[k] = v
    return out


def _new_password(username: str) -> str:
    while True:
        pw = getpass("New password: ")
        try:
            hashing.check_password_strength(pw, username)
        except ValueError as e:
            print(e)
            continue
        if pw != getpass("Repeat password: "):
            print("Passwords do not match.")
            continue
        return pw


def _check_roles(roles: list[str], policy) -> None:
    unknown = [r for r in roles if r not in policy.roles]
    if unknown:
        raise ValueError(
            f"unknown role(s) {unknown}; policy defines {sorted(policy.roles)}"
        )


# ------------------------------------------------------------------ user commands
def user_command(args) -> int:
    security = Settings.get_security_config()
    policy = load_workspace_policy(security)
    provider, _ = build_auth(security)
    if _local(provider) is None:
        return 1
    audit = AuditLog()
    store = provider.store

    # Bootstrap: with no accounts at all, the very first `user add` needs no login,
    # but it must create someone who can manage users, or nobody ever could.
    actor = None
    if args.user_cmd == "add" and store.count() == 0:
        roles = [r for r in args.roles.split(",") if r]
        if not any(
            r in policy.roles and policy.resolve(r).can("can_manage_users")
            for r in roles
        ):
            print(
                "The first account must hold a role with 'can_manage_users' (see your policy file)."
            )
            return 1
        print("No accounts exist yet - creating the first administrator.")
        actor = {"username": "<bootstrap>"}
    else:
        session = admin_login("can_manage_users")
        if session is None:
            return 1
        actor = session[0]

    try:
        if args.user_cmd == "add":
            if not _USERNAME.fullmatch(args.username):
                raise ValueError(
                    "username must be 3-64 chars of letters, digits and _ . @ -"
                )
            roles = [r for r in args.roles.split(",") if r]
            _check_roles(roles, policy)
            attrs = _parse_attrs(args.attr)
            store.add(
                args.username,
                hashing.hash_password(_new_password(args.username)),
                roles,
                attrs,
            )
            audit.record(
                "user_add",
                actor=actor,
                target=args.username,
                roles=roles,
                attributes=attrs,
            )
            print(f"User '{args.username}' created.")
        elif args.user_cmd == "list":
            for u in store.list_users():
                state = "active" if u.active else "DISABLED"
                print(
                    f"{u.username:24} {state:9} roles={sorted(u.roles)} attrs={u.attributes}"
                )
        elif args.user_cmd == "roles":
            roles = [r for r in args.roles.split(",") if r]
            _check_roles(roles, policy)
            store.set_roles(args.username, roles)
            audit.record("user_roles", actor=actor, target=args.username, roles=roles)
            print("Roles updated (effective on the user's next request).")
        elif args.user_cmd == "attrs":
            attrs = _parse_attrs(args.attr)
            store.set_attributes(args.username, attrs)
            audit.record(
                "user_attrs", actor=actor, target=args.username, attributes=attrs
            )
            print("Attributes updated.")
        elif args.user_cmd == "passwd":
            store.set_password_hash(
                args.username, hashing.hash_password(_new_password(args.username))
            )
            audit.record("user_passwd", actor=actor, target=args.username)
            print("Password changed.")
        elif args.user_cmd in ("disable", "enable"):
            store.set_active(args.username, args.user_cmd == "enable")
            audit.record(f"user_{args.user_cmd}", actor=actor, target=args.username)
            print(
                f"User '{args.username}' {args.user_cmd}d (sessions end on their next request)."
            )
    except (ValueError, NatQueryError) as e:
        print(f"Error: {getattr(e, 'internal', e)}")
        return 1
    return 0


# ------------------------------------------------------------------ schema / policy / audit
def schema_refresh_command() -> int:
    session = admin_login("can_refresh_schema")
    if session is None:
        return 1
    identity, _, policy, audit = session
    db_config = Settings.get_db_config()
    backend = create_backend(db_config)
    try:
        schema = backend.extract_schema()
    finally:
        backend.close()
    path = save_schema(db_config["dbname"], schema)
    audit.record("schema_refresh", actor=identity, tables=len(schema["tables"]))
    print(f"Schema saved ({len(schema['tables'])} tables) to {path}")
    for w in policy.validate_against_schema(load_schema(db_config["dbname"])):
        print(f"  policy warning: {w}")
    return 0


def schema_hints_command(db_role: str | None = None) -> int:
    """Store example values of categorical columns in the semantics file (shown to the model, per role view)."""
    from natquery.schema.store import semantics_path
    from natquery.schema.value_hints import collect_value_hints

    session = admin_login("can_refresh_schema")
    if session is None:
        return 1
    identity, _, _, audit = session
    db_config = Settings.get_db_config()
    schema = load_schema(db_config["dbname"])
    if not schema:
        print("No cached schema: run `natquery schema refresh` first.")
        return 1
    backend = create_backend(db_config)
    try:
        hints = collect_value_hints(
            lambda sql: [
                tuple(r.values())
                for r in backend.execute_readonly(
                    sql, max_rows=100, timeout_ms=30000, db_role=db_role
                ).rows
            ],
            schema,
        )
    finally:
        backend.close()
    path = semantics_path(db_config["dbname"])
    sem = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    tables = sem.setdefault("tables", {})
    for t in tables.values():
        t.pop("value_hints", None)
    for t, h in hints.items():
        tables.setdefault(t, {})["value_hints"] = h
    paths.write_private(path, json.dumps(sem, indent=2))
    audit.record(
        "schema_hints", actor=identity, columns=sum(len(h) for h in hints.values())
    )
    print(
        f"Value hints for {sum(len(h) for h in hints.values())} columns in {len(hints)} tables saved to {path}"
    )
    return 0


def policy_init_command() -> int:
    from pathlib import Path

    examples = Path(__file__).resolve().parent.parent / "examples"
    security = Settings.get_security_config()
    target = Path(security["policy_file"])
    sem_target = semantics_path(Settings.get_db_config()["dbname"])
    for src, dst in (
        (examples / "policy.example.yaml", target),
        (examples / "semantics.example.json", sem_target),
    ):
        if dst.exists():
            print(f"Keeping existing {dst}")
        else:
            paths.write_private(dst, src.read_text(encoding="utf-8"))
            print(f"Created {dst}")
    print(
        "Edit the policy to match your real tables and roles, then run: natquery policy check"
    )
    return 0


def policy_check_command() -> int:
    security = Settings.get_security_config()
    try:
        policy = load_policy(security["policy_file"])
    except NatQueryError as e:
        print(f"INVALID policy: {e.internal}")
        return 1
    print(f"Policy OK: {len(policy.roles)} role(s): {', '.join(sorted(policy.roles))}")
    schema = load_schema(Settings.get_db_config()["dbname"])
    if schema.get("tables"):
        from natquery.access.schema_view import build_view

        for name in sorted(policy.roles):
            view = build_view(schema, policy, policy.resolve(name))
            print(
                f"  {name:12} sees {len(view.tables)} table(s); masked: "
                f"{sum(len(v) for v in view.masked.values())} column(s); "
                f"row-filtered tables: {len(view.row_filters)}"
            )
        for w in policy.validate_against_schema(schema):
            print(f"  warning: {w}")
    else:
        print(
            "(no schema yet: run `natquery schema refresh` to also check names against the database)"
        )
    return 0


def audit_command(args) -> int:
    audit = AuditLog()
    if args.audit_cmd == "verify":
        ok, n, problem = audit.verify()
        print(
            f"Audit log OK: {n} records, chain intact."
            if ok
            else f"AUDIT LOG TAMPERED after {n} records: {problem}"
        )
        return 0 if ok else 2
    session = admin_login("can_view_audit")
    if session is None:
        return 1
    for rec in audit.tail(args.n):
        who = (rec.get("actor") or {}).get("username", "-")
        print(
            f"{rec['ts'][:19]}  #{rec['seq']:<5} {rec['event']:18} {rec['outcome']:8} {who:16} "
            f"{rec['details'].get('reason', '')}"
        )
    return 0
