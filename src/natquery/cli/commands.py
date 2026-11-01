"""Workspace/config commands (run by whoever operates the machine, not by end users)."""

import json
from getpass import getpass
from urllib.parse import urlparse

from natquery.config import paths
from natquery.config.settings import Settings
from natquery.security.redaction import redact_secrets


def extract_db_name_from_dsn(dsn: str) -> str:
    """Database name from a URL-style or key=value PostgreSQL DSN."""
    parsed = urlparse(dsn)
    if parsed.scheme:
        return parsed.path.lstrip("/").split("?")[0] or "unknown_db"
    for part in dsn.split():
        if part.startswith("dbname="):
            return part.split("=", 1)[1]
    return "unknown_db"


def _ask_workspace_name(suggested: str = "") -> str:
    while True:
        name = (
            input(
                f"Workspace / database name{f' [{suggested}]' if suggested else ''}: "
            ).strip()
            or suggested
        )
        try:
            return paths.safe_name(name)
        except ValueError as e:
            print(e)


def connect_command(dsn_arg=None):
    """Create/refresh a workspace: DB connection + LLM settings (saved 0600)."""
    print("============ NatQuery Setup ============")
    paths.ensure_private_dir(paths.base_dir())

    if dsn_arg:
        print(
            "Note: a DSN typed on the command line is visible in shell history and process lists."
        )
        print(
            "      Prefer the interactive prompts, or set NATQUERY_DB_DSN in the environment."
        )
        suggested = extract_db_name_from_dsn(dsn_arg)
        db_name = _ask_workspace_name(suggested if suggested != "unknown_db" else "")
        config = {
            "connection_type": "dsn",
            "db_dsn": dsn_arg,
            "db_name": db_name,
            "workspace_name": db_name,
        }
    else:
        db_name = _ask_workspace_name()
        use_ssl = input("Use SSL? (yes/no): ").strip().lower() in {"yes", "y"}
        print("Use a dedicated READ-ONLY database account for NatQuery (SELECT only).")
        config = {
            "connection_type": "standard",
            "db_host": input("DB Host (e.g. 127.0.0.1): ").strip(),
            "db_port": input("DB Port (e.g. 5432): ").strip(),
            "db_name": db_name,
            "db_user": input("DB User: ").strip(),
            "workspace_name": db_name,
        }
        password = getpass(
            "DB Password (blank = read NATQUERY_DB_PASSWORD from the environment): "
        )
        if password:
            config["db_password"] = password
        if use_ssl:
            config["sslmode"] = "require"

    config["llm_provider"] = input("LLM Provider (groq): ").strip() or "groq"
    key = getpass(
        "LLM API Key (blank = read NATQUERY_LLM_API_KEY from the environment): "
    )
    if key:
        config["llm_api_key"] = key
    config["llm_model"] = input("LLM Model (e.g. llama-3.3-70b-versatile): ").strip()

    Settings.save_config(db_name, config)
    paths.write_private(paths.base_dir() / "current_db", db_name)

    print(f"\nConfiguration saved for workspace: {db_name}")
    print("Next steps:")
    print(
        "  1. natquery policy init        # create an editable access policy, then edit it"
    )
    print(
        "  2. natquery schema refresh     # read the DB structure (needs an admin login)"
    )
    print(
        "  3. natquery user add <name> --roles <role>   # create the first admin account"
    )
    print("  4. natquery                    # log in and ask questions\n")


def reset_command():
    """Remove the active workspace's connection config and cached schema.

    Policy, users and audit/system logs are deliberately kept: they are not
    secrets of the DB connection and audit history must survive a reset.
    """
    try:
        workspace = Settings.workspace()
    except RuntimeError:
        print("No active database configured.")
        return
    removed = []
    for name in ("config.json", "schema.json"):
        f = workspace / name
        if f.exists():
            f.unlink()
            removed.append(name)
    print(f"Removed: {', '.join(removed)}" if removed else "Nothing to remove.")


def show_config_command():
    """Display the active configuration with every secret masked."""
    try:
        config = Settings.load_config()
    except RuntimeError:
        print("NatQuery not configured.\nRun: natquery connect")
        return
    print(json.dumps(redact_secrets(config), indent=2))


def select_database_command():
    """List configured workspaces and switch the active one."""
    base = paths.base_dir()
    paths.ensure_private_dir(base)
    databases = []
    for item in base.iterdir():
        try:
            paths.safe_name(item.name)
        except ValueError:
            continue
        if item.is_dir() and (item / "config.json").exists():
            databases.append(item.name)
    databases.sort()
    if not databases:
        print("No configured databases found.\nRun: natquery connect")
        return

    cur = base / "current_db"
    current = cur.read_text().strip() if cur.exists() else None
    print("\n========== Configured Databases ==========")
    for i, db in enumerate(databases, 1):
        print(f"{i}. {db}{' (current)' if db == current else ''}")
    print()
    while True:
        try:
            index = int(input("Select a database (enter number): ").strip()) - 1
        except ValueError:
            print("Please enter a number.")
            continue
        if 0 <= index < len(databases):
            break
        print(f"Please enter a number between 1 and {len(databases)}.")
    paths.write_private(cur, databases[index])
    print(f"\nActive database switched to: {databases[index]}")
