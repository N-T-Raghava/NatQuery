import json
from pathlib import Path
from getpass import getpass
from urllib.parse import urlparse
from natquery.config.settings import Settings


BASE_DIR = Path.home() / ".natquery"
CONFIG_FILE = BASE_DIR / "config.json"


def extract_db_name_from_dsn(dsn: str) -> str:
    """
    Extract database name from a PostgreSQL DSN.
    Supports both URL format (postgresql://) and key-value format (dbname=).
    """
    parsed = urlparse(dsn)

    if parsed.scheme:  # URL format
        db_name = parsed.path.lstrip("/").split("?")[
            0
        ]  # Remove leading / and query params
        return db_name if db_name else "unknown_db"

    # Fallback: key-value DSN (e.g., "dbname=mydb user=postgres host=localhost")
    parts = dsn.split()
    for part in parts:
        if part.startswith("dbname="):
            return part.split("=", 1)[1]

    return "unknown_db"


def connect_command(dsn_arg=None):
    """
    Setup for NatQuery.
    If dsn_arg is provided, extracts DB name and uses it as workspace.
    Otherwise, runs interactive setup for standard or DSN connections.
    """

    print("============ NatQuery Setup ============")

    BASE_DIR.mkdir(exist_ok=True)

    # If DSN was provided as argument
    if dsn_arg:
        db_name = extract_db_name_from_dsn(dsn_arg)
        workspace_name = db_name

        print(f"Your Datbase Name: {db_name}")
        print(f"Workspace Name: {workspace_name}")

        config = {
            "connection_type": "dsn",
            "db_dsn": dsn_arg,
            "db_name": db_name,
            "workspace_name": workspace_name,
        }

    else:
        db_name = input("Database Name: ").strip()
        workspace_name = db_name

        db_dir = BASE_DIR / workspace_name
        db_dir.mkdir(parents=True, exist_ok=True)

        ssl_input = input("Use SSL? (yes/no): ").strip().lower()
        use_ssl = ssl_input in {"yes", "y"}

        config = {
            "connection_type": "standard",
            "db_host": input("DB Host (e.g. 127.0.0.1): ").strip(),
            "db_port": input("DB Port (e.g. 5432): ").strip(),
            "db_name": db_name,
            "db_user": input("DB User: ").strip(),
            "db_password": getpass("DB Password: "),
            "workspace_name": workspace_name,
        }

        if use_ssl:
            config["sslmode"] = "require"

    db_dir = BASE_DIR / workspace_name
    db_dir.mkdir(parents=True, exist_ok=True)

    # LLM config (common to both)
    config.update(
        {
            "llm_provider": input("LLM Provider (groq): ").strip(),
            "llm_api_key": getpass("LLM API Key: "),
            "llm_model": input("LLM Model (e.g. llama-3.1-70b-versatile): ").strip(),
        }
    )

    config_file = db_dir / "config.json"

    with open(config_file, "w") as f:
        json.dump(config, f, indent=2)

    # Set active database
    (BASE_DIR / "current_db").write_text(workspace_name)

    print(f"\nConfiguration saved for workspace: {workspace_name}")
    print("You can now run: natquery\n")


def reset_command():
    """
    Deletes the active database configuration.
    """

    try:
        config_path = Settings._get_config_path()
    except RuntimeError:
        print("No active database configured.")
        return

    if config_path.exists():
        config_path.unlink()
        print("Configuration removed for active database.")
    else:
        print("No configuration found.")


def show_config_command():
    """
    Display current active DB configuration (without secrets).
    """

    try:
        config = Settings.load_config()
    except RuntimeError:
        print("NatQuery not configured.")
        print("Run: natquery connect")
        return

    safe_config = config.copy()

    if "db_password" in safe_config:
        safe_config["db_password"] = "********"

    if "llm_api_key" in safe_config:
        safe_config["llm_api_key"] = "********"

    print(json.dumps(safe_config, indent=2))
