import json
from pathlib import Path
from getpass import getpass
from urllib.parse import urlparse
from natquery.config.settings import Settings
from natquery.observability.performance_api import (
    get_last_run,
    compare_last_two_runs,
    get_slowest_queries,
)


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


def select_database_command():
    """
    List all configured databases and allow user to select one as the active database.
    Updates the current_db file without requiring reconnection.
    """

    BASE_DIR.mkdir(exist_ok=True)

    # Find all database directories (those containing config.json)
    databases = []

    if BASE_DIR.exists():
        for item in BASE_DIR.iterdir():
            if item.is_dir() and (item / "config.json").exists():
                databases.append(item.name)

    if not databases:
        print("No configured databases found.")
        print("Run: natquery connect")
        return

    # Get current database
    current_db_file = BASE_DIR / "current_db"
    current_db = (
        current_db_file.read_text().strip() if current_db_file.exists() else None
    )

    # Display databases
    print("\n========== Configured Databases ==========")
    for i, db in enumerate(databases, 1):
        marker = " (current)" if db == current_db else ""
        print(f"{i}. {db}{marker}")

    print("\n")

    # Get user selection
    while True:
        try:
            selection = input("Select a database (enter number): ").strip()
            index = int(selection) - 1

            if 0 <= index < len(databases):
                selected_db = databases[index]
                break
            else:
                print(
                    f"Invalid selection. Please enter a number between 1 and {len(databases)}."
                )
        except ValueError:
            print("Invalid input. Please enter a number.")

    # Update current database
    current_db_file.write_text(selected_db)
    print(f"\nActive database switched to: {selected_db}")


def performance_summary_command():
    db_name = Settings.get_db_config()["dbname"]

    last = get_last_run(db_name)

    if not last:
        print("No performance data available.")
        return

    print("\n====== Performance Summary ======")
    print(f"Execution Time: {last['execution_time_ms']} ms")
    print(f"Total Cost: {last['total_cost']}")
    print(f"Planning Time: {last['planning_time_ms']} ms")

    print("\n--- Suggestions ---")
    for s in last["suggestions"]:
        print(f"- {s}")


def performance_compare_command():
    db_name = Settings.get_db_config()["dbname"]

    comp = compare_last_two_runs(db_name)

    if not comp:
        print("Oops! Not enough data to compare.")
        return

    print("\n====== Performance Comparison ======")

    print(f"Execution Time Change: {comp['execution_time_diff']} ms")
    print(f"Cost Change: {comp['cost_diff']}")

    if comp["execution_time_diff"] < 0:
        print("Booyah! Query improved!")
    else:
        print("Oops! Query got slower.")


def performance_slowest_command():
    db_name = Settings.get_db_config()["dbname"]

    slow = get_slowest_queries(db_name)

    print("\n====== Slowest Queries ======")

    for i, q in enumerate(slow, 1):
        print(f"{i}. {q['execution_time_ms']} ms → {q['sql']}")
