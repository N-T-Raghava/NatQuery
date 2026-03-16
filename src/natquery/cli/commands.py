import json
from pathlib import Path
from getpass import getpass
from natquery.config.settings import Settings


BASE_DIR = Path.home() / ".natquery"
CONFIG_FILE = BASE_DIR / "config.json"


def connect_command():
    """
    Interactive setup for NatQuery.
    Creates:
        ~/.natquery/<db_name>/config.json
    Also sets active DB in:
        ~/.natquery/current_db
    """

    print("============ NatQuery Setup ============")

    BASE_DIR.mkdir(exist_ok=True)

    db_name = input("DB Name: ").strip()

    db_dir = BASE_DIR / db_name
    db_dir.mkdir(parents=True, exist_ok=True)

    config = {
        "db_host": input("DB Host (e.g. 127.0.0.1): ").strip(),
        "db_port": input("DB Port (e.g. 5432): ").strip(),
        "db_name": db_name,
        "db_user": input("DB User: ").strip(),
        "db_password": getpass("DB Password: "),
        "llm_provider": input("LLM Provider (groq): ").strip(),
        "llm_api_key": getpass("LLM API Key: "),
        "llm_model": input("LLM Model (e.g. llama-3.1-70b-versatile): ").strip(),
    }

    config_file = db_dir / "config.json"
    with open(config_file, "w") as f:
        json.dump(config, f, indent=2)

    # Set active database
    (BASE_DIR / "current_db").write_text(db_name)

    print(f"\nConfiguration saved for database: {db_name}")
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
