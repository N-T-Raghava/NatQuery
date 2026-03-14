# src/natquery/cli/commands.py

import json
from pathlib import Path
from getpass import getpass


BASE_DIR = Path(".natquery")
CONFIG_FILE = BASE_DIR / "config.json"


def connect_command():
    """
    Interactive setup for NatQuery.
    Creates .natquery/config.json
    """

    print("=== NatQuery Setup ===")

    BASE_DIR.mkdir(exist_ok=True)

    config = {
        "db_host": input("DB Host (e.g. 127.0.0.1): ").strip(),
        "db_port": input("DB Port (e.g. 5432): ").strip(),
        "db_name": input("DB Name: ").strip(),
        "db_user": input("DB User: ").strip(),
        "db_password": getpass("DB Password: "),
        "llm_provider": input("LLM Provider (groq): ").strip(),
        "llm_api_key": getpass("LLM API Key: "),
        "llm_model": input("LLM Model (e.g. llama-3.1-70b-versatile): ").strip(),
    }

    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)

    print("\nConfiguration saved successfully.")
    print("You can now run: natquery\n")


def reset_command():
    """
    Deletes configuration.
    """

    if CONFIG_FILE.exists():
        CONFIG_FILE.unlink()
        print("Configuration removed.")
    else:
        print("No configuration found.")


def show_config_command():
    """
    Display current configuration (without secrets).
    """

    if not CONFIG_FILE.exists():
        print("NatQuery not configured.")
        print("Run: natquery connect")
        return

    with open(CONFIG_FILE, "r") as f:
        config = json.load(f)

    safe_config = config.copy()

    # Hide sensitive fields
    if "db_password" in safe_config:
        safe_config["db_password"] = "********"

    if "llm_api_key" in safe_config:
        safe_config["llm_api_key"] = "********"

    print(json.dumps(safe_config, indent=2))