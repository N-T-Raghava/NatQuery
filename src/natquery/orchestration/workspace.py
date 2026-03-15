from pathlib import Path
import json
from natquery.config.connection import get_connection, close_connection
from natquery.schema.extractor import extract_schema
from natquery.observability.logger import NatQueryLogger
from natquery.config.settings import Settings

def initialize_workspace():

    base_dir = Path(".natquery")
    config = base_dir / "config.json"

    if not config.exists():
        return

    # Get DB name from config
    db_config = Settings.get_db_config()
    db_name = db_config["dbname"]

    workspace_dir = base_dir / db_name
    workspace_dir.mkdir(exist_ok=True)

    schema_file = workspace_dir / "schema.json"

    # If schema already exists, skip
    if schema_file.exists():
        return

    conn = get_connection()
    schema = extract_schema(conn)
    close_connection(conn)

    with open(schema_file, "w") as f:
        json.dump(schema, f, indent=2)

    NatQueryLogger.log_event(
        level="INFO",
        event="schema_extracted",
        db_name=db_name,
        conv_id=None,
        details={"schema_path": str(schema_file)}
    )
    print(f"Schema saved to {schema_file}")