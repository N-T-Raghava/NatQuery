from pathlib import Path
import json
from groq import Groq
from natquery.config.settings import Settings


def generate_sql(user_query: str) -> str:

    llm_config = Settings.get_llm_config()
    client = Groq(api_key=llm_config["api_key"])

    # Load schema
    db_config = Settings.get_db_config()
    db_name = db_config["dbname"]

    schema_path = Path.home() / ".natquery" / db_name / "schema.json"

    if schema_path.exists():
        with open(schema_path, "r") as f:
            schema = json.load(f)
    else:
        schema = {}

    schema_text = ""
    for table, columns in schema.items():
        schema_text += f"\nTable: {table}\nColumns: {', '.join(columns)}\n"

    system_prompt = f"""
    You are a PostgreSQL expert.

    Database Schema:
    {schema_text}

    Convert user request into valid PostgreSQL SQL.
    Only return SQL.
    """

    response = client.chat.completions.create(
        model=llm_config["model"],
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_query},
        ],
        temperature=0.0,
    )

    sql = response.choices[0].message.content.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()

    return sql
