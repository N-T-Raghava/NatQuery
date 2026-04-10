from groq import Groq
from natquery.config.settings import Settings
from natquery.schema.formatter import format_schema
from natquery.prompt.builder import build_prompt
from pathlib import Path
import json


def generate_sql(user_query: str) -> str:

    llm_config = Settings.get_llm_config()
    client = Groq(api_key=llm_config["api_key"])

    db_config = Settings.get_db_config()
    db_name = db_config["dbname"]

    schema_path = Path.home() / ".natquery" / db_name / "schema.json"

    if schema_path.exists():
        with open(schema_path, "r") as f:
            schema = json.load(f)
    else:
        schema = {}

    schema_text = format_schema(schema)
    messages = build_prompt(user_query, schema_text)

    response = client.chat.completions.create(
        model=llm_config["model"],
        messages=messages,
        temperature=0.0,
    )

    sql = response.choices[0].message.content.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()

    return sql
