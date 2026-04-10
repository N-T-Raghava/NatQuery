from groq import Groq
from natquery.config.settings import Settings
from natquery.schema.formatter import format_schema
from natquery.orchestration.error_classifier import classify_error
from pathlib import Path
import json


def correct_sql(user_query: str, failed_sql: str, error_message: str) -> str:

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

    error_type = classify_error(error_message)

    # Dynamic instruction based on error type
    correction_hint = {
        "SYNTAX_ERROR": "Fix SQL syntax.",
        "UNDEFINED_TABLE": "Use only valid table names from schema.",
        "UNDEFINED_COLUMN": "Fix incorrect column names using schema.",
        "AMBIGUOUS_COLUMN": "Disambiguate columns using table aliases.",
        "TYPE_ERROR": "Fix type mismatches and operators.",
        "GROUP_BY_ERROR": "Ensure proper GROUP BY usage.",
        "JOIN_ERROR": "Fix joins using foreign key relationships.",
        "UNKNOWN": "Fix the SQL query to make it executable.",
    }[error_type]

    system_prompt = f"""
You are a PostgreSQL expert specializing in fixing SQL queries.

ERROR TYPE: {error_type}

INSTRUCTION:
{correction_hint}

RULES:
- Output ONLY corrected SQL.
- Do NOT explain anything.
- Use only schema provided.
- Ensure query executes correctly.

DATABASE SCHEMA:
{schema_text}
"""

    user_prompt = f"""
User Query:
{user_query}

Previous SQL:
{failed_sql}

Error:
{error_message}

Return corrected SQL:
"""

    response = client.chat.completions.create(
        model=llm_config["model"],
        messages=[
            {"role": "system", "content": system_prompt.strip()},
            {"role": "user", "content": user_prompt.strip()},
        ],
        temperature=0.0,
    )

    sql = response.choices[0].message.content.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()

    return sql
