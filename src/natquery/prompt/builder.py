def build_prompt(user_query: str, schema_text: str) -> list:
    """
    Build structured messages for LLM.
    Returns messages list (for chat APIs).
    """

    system_prompt = f"""
You are a PostgreSQL expert.

STRICT RULES:
- Generate ONLY valid PostgreSQL SQL.
- DO NOT include explanations.
- DO NOT use INSERT, UPDATE, DELETE, DROP.
- Use only tables and columns from the schema.
- Prefer explicit JOINs using foreign keys.
- If query is ambiguous, choose the most relevant interpretation.

DATABASE SCHEMA:
{schema_text}
"""

    messages = [
        {"role": "system", "content": system_prompt.strip()},
        {"role": "user", "content": user_query.strip()},
    ]

    return messages
