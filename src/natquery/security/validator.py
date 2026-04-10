import re


def validate_sql(sql: str):
    """
    Ensure SQL is safe (SELECT only).
    """

    sql_clean = sql.strip().lower()

    if not sql_clean.startswith("select"):
        raise ValueError("Only SELECT queries are allowed.")

    forbidden = ["insert", "update", "delete", "drop", "alter", "truncate"]

    for word in forbidden:
        if re.search(rf"\b{word}\b", sql_clean):
            raise ValueError(f"Forbidden keyword detected: {word}")

    return True
