from natquery.llm.client import generate_sql
from natquery.execution.engine import execute_sql


def run_query(user_query: str):
    # Minimal pipeline (no schema injection yet)
    sql = generate_sql(user_query)
    return execute_sql(sql)