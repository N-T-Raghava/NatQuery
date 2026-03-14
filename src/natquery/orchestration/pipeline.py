from natquery.llm.client import generate_sql
from natquery.execution.engine import execute_sql


def run_query(user_query: str):
    sql = generate_sql(user_query)

    print(f"[Generated SQL]: {sql}")  

    return execute_sql(sql)