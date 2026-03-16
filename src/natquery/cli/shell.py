# src/natquery/cli/shell.py

from natquery.orchestration.pipeline import run_query


def start_shell():
    print("NatQuery CLI. Type 'exit' to quit.")

    while True:
        query = input("> ")

        if query.lower() in ["exit", "quit"]:
            break

        try:
            result = run_query(query)
            print(result)
        except Exception as e:
            print(f"Error: {e}")
