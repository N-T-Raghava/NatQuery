# src/natquery/cli/shell.py

from natquery.orchestration.pipeline import run_query


def start_shell():
    print("NatQuery CLI. Type 'exit' to quit.")

    while True:
        query = input("> ")

        if query.lower() in ["exit", "quit"]:
            break

        result = run_query(query)
        print(result)