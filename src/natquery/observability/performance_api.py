import json
from pathlib import Path


BASE_DIR = Path.home() / ".natquery"


def load_performance(db_name: str):
    perf_file = BASE_DIR / db_name / "logs" / "performance.jsonl"

    if not perf_file.exists():
        return []

    entries = []
    with open(perf_file, "r") as f:
        for line in f:
            entries.append(json.loads(line))

    return entries


def get_last_run(db_name: str):
    data = load_performance(db_name)
    return data[-1] if data else None


def get_query_history(db_name: str, query_hash: str):
    data = load_performance(db_name)
    return [d for d in data if d["query_hash"] == query_hash]


def compare_last_two_runs(db_name: str):
    data = load_performance(db_name)

    if len(data) < 2:
        return None

    last = data[-1]
    prev = data[-2]

    return {
        "execution_time_diff": last["execution_time_ms"] - prev["execution_time_ms"],
        "cost_diff": last["total_cost"] - prev["total_cost"],
        "last": last,
        "previous": prev,
    }


def get_slowest_queries(db_name: str, top_k=5):
    data = load_performance(db_name)

    sorted_data = sorted(
        data,
        key=lambda x: x.get("execution_time_ms", 0),
        reverse=True,
    )

    return sorted_data[:top_k]
