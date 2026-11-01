from __future__ import annotations

from typing import Any


def evaluate_query_plan(plan_analysis: dict[str, Any]) -> dict[str, Any]:
    summary = plan_analysis.get("summary", {})
    nodes = plan_analysis.get("nodes", [])
    sequential_scans = sum(node.get("node_type") == "Seq Scan" for node in nodes)
    nested_loops = sum(node.get("node_type") == "Nested Loop" for node in nodes)
    row_amplification = sum(
        max(0, (node.get("actual_rows") or 0) - (node.get("plan_rows") or 0))
        for node in nodes
    )
    score = float(summary.get("total_cost") or 0)
    score += sequential_scans * 100
    score += nested_loops * 50
    score += row_amplification / 1000
    return {
        "score": score,
        "total_cost": summary.get("total_cost"),
        "sequential_scans": sequential_scans,
        "nested_loops": nested_loops,
        "row_amplification": row_amplification,
        "summary": summary,
    }


def should_rewrite(plan_metrics: dict[str, Any], cost_threshold: float = 1000) -> bool:
    return plan_metrics.get("score", 0) > cost_threshold
