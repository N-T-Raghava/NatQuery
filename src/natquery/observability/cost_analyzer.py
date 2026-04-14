import re


def extract_plan_root(plan_json):
    if isinstance(plan_json, list):
        plan_json = plan_json[0]
    return plan_json


def extract_summary(plan_json):
    plan_json = extract_plan_root(plan_json)

    root = plan_json.get("Plan", {})

    return {
        "execution_time_ms": plan_json.get("Execution Time"),
        "planning_time_ms": plan_json.get("Planning Time"),
        "total_cost": root.get("Total Cost"),
        "plan_rows": root.get("Plan Rows"),
    }


def extract_columns_from_condition(condition: str):
    """
    Extract column names from filter/index conditions.
    Example:
        "((email = 'x') AND (status = 'active'))"
    """

    if not condition:
        return []

    # Simple heuristic (robust enough for now)
    matches = re.findall(r"([a-zA-Z_][a-zA-Z0-9_]*)\s*(=|>|<|>=|<=)", condition)

    return list(set([m[0] for m in matches]))


def flatten_plan(node, nodes=None):
    if nodes is None:
        nodes = []

    filter_cond = node.get("Filter")
    index_cond = node.get("Index Cond")

    extracted_columns = []
    extracted_columns += extract_columns_from_condition(filter_cond)
    extracted_columns += extract_columns_from_condition(index_cond)

    node_info = {
        "node_type": node.get("Node Type"),
        "relation": node.get("Relation Name"),
        "startup_cost": node.get("Startup Cost"),
        "total_cost": node.get("Total Cost"),
        "plan_rows": node.get("Plan Rows"),
        "actual_rows": node.get("Actual Rows"),
        "actual_time": node.get("Actual Total Time"),
        "filter": filter_cond,
        "index_cond": index_cond,
        "columns": extracted_columns,
        "join_type": node.get("Join Type"),
    }

    nodes.append(node_info)

    for child in node.get("Plans", []) or []:
        flatten_plan(child, nodes)

    return nodes


def analyze_cost(plan_json):
    plan_json = extract_plan_root(plan_json)
    root = plan_json.get("Plan", {})

    return {
        "summary": extract_summary(plan_json),
        "nodes": flatten_plan(root),
    }
