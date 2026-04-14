def suggest_indexes(plan_analysis):
    nodes = plan_analysis.get("nodes", [])
    suggestions = []

    for node in nodes:
        node_type = node.get("node_type")
        table = node.get("relation")
        columns = node.get("columns", [])

        # SEQ SCAN (main case)
        if node_type == "Seq Scan" and table:

            if columns:
                if len(columns) == 1:
                    col = columns[0]

                    suggestions.append(
                        {
                            "type": "index",
                            "table": table,
                            "columns": columns,
                            "sql": f"CREATE INDEX idx_{table}_{col} ON {table}({col});",
                            "reason": "Sequential scan with filter condition",
                        }
                    )

                else:
                    cols = ", ".join(columns)

                    suggestions.append(
                        {
                            "type": "composite_index",
                            "table": table,
                            "columns": columns,
                            "sql": f"CREATE INDEX idx_{table}_{'_'.join(columns)} ON {table}({cols});",
                            "reason": "Multiple filter conditions detected",
                        }
                    )

            else:
                suggestions.append(
                    {
                        "type": "seq_scan_warning",
                        "table": table,
                        "reason": "Full table scan detected (no filter)",
                    }
                )

        # Join optimization
        if node.get("join_type"):
            suggestions.append(
                {
                    "type": "join",
                    "reason": "Ensure indexes exist on join keys",
                }
            )

    return suggestions
