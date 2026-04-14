from natquery.observability.cost_analyzer import (
    extract_plan_root,
    extract_summary,
    flatten_plan,
    analyze_cost,
    extract_columns_from_condition,
)


class TestExtractPlanRoot:
    """Test extract_plan_root() function."""

    def test_extract_plan_root_from_dict(self):
        """Test extracting root from a dict plan."""
        plan_json = {
            "Plan": {"Node Type": "Seq Scan"},
            "Planning Time": 0.5,
            "Execution Time": 1.0,
        }

        result = extract_plan_root(plan_json)

        assert result == plan_json

    def test_extract_plan_root_from_list(self):
        """Test extracting first element from a list plan."""
        plan_json = [
            {
                "Plan": {"Node Type": "Seq Scan"},
                "Planning Time": 0.5,
                "Execution Time": 1.0,
            }
        ]

        result = extract_plan_root(plan_json)

        assert result == plan_json[0]

    def test_extract_plan_root_from_nested_list(self):
        """Test extracting root from nested list structure."""
        plan_json = [
            [
                {
                    "Plan": {"Node Type": "Seq Scan"},
                    "Planning Time": 0.5,
                }
            ]
        ]

        result = extract_plan_root(plan_json)

        assert result == plan_json[0]


class TestExtractColumnsFromCondition:
    """Test extract_columns_from_condition() function."""

    def test_extract_single_column_filter(self):
        """Test extracting column from simple filter."""
        condition = "email = 'test@example.com'"
        result = extract_columns_from_condition(condition)
        assert "email" in result

    def test_extract_multiple_columns_filter(self):
        """Test extracting multiple columns from compound filter."""
        condition = "((email = 'x') AND (status = 'active'))"
        result = extract_columns_from_condition(condition)
        assert "email" in result
        assert "status" in result

    def test_extract_columns_with_operators(self):
        """Test extraction with various operators."""
        condition = "(age >= 18 AND salary > 50000 AND active <= 1)"
        result = extract_columns_from_condition(condition)
        assert "age" in result
        assert "salary" in result
        assert "active" in result

    def test_extract_columns_empty_condition(self):
        """Test extraction from empty/None condition."""
        assert extract_columns_from_condition("") == []
        assert extract_columns_from_condition(None) == []

    def test_extract_columns_removes_duplicates(self):
        """Test that duplicate columns are removed."""
        condition = "(status = 'active' OR status = 'pending')"
        result = extract_columns_from_condition(condition)
        assert result.count("status") == 1

    def test_extract_columns_with_underscores(self):
        """Test extraction of columns with underscores."""
        condition = "(user_id = 1 AND user_status = 'active')"
        result = extract_columns_from_condition(condition)
        assert "user_id" in result
        assert "user_status" in result


class TestExtractSummary:
    """Test extract_summary() function."""

    def test_extract_summary_from_plan_dict(self):
        """Test extracting summary from fully populated plan."""
        plan_json = {
            "Plan": {
                "Node Type": "Seq Scan",
                "Total Cost": 45.00,
                "Plan Rows": 100,
            },
            "Execution Time": 2.345,
            "Planning Time": 0.234,
        }

        result = extract_summary(plan_json)

        assert result["total_cost"] == 45.00
        assert result["plan_rows"] == 100
        assert result["execution_time_ms"] == 2.345
        assert result["planning_time_ms"] == 0.234

    def test_extract_summary_from_plan_list(self):
        """Test extracting summary from list-wrapped plan."""
        plan_json = [
            {
                "Plan": {"Total Cost": 50.00, "Plan Rows": 200},
                "Execution Time": 3.0,
                "Planning Time": 0.5,
            }
        ]

        result = extract_summary(plan_json)

        assert result["total_cost"] == 50.00
        assert result["plan_rows"] == 200

    def test_extract_summary_with_missing_fields(self):
        """Test extraction with missing optional fields."""
        plan_json = {
            "Plan": {"Total Cost": 30.00},
            "Execution Time": 1.5,
        }

        result = extract_summary(plan_json)

        assert result["total_cost"] == 30.00
        assert result["execution_time_ms"] == 1.5
        assert result["planning_time_ms"] is None
        assert result["plan_rows"] is None


class TestFlattenPlan:
    """Test flatten_plan() function."""

    def test_flatten_single_node_plan(self):
        """Test flattening a plan with a single node."""
        plan = {
            "Node Type": "Seq Scan",
            "Relation Name": "users",
            "Total Cost": 45.00,
            "Plan Rows": 100,
        }

        result = flatten_plan(plan)

        assert len(result) == 1
        assert result[0]["node_type"] == "Seq Scan"
        assert result[0]["relation"] == "users"
        assert result[0]["total_cost"] == 45.00

    def test_flatten_nested_plan_with_children(self):
        """Test flattening a plan with child nodes."""
        plan = {
            "Node Type": "Hash Join",
            "Join Type": "Inner",
            "Plans": [
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "users",
                    "Total Cost": 35.00,
                },
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "orders",
                    "Total Cost": 25.00,
                },
            ],
        }

        result = flatten_plan(plan)

        assert len(result) == 3  # Parent + 2 children
        assert result[0]["node_type"] == "Hash Join"
        assert result[1]["node_type"] == "Seq Scan"
        assert result[2]["node_type"] == "Seq Scan"

    def test_flatten_plan_extracts_filter_columns(self):
        """Test that columns are extracted from filter conditions."""
        plan = {
            "Node Type": "Seq Scan",
            "Relation Name": "users",
            "Filter": "((status = 'active') AND (age >= 18))",
        }

        result = flatten_plan(plan)

        assert len(result[0]["columns"]) > 0
        assert "status" in result[0]["columns"]
        assert "age" in result[0]["columns"]

    def test_flatten_plan_extracts_index_condition_columns(self):
        """Test that columns are extracted from index conditions."""
        plan = {
            "Node Type": "Index Scan",
            "Relation Name": "users",
            "Index Cond": "(user_id = 42)",
        }

        result = flatten_plan(plan)

        assert "user_id" in result[0]["columns"]

    def test_flatten_plan_deeply_nested(self):
        """Test flattening a deeply nested plan structure."""
        plan = {
            "Node Type": "Nested Loop",
            "Plans": [
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "a",
                    "Plans": [
                        {
                            "Node Type": "Index Scan",
                            "Relation Name": "b",
                        }
                    ],
                },
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "c",
                },
            ],
        }

        result = flatten_plan(plan)

        node_types = [node["node_type"] for node in result]
        assert "Nested Loop" in node_types
        assert "Seq Scan" in node_types
        assert "Index Scan" in node_types
        assert len(result) == 4

    def test_flatten_plan_with_empty_plans_list(self):
        """Test that empty Plans list doesn't cause issues."""
        plan = {
            "Node Type": "Seq Scan",
            "Relation Name": "users",
            "Plans": [],
        }

        result = flatten_plan(plan)

        assert len(result) == 1
        assert result[0]["node_type"] == "Seq Scan"

    def test_flatten_plan_with_none_plans(self):
        """Test that None Plans value doesn't cause issues."""
        plan = {
            "Node Type": "Seq Scan",
            "Relation Name": "users",
            "Plans": None,
        }

        result = flatten_plan(plan)

        assert len(result) == 1


class TestAnalyzeCost:
    """Test analyze_cost() function."""

    def test_analyze_cost_simple_plan(self):
        """Test analyzing a simple single-node plan."""
        plan_json = {
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "users",
                "Total Cost": 45.00,
                "Plan Rows": 100,
            },
            "Execution Time": 2.345,
            "Planning Time": 0.234,
        }

        result = analyze_cost(plan_json)

        assert "summary" in result
        assert "nodes" in result
        assert result["summary"]["total_cost"] == 45.00
        assert len(result["nodes"]) == 1
        assert result["nodes"][0]["node_type"] == "Seq Scan"

    def test_analyze_cost_with_list_wrapped_plan(self):
        """Test analyzing a list-wrapped plan."""
        plan_json = [
            {
                "Plan": {
                    "Node Type": "Seq Scan",
                    "Total Cost": 50.00,
                    "Plan Rows": 200,
                },
                "Execution Time": 3.0,
                "Planning Time": 0.5,
            }
        ]

        result = analyze_cost(plan_json)

        assert result["summary"]["total_cost"] == 50.00
        assert result["nodes"][0]["node_type"] == "Seq Scan"

    def test_analyze_cost_complex_join_plan(self):
        """Test analyzing a complex join plan."""
        plan_json = {
            "Plan": {
                "Node Type": "Hash Join",
                "Join Type": "Inner",
                "Total Cost": 78.50,
                "Plan Rows": 500,
                "Plans": [
                    {
                        "Node Type": "Seq Scan",
                        "Relation Name": "users",
                        "Total Cost": 35.00,
                        "Filter": "(status = 'active')",
                    },
                    {
                        "Node Type": "Seq Scan",
                        "Relation Name": "orders",
                        "Total Cost": 25.00,
                    },
                ],
            },
            "Execution Time": 5.678,
            "Planning Time": 0.456,
        }

        result = analyze_cost(plan_json)

        assert result["summary"]["total_cost"] == 78.50
        assert len(result["nodes"]) == 3
        assert any(node["node_type"] == "Hash Join" for node in result["nodes"])

    def test_analyze_cost_extracts_all_columns(self):
        """Test that cost analysis extracts filter columns."""
        plan_json = {
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "users",
                "Filter": "(age > 18 AND status = 'active')",
                "Total Cost": 45.00,
            },
            "Execution Time": 1.5,
            "Planning Time": 0.2,
        }

        result = analyze_cost(plan_json)

        columns = result["nodes"][0]["columns"]
        assert "age" in columns
        assert "status" in columns

    def test_analyze_cost_actual_execution_metrics(self):
        """Test that actual execution metrics are captured."""
        plan_json = {
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "users",
                "Total Cost": 45.00,
                "Plan Rows": 100,
                "Actual Rows": 95,
                "Actual Total Time": 2.123,
            },
            "Execution Time": 2.5,
            "Planning Time": 0.3,
        }

        result = analyze_cost(plan_json)

        node = result["nodes"][0]
        assert node["plan_rows"] == 100
        assert node["actual_rows"] == 95
        assert node["actual_time"] == 2.123
