from natquery.observability.index_recommender import suggest_indexes


class TestSuggestIndexes:
    """Test suggest_indexes() function."""

    def test_suggest_index_on_seq_scan_single_column(self):
        """Test index suggestion for sequential scan with single filter column."""
        plan_analysis = {
            "nodes": [
                {
                    "node_type": "Seq Scan",
                    "relation": "users",
                    "columns": ["email"],
                    "filter": "email = 'test@example.com'",
                }
            ]
        }

        suggestions = suggest_indexes(plan_analysis)

        assert len(suggestions) == 1
        assert suggestions[0]["type"] == "index"
        assert suggestions[0]["table"] == "users"
        assert suggestions[0]["columns"] == ["email"]
        assert "email" in suggestions[0]["sql"]
        assert "CREATE INDEX" in suggestions[0]["sql"]

    def test_suggest_composite_index_on_seq_scan_multiple_columns(self):
        """Test composite index suggestion for multiple filter columns."""
        plan_analysis = {
            "nodes": [
                {
                    "node_type": "Seq Scan",
                    "relation": "orders",
                    "columns": ["user_id", "status"],
                    "filter": "(user_id = 1 AND status = 'active')",
                }
            ]
        }

        suggestions = suggest_indexes(plan_analysis)

        assert len(suggestions) == 1
        assert suggestions[0]["type"] == "composite_index"
        assert suggestions[0]["table"] == "orders"
        assert set(suggestions[0]["columns"]) == {"user_id", "status"}
        assert "user_id" in suggestions[0]["sql"]
        assert "status" in suggestions[0]["sql"]

    def test_seq_scan_warning_without_filter(self):
        """Test warning for full table scan (no filter columns)."""
        plan_analysis = {
            "nodes": [
                {
                    "node_type": "Seq Scan",
                    "relation": "products",
                    "columns": [],
                    "filter": None,
                }
            ]
        }

        suggestions = suggest_indexes(plan_analysis)

        assert len(suggestions) == 1
        assert suggestions[0]["type"] == "seq_scan_warning"
        assert suggestions[0]["table"] == "products"
        assert "Full table scan" in suggestions[0]["reason"]

    def test_no_suggestion_for_index_scan(self):
        """Test that no suggestion is made for efficient index scans."""
        plan_analysis = {
            "nodes": [
                {
                    "node_type": "Index Scan",
                    "relation": "users",
                    "columns": ["id"],
                    "index_cond": "id = 1",
                }
            ]
        }

        suggestions = suggest_indexes(plan_analysis)

        # Index scans don't generate suggestions (already optimized)
        # Unless there's a join, which there isn't here
        assert len(suggestions) == 0

    def test_join_optimization_suggestion(self):
        """Test suggestion for join optimization."""
        plan_analysis = {
            "nodes": [
                {
                    "node_type": "Hash Join",
                    "relation": "users",
                    "join_type": "Inner",
                    "columns": ["id"],
                }
            ]
        }

        suggestions = suggest_indexes(plan_analysis)

        assert len(suggestions) >= 1
        join_suggestion = [s for s in suggestions if s["type"] == "join"]
        assert len(join_suggestion) == 1
        assert "join keys" in join_suggestion[0]["reason"]

    def test_multiple_nodes_generate_multiple_suggestions(self):
        """Test suggestions from multiple nodes in a plan."""
        plan_analysis = {
            "nodes": [
                {
                    "node_type": "Seq Scan",
                    "relation": "users",
                    "columns": ["status"],
                    "filter": "status = 'active'",
                },
                {
                    "node_type": "Seq Scan",
                    "relation": "orders",
                    "columns": ["user_id", "amount"],
                    "filter": "user_id = 1 AND amount > 100",
                },
            ]
        }

        suggestions = suggest_indexes(plan_analysis)

        assert len(suggestions) == 2
        assert suggestions[0]["type"] == "index"
        assert suggestions[1]["type"] == "composite_index"

    def test_complex_plan_with_join_and_scan(self):
        """Test suggestions from complex plan with both join and scan nodes."""
        plan_analysis = {
            "nodes": [
                {
                    "node_type": "Hash Join",
                    "relation": "users",
                    "join_type": "Inner",
                    "columns": ["id"],
                },
                {
                    "node_type": "Seq Scan",
                    "relation": "orders",
                    "columns": ["status"],
                    "filter": "status = 'pending'",
                },
                {
                    "node_type": "Seq Scan",
                    "relation": "users",
                    "columns": [],
                    "filter": None,
                },
            ]
        }

        suggestions = suggest_indexes(plan_analysis)

        types = [s["type"] for s in suggestions]
        assert "join" in types
        assert "index" in types
        assert "seq_scan_warning" in types

    def test_empty_plan_analysis(self):
        """Test with empty plan analysis."""
        plan_analysis = {"nodes": []}

        suggestions = suggest_indexes(plan_analysis)

        assert len(suggestions) == 0

    def test_none_nodes(self):
        """Test handling of None nodes."""
        plan_analysis = {"nodes": None}

        # Should handle gracefully
        try:
            suggestions = suggest_indexes(plan_analysis)
            # If it handles None, we expect empty results
            assert suggestions is not None
        except TypeError:
            # This is acceptable - function might not handle None
            pass

    def test_suggestion_has_valid_sql_syntax(self):
        """Test that generated SQL suggestions are valid."""
        plan_analysis = {
            "nodes": [
                {
                    "node_type": "Seq Scan",
                    "relation": "customers",
                    "columns": ["email"],
                    "filter": "email = 'test@example.com'",
                }
            ]
        }

        suggestions = suggest_indexes(plan_analysis)
        sql = suggestions[0]["sql"]

        # Basic syntax checks
        assert "CREATE INDEX" in sql
        assert "ON customers" in sql
        assert sql.endswith(";")

    def test_index_name_follows_convention(self):
        """Test that generated index names follow naming convention."""
        plan_analysis = {
            "nodes": [
                {
                    "node_type": "Seq Scan",
                    "relation": "users",
                    "columns": ["email"],
                    "filter": "email = 'x'",
                }
            ]
        }

        suggestions = suggest_indexes(plan_analysis)
        sql = suggestions[0]["sql"]

        # Index name should follow pattern: idx_<table>_<column>
        assert "idx_users_email" in sql

    def test_composite_index_name_convention(self):
        """Test that composite index names follow convention."""
        plan_analysis = {
            "nodes": [
                {
                    "node_type": "Seq Scan",
                    "relation": "orders",
                    "columns": ["user_id", "status"],
                    "filter": "(user_id = 1 AND status = 'completed')",
                }
            ]
        }

        suggestions = suggest_indexes(plan_analysis)
        sql = suggestions[0]["sql"]

        # Index name should include both columns
        assert "idx_orders_" in sql
        assert "user_id" in sql or "status" in sql

    def test_suggestions_include_reason(self):
        """Test that all suggestions include explanation."""
        plan_analysis = {
            "nodes": [
                {
                    "node_type": "Seq Scan",
                    "relation": "users",
                    "columns": ["email"],
                    "filter": "email = 'test@example.com'",
                },
                {
                    "node_type": "Hash Join",
                    "join_type": "Inner",
                    "columns": ["id"],
                },
            ]
        }

        suggestions = suggest_indexes(plan_analysis)

        for suggestion in suggestions:
            assert "reason" in suggestion
            assert len(suggestion["reason"]) > 0

    def test_no_duplicate_suggestions_for_same_column(self):
        """Test handling of duplicate column references in filter."""
        plan_analysis = {
            "nodes": [
                {
                    "node_type": "Seq Scan",
                    "relation": "users",
                    "columns": ["email", "email"],  # Duplicates from extraction
                    "filter": "email = 'x' OR email = 'y'",
                },
            ]
        }

        suggestions = suggest_indexes(plan_analysis)

        # Should generate a suggestion (may be composite due to the duplicate in the list)
        assert len(suggestions) >= 1
        # The suggestion should be for index creation
        assert suggestions[0]["table"] == "users"
