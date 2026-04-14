import json
import tempfile
from pathlib import Path
from unittest.mock import patch
import pytest
from natquery.observability.performance_api import (
    load_performance,
    get_last_run,
    get_query_history,
    compare_last_two_runs,
    get_slowest_queries,
)


@pytest.fixture
def temp_perf_dir():
    """Create temporary directory with performance logs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        db_dir = base / "testdb" / "logs"
        db_dir.mkdir(parents=True)

        perf_file = db_dir / "performance.jsonl"

        # Create sample performance data
        entries = [
            {
                "query_hash": "q1",
                "execution_time_ms": 100,
                "total_cost": 50.0,
                "timestamp": "2024-01-01T10:00:00",
            },
            {
                "query_hash": "q2",
                "execution_time_ms": 200,
                "total_cost": 100.0,
                "timestamp": "2024-01-01T10:05:00",
            },
            {
                "query_hash": "q1",
                "execution_time_ms": 150,
                "total_cost": 75.0,
                "timestamp": "2024-01-01T10:10:00",
            },
            {
                "query_hash": "q3",
                "execution_time_ms": 300,
                "total_cost": 150.0,
                "timestamp": "2024-01-01T10:15:00",
            },
        ]

        with open(perf_file, "w") as f:
            for entry in entries:
                f.write(json.dumps(entry) + "\n")

        yield base, "testdb"


class TestLoadPerformance:
    """Test load_performance() function."""

    def test_load_performance_no_file(self):
        """Test loading when no performance file exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "natquery.observability.performance_api.BASE_DIR",
                Path(tmpdir),
            ):
                result = load_performance("nonexistent")
                assert result == []

    def test_load_performance_with_data(self, temp_perf_dir):
        """Test loading performance data."""
        base, db_name = temp_perf_dir
        with patch(
            "natquery.observability.performance_api.BASE_DIR",
            base,
        ):
            result = load_performance(db_name)
            assert len(result) == 4
            assert result[0]["query_hash"] == "q1"
            assert result[1]["execution_time_ms"] == 200


class TestGetLastRun:
    """Test get_last_run() function."""

    def test_get_last_run_no_data(self):
        """Test getting last run when no data exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "natquery.observability.performance_api.BASE_DIR",
                Path(tmpdir),
            ):
                result = get_last_run("nonexistent")
                assert result is None

    def test_get_last_run_with_data(self, temp_perf_dir):
        """Test getting last run with data."""
        base, db_name = temp_perf_dir
        with patch(
            "natquery.observability.performance_api.BASE_DIR",
            base,
        ):
            result = get_last_run(db_name)
            assert result is not None
            assert result["query_hash"] == "q3"
            assert result["execution_time_ms"] == 300


class TestGetQueryHistory:
    """Test get_query_history() function."""

    def test_get_query_history_empty(self):
        """Test getting history for non-existent query."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "natquery.observability.performance_api.BASE_DIR",
                Path(tmpdir),
            ):
                result = get_query_history("testdb", "nonexistent")
                assert result == []

    def test_get_query_history_single_query(self, temp_perf_dir):
        """Test getting history for query with multiple runs."""
        base, db_name = temp_perf_dir
        with patch(
            "natquery.observability.performance_api.BASE_DIR",
            base,
        ):
            result = get_query_history(db_name, "q1")
            assert len(result) == 2
            assert all(r["query_hash"] == "q1" for r in result)
            assert result[0]["execution_time_ms"] == 100
            assert result[1]["execution_time_ms"] == 150

    def test_get_query_history_different_query(self, temp_perf_dir):
        """Test getting history for different query."""
        base, db_name = temp_perf_dir
        with patch(
            "natquery.observability.performance_api.BASE_DIR",
            base,
        ):
            result = get_query_history(db_name, "q2")
            assert len(result) == 1
            assert result[0]["query_hash"] == "q2"
            assert result[0]["execution_time_ms"] == 200


class TestCompareLastTwoRuns:
    """Test compare_last_two_runs() function."""

    def test_compare_last_two_runs_no_data(self):
        """Test comparison when no data exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "natquery.observability.performance_api.BASE_DIR",
                Path(tmpdir),
            ):
                result = compare_last_two_runs("nonexistent")
                assert result is None

    def test_compare_last_two_runs_one_entry(self):
        """Test comparison with only one entry."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            db_dir = base / "testdb" / "logs"
            db_dir.mkdir(parents=True)

            perf_file = db_dir / "performance.jsonl"
            with open(perf_file, "w") as f:
                f.write(
                    json.dumps({"execution_time_ms": 100, "total_cost": 50.0}) + "\n"
                )

            with patch(
                "natquery.observability.performance_api.BASE_DIR",
                base,
            ):
                result = compare_last_two_runs("testdb")
                assert result is None

    def test_compare_last_two_runs_with_data(self, temp_perf_dir):
        """Test comparison with multiple entries."""
        base, db_name = temp_perf_dir
        with patch(
            "natquery.observability.performance_api.BASE_DIR",
            base,
        ):
            result = compare_last_two_runs(db_name)
            assert result is not None
            assert result["execution_time_diff"] == 300 - 150  # last - prev
            assert result["cost_diff"] == 150.0 - 75.0
            assert result["last"]["query_hash"] == "q3"
            assert result["previous"]["query_hash"] == "q1"


class TestGetSlowestQueries:
    """Test get_slowest_queries() function."""

    def test_get_slowest_queries_no_data(self):
        """Test getting slowest queries when no data exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "natquery.observability.performance_api.BASE_DIR",
                Path(tmpdir),
            ):
                result = get_slowest_queries("nonexistent")
                assert result == []

    def test_get_slowest_queries_default_top_k(self, temp_perf_dir):
        """Test getting slowest queries with default top_k=5."""
        base, db_name = temp_perf_dir
        with patch(
            "natquery.observability.performance_api.BASE_DIR",
            base,
        ):
            result = get_slowest_queries(db_name)
            assert len(result) == 4
            # Should be sorted by execution_time_ms descending
            assert result[0]["execution_time_ms"] == 300  # q3
            assert result[1]["execution_time_ms"] == 200  # q2
            assert result[2]["execution_time_ms"] == 150  # q1 (second run)
            assert result[3]["execution_time_ms"] == 100  # q1 (first run)

    def test_get_slowest_queries_top_k_limit(self, temp_perf_dir):
        """Test getting top 2 slowest queries."""
        base, db_name = temp_perf_dir
        with patch(
            "natquery.observability.performance_api.BASE_DIR",
            base,
        ):
            result = get_slowest_queries(db_name, top_k=2)
            assert len(result) == 2
            assert result[0]["execution_time_ms"] == 300
            assert result[1]["execution_time_ms"] == 200

    def test_get_slowest_queries_top_k_larger_than_data(self, temp_perf_dir):
        """Test getting slowest queries when top_k larger than data."""
        base, db_name = temp_perf_dir
        with patch(
            "natquery.observability.performance_api.BASE_DIR",
            base,
        ):
            result = get_slowest_queries(db_name, top_k=10)
            assert len(result) == 4  # Only 4 entries available
