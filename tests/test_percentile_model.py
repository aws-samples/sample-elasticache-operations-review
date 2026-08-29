"""Unit tests for PercentileModel (Task 3).

Tests the PercentileModel class which computes p50/p95/p99/max and spike ratio
for each metric time-series in a cluster.
"""

import math
import os
import sys

# Add scripts to path so we can import the module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from analyze_metrics import PercentileModel

# ---------------------------------------------------------------------------
# Helper: build cluster_data structures
# ---------------------------------------------------------------------------


def make_node_cluster(metrics_dict: dict) -> dict:
    """Build a node-based cluster_data with one node containing given metrics."""
    return {
        "nodes": {
            "test-cluster-001": {
                "metrics": metrics_dict,
            }
        }
    }


def make_serverless_cluster(metrics_dict: dict) -> dict:
    """Build a serverless cluster_data with metrics at top level."""
    return {"metrics": metrics_dict}


def make_metric_series(values: list[float], statistic: str = "Maximum") -> dict:
    """Build a metric series with timestamps and values."""
    timestamps = [
        f"2024-01-01T{i // 12:02d}:{(i % 12) * 5:02d}:00Z"
        for i in range(len(values))
    ]
    return {statistic: {"timestamps": timestamps, "values": values}}


# ---------------------------------------------------------------------------
# TestPercentileModel
# ---------------------------------------------------------------------------


class TestPercentileModelBasic:
    """Basic percentile computation tests."""

    def test_compute_basic_percentiles(self):
        """Correct p50/p95/p99/max computation for a simple series."""
        model = PercentileModel()
        values = list(range(1, 101))  # 1 to 100
        cluster_data = make_node_cluster(
            {"EngineCPUUtilization": make_metric_series(values)}
        )
        result = model.compute(cluster_data, None)

        key = "EngineCPUUtilization_Maximum"
        assert key in result
        assert result[key]["p50"] == 50.5
        assert result[key]["p95"] == 95.05
        assert result[key]["p99"] == 99.01
        assert result[key]["max"] == 100.0

    def test_percentile_ordering(self):
        """p50 <= p95 <= p99 <= max always holds."""
        model = PercentileModel()
        # Random-ish values
        values = [10.0, 5.0, 80.0, 20.0, 90.0, 15.0, 75.0, 30.0, 85.0, 50.0,
                  60.0, 45.0, 70.0, 35.0, 55.0, 25.0, 95.0, 40.0, 65.0, 100.0]
        cluster_data = make_node_cluster(
            {"TestMetric": make_metric_series(values)}
        )
        result = model.compute(cluster_data, None)

        r = result["TestMetric_Maximum"]
        assert r["p50"] <= r["p95"] <= r["p99"] <= r["max"]


class TestPercentileModelInsufficientData:
    """Tests for insufficient data handling."""

    def test_fewer_than_10_valid_points(self):
        """Marks metric as insufficient_data when < 10 valid points."""
        model = PercentileModel()
        values = [1.0, 2.0, 3.0, 4.0, 5.0]  # Only 5 valid points
        cluster_data = make_node_cluster(
            {"TestMetric": make_metric_series(values)}
        )
        result = model.compute(cluster_data, None)

        key = "TestMetric_Maximum"
        assert key in result
        assert result[key]["status"] == "insufficient_data"
        assert result[key]["valid_points"] == 5

    def test_exactly_10_valid_points(self):
        """Computes percentiles when exactly 10 valid points."""
        model = PercentileModel()
        values = list(range(1, 11))  # 10 valid points
        cluster_data = make_node_cluster(
            {"TestMetric": make_metric_series(values)}
        )
        result = model.compute(cluster_data, None)

        key = "TestMetric_Maximum"
        assert key in result
        assert "p50" in result[key]
        assert "status" not in result[key]

    def test_nan_values_reduce_valid_count(self):
        """NaN values are filtered — only non-NaN count toward threshold."""
        model = PercentileModel()
        # 12 total values, but 5 are NaN → only 7 valid (< 10)
        values = [1.0, float("nan"), 2.0, float("nan"), 3.0,
                  float("nan"), 4.0, float("nan"), 5.0, float("nan"),
                  6.0, 7.0]
        cluster_data = make_node_cluster(
            {"TestMetric": make_metric_series(values)}
        )
        result = model.compute(cluster_data, None)

        key = "TestMetric_Maximum"
        assert result[key]["status"] == "insufficient_data"
        assert result[key]["valid_points"] == 7


class TestPercentileModelSpikeRatio:
    """Tests for spike_ratio computation."""

    def test_spike_ratio_basic(self):
        """Spike ratio = max / p95."""
        model = PercentileModel()
        # 100 values at 10.0, one spike at 50.0
        values = [10.0] * 99 + [50.0]
        cluster_data = make_node_cluster(
            {"TestMetric": make_metric_series(values)}
        )
        result = model.compute(cluster_data, None)

        r = result["TestMetric_Maximum"]
        # p95 should be 10.0, max should be 50.0
        assert r["spike_ratio"] == 5.0

    def test_spike_ratio_zero_p95(self):
        """Returns 1.0 when p95 is zero."""
        model = PercentileModel()
        # 96 zeros, then a few non-zeros to ensure p95 is still 0
        values = [0.0] * 96 + [1.0, 2.0, 3.0, 4.0]
        cluster_data = make_node_cluster(
            {"TestMetric": make_metric_series(values)}
        )
        result = model.compute(cluster_data, None)

        r = result["TestMetric_Maximum"]
        assert r["spike_ratio"] == 1.0

    def test_spike_ratio_at_least_one(self):
        """Spike ratio is always >= 1.0 since max >= p95."""
        model = PercentileModel()
        values = [42.0] * 20  # All same values → max == p95
        cluster_data = make_node_cluster(
            {"TestMetric": make_metric_series(values)}
        )
        result = model.compute(cluster_data, None)

        r = result["TestMetric_Maximum"]
        assert r["spike_ratio"] >= 1.0


class TestPercentileModelSpikeClassification:
    """Tests for spike severity classification."""

    def test_stable(self):
        """spike_ratio < 2 → stable."""
        model = PercentileModel()
        values = list(range(1, 101))  # Uniform distribution → low spike ratio
        cluster_data = make_node_cluster(
            {"TestMetric": make_metric_series(values)}
        )
        result = model.compute(cluster_data, None)
        assert result["TestMetric_Maximum"]["spike_class"] == "stable"

    def test_notable(self):
        """spike_ratio 2-3 → notable."""
        model = PercentileModel()
        # p95 will be around 10, max will be around 25
        values = [10.0] * 95 + [25.0] * 5
        cluster_data = make_node_cluster(
            {"TestMetric": make_metric_series(values)}
        )
        result = model.compute(cluster_data, None)
        r = result["TestMetric_Maximum"]
        assert r["spike_class"] == "notable"

    def test_significant(self):
        """spike_ratio 3-5 → significant."""
        model = PercentileModel()
        values = [10.0] * 99 + [40.0]
        cluster_data = make_node_cluster(
            {"TestMetric": make_metric_series(values)}
        )
        result = model.compute(cluster_data, None)
        r = result["TestMetric_Maximum"]
        assert r["spike_class"] == "significant"

    def test_severe(self):
        """spike_ratio > 5 → severe."""
        model = PercentileModel()
        values = [10.0] * 99 + [100.0]
        cluster_data = make_node_cluster(
            {"TestMetric": make_metric_series(values)}
        )
        result = model.compute(cluster_data, None)
        r = result["TestMetric_Maximum"]
        assert r["spike_class"] == "severe"


class TestPercentileModelClusterStructure:
    """Tests for handling different cluster data structures."""

    def test_node_based_cluster(self):
        """Processes node-based cluster with multiple nodes."""
        model = PercentileModel()
        cluster_data = {
            "nodes": {
                "cluster-001": {
                    "metrics": {
                        "EngineCPUUtilization": make_metric_series(
                            list(range(10, 30))
                        )
                    }
                },
                "cluster-002": {
                    "metrics": {
                        "EngineCPUUtilization": make_metric_series(
                            list(range(30, 50))
                        )
                    }
                },
            }
        }
        result = model.compute(cluster_data, None)

        key = "EngineCPUUtilization_Maximum"
        assert key in result
        # Combined series has 40 points (20 from each node)
        assert "p50" in result[key]

    def test_serverless_cluster(self):
        """Processes serverless cluster with metrics at top level."""
        model = PercentileModel()
        cluster_data = make_serverless_cluster(
            {"ThrottledCmds": make_metric_series(list(range(10, 30)), "Sum")}
        )
        result = model.compute(cluster_data, None)

        key = "ThrottledCmds_Sum"
        assert key in result
        assert "p50" in result[key]

    def test_multiple_metrics(self):
        """Processes multiple metrics in the same node."""
        model = PercentileModel()
        cluster_data = make_node_cluster({
            "EngineCPUUtilization": make_metric_series(list(range(10, 30))),
            "DatabaseMemoryUsagePercentage": make_metric_series(
                list(range(40, 60))
            ),
        })
        result = model.compute(cluster_data, None)

        assert "EngineCPUUtilization_Maximum" in result
        assert "DatabaseMemoryUsagePercentage_Maximum" in result

    def test_multiple_statistics_per_metric(self):
        """Processes multiple statistics for the same metric."""
        model = PercentileModel()
        cluster_data = make_node_cluster({
            "EngineCPUUtilization": {
                "Maximum": {
                    "timestamps": [f"2024-01-01T{i:02d}:00:00Z" for i in range(20)],
                    "values": list(range(20, 40)),
                },
                "Average": {
                    "timestamps": [f"2024-01-01T{i:02d}:00:00Z" for i in range(20)],
                    "values": list(range(10, 30)),
                },
            }
        })
        result = model.compute(cluster_data, None)

        assert "EngineCPUUtilization_Maximum" in result
        assert "EngineCPUUtilization_Average" in result

    def test_empty_cluster_data(self):
        """Handles cluster with no nodes or metrics."""
        model = PercentileModel()
        result = model.compute({}, None)
        assert result == {}

    def test_nan_aware_computation(self):
        """Uses NaN-aware functions — NaN values don't corrupt results."""
        model = PercentileModel()
        values = [float("nan")] * 5 + list(range(1, 21))  # 5 NaN + 20 valid
        cluster_data = make_node_cluster(
            {"TestMetric": make_metric_series(values)}
        )
        result = model.compute(cluster_data, None)

        r = result["TestMetric_Maximum"]
        assert not math.isnan(r["p50"])
        assert not math.isnan(r["p95"])
        assert not math.isnan(r["p99"])
        assert not math.isnan(r["max"])

    def test_metric_key_format(self):
        """Metric keys follow {MetricName}_{Statistic} format."""
        model = PercentileModel()
        cluster_data = make_node_cluster({
            "EngineCPUUtilization": make_metric_series(list(range(1, 21))),
        })
        result = model.compute(cluster_data, None)
        assert "EngineCPUUtilization_Maximum" in result

    def test_numeric_precision(self):
        """Results are rounded to 2 decimal places."""
        model = PercentileModel()
        values = [1.123456, 2.789012, 3.456789] * 10
        cluster_data = make_node_cluster(
            {"TestMetric": make_metric_series(values)}
        )
        result = model.compute(cluster_data, None)

        r = result["TestMetric_Maximum"]
        # All numeric values should be rounded to 2 decimal places
        for key in ["p50", "p95", "p99", "max", "spike_ratio"]:
            val_str = str(r[key])
            if "." in val_str:
                decimals = len(val_str.split(".")[1])
                assert decimals <= 2
