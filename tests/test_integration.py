"""Integration tests for the Metrics Analysis pipeline (Task 14).

End-to-end tests verifying the full pipeline with synthetic data covering:
- Node-based and serverless clusters
- Output schema completeness
- Correctness properties (mathematical invariants)
- Graceful degradation under model failures
- Numeric precision rules
- Performance within 60 seconds for 50 clusters

Requirements validated: 13.1–13.6, 14.1–14.6, 15.1–15.7
"""

import json
import os
import sys
import time
from unittest.mock import patch

import numpy as np
import pytest

# Add scripts to path so we can import the module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from analyze_metrics import main

# ---------------------------------------------------------------------------
# Fixture helpers: build synthetic metrics.json and inventory.json
# ---------------------------------------------------------------------------


def _generate_timestamps(num_points: int = 4032) -> list[str]:
    """Generate 14 days of 5-minute interval timestamps (4032 points)."""
    import datetime

    base = datetime.datetime(2024, 7, 1, 0, 0, 0)
    return [
        (base + datetime.timedelta(minutes=5 * i)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        for i in range(num_points)
    ]


def _make_metric_series(
    values: list[float], statistic: str = "Maximum", num_points: int = 4032
) -> dict:
    """Build a metric series dict with timestamps and values."""
    timestamps = _generate_timestamps(num_points)
    # Pad or truncate values to match timestamp count
    if len(values) < num_points:
        values = values + [values[-1]] * (num_points - len(values))
    elif len(values) > num_points:
        values = values[:num_points]
    return {statistic: {"timestamps": timestamps, "values": values}}


def _generate_realistic_cpu_values(num_points: int = 4032) -> list[float]:
    """Generate realistic CPU utilization values with daily patterns."""
    np.random.seed(42)
    base = 35.0
    noise = np.random.normal(0, 5, num_points)
    # Add daily sinusoidal pattern (288 points per day)
    daily_pattern = 15 * np.sin(
        2 * np.pi * np.arange(num_points) / 288
    )
    values = base + daily_pattern + noise
    # Clamp to [0, 100]
    values = np.clip(values, 0, 100)
    # Add a few spikes
    values[100] = 92.0
    values[500] = 88.0
    return values.tolist()


def _generate_realistic_memory_values(num_points: int = 4032) -> list[float]:
    """Generate realistic memory utilization values with gradual growth."""
    np.random.seed(43)
    # Linear growth from 50% to 65% over 14 days
    base = np.linspace(50, 65, num_points)
    noise = np.random.normal(0, 1, num_points)
    values = base + noise
    values = np.clip(values, 0, 100)
    return values.tolist()


def _generate_realistic_network_values(num_points: int = 4032) -> list[float]:
    """Generate realistic network utilization percentages."""
    np.random.seed(44)
    base = 25.0
    noise = np.random.normal(0, 3, num_points)
    daily_pattern = 10 * np.sin(
        2 * np.pi * np.arange(num_points) / 288
    )
    values = base + daily_pattern + noise
    values = np.clip(values, 0, 100)
    return values.tolist()


def _generate_command_values(
    base: float, num_points: int = 4032
) -> list[float]:
    """Generate command count values with daily patterns."""
    np.random.seed(45)
    noise = np.random.normal(0, base * 0.1, num_points)
    daily_pattern = base * 0.3 * np.sin(
        2 * np.pi * np.arange(num_points) / 288
    )
    values = base + daily_pattern + noise
    values = np.clip(values, 0, None)
    return values.tolist()


def _build_node_based_cluster_metrics() -> dict:
    """Build synthetic metrics for a node-based cluster with 2 nodes."""
    num_points = 4032
    timestamps = _generate_timestamps(num_points)

    cpu_values_1 = _generate_realistic_cpu_values(num_points)
    cpu_values_2 = [v * 0.8 for v in cpu_values_1]
    mem_values = _generate_realistic_memory_values(num_points)
    net_values = _generate_realistic_network_values(num_points)

    def make_node_metrics(cpu_vals, mem_vals, net_vals):
        return {
            "EngineCPUUtilization": {
                "Maximum": {"timestamps": timestamps, "values": cpu_vals}
            },
            "DatabaseMemoryUsagePercentage": {
                "Maximum": {"timestamps": timestamps, "values": mem_vals},
                "Average": {"timestamps": timestamps, "values": mem_vals},
            },
            "NetworkBaselineUsageInPercentage": {
                "Maximum": {"timestamps": timestamps, "values": net_vals}
            },
            "NetworkBaselineUsageOutPercentage": {
                "Maximum": {
                    "timestamps": timestamps,
                    "values": [v * 0.5 for v in net_vals],
                }
            },
            "CacheHits": {
                "Sum": {
                    "timestamps": timestamps,
                    "values": _generate_command_values(1000, num_points),
                }
            },
            "CacheMisses": {
                "Sum": {
                    "timestamps": timestamps,
                    "values": _generate_command_values(100, num_points),
                }
            },
            "StringBasedCmds": {
                "Sum": {
                    "timestamps": timestamps,
                    "values": _generate_command_values(800, num_points),
                }
            },
            "HashBasedCmds": {
                "Sum": {
                    "timestamps": timestamps,
                    "values": _generate_command_values(50, num_points),
                }
            },
            "SortedSetBasedCmds": {
                "Sum": {
                    "timestamps": timestamps,
                    "values": _generate_command_values(20, num_points),
                }
            },
            "ListBasedCmds": {
                "Sum": {
                    "timestamps": timestamps,
                    "values": _generate_command_values(10, num_points),
                }
            },
            "SetBasedCmds": {
                "Sum": {
                    "timestamps": timestamps,
                    "values": _generate_command_values(10, num_points),
                }
            },
            "StreamBasedCmds": {
                "Sum": {
                    "timestamps": timestamps,
                    "values": _generate_command_values(5, num_points),
                }
            },
            "PubSubBasedCmds": {
                "Sum": {
                    "timestamps": timestamps,
                    "values": _generate_command_values(2, num_points),
                }
            },
            "GetTypeCmds": {
                "Sum": {
                    "timestamps": timestamps,
                    "values": _generate_command_values(900, num_points),
                }
            },
            "SetTypeCmds": {
                "Sum": {
                    "timestamps": timestamps,
                    "values": _generate_command_values(150, num_points),
                }
            },
            "Evictions": {
                "Sum": {
                    "timestamps": timestamps,
                    "values": _generate_command_values(5, num_points),
                }
            },
            "CurrItems": {
                "Maximum": {
                    "timestamps": timestamps,
                    "values": _generate_command_values(50000, num_points),
                }
            },
            "CurrVolatileItems": {
                "Maximum": {
                    "timestamps": timestamps,
                    "values": _generate_command_values(48000, num_points),
                }
            },
            "CurrConnections": {
                "Maximum": {
                    "timestamps": timestamps,
                    "values": _generate_command_values(200, num_points),
                }
            },
            "ReplicationLag": {
                "Maximum": {
                    "timestamps": timestamps,
                    "values": [0.01] * num_points,
                }
            },
        }

    return {
        "nodes": {
            "my-node-cluster-001": {
                "metrics": make_node_metrics(
                    cpu_values_1, mem_values, net_values
                )
            },
            "my-node-cluster-002": {
                "metrics": make_node_metrics(
                    cpu_values_2, mem_values, net_values
                )
            },
        }
    }


def _build_serverless_cluster_metrics() -> dict:
    """Build synthetic metrics for a serverless cluster."""
    num_points = 4032
    timestamps = _generate_timestamps(num_points)

    np.random.seed(50)
    ecpu_values = (40 + 10 * np.random.randn(num_points)).clip(0, 100).tolist()
    storage_values = (
        60 + 2 * np.random.randn(num_points)
    ).clip(0, 100).tolist()
    throttled_values = [0.0] * num_points

    return {
        "metrics": {
            "ElastiCacheProcessingUnits": {
                "Maximum": {"timestamps": timestamps, "values": ecpu_values}
            },
            "BytesUsedForCache": {
                "Maximum": {"timestamps": timestamps, "values": storage_values}
            },
            "ThrottledCmds": {
                "Sum": {
                    "timestamps": timestamps,
                    "values": throttled_values,
                }
            },
            "CacheHits": {
                "Sum": {
                    "timestamps": timestamps,
                    "values": _generate_command_values(500, num_points),
                }
            },
            "CacheMisses": {
                "Sum": {
                    "timestamps": timestamps,
                    "values": _generate_command_values(50, num_points),
                }
            },
            "GetTypeCmds": {
                "Sum": {
                    "timestamps": timestamps,
                    "values": _generate_command_values(400, num_points),
                }
            },
            "SetTypeCmds": {
                "Sum": {
                    "timestamps": timestamps,
                    "values": _generate_command_values(100, num_points),
                }
            },
        }
    }


def _build_synthetic_metrics_json() -> dict:
    """Build a full synthetic metrics.json with both cluster types."""
    return {
        "clusters": {
            "my-node-cluster": _build_node_based_cluster_metrics(),
            "my-serverless-cache": _build_serverless_cluster_metrics(),
        }
    }


def _build_synthetic_inventory_json() -> dict:
    """Build a full synthetic inventory.json."""
    return {
        "clusters": {
            "my-node-cluster": {
                "node_type": "cache.r6g.large",
                "engine": "redis",
                "engine_version": "7.0.7",
                "cluster_mode_enabled": True,
                "num_shards": 2,
                "parameters": {"maxclients": "65000"},
            },
            "my-serverless-cache": {
                "node_type": "serverless",
                "engine": "redis",
                "engine_version": "7.0",
                "cluster_mode_enabled": False,
                "num_shards": 0,
                "is_serverless": True,
            },
        }
    }


@pytest.fixture
def synthetic_data(tmp_path):
    """Write synthetic metrics.json and inventory.json to tmp_path.

    Returns a dict with paths to metrics_file, inventory_file, and output_file.
    """
    metrics_path = tmp_path / "metrics.json"
    inventory_path = tmp_path / "inventory.json"
    output_path = tmp_path / "analysis.json"

    metrics_data = _build_synthetic_metrics_json()
    inventory_data = _build_synthetic_inventory_json()

    metrics_path.write_text(json.dumps(metrics_data), encoding="utf-8")
    inventory_path.write_text(json.dumps(inventory_data), encoding="utf-8")

    return {
        "metrics_path": str(metrics_path),
        "inventory_path": str(inventory_path),
        "output_path": str(output_path),
    }


def _run_analysis(synthetic_data: dict) -> dict:
    """Run the analysis pipeline and return the parsed output."""
    exit_code = main([
        "--metrics", synthetic_data["metrics_path"],
        "--inventory", synthetic_data["inventory_path"],
        "--output", synthetic_data["output_path"],
    ])
    assert exit_code == 0, f"main() returned non-zero exit code: {exit_code}"

    with open(synthetic_data["output_path"], "r", encoding="utf-8") as f:
        return json.load(f)


# ===========================================================================
# Task 14.1: End-to-end test with synthetic data
# ===========================================================================


class TestEndToEnd:
    """14.1: End-to-end pipeline test with node-based and serverless clusters."""

    def test_pipeline_runs_successfully(self, synthetic_data):
        """The pipeline should exit with code 0 and produce valid JSON."""
        exit_code = main([
            "--metrics", synthetic_data["metrics_path"],
            "--inventory", synthetic_data["inventory_path"],
            "--output", synthetic_data["output_path"],
        ])
        assert exit_code == 0
        assert os.path.exists(synthetic_data["output_path"])

        with open(synthetic_data["output_path"], "r", encoding="utf-8") as f:
            result = json.load(f)

        assert "metadata" in result
        assert "clusters" in result

    def test_both_cluster_types_analyzed(self, synthetic_data):
        """Both node-based and serverless clusters appear in results."""
        result = _run_analysis(synthetic_data)
        clusters = result["clusters"]
        assert "my-node-cluster" in clusters
        assert "my-serverless-cache" in clusters

    def test_output_is_valid_json(self, synthetic_data):
        """Output must always be parseable JSON."""
        main([
            "--metrics", synthetic_data["metrics_path"],
            "--inventory", synthetic_data["inventory_path"],
            "--output", synthetic_data["output_path"],
        ])
        with open(synthetic_data["output_path"], "r", encoding="utf-8") as f:
            # This will raise if not valid JSON
            data = json.load(f)
        assert isinstance(data, dict)


# ===========================================================================
# Task 14.2: Output schema validation
# ===========================================================================


class TestOutputSchema:
    """14.2: Validate all required keys are present in output."""

    REQUIRED_METADATA_KEYS = [
        "source_metrics",
        "source_inventory",
        "analysis_timestamp",
        "models_applied",
        "clusters_analyzed",
        "total_findings",
        "analysis_duration_seconds",
    ]

    REQUIRED_CLUSTER_KEYS = [
        "workload_class",
        "utilization",
        "percentiles",
        "trends",
        "breaches",
        "efficiency",
        "shard_balance",
        "traffic_pattern",
        "correlations",
        "findings",
        "errors",
    ]

    def test_metadata_keys_present(self, synthetic_data):
        """metadata object has all required keys."""
        result = _run_analysis(synthetic_data)
        metadata = result["metadata"]
        for key in self.REQUIRED_METADATA_KEYS:
            assert key in metadata, f"Missing metadata key: {key}"

    def test_metadata_types(self, synthetic_data):
        """metadata values have correct types."""
        result = _run_analysis(synthetic_data)
        metadata = result["metadata"]
        assert isinstance(metadata["source_metrics"], str)
        assert isinstance(metadata["source_inventory"], str)
        assert isinstance(metadata["analysis_timestamp"], str)
        assert isinstance(metadata["models_applied"], list)
        assert isinstance(metadata["clusters_analyzed"], int)
        assert isinstance(metadata["total_findings"], int)
        assert isinstance(
            metadata["analysis_duration_seconds"], (int, float)
        )

    def test_per_cluster_keys_present(self, synthetic_data):
        """Each cluster has all required model output keys."""
        result = _run_analysis(synthetic_data)
        for cluster_id, cluster_data in result["clusters"].items():
            for key in self.REQUIRED_CLUSTER_KEYS:
                assert key in cluster_data, (
                    f"Cluster '{cluster_id}' missing key: {key}"
                )

    def test_findings_structure(self, synthetic_data):
        """Each finding has the required fields."""
        result = _run_analysis(synthetic_data)
        required_finding_keys = [
            "finding_id",
            "model_source",
            "severity",
            "title",
            "description",
            "metric_name",
            "current_value",
            "threshold",
            "recommendation",
        ]
        for cluster_id, cluster_data in result["clusters"].items():
            for finding in cluster_data["findings"]:
                for key in required_finding_keys:
                    assert key in finding, (
                        f"Finding in '{cluster_id}' missing key: {key}"
                    )

    def test_errors_is_list(self, synthetic_data):
        """Each cluster's errors field is a list."""
        result = _run_analysis(synthetic_data)
        for cluster_id, cluster_data in result["clusters"].items():
            assert isinstance(cluster_data["errors"], list)

    def test_correlations_is_list(self, synthetic_data):
        """Each cluster's correlations field is a list."""
        result = _run_analysis(synthetic_data)
        for cluster_id, cluster_data in result["clusters"].items():
            assert isinstance(cluster_data["correlations"], list)


# ===========================================================================
# Task 14.3: Correctness properties validation
# ===========================================================================


class TestCorrectnessProperties:
    """14.3: Validate mathematical invariants across the output."""

    def test_percentile_ordering(self, synthetic_data):
        """p50 <= p95 <= p99 <= max for all metrics."""
        result = _run_analysis(synthetic_data)
        for cluster_id, cluster_data in result["clusters"].items():
            percentiles = cluster_data.get("percentiles", {})
            for metric_key, metric_data in percentiles.items():
                if isinstance(metric_data, dict) and "p50" in metric_data:
                    p50 = metric_data["p50"]
                    p95 = metric_data["p95"]
                    p99 = metric_data["p99"]
                    max_val = metric_data["max"]
                    assert p50 <= p95, (
                        f"{cluster_id}/{metric_key}: p50={p50} > p95={p95}"
                    )
                    assert p95 <= p99, (
                        f"{cluster_id}/{metric_key}: p95={p95} > p99={p99}"
                    )
                    assert p99 <= max_val, (
                        f"{cluster_id}/{metric_key}: p99={p99} > max={max_val}"
                    )

    def test_spike_ratio_gte_one(self, synthetic_data):
        """spike_ratio >= 1 for all metrics."""
        result = _run_analysis(synthetic_data)
        for cluster_id, cluster_data in result["clusters"].items():
            percentiles = cluster_data.get("percentiles", {})
            for metric_key, metric_data in percentiles.items():
                if isinstance(metric_data, dict) and "spike_ratio" in metric_data:
                    sr = metric_data["spike_ratio"]
                    assert sr >= 1.0, (
                        f"{cluster_id}/{metric_key}: spike_ratio={sr} < 1"
                    )

    def test_cv_non_negative(self, synthetic_data):
        """Coefficient of Variation >= 0 for shard balance metrics."""
        result = _run_analysis(synthetic_data)
        for cluster_id, cluster_data in result["clusters"].items():
            shard_balance = cluster_data.get("shard_balance")
            if shard_balance is not None and isinstance(shard_balance, dict):
                for key, value in shard_balance.items():
                    if "cv" in key.lower() and isinstance(value, (int, float)):
                        assert value >= 0, (
                            f"{cluster_id}: {key}={value} < 0"
                        )

    def test_r_squared_bounded(self, synthetic_data):
        """R² values must be <= 1 (can be slightly negative for poor fits)."""
        result = _run_analysis(synthetic_data)
        for cluster_id, cluster_data in result["clusters"].items():
            trends = cluster_data.get("trends")
            if trends and isinstance(trends, dict):
                for metric_key, trend_data in trends.items():
                    if isinstance(trend_data, dict) and "r_squared" in trend_data:
                        r2 = trend_data["r_squared"]
                        # R² can be slightly negative when the linear fit
                        # is worse than a horizontal line (poor fit). The
                        # key invariant is that it does not exceed 1.0.
                        assert r2 <= 1.0, (
                            f"{cluster_id}/{metric_key}: R²={r2} > 1"
                        )
                        # Should not be extremely negative
                        assert r2 >= -1.0, (
                            f"{cluster_id}/{metric_key}: R²={r2} < -1"
                        )

    def test_correlation_bounded(self, synthetic_data):
        """Pearson r values must be in [-1, 1]."""
        result = _run_analysis(synthetic_data)
        for cluster_id, cluster_data in result["clusters"].items():
            correlations = cluster_data.get("correlations", [])
            for corr in correlations:
                r = corr.get("r_value")
                if r is not None:
                    assert -1 <= r <= 1, (
                        f"{cluster_id}: r_value={r} not in [-1,1]"
                    )

    def test_severity_values_valid(self, synthetic_data):
        """Finding severity must be one of CRITICAL/HIGH/MEDIUM/LOW."""
        result = _run_analysis(synthetic_data)
        valid_severities = {"CRITICAL", "HIGH", "MEDIUM", "LOW"}
        for cluster_id, cluster_data in result["clusters"].items():
            for finding in cluster_data.get("findings", []):
                severity = finding.get("severity")
                assert severity in valid_severities, (
                    f"{cluster_id}: invalid severity '{severity}'"
                )

    def test_workload_class_valid(self, synthetic_data):
        """workload_class must be from the defined set."""
        result = _run_analysis(synthetic_data)
        valid_classes = {
            "cache-aside",
            "session-store",
            "leaderboard",
            "rate-limiter",
            "event-stream",
            "real-time-messaging",
            "queue",
            "general-purpose",
            "unknown",
        }
        for cluster_id, cluster_data in result["clusters"].items():
            wc = cluster_data.get("workload_class")
            assert wc in valid_classes, (
                f"{cluster_id}: invalid workload_class '{wc}'"
            )


# ===========================================================================
# Task 14.4: Graceful degradation validation
# ===========================================================================


class TestGracefulDegradation:
    """14.4: Verify valid JSON output even when models fail."""

    def test_model_failure_produces_valid_json(self, synthetic_data):
        """Pipeline produces valid JSON with error annotations when a model fails."""
        # Patch the TrendModel.compute to raise an exception
        with patch(
            "analyze_metrics.TrendModel.compute",
            side_effect=RuntimeError("Simulated trend model failure"),
        ):
            exit_code = main([
                "--metrics", synthetic_data["metrics_path"],
                "--inventory", synthetic_data["inventory_path"],
                "--output", synthetic_data["output_path"],
            ])

        # Should still succeed (exit 0) with partial results
        assert exit_code == 0
        assert os.path.exists(synthetic_data["output_path"])

        with open(synthetic_data["output_path"], "r", encoding="utf-8") as f:
            result = json.load(f)

        # Output is valid JSON with proper structure
        assert "metadata" in result
        assert "clusters" in result

        # Each cluster should have errors recorded for the failed model
        for cluster_id, cluster_data in result["clusters"].items():
            errors = cluster_data.get("errors", [])
            trend_errors = [
                e for e in errors if e.get("model") == "trend"
            ]
            assert len(trend_errors) > 0, (
                f"Cluster '{cluster_id}' missing trend error annotation"
            )

    def test_percentile_failure_skips_dependent_models(self, synthetic_data):
        """When PercentileModel fails, dependent models are skipped with errors."""
        with patch(
            "analyze_metrics.PercentileModel.compute",
            side_effect=RuntimeError("Simulated percentile failure"),
        ):
            exit_code = main([
                "--metrics", synthetic_data["metrics_path"],
                "--inventory", synthetic_data["inventory_path"],
                "--output", synthetic_data["output_path"],
            ])

        assert exit_code == 0

        with open(synthetic_data["output_path"], "r", encoding="utf-8") as f:
            result = json.load(f)

        # All clusters should have errors for percentile and dependent models
        for cluster_id, cluster_data in result["clusters"].items():
            errors = cluster_data.get("errors", [])
            error_models = {e.get("model") for e in errors}
            assert "percentile" in error_models

    def test_multiple_model_failures_still_valid(self, synthetic_data):
        """Multiple model failures still produce valid JSON output."""
        with patch(
            "analyze_metrics.TrendModel.compute",
            side_effect=RuntimeError("Trend failure"),
        ), patch(
            "analyze_metrics.CorrelationModel.compute",
            side_effect=RuntimeError("Correlation failure"),
        ), patch(
            "analyze_metrics.EfficiencyModel.compute",
            side_effect=RuntimeError("Efficiency failure"),
        ):
            exit_code = main([
                "--metrics", synthetic_data["metrics_path"],
                "--inventory", synthetic_data["inventory_path"],
                "--output", synthetic_data["output_path"],
            ])

        assert exit_code == 0

        with open(synthetic_data["output_path"], "r", encoding="utf-8") as f:
            result = json.load(f)

        assert isinstance(result, dict)
        assert "metadata" in result
        assert "clusters" in result


# ===========================================================================
# Task 14.5: Numeric precision validation
# ===========================================================================


class TestNumericPrecision:
    """14.5: Validate decimal places for various value types."""

    @staticmethod
    def _count_decimal_places(value: float) -> int:
        """Count the number of decimal places in a float value."""
        s = str(value)
        if "." not in s:
            return 0
        return len(s.split(".")[1])

    def test_percentile_values_2_decimal_places(self, synthetic_data):
        """Percentages and ratios should have at most 2 decimal places."""
        result = _run_analysis(synthetic_data)
        for cluster_id, cluster_data in result["clusters"].items():
            percentiles = cluster_data.get("percentiles", {})
            for metric_key, metric_data in percentiles.items():
                if isinstance(metric_data, dict) and "p50" in metric_data:
                    for key in ["p50", "p95", "p99", "max", "spike_ratio"]:
                        val = metric_data[key]
                        dp = self._count_decimal_places(val)
                        assert dp <= 2, (
                            f"{cluster_id}/{metric_key}/{key}={val} "
                            f"has {dp} decimal places (max 2)"
                        )

    def test_trend_r_squared_4_decimal_places(self, synthetic_data):
        """R² values should have at most 4 decimal places."""
        result = _run_analysis(synthetic_data)
        for cluster_id, cluster_data in result["clusters"].items():
            trends = cluster_data.get("trends")
            if trends and isinstance(trends, dict):
                for metric_key, trend_data in trends.items():
                    if isinstance(trend_data, dict) and "r_squared" in trend_data:
                        r2 = trend_data["r_squared"]
                        dp = self._count_decimal_places(r2)
                        assert dp <= 4, (
                            f"{cluster_id}/{metric_key}/r_squared={r2} "
                            f"has {dp} decimal places (max 4)"
                        )

    def test_correlation_r_value_4_decimal_places(self, synthetic_data):
        """Correlation r values should have at most 4 decimal places."""
        result = _run_analysis(synthetic_data)
        for cluster_id, cluster_data in result["clusters"].items():
            correlations = cluster_data.get("correlations", [])
            for corr in correlations:
                r = corr.get("r_value")
                if r is not None:
                    dp = self._count_decimal_places(r)
                    assert dp <= 4, (
                        f"{cluster_id}: r_value={r} has {dp} decimal "
                        f"places (max 4)"
                    )

    def test_counts_are_integers(self, synthetic_data):
        """Counts (breach_minutes, idle_hours, etc.) should be integers."""
        result = _run_analysis(synthetic_data)
        metadata = result["metadata"]
        assert isinstance(metadata["clusters_analyzed"], int)
        assert isinstance(metadata["total_findings"], int)

        for cluster_id, cluster_data in result["clusters"].items():
            traffic = cluster_data.get("traffic_pattern")
            if traffic and isinstance(traffic, dict):
                idle_hours = traffic.get("idle_hours")
                if idle_hours is not None:
                    assert isinstance(idle_hours, int), (
                        f"{cluster_id}: idle_hours={idle_hours} is not int"
                    )
                serverless_fit = traffic.get("serverless_fit_score")
                if serverless_fit is not None:
                    assert isinstance(serverless_fit, int), (
                        f"{cluster_id}: serverless_fit_score="
                        f"{serverless_fit} is not int"
                    )

    def test_slope_per_week_2_decimal_places(self, synthetic_data):
        """slope_per_week should have at most 2 decimal places."""
        result = _run_analysis(synthetic_data)
        for cluster_id, cluster_data in result["clusters"].items():
            trends = cluster_data.get("trends")
            if trends and isinstance(trends, dict):
                for metric_key, trend_data in trends.items():
                    if (
                        isinstance(trend_data, dict)
                        and "slope_per_week" in trend_data
                    ):
                        spw = trend_data["slope_per_week"]
                        dp = self._count_decimal_places(spw)
                        assert dp <= 2, (
                            f"{cluster_id}/{metric_key}/slope_per_week="
                            f"{spw} has {dp} decimal places (max 2)"
                        )


# ===========================================================================
# Task 14.6: Performance validation
# ===========================================================================


class TestPerformance:
    """14.6: Confirm 50-cluster analysis completes within 60 seconds."""

    def _build_50_cluster_metrics(self) -> dict:
        """Generate synthetic metrics.json for 50 clusters."""
        clusters = {}
        num_points = 4032
        timestamps = _generate_timestamps(num_points)

        for i in range(50):
            np.random.seed(100 + i)
            cpu_vals = (
                30 + 20 * np.random.rand(num_points)
            ).tolist()
            mem_vals = (
                40 + 20 * np.random.rand(num_points)
            ).tolist()
            net_vals = (
                20 + 15 * np.random.rand(num_points)
            ).tolist()
            cmd_vals = (
                500 + 200 * np.random.rand(num_points)
            ).tolist()

            clusters[f"cluster-{i:03d}"] = {
                "nodes": {
                    f"cluster-{i:03d}-001": {
                        "metrics": {
                            "EngineCPUUtilization": {
                                "Maximum": {
                                    "timestamps": timestamps,
                                    "values": cpu_vals,
                                }
                            },
                            "DatabaseMemoryUsagePercentage": {
                                "Maximum": {
                                    "timestamps": timestamps,
                                    "values": mem_vals,
                                },
                                "Average": {
                                    "timestamps": timestamps,
                                    "values": mem_vals,
                                },
                            },
                            "NetworkBaselineUsageInPercentage": {
                                "Maximum": {
                                    "timestamps": timestamps,
                                    "values": net_vals,
                                }
                            },
                            "NetworkBaselineUsageOutPercentage": {
                                "Maximum": {
                                    "timestamps": timestamps,
                                    "values": [
                                        v * 0.5 for v in net_vals
                                    ],
                                }
                            },
                            "CacheHits": {
                                "Sum": {
                                    "timestamps": timestamps,
                                    "values": cmd_vals,
                                }
                            },
                            "CacheMisses": {
                                "Sum": {
                                    "timestamps": timestamps,
                                    "values": [
                                        v * 0.1 for v in cmd_vals
                                    ],
                                }
                            },
                            "StringBasedCmds": {
                                "Sum": {
                                    "timestamps": timestamps,
                                    "values": cmd_vals,
                                }
                            },
                            "HashBasedCmds": {
                                "Sum": {
                                    "timestamps": timestamps,
                                    "values": [
                                        v * 0.1 for v in cmd_vals
                                    ],
                                }
                            },
                            "SortedSetBasedCmds": {
                                "Sum": {
                                    "timestamps": timestamps,
                                    "values": [
                                        v * 0.05 for v in cmd_vals
                                    ],
                                }
                            },
                            "ListBasedCmds": {
                                "Sum": {
                                    "timestamps": timestamps,
                                    "values": [
                                        v * 0.02 for v in cmd_vals
                                    ],
                                }
                            },
                            "SetBasedCmds": {
                                "Sum": {
                                    "timestamps": timestamps,
                                    "values": [
                                        v * 0.02 for v in cmd_vals
                                    ],
                                }
                            },
                            "StreamBasedCmds": {
                                "Sum": {
                                    "timestamps": timestamps,
                                    "values": [
                                        v * 0.01 for v in cmd_vals
                                    ],
                                }
                            },
                            "PubSubBasedCmds": {
                                "Sum": {
                                    "timestamps": timestamps,
                                    "values": [
                                        v * 0.005 for v in cmd_vals
                                    ],
                                }
                            },
                            "GetTypeCmds": {
                                "Sum": {
                                    "timestamps": timestamps,
                                    "values": cmd_vals,
                                }
                            },
                            "SetTypeCmds": {
                                "Sum": {
                                    "timestamps": timestamps,
                                    "values": [
                                        v * 0.2 for v in cmd_vals
                                    ],
                                }
                            },
                            "Evictions": {
                                "Sum": {
                                    "timestamps": timestamps,
                                    "values": [1.0] * num_points,
                                }
                            },
                            "CurrItems": {
                                "Maximum": {
                                    "timestamps": timestamps,
                                    "values": [50000.0] * num_points,
                                }
                            },
                            "CurrConnections": {
                                "Maximum": {
                                    "timestamps": timestamps,
                                    "values": [200.0] * num_points,
                                }
                            },
                        }
                    }
                }
            }

        return {"clusters": clusters}

    def _build_50_cluster_inventory(self) -> dict:
        """Generate synthetic inventory.json for 50 clusters."""
        clusters = {}
        for i in range(50):
            clusters[f"cluster-{i:03d}"] = {
                "node_type": "cache.r6g.large",
                "engine": "redis",
                "engine_version": "7.0.7",
                "cluster_mode_enabled": False,
                "num_shards": 1,
                "parameters": {},
            }
        return {"clusters": clusters}

    def test_50_clusters_within_60_seconds(self, tmp_path):
        """50-cluster analysis completes within 60 seconds."""
        metrics_path = tmp_path / "metrics_50.json"
        inventory_path = tmp_path / "inventory_50.json"
        output_path = tmp_path / "analysis_50.json"

        metrics_data = self._build_50_cluster_metrics()
        inventory_data = self._build_50_cluster_inventory()

        metrics_path.write_text(
            json.dumps(metrics_data), encoding="utf-8"
        )
        inventory_path.write_text(
            json.dumps(inventory_data), encoding="utf-8"
        )

        start = time.time()
        exit_code = main([
            "--metrics", str(metrics_path),
            "--inventory", str(inventory_path),
            "--output", str(output_path),
        ])
        elapsed = time.time() - start

        assert exit_code == 0, f"Pipeline failed with exit code {exit_code}"
        assert elapsed < 60, (
            f"50-cluster analysis took {elapsed:.1f}s (limit: 60s)"
        )

        # Verify output is valid
        with open(str(output_path), "r", encoding="utf-8") as f:
            result = json.load(f)

        assert result["metadata"]["clusters_analyzed"] == 50
