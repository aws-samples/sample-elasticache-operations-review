"""Contract test: Stage 2 output shape → Stage 3 model input shape.

fetch_metrics.py writes a compact format — one shared `timestamps_5min` array
per cluster and bare value lists per metric/statistic. Every model in
analyze_metrics.py reads a nested format where each series carries its own
timestamps. The existing unit tests all build fixtures in the nested shape, so
they passed while the real pipeline silently produced empty percentiles for
every cluster.

These tests pin the shape fetch_metrics.py actually emits.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from analyze_metrics import (  # noqa: E402
    DEFAULT_RESOLUTION_SECONDS,
    PercentileModel,
    ThresholdRegistry,
    _resolution_seconds,
    normalize_cluster_data,
)


def _stage2_node_cluster(values):
    """Build a cluster in the exact compact shape fetch_metrics.py writes."""
    return {
        "cluster_type": "node-based",
        "region": "us-east-1",
        "timestamps_5min": [
            f"2026-08-01T{i // 12:02d}:{(i % 12) * 5:02d}:00Z"
            for i in range(len(values))
        ],
        "nodes": {
            "cluster-0001-001": {"EngineCPUUtilization": {"Maximum": values}}
        },
        "errors": [],
    }


class TestStage2CompactFormat:
    def test_percentiles_computed_from_compact_stage2_output(self):
        values = [float(i % 50) for i in range(200)]
        cluster = normalize_cluster_data(_stage2_node_cluster(values))

        result = PercentileModel().compute(cluster, None)

        assert "EngineCPUUtilization_Maximum" in result
        stats = result["EngineCPUUtilization_Maximum"]
        assert stats.get("status") != "insufficient_data"
        assert stats["max"] == 49.0

    def test_serverless_compact_metrics_are_normalized(self):
        cluster = {
            "cluster_type": "serverless",
            "timestamps_5min": [f"2026-08-01T00:{i:02d}:00Z" for i in range(20)],
            "metrics": {"ElastiCacheProcessingUnits": {"Sum": list(range(20))}},
        }

        result = PercentileModel().compute(normalize_cluster_data(cluster), None)

        stats = result["ElastiCacheProcessingUnits_Sum"]
        assert stats.get("status") != "insufficient_data"
        assert stats["max"] == 19.0

    def test_serverless_list_shaped_metrics_do_not_raise(self):
        """A bare list where a dict is expected must not abort the model run."""
        cluster = {"cluster_type": "serverless", "metrics": {"CacheHits": []}}

        # Previously raised "'list' object has no attribute 'get'", which
        # skipped every dependent model for the cluster.
        assert PercentileModel().compute(normalize_cluster_data(cluster), None) == {}

    def test_already_nested_format_passes_through(self):
        """Back-compat: the nested shape used by other fixtures still works."""
        nested = {
            "nodes": {
                "n-001": {
                    "metrics": {
                        "EngineCPUUtilization": {
                            "Maximum": {
                                "timestamps": ["2026-08-01T00:00:00Z"] * 20,
                                "values": [10.0] * 20,
                            }
                        }
                    }
                }
            }
        }

        result = PercentileModel().compute(normalize_cluster_data(nested), None)

        assert result["EngineCPUUtilization_Maximum"]["p95"] == 10.0

    def test_timestamps_are_paired_with_values(self):
        values = [1.0] * 30
        cluster = normalize_cluster_data(_stage2_node_cluster(values))

        series = cluster["nodes"]["cluster-0001-001"]["metrics"][
            "EngineCPUUtilization"
        ]["Maximum"]

        assert len(series["timestamps"]) == len(series["values"]) == 30


class TestStorageUnitsContract:
    """BytesUsedForCache is emitted in bytes, so it needs percentage derivation."""

    def test_raw_bytes_are_not_compared_against_percentage_thresholds(self):
        assert ThresholdRegistry().classify_value("BytesUsedForCache", 36e6) == (
            "HEALTHY"
        )

    def test_percent_series_derived_from_serverless_storage_limit(self):
        cluster = {
            "cluster_type": "serverless",
            "timestamps_5min": [f"2026-08-01T00:{i:02d}:00Z" for i in range(12)],
            # 5 GiB used against a 10 GB configured ceiling → ~50%.
            "metrics": {"BytesUsedForCache": {"Maximum": [5 * 1024**3] * 12}},
        }
        inventory = {
            "cache_usage_limits": {"data_storage": {"maximum": 10, "unit": "GB"}}
        }

        normalized = normalize_cluster_data(cluster, inventory)
        pct = normalized["metrics"]["BytesUsedForCachePercent"]["Maximum"]["values"]

        assert pct[0] == 50.0
        assert ThresholdRegistry().classify_value(
            "BytesUsedForCachePercent", pct[0]
        ) == "HEALTHY"

    def test_idle_cache_hit_rate_is_dropped_not_flagged_critical(self):
        """0 hits + 0 misses is an undefined ratio, not a 0% hit rate."""
        cluster = normalize_cluster_data(
            {
                "timestamps_5min": [f"2026-08-01T00:{i:02d}:00Z" for i in range(12)],
                "metrics": {
                    "CacheHitRate": {"Average": [0.0] * 12},
                    "CacheHits": {"Sum": [0] * 12},
                    "CacheMisses": {"Sum": [0] * 12},
                },
            }
        )

        assert "CacheHitRate" not in cluster["metrics"]

    def test_genuine_low_hit_rate_is_still_flagged(self):
        """A cache with real traffic and a poor hit rate must survive."""
        cluster = normalize_cluster_data(
            {
                "timestamps_5min": [f"2026-08-01T00:{i:02d}:00Z" for i in range(12)],
                "metrics": {
                    "CacheHitRate": {"Average": [12.0] * 12},
                    "CacheHits": {"Sum": [120] * 12},
                    "CacheMisses": {"Sum": [880] * 12},
                },
            }
        )

        assert "CacheHitRate" in cluster["metrics"]
        assert ThresholdRegistry().classify_value("CacheHitRate", 12.0) == "CRITICAL"

    def test_no_percent_series_without_a_configured_maximum(self):
        """Node-based clusters have no storage ceiling — don't invent one."""
        cluster = normalize_cluster_data(
            {
                "timestamps_5min": ["2026-08-01T00:00:00Z"] * 12,
                "nodes": {"n-001": {"BytesUsedForCache": {"Maximum": [1e9] * 12}}},
            },
            {"cluster_type": "node-based"},
        )

        metrics = cluster["nodes"]["n-001"]["metrics"]
        assert "BytesUsedForCachePercent" not in metrics


class TestEcpuUnitsContract:
    """ElastiCacheProcessingUnits is a per-period COUNT, not a percentage.

    Stage 2 collects it with the Sum statistic, so each datapoint is the total
    ECPUs consumed during the period. The configured ceiling is a per-SECOND
    rate, so comparing the two needs the collection period — which is why the
    resolution is read from metrics.json rather than assumed.
    """

    def _serverless(self, ecpu_sum_per_point, points=12):
        return {
            "cluster_type": "serverless",
            "timestamps_5min": [
                f"2026-08-01T00:{i:02d}:00Z" for i in range(points)
            ],
            "metrics": {
                "ElastiCacheProcessingUnits": {
                    "Sum": [ecpu_sum_per_point] * points
                }
            },
        }

    def test_raw_ecpu_count_is_not_compared_against_percentage_thresholds(self):
        assert ThresholdRegistry().classify_value(
            "ElastiCacheProcessingUnits", 132_481) == "HEALTHY"

    def test_percent_series_derived_from_ecpu_rate_limit(self):
        # 150,000 ECPUs per 300s window = 500/s against a 5,000/s ceiling = 10%.
        inventory = {
            "cache_usage_limits": {"ecpu_per_second": {"maximum": 5000}}
        }
        normalized = normalize_cluster_data(
            self._serverless(150_000.0), inventory, 300.0)
        pct = normalized["metrics"]["ECPUUtilizationPercent"]["Sum"]["values"]

        assert pct[0] == 10.0
        assert ThresholdRegistry().classify_value(
            "ECPUUtilizationPercent", pct[0]) == "HEALTHY"

    def test_resolution_changes_the_derived_rate(self):
        # The same per-period sum at 60s resolution is a five-times-higher rate.
        # Assuming 300s where Stage 2 collected 60s understates it five-fold.
        inventory = {
            "cache_usage_limits": {"ecpu_per_second": {"maximum": 5000}}
        }
        at_60 = normalize_cluster_data(
            self._serverless(150_000.0), inventory, 60.0)
        pct = at_60["metrics"]["ECPUUtilizationPercent"]["Sum"]["values"]

        assert pct[0] == 50.0

    def test_no_percent_series_without_a_configured_ecpu_maximum(self):
        # Default-limits serverless caches report no ECPU ceiling. Deriving a
        # percentage from an assumed limit would invent the denominator.
        normalized = normalize_cluster_data(
            self._serverless(150_000.0),
            {"cluster_type": "serverless", "cache_usage_limits": {}},
            300.0,
        )
        assert "ECPUUtilizationPercent" not in normalized["metrics"]

    def test_resolution_read_from_stage2_metadata(self):
        assert _resolution_seconds(
            {"metadata": {"resolution_seconds": 60}}) == 60.0

    def test_missing_resolution_falls_back_and_warns(self, caplog):
        with caplog.at_level("WARNING"):
            assert _resolution_seconds({"metadata": {}}) == \
                DEFAULT_RESOLUTION_SECONDS
        assert "resolution_seconds" in caplog.text
