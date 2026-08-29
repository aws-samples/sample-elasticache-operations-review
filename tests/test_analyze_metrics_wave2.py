"""Unit tests for TrendModel, UtilizationModel, and BreachModel (Tasks 4, 6, 7).

Tests the Wave 2 models: trend detection, utilization matrix classification,
and breach analysis.
"""

import os
import sys

import numpy as np

# Add scripts to path so we can import the module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from analyze_metrics import (
    BreachModel,
    ThresholdRegistry,
    TrendModel,
    UtilizationModel,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_timestamps(num_days=14, points_per_day=288):
    """Generate ISO timestamps for num_days at 5-minute intervals."""
    ts = []
    for day in range(num_days):
        for point in range(points_per_day):
            hour = point * 5 // 60
            minute = (point * 5) % 60
            ts.append(f"2024-01-{day + 1:02d}T{hour:02d}:{minute:02d}:00Z")
    return ts


def make_cluster_data(metric_name, statistic, timestamps, values):
    """Build a minimal cluster_data dict for a single node with one metric."""
    return {
        "nodes": {
            "node-001": {
                "metrics": {
                    metric_name: {
                        statistic: {
                            "timestamps": timestamps,
                            "values": values,
                        }
                    }
                }
            }
        }
    }


# ---------------------------------------------------------------------------
# TrendModel Tests
# ---------------------------------------------------------------------------


class TestTrendModel:
    def setup_method(self):
        self.model = TrendModel()

    def test_rising_trend_detected(self):
        """A linearly rising metric should be detected with high R²."""
        timestamps = make_timestamps(14)
        # Create a linearly rising series: day 1 = 30%, day 14 = 72% (≈3%/day)
        values = []
        for day in range(14):
            daily_val = 30.0 + day * 3.0
            values.extend([daily_val] * 288)

        cluster_data = make_cluster_data(
            "DatabaseMemoryUsagePercentage", "Average", timestamps, values
        )
        result = self.model.compute(cluster_data, None)

        assert "DatabaseMemoryUsagePercentage" in result
        metric = result["DatabaseMemoryUsagePercentage"]
        assert metric["direction"] == "rising"
        assert metric["r_squared"] > 0.9
        assert metric["slope_per_week"] > 2.0
        assert metric["is_finding"] is True

    def test_declining_hit_rate(self):
        """A declining CacheHitRate should be detected."""
        timestamps = make_timestamps(14)
        # CacheHitRate declining from 95% to 60%
        values = []
        for day in range(14):
            daily_val = 95.0 - day * 2.5
            values.extend([daily_val] * 288)

        cluster_data = make_cluster_data(
            "CacheHitRate", "Average", timestamps, values
        )
        result = self.model.compute(cluster_data, None)

        metric = result["CacheHitRate"]
        assert metric["direction"] == "declining"
        assert metric["r_squared"] > 0.9
        assert metric["is_finding"] is True

    def test_stable_no_trend(self):
        """Random noise with no trend should be classified as stable."""
        timestamps = make_timestamps(14)
        np.random.seed(42)
        values = list(np.random.normal(50.0, 5.0, 14 * 288))

        cluster_data = make_cluster_data(
            "DatabaseMemoryUsagePercentage", "Average", timestamps, values
        )
        result = self.model.compute(cluster_data, None)

        metric = result["DatabaseMemoryUsagePercentage"]
        assert metric["direction"] == "stable"
        assert metric["is_finding"] is False

    def test_insufficient_data(self):
        """Less than 7 days of data should be marked insufficient."""
        timestamps = make_timestamps(5)
        values = [50.0] * (5 * 288)

        cluster_data = make_cluster_data(
            "DatabaseMemoryUsagePercentage", "Average", timestamps, values
        )
        result = self.model.compute(cluster_data, None)

        metric = result["DatabaseMemoryUsagePercentage"]
        assert metric["status"] == "insufficient_data"

    def test_only_trendable_metrics(self):
        """Non-trendable metrics should not appear in results."""
        timestamps = make_timestamps(14)
        values = [50.0] * (14 * 288)

        cluster_data = make_cluster_data(
            "EngineCPUUtilization", "Maximum", timestamps, values
        )
        result = self.model.compute(cluster_data, None)

        # EngineCPUUtilization is not in TRENDABLE_METRICS
        assert "EngineCPUUtilization" not in result

    def test_r_squared_bounded(self):
        """R² should always be between 0 and 1."""
        timestamps = make_timestamps(14)
        values = []
        for day in range(14):
            daily_val = 30.0 + day * 2.0
            values.extend([daily_val + np.random.normal(0, 1)] * 288)

        cluster_data = make_cluster_data(
            "CurrItems", "Average", timestamps, values
        )
        result = self.model.compute(cluster_data, None)

        metric = result["CurrItems"]
        assert 0 <= metric["r_squared"] <= 1.0

    def test_missing_metric_data(self):
        """Missing metric data should be marked insufficient."""
        cluster_data = {"nodes": {"node-001": {"metrics": {}}}}
        result = self.model.compute(cluster_data, None)

        for metric_name in TrendModel.TRENDABLE_METRICS:
            assert result[metric_name]["status"] == "insufficient_data"


# ---------------------------------------------------------------------------
# UtilizationModel Tests
# ---------------------------------------------------------------------------


class TestUtilizationModel:
    def setup_method(self):
        self.model = UtilizationModel()

    def test_balanced_classification(self):
        """Medium CPU + Medium Memory + Low Network = BALANCED."""
        timestamps = make_timestamps(14)
        # CPU at 45%, Memory at 50%, Network at 20%
        cluster_data = {
            "nodes": {
                "node-001": {
                    "metrics": {
                        "EngineCPUUtilization": {
                            "Maximum": {
                                "timestamps": timestamps,
                                "values": [45.0] * len(timestamps),
                            }
                        },
                        "DatabaseMemoryUsagePercentage": {
                            "Maximum": {
                                "timestamps": timestamps,
                                "values": [50.0] * len(timestamps),
                            }
                        },
                        "NetworkBaselineUsageInPercentage": {
                            "Average": {
                                "timestamps": timestamps,
                                "values": [20.0] * len(timestamps),
                            }
                        },
                        "NetworkBaselineUsageOutPercentage": {
                            "Average": {
                                "timestamps": timestamps,
                                "values": [15.0] * len(timestamps),
                            }
                        },
                    }
                }
            }
        }

        result = self.model.compute(cluster_data, None)
        assert result["classification"] == "BALANCED"
        assert result["cpu_level"] == "Medium"
        assert result["memory_level"] == "Medium"
        assert result["network_level"] == "Low"

    def test_idle_classification(self):
        """All Low AND no traffic = IDLE.

        IDLE now rests on confirmed zero traffic, not on the axes alone: a
        low-resource cache that is still serving requests is OVER-PROVISIONED
        (right-size), not IDLE (decommission). The zero CacheHits/CacheMisses
        counters below are what make this cluster genuinely idle.
        """
        timestamps = make_timestamps(14)
        cluster_data = {
            "nodes": {
                "node-001": {
                    "metrics": {
                        "EngineCPUUtilization": {
                            "Maximum": {
                                "timestamps": timestamps,
                                "values": [5.0] * len(timestamps),
                            }
                        },
                        "DatabaseMemoryUsagePercentage": {
                            "Maximum": {
                                "timestamps": timestamps,
                                "values": [10.0] * len(timestamps),
                            }
                        },
                        "NetworkBaselineUsageInPercentage": {
                            "Average": {
                                "timestamps": timestamps,
                                "values": [5.0] * len(timestamps),
                            }
                        },
                        "NetworkBaselineUsageOutPercentage": {
                            "Average": {
                                "timestamps": timestamps,
                                "values": [5.0] * len(timestamps),
                            }
                        },
                        # Zero traffic over the window -- confirms idleness.
                        "CacheHits": {
                            "Sum": {"timestamps": timestamps,
                                    "values": [0.0] * len(timestamps)}
                        },
                        "CacheMisses": {
                            "Sum": {"timestamps": timestamps,
                                    "values": [0.0] * len(timestamps)}
                        },
                    }
                }
            }
        }

        result = self.model.compute(cluster_data, None)
        assert result["classification"] == "IDLE"

    def test_low_utilization_but_serving_traffic_is_over_provisioned(self):
        """A lightly-loaded cache that IS serving traffic is not IDLE.

        The low-CPU-but-serving case: CPU/memory Low but real cache hits. The
        verdict must be OVER-PROVISIONED (right-size), never IDLE (decommission).
        """
        timestamps = make_timestamps(14)
        cluster_data = {
            "nodes": {
                "node-001": {
                    "metrics": {
                        "EngineCPUUtilization": {
                            "Maximum": {"timestamps": timestamps,
                                        "values": [5.0] * len(timestamps)}
                        },
                        "DatabaseMemoryUsagePercentage": {
                            "Maximum": {"timestamps": timestamps,
                                        "values": [10.0] * len(timestamps)}
                        },
                        "CacheHits": {
                            "Sum": {"timestamps": timestamps,
                                    "values": [1_000_000.0] * len(timestamps)}
                        },
                    }
                }
            }
        }
        result = self.model.compute(cluster_data, None)
        assert result["classification"] == "OVER-PROVISIONED"

    def test_saturated_classification(self):
        """CPU High + Memory High = SATURATED."""
        timestamps = make_timestamps(14)
        cluster_data = {
            "nodes": {
                "node-001": {
                    "metrics": {
                        "EngineCPUUtilization": {
                            "Maximum": {
                                "timestamps": timestamps,
                                "values": [85.0] * len(timestamps),
                            }
                        },
                        "DatabaseMemoryUsagePercentage": {
                            "Maximum": {
                                "timestamps": timestamps,
                                "values": [80.0] * len(timestamps),
                            }
                        },
                        "NetworkBaselineUsageInPercentage": {
                            "Average": {
                                "timestamps": timestamps,
                                "values": [50.0] * len(timestamps),
                            }
                        },
                        "NetworkBaselineUsageOutPercentage": {
                            "Average": {
                                "timestamps": timestamps,
                                "values": [50.0] * len(timestamps),
                            }
                        },
                    }
                }
            }
        }

        result = self.model.compute(cluster_data, None)
        assert result["classification"] == "SATURATED"

    def test_cpu_bound_classification(self):
        """CPU High + Memory Medium = CPU-BOUND."""
        timestamps = make_timestamps(14)
        cluster_data = {
            "nodes": {
                "node-001": {
                    "metrics": {
                        "EngineCPUUtilization": {
                            "Maximum": {
                                "timestamps": timestamps,
                                "values": [80.0] * len(timestamps),
                            }
                        },
                        "DatabaseMemoryUsagePercentage": {
                            "Maximum": {
                                "timestamps": timestamps,
                                "values": [50.0] * len(timestamps),
                            }
                        },
                        "NetworkBaselineUsageInPercentage": {
                            "Average": {
                                "timestamps": timestamps,
                                "values": [30.0] * len(timestamps),
                            }
                        },
                        "NetworkBaselineUsageOutPercentage": {
                            "Average": {
                                "timestamps": timestamps,
                                "values": [30.0] * len(timestamps),
                            }
                        },
                    }
                }
            }
        }

        result = self.model.compute(cluster_data, None)
        assert result["classification"] == "CPU-BOUND"

    def test_output_keys(self):
        """Output should contain all required keys."""
        timestamps = make_timestamps(14)
        cluster_data = {
            "nodes": {
                "node-001": {
                    "metrics": {
                        "EngineCPUUtilization": {
                            "Maximum": {
                                "timestamps": timestamps,
                                "values": [50.0] * len(timestamps),
                            }
                        },
                        "DatabaseMemoryUsagePercentage": {
                            "Maximum": {
                                "timestamps": timestamps,
                                "values": [50.0] * len(timestamps),
                            }
                        },
                    }
                }
            }
        }

        result = self.model.compute(cluster_data, None)
        assert "cpu_level" in result
        assert "memory_level" in result
        assert "network_level" in result
        assert "classification" in result
        assert "recommendation" in result
        assert "utilization_scores" in result

    def _serverless_data(self, timestamps, storage_percent=None,
                         ecpu_percent=40.0):
        """Serverless metrics in the statistics fetch_metrics.py actually emits.

        ElastiCacheProcessingUnits is collected as Sum, not Average. The old
        fixture used Average, so the model read an empty series and scored the
        CPU axis 0.0 -- and the test still passed because it only asserted the
        level was one of three strings.

        Both percentage series are derived by normalize_cluster_data from a
        configured usage limit -- BytesUsedForCachePercent from data_storage,
        ECPUUtilizationPercent from ecpu_per_second. Passing them explicitly
        mirrors what Stage 3 receives in production; pass None for either to
        model a cache without that ceiling configured.
        """
        metrics = {
            "ElastiCacheProcessingUnits": {
                "Sum": {"timestamps": timestamps,
                        "values": [132_481.0] * len(timestamps)},
            },
            "ThrottledCmds": {
                "Sum": {"timestamps": timestamps,
                        "values": [0.0] * len(timestamps)},
            },
        }
        if storage_percent is not None:
            metrics["BytesUsedForCachePercent"] = {
                "Maximum": {"timestamps": timestamps,
                            "values": [storage_percent] * len(timestamps)},
            }
        if ecpu_percent is not None:
            metrics["ECPUUtilizationPercent"] = {
                "Sum": {"timestamps": timestamps,
                        "values": [ecpu_percent] * len(timestamps)},
            }
        return {"metrics": metrics}

    def test_serverless_cluster(self):
        """Serverless clusters use different metrics."""
        timestamps = make_timestamps(14)
        result = self.model.compute(
            self._serverless_data(timestamps, storage_percent=50.0), None)
        assert "classification" in result
        assert result["cpu_level"] in ("Low", "Medium", "High")
        assert result["memory_level"] == "Medium"  # 50% storage
        assert result["network_level"] in ("Low", "Medium", "High")

    def test_serverless_storage_read_as_percent_not_bytes(self):
        """Storage must be scored from the derived percent, never raw bytes.

        The model previously read BytesUsedForCache, which is in BYTES, and
        compared it against _classify_memory_level's 30/70 percentage bands. Any
        cache holding more than 70 bytes scored "High". Same bytes-vs-percent
        confusion that produced a false CRITICAL earlier in this pipeline.
        """
        timestamps = make_timestamps(14)
        data = self._serverless_data(timestamps, storage_percent=12.0)
        # A large raw byte series alongside a low percentage: if the model reads
        # the wrong one, 19 GB of bytes cannot possibly score "Low".
        data["metrics"]["BytesUsedForCache"] = {
            "Maximum": {"timestamps": timestamps,
                        "values": [19_000_000_000.0] * len(timestamps)},
        }
        result = self.model.compute(data, None)
        assert result["memory_level"] == "Low"
        assert result["utilization_scores"]["memory_max"] == 12.0

    def test_serverless_without_storage_limit_is_unknown_not_low(self):
        """No configured ceiling means unmeasurable, which is not "ample".

        With no data_storage maximum there is no percent series, so the score
        was 0.0 and the axis read "Low" -- and with a quiet cache that produced
        an IDLE verdict recommending decommission, resting on a metric that was
        never collected.
        """
        timestamps = make_timestamps(14)
        result = self.model.compute(self._serverless_data(timestamps), None)
        assert result["memory_level"] == "Unknown"
        assert result["utilization_scores"]["memory_max"] is None
        assert result["classification"] not in ("IDLE", "OVER-PROVISIONED",
                                                "SATURATED", "MEMORY-BOUND")

    def test_serverless_ecpu_read_from_sum(self):
        """ECPU is collected as Sum; reading Average scored every cache 0.0."""
        timestamps = make_timestamps(14)
        data = self._serverless_data(timestamps, storage_percent=10.0,
                                     ecpu_percent=95.0)
        result = self.model.compute(data, None)
        assert result["utilization_scores"]["cpu_p95"] == 95.0
        assert result["cpu_level"] == "High"

    def test_serverless_ecpu_read_as_percent_not_raw_count(self):
        """The CPU axis must score the derived percent, never the raw Sum.

        ElastiCacheProcessingUnits Sum is a COUNT of ECPUs consumed per period --
        132,481 in a five-minute window is about 441/s, unremarkable. Fed to
        _classify_cpu_level, whose bands are 20 and 70 as percentages, it scored
        "High" and classified the cache CPU-BOUND, whose recommendation ("add
        shards") is not even an action serverless offers. Same count-vs-percent
        class as the BytesUsedForCache bug above.
        """
        timestamps = make_timestamps(14)
        data = self._serverless_data(timestamps, storage_percent=10.0,
                                     ecpu_percent=8.0)
        result = self.model.compute(data, None)
        assert result["utilization_scores"]["cpu_p95"] == 8.0
        assert result["cpu_level"] == "Low"
        assert result["classification"] != "CPU-BOUND"

    def test_serverless_without_ecpu_limit_is_unknown_not_low(self):
        """No configured ECPU ceiling means unmeasurable, which is not "quiet".

        A serverless cache on default limits is the common case. With no
        ecpu_per_second maximum there is no percent series, and scoring that 0.0
        would report a busy cache as having idle CPU.
        """
        timestamps = make_timestamps(14)
        data = self._serverless_data(timestamps, storage_percent=50.0,
                                     ecpu_percent=None)
        result = self.model.compute(data, None)
        assert result["cpu_level"] == "Unknown"
        assert result["utilization_scores"]["cpu_p95"] is None
        assert result["classification"] not in ("IDLE", "OVER-PROVISIONED",
                                                "SATURATED", "CPU-BOUND")

    def test_valid_classifications(self):
        """All possible classifications should be in the valid set."""
        valid = {
            "IDLE", "OVER-PROVISIONED", "CPU-BOUND", "MEMORY-BOUND",
            "NETWORK-BOUND", "BALANCED", "HEAVY", "SATURATED",
        }
        # Test a few combos
        for cpu, mem, net in [
            ("Low", "Low", "Low"),
            ("Medium", "Medium", "Medium"),
            ("High", "High", "High"),
            ("High", "Low", "Low"),
            ("Low", "High", "Low"),
        ]:
            cls = UtilizationModel._classify_combination(cpu, mem, net)
            assert cls in valid

    def test_unknown_memory_axis_claims_neither_capacity_nor_pressure(self):
        """An unmeasured axis must not drive a verdict either way."""
        for cpu, net in [("Low", "Low"), ("Medium", "Medium"), ("High", "Low")]:
            cls = UtilizationModel._classify_combination(cpu, "Unknown", net)
            assert cls not in ("IDLE", "OVER-PROVISIONED", "SATURATED",
                              "MEMORY-BOUND"), f"{cpu}/Unknown/{net} -> {cls}"

    def test_memory_axis_reads_the_statistic_stage2_actually_collects(self):
        """The memory axis must read Maximum, the only statistic collected.

        This is the test whose absence let the bug ship. fetch_metrics.py
        collects DatabaseMemoryUsagePercentage with Maximum ONLY, but the model
        asked for Average -- so _extract_last_7_days returned [], the score was
        0.0, and every cluster in every report was scored as having no memory
        pressure. A cluster at 87% memory was classified IDLE and recommended
        for decommission.

        The old fixtures supplied "Average" and so agreed with the bug. This
        asserts against Stage 2's real MetricDefinition instead of a fixture.
        """
        timestamps = make_timestamps(14)
        cluster_data = {
            "nodes": {
                "node-001": {
                    "metrics": {
                        "EngineCPUUtilization": {
                            "Maximum": {"timestamps": timestamps,
                                        "values": [30.0] * len(timestamps)},
                        },
                        # Maximum only -- exactly what Stage 2 emits.
                        "DatabaseMemoryUsagePercentage": {
                            "Maximum": {"timestamps": timestamps,
                                        "values": [88.0] * len(timestamps)},
                        },
                    }
                }
            }
        }
        result = self.model.compute(cluster_data, None)
        assert result["utilization_scores"]["memory_max"] == 88.0
        assert result["memory_level"] == "High"
        assert result["classification"] == "MEMORY-BOUND"

    def test_high_memory_cluster_is_never_classified_idle(self):
        """The specific false verdict the statistic mismatch produced."""
        timestamps = make_timestamps(14)
        cluster_data = {
            "nodes": {
                "node-001": {
                    "metrics": {
                        "EngineCPUUtilization": {
                            "Maximum": {"timestamps": timestamps,
                                        "values": [1.0] * len(timestamps)},
                        },
                        "DatabaseMemoryUsagePercentage": {
                            "Maximum": {"timestamps": timestamps,
                                        "values": [92.0] * len(timestamps)},
                        },
                    }
                }
            }
        }
        result = self.model.compute(cluster_data, None)
        assert result["classification"] != "IDLE"
        assert result["classification"] != "OVER-PROVISIONED"


# ---------------------------------------------------------------------------
# BreachModel Tests
# ---------------------------------------------------------------------------


class TestBreachModel:
    def setup_method(self):
        self.registry = ThresholdRegistry()
        self.model = BreachModel(self.registry)

    def test_no_breach_healthy(self):
        """Values below threshold should produce HEALTHY severity."""
        timestamps = make_timestamps(14)
        # CPU always at 50% (threshold is 90%)
        values = [50.0] * len(timestamps)

        cluster_data = make_cluster_data(
            "EngineCPUUtilization", "Maximum", timestamps, values
        )
        result = self.model.compute(cluster_data, None)

        assert "EngineCPUUtilization" in result
        metric = result["EngineCPUUtilization"]
        assert metric["severity"] == "HEALTHY"
        assert metric["breach_minutes"] == 0
        assert metric["currently_breaching"] is False

    def test_critical_breach_detected(self):
        """Sustained breach over 4 hours + currently breaching = CRITICAL."""
        timestamps = make_timestamps(14)
        # CPU at 95% for entire period (breaching 90% threshold)
        values = [95.0] * len(timestamps)

        cluster_data = make_cluster_data(
            "EngineCPUUtilization", "Maximum", timestamps, values
        )
        result = self.model.compute(cluster_data, None)

        metric = result["EngineCPUUtilization"]
        assert metric["severity"] == "CRITICAL"
        assert metric["currently_breaching"] is True
        assert metric["breach_minutes"] > 240

    def test_medium_severity_recovered(self):
        """Breach > 30 min but recovered (not currently breaching) = MEDIUM."""
        timestamps = make_timestamps(14)
        total_points = len(timestamps)
        # First 20 points (100 min) above threshold, rest normal
        values = [95.0] * 20 + [50.0] * (total_points - 20)

        cluster_data = make_cluster_data(
            "EngineCPUUtilization", "Maximum", timestamps, values
        )
        result = self.model.compute(cluster_data, None)

        metric = result["EngineCPUUtilization"]
        assert metric["severity"] == "MEDIUM"
        assert metric["breach_minutes"] == 100
        assert metric["currently_breaching"] is False

    def test_incident_clustering(self):
        """Consecutive breach windows should be grouped into incidents."""
        timestamps = make_timestamps(14)
        total_points = len(timestamps)
        # Two distinct breaches: points 0-5 and points 100-110
        values = [50.0] * total_points
        for i in range(6):
            values[i] = 95.0
        for i in range(100, 111):
            values[i] = 95.0

        cluster_data = make_cluster_data(
            "EngineCPUUtilization", "Maximum", timestamps, values
        )
        result = self.model.compute(cluster_data, None)

        metric = result["EngineCPUUtilization"]
        assert len(metric["incidents"]) == 2

    def test_top_3_incidents(self):
        """Only top 3 incidents by duration should be returned."""
        timestamps = make_timestamps(14)
        total_points = len(timestamps)
        values = [50.0] * total_points

        # Create 5 distinct breach windows of varying length
        breach_specs = [
            (0, 10),      # 50 min
            (50, 60),     # 50 min
            (100, 120),   # 100 min
            (200, 210),   # 50 min
            (300, 330),   # 150 min
        ]
        for start, end in breach_specs:
            for i in range(start, end):
                values[i] = 95.0

        cluster_data = make_cluster_data(
            "EngineCPUUtilization", "Maximum", timestamps, values
        )
        result = self.model.compute(cluster_data, None)

        metric = result["EngineCPUUtilization"]
        assert len(metric["incidents"]) <= 3
        # Top incident should be the longest (150 min)
        assert metric["incidents"][0]["duration_minutes"] == 150

    def test_spike_ratio_default(self):
        """Spike ratio defaults to 1.0 when p95 is 0."""
        timestamps = make_timestamps(14)
        # All zeros except one spike
        values = [0.0] * len(timestamps)
        values[0] = 95.0  # One spike above threshold

        cluster_data = make_cluster_data(
            "EngineCPUUtilization", "Maximum", timestamps, values
        )
        result = self.model.compute(cluster_data, None)

        # p95 of mostly zeros is 0, spike_ratio defaults to 1.0
        metric = result["EngineCPUUtilization"]
        assert metric["spike_ratio"] >= 1.0

    def test_breach_percent_calculation(self):
        """Breach percent should be correctly computed."""
        timestamps = make_timestamps(14)
        total_points = len(timestamps)
        total_minutes = total_points * 5

        # Exactly 10% breach
        breach_points = total_points // 10
        values = [95.0] * breach_points + [50.0] * (total_points - breach_points)

        cluster_data = make_cluster_data(
            "EngineCPUUtilization", "Maximum", timestamps, values
        )
        result = self.model.compute(cluster_data, None)

        metric = result["EngineCPUUtilization"]
        expected_breach_minutes = breach_points * 5
        assert metric["breach_minutes"] == expected_breach_minutes
        expected_percent = (expected_breach_minutes / total_minutes) * 100
        assert abs(metric["breach_percent"] - round(expected_percent, 2)) < 0.1

    def test_inverted_metric_cache_hit_rate(self):
        """CacheHitRate breach is when value drops BELOW threshold."""
        timestamps = make_timestamps(14)
        # CacheHitRate at 30% (below CRITICAL threshold of 40%)
        values = [30.0] * len(timestamps)

        cluster_data = make_cluster_data(
            "CacheHitRate", "Average", timestamps, values
        )
        result = self.model.compute(cluster_data, None)

        metric = result["CacheHitRate"]
        assert metric["currently_breaching"] is True
        assert metric["breach_minutes"] > 0
        assert metric["severity"] == "CRITICAL"

    def test_severity_classification_all_levels(self):
        """Test all severity classification levels."""
        # HEALTHY: no breach, spike_ratio < 3
        assert BreachModel._classify_severity(False, 0, 1.5) == "HEALTHY"
        # LOW: spike_ratio > 5, no sustained
        assert BreachModel._classify_severity(False, 0, 6.0) == "LOW"
        # MEDIUM: breach > 30 min, recovered
        assert BreachModel._classify_severity(False, 60, 2.0) == "MEDIUM"
        # HIGH: currently breaching
        assert BreachModel._classify_severity(True, 30, 2.0) == "HIGH"
        # HIGH: breach > 2 hours
        assert BreachModel._classify_severity(False, 130, 2.0) == "HIGH"
        # CRITICAL: currently breaching AND > 4 hours
        assert BreachModel._classify_severity(True, 300, 2.0) == "CRITICAL"
