"""Unit tests for ThresholdLevel dataclass and ThresholdRegistry class (Task 2).

Tests threshold registration, lookup, and severity classification for both
standard metrics (higher is worse) and inverted metrics (lower is worse).
"""

import os
import sys

# Add scripts to path so we can import the module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from analyze_metrics import ThresholdLevel, ThresholdRegistry

# ---------------------------------------------------------------------------
# ThresholdLevel dataclass
# ---------------------------------------------------------------------------


class TestThresholdLevel:
    def test_all_fields(self):
        t = ThresholdLevel(critical=90.0, high=70.0, medium=50.0, low=20.0)
        assert t.critical == 90.0
        assert t.high == 70.0
        assert t.medium == 50.0
        assert t.low == 20.0

    def test_defaults_none(self):
        t = ThresholdLevel()
        assert t.critical is None
        assert t.high is None
        assert t.medium is None
        assert t.low is None

    def test_partial_fields(self):
        t = ThresholdLevel(critical=90.0, high=80.0)
        assert t.critical == 90.0
        assert t.high == 80.0
        assert t.medium is None
        assert t.low is None


# ---------------------------------------------------------------------------
# ThresholdRegistry — registration (node-based)
# ---------------------------------------------------------------------------


class TestThresholdRegistryNodeBased:
    def setup_method(self):
        self.reg = ThresholdRegistry()

    def test_engine_cpu_thresholds(self):
        t = self.reg.get_threshold("EngineCPUUtilization")
        assert t is not None
        assert t.critical == 90.0
        assert t.high == 70.0
        assert t.medium == 50.0
        assert t.low == 20.0

    def test_database_memory_thresholds(self):
        t = self.reg.get_threshold("DatabaseMemoryUsagePercentage")
        assert t is not None
        assert t.critical == 90.0
        assert t.high == 80.0
        assert t.medium is None
        assert t.low == 30.0

    def test_cache_hit_rate_thresholds(self):
        t = self.reg.get_threshold("CacheHitRate")
        assert t is not None
        assert t.critical == 40.0
        assert t.high == 60.0
        assert t.medium == 80.0
        assert t.low is None

    def test_replication_lag_thresholds(self):
        t = self.reg.get_threshold("ReplicationLag")
        assert t is not None
        assert t.critical == 5.0
        assert t.high == 1.0
        assert t.medium == 0.1

    def test_evictions_thresholds(self):
        # Registered under the derived per-minute name. Evictions is collected
        # as a Sum over the 300s period, so the raw series is a 5-minute total.
        #
        # These numbers are the old per-period boundaries (1000 and 100)
        # restated per minute, NOT a change in strictness: Evictions was the one
        # rate metric whose doc and code already agreed on the period unit. The
        # unit moved so that all three count metrics are graded the same way.
        t = self.reg.get_threshold("EvictionsPerMinute")
        assert t is not None
        assert t.critical is None
        assert t.high == 200.0    # was > 1000 per 5-min period
        assert t.medium == 20.0   # was > 100 per 5-min period
        assert t.unit == "per_minute"

    def test_raw_evictions_has_no_threshold(self):
        """The raw Sum must not be gradeable — that is the defect being fixed.

        If a threshold is ever re-registered under the bare name, the p95 of a
        5-minute total gets compared against a per-minute boundary and the check
        fires at a fifth of the rate it reports.
        """
        assert self.reg.get_threshold("Evictions") is None

    def test_read_latency_thresholds(self):
        t = self.reg.get_threshold("SuccessfulReadRequestLatency")
        assert t is not None
        assert t.critical is None
        assert t.high == 10000.0
        assert t.medium == 5000.0

    def test_new_connections_thresholds(self):
        t = self.reg.get_threshold("NewConnectionsPerMinute")
        assert t is not None
        assert t.critical is None
        assert t.high == 5000.0
        assert t.medium == 1000.0
        assert t.unit == "per_minute"

    def test_raw_new_connections_has_no_threshold(self):
        assert self.reg.get_threshold("NewConnections") is None


# ---------------------------------------------------------------------------
# ThresholdRegistry — registration (serverless)
# ---------------------------------------------------------------------------


class TestThresholdRegistryServerless:
    def setup_method(self):
        self.reg = ThresholdRegistry()

    def test_throttled_cmds_thresholds(self):
        t = self.reg.get_threshold("ThrottledCmdsPerMinute")
        assert t is not None
        assert t.critical == 0.0
        assert t.unit == "per_minute"

    def test_raw_throttled_cmds_has_no_threshold(self):
        assert self.reg.get_threshold("ThrottledCmds") is None

    def test_ecpu_utilization_thresholds(self):
        # Thresholds are percentages, so they key on the derived percent series.
        # ElastiCacheProcessingUnits is collected with Sum -- a count of ECPUs
        # consumed per period -- and has no threshold of its own.
        t = self.reg.get_threshold("ECPUUtilizationPercent")
        assert t is not None
        assert t.critical is None
        assert t.high == 80.0
        assert t.medium == 50.0
        assert self.reg.get_threshold("ElastiCacheProcessingUnits") is None

    def test_raw_ecpu_count_is_never_flagged_as_percentage(self):
        # 132,481 ECPUs in a five-minute window is ~441/s -- unremarkable for a
        # busy cache, and nowhere near "80% of the limit".
        assert self.reg.classify_value(
            "ElastiCacheProcessingUnits", 132_481) == "HEALTHY"

    def test_bytes_used_for_cache_thresholds(self):
        # Thresholds are percentages, so they key on the derived percent series —
        # the raw BytesUsedForCache metric is in bytes and has no threshold.
        t = self.reg.get_threshold("BytesUsedForCachePercent")
        assert t is not None
        assert t.critical == 90.0
        assert t.high == 70.0
        assert self.reg.get_threshold("BytesUsedForCache") is None

    def test_raw_byte_metric_is_never_flagged_as_percentage(self):
        # 36 MB of cached data must not read as "90% full".
        assert self.reg.classify_value("BytesUsedForCache", 36_069_960) == "HEALTHY"


# ---------------------------------------------------------------------------
# ThresholdRegistry — get_threshold
# ---------------------------------------------------------------------------


class TestGetThreshold:
    def setup_method(self):
        self.reg = ThresholdRegistry()

    def test_known_metric(self):
        result = self.reg.get_threshold("EngineCPUUtilization")
        assert isinstance(result, ThresholdLevel)

    def test_unknown_metric_returns_none(self):
        result = self.reg.get_threshold("NonExistentMetric")
        assert result is None


# ---------------------------------------------------------------------------
# ThresholdRegistry — get_critical
# ---------------------------------------------------------------------------


class TestGetCritical:
    def setup_method(self):
        self.reg = ThresholdRegistry()

    def test_metric_with_critical(self):
        assert self.reg.get_critical("EngineCPUUtilization") == 90.0
        assert self.reg.get_critical("DatabaseMemoryUsagePercentage") == 90.0
        assert self.reg.get_critical("CacheHitRate") == 40.0
        assert self.reg.get_critical("ReplicationLag") == 5.0
        assert self.reg.get_critical("ThrottledCmdsPerMinute") == 0.0
        assert self.reg.get_critical("BytesUsedForCachePercent") == 90.0

    def test_metric_without_critical(self):
        assert self.reg.get_critical("EvictionsPerMinute") is None
        assert self.reg.get_critical("SuccessfulReadRequestLatency") is None
        assert self.reg.get_critical("NewConnectionsPerMinute") is None
        assert self.reg.get_critical("ElastiCacheProcessingUnits") is None

    def test_unknown_metric(self):
        assert self.reg.get_critical("FakeMetric") is None


# ---------------------------------------------------------------------------
# ThresholdRegistry — classify_value (standard metrics)
# ---------------------------------------------------------------------------


class TestClassifyValueStandard:
    def setup_method(self):
        self.reg = ThresholdRegistry()

    def test_cpu_critical(self):
        assert self.reg.classify_value("EngineCPUUtilization", 95.0) == "CRITICAL"

    def test_cpu_high(self):
        assert self.reg.classify_value("EngineCPUUtilization", 75.0) == "HIGH"

    def test_cpu_medium(self):
        assert self.reg.classify_value("EngineCPUUtilization", 55.0) == "MEDIUM"

    def test_cpu_low_over_provisioned(self):
        assert self.reg.classify_value("EngineCPUUtilization", 10.0) == "LOW"

    def test_cpu_healthy(self):
        assert self.reg.classify_value("EngineCPUUtilization", 45.0) == "HEALTHY"

    def test_memory_critical(self):
        assert self.reg.classify_value("DatabaseMemoryUsagePercentage", 95.0) == "CRITICAL"

    def test_memory_low(self):
        assert self.reg.classify_value("DatabaseMemoryUsagePercentage", 25.0) == "LOW"

    def test_replication_lag_critical(self):
        assert self.reg.classify_value("ReplicationLag", 6.0) == "CRITICAL"

    def test_replication_lag_high(self):
        assert self.reg.classify_value("ReplicationLag", 2.0) == "HIGH"

    def test_replication_lag_medium(self):
        assert self.reg.classify_value("ReplicationLag", 0.5) == "MEDIUM"

    def test_replication_lag_healthy(self):
        assert self.reg.classify_value("ReplicationLag", 0.05) == "HEALTHY"

    def test_throttled_cmds_critical(self):
        assert self.reg.classify_value("ThrottledCmdsPerMinute", 1.0) == "CRITICAL"

    def test_throttled_cmds_healthy(self):
        # 0.0 is NOT > 0.0 so it's HEALTHY
        assert self.reg.classify_value("ThrottledCmdsPerMinute", 0.0) == "HEALTHY"

    def test_raw_rate_metric_names_classify_as_healthy_not_by_accident(self):
        """A raw rate name reaching classify_value returns HEALTHY, silently.

        classify_value's contract for an unknown metric is HEALTHY, so passing
        "NewConnections" does not raise — it quietly grades nothing. That is
        acceptable only because the raw names are unregistered *and* nothing
        looks them up; this test pins the second half by documenting that the
        silence is known, so the coverage test below is what actually guards it.
        """
        assert self.reg.classify_value("NewConnections", 999999.0) == "HEALTHY"


# ---------------------------------------------------------------------------
# ThresholdRegistry — classify_value (inverted metrics)
# ---------------------------------------------------------------------------


class TestClassifyValueInverted:
    def setup_method(self):
        self.reg = ThresholdRegistry()

    def test_hit_rate_critical(self):
        assert self.reg.classify_value("CacheHitRate", 30.0) == "CRITICAL"

    def test_hit_rate_high(self):
        assert self.reg.classify_value("CacheHitRate", 50.0) == "HIGH"

    def test_hit_rate_medium(self):
        assert self.reg.classify_value("CacheHitRate", 70.0) == "MEDIUM"

    def test_hit_rate_healthy(self):
        assert self.reg.classify_value("CacheHitRate", 85.0) == "HEALTHY"

    def test_hit_rate_boundary_critical(self):
        # Exactly at 40 is NOT < 40, so it's HIGH (< 60)
        assert self.reg.classify_value("CacheHitRate", 40.0) == "HIGH"

    def test_hit_rate_boundary_high(self):
        # Exactly at 60 is NOT < 60, so it's MEDIUM (< 80)
        assert self.reg.classify_value("CacheHitRate", 60.0) == "MEDIUM"

    def test_hit_rate_boundary_medium(self):
        # Exactly at 80 is NOT < 80, so it's HEALTHY
        assert self.reg.classify_value("CacheHitRate", 80.0) == "HEALTHY"


# ---------------------------------------------------------------------------
# ThresholdRegistry — classify_value (unknown metric)
# ---------------------------------------------------------------------------


class TestClassifyValueUnknown:
    def setup_method(self):
        self.reg = ThresholdRegistry()

    def test_unknown_metric_always_healthy(self):
        assert self.reg.classify_value("UnknownMetric", 0.0) == "HEALTHY"
        assert self.reg.classify_value("UnknownMetric", 100.0) == "HEALTHY"
        assert self.reg.classify_value("UnknownMetric", -50.0) == "HEALTHY"


# ---------------------------------------------------------------------------
# ThresholdRegistry — boundary value tests
# ---------------------------------------------------------------------------


class TestClassifyValueBoundaries:
    def setup_method(self):
        self.reg = ThresholdRegistry()

    def test_cpu_at_exactly_90(self):
        # 90 is NOT > 90, so it's HIGH (> 70)
        assert self.reg.classify_value("EngineCPUUtilization", 90.0) == "HIGH"

    def test_cpu_just_above_90(self):
        assert self.reg.classify_value("EngineCPUUtilization", 90.1) == "CRITICAL"

    def test_cpu_at_exactly_70(self):
        # 70 is NOT > 70, so it's MEDIUM (> 50)
        assert self.reg.classify_value("EngineCPUUtilization", 70.0) == "MEDIUM"

    def test_cpu_at_exactly_50(self):
        # 50 is NOT > 50, so check LOW (< 20) → no, so HEALTHY
        assert self.reg.classify_value("EngineCPUUtilization", 50.0) == "HEALTHY"

    def test_cpu_at_exactly_20(self):
        # 20 is NOT < 20, so HEALTHY
        assert self.reg.classify_value("EngineCPUUtilization", 20.0) == "HEALTHY"

    def test_cpu_just_below_20(self):
        assert self.reg.classify_value("EngineCPUUtilization", 19.9) == "LOW"
