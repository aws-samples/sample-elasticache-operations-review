#!/usr/bin/env python3
"""
ElastiCache Metrics Analysis — Stage 3

Reads the metrics JSON produced by Stage 2 (fetch_metrics.py) and the inventory
JSON from Stage 1 (discover_inventory.py), applies 9 mathematical models to
characterize cluster health, and produces a structured analysis JSON consumed
by Stage 4 (Well-Architected Assessment).

Models applied:
  1. Percentile Summary — p50/p95/p99/max and spike ratio per metric
  2. Trend Detection — linear regression on daily averages with R² gating
  3. Utilization Matrix — 3-axis CPU×Memory×Network classification
  4. Breach Analysis — spike ratio, breach duration, incident clustering
  5. Efficiency Ratios — hit rate, TTL coverage, eviction pressure, etc.
  6. Shard Balance — Coefficient of Variation across primary nodes
  7. Traffic Pattern — peak-to-trough ratio, idle hours, serverless fit
  8. Correlation Analysis — Pearson correlation between metric pairs
  9. Workload Classification — command-mix based workload typing

Uses only numpy and Python standard library — no scipy, pandas, or ML libraries.

Usage:
    python3 analyze_metrics.py --metrics metrics.json --inventory inventory.json
    python3 analyze_metrics.py --metrics metrics.json --inventory inventory.json --output analysis.json
    python3 analyze_metrics.py --metrics metrics.json --inventory inventory.json --verbose
"""

import argparse
import dataclasses
import datetime
import json
import logging
import math
import os
import sys
import tempfile

import numpy as np
from _metrics_store import iter_cluster_ids, load_cluster_metrics
from _pipeline_version import PIPELINE_VERSION

logger = logging.getLogger(__name__)

# Seconds per datapoint in Stage 2's standard collection. Only a fallback: the
# real value is read from metrics.json's metadata.resolution_seconds, since a
# run collected at a different resolution would convert per-period sums to
# per-second rates by the wrong factor.
DEFAULT_RESOLUTION_SECONDS = 300.0


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class AnalysisConfig:
    """Configuration for the metrics analysis run."""

    metrics_path: str
    inventory_path: str
    output_path: str = "analysis.json"
    profile: str | None = None
    verbose: bool = False


# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------


def setup_logging(verbose: bool) -> None:
    """Configure structured logging with timestamps.

    Args:
        verbose: If True, set level to DEBUG with detailed format.
                 If False, set level to INFO with concise format.
    """
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
    logging.basicConfig(
        level=level,
        format=fmt,
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stderr,
    )


# ---------------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------------


def interpolate_small_gaps(values: list[float], max_gap: int = 3) -> list[float]:
    """Fill gaps of < max_gap consecutive NaN values with linear interpolation.

    Gaps of max_gap or more consecutive NaN values are left as NaN.

    Args:
        values: List of float values (may contain NaN).
        max_gap: Maximum gap size to interpolate. Gaps smaller than this
                 threshold are filled; gaps of this size or larger are left.

    Returns:
        New list with small gaps filled via linear interpolation.
    """
    arr = np.array(values, dtype=np.float64)
    n = len(arr)
    if n == 0:
        return []

    result = arr.copy()
    i = 0
    while i < n:
        if np.isnan(result[i]):
            # Find the extent of this NaN gap
            gap_start = i
            while i < n and np.isnan(result[i]):
                i += 1
            gap_end = i  # first non-NaN index after gap (or n)
            gap_size = gap_end - gap_start

            if gap_size < max_gap:
                # Interpolate only if we have valid boundary values
                left_val = result[gap_start - 1] if gap_start > 0 else None
                right_val = result[gap_end] if gap_end < n else None

                if left_val is not None and not np.isnan(left_val) and \
                   right_val is not None and not np.isnan(right_val):
                    # Linear interpolation between boundaries
                    for j in range(gap_start, gap_end):
                        t = (j - gap_start + 1) / (gap_size + 1)
                        result[j] = left_val + t * (right_val - left_val)
                elif left_val is not None and not np.isnan(left_val):
                    # Only left boundary — forward fill
                    for j in range(gap_start, gap_end):
                        result[j] = left_val
                elif right_val is not None and not np.isnan(right_val):
                    # Only right boundary — backward fill
                    for j in range(gap_start, gap_end):
                        result[j] = right_val
            # Gaps >= max_gap are left as NaN (no action needed)
        else:
            i += 1

    return result.tolist()


def _storage_maximum_bytes(inventory_data: dict | None) -> float | None:
    """Return the configured data-storage ceiling in bytes, if one is set.

    Only serverless caches carry an explicit limit
    (cache_usage_limits.data_storage.maximum, in GB). Node-based clusters have
    no configured ceiling — their memory headroom is already reported directly
    by DatabaseMemoryUsagePercentage — so this returns None for them.

    Args:
        inventory_data: Per-cluster inventory metadata, or None.

    Returns:
        The maximum in bytes, or None when unset/unparseable.
    """
    if not isinstance(inventory_data, dict):
        return None

    limits = inventory_data.get("cache_usage_limits")
    if not isinstance(limits, dict):
        return None

    storage = limits.get("data_storage")
    if not isinstance(storage, dict):
        return None

    maximum = storage.get("maximum")
    if not isinstance(maximum, (int, float)):
        return None

    unit = str(storage.get("unit") or "GB").upper()
    multiplier = {"GB": 1024**3, "MB": 1024**2, "KB": 1024, "BYTES": 1}.get(unit)
    if multiplier is None:
        return None

    return float(maximum) * multiplier


def _resolution_seconds(metrics_data: dict | None) -> float:
    """Read Stage 2's collection resolution, falling back to the standard 300s.

    A per-period Sum can only be converted to a per-second rate if the period is
    known, so this is read from the file rather than assumed. A run collected at
    60s whose sums were divided by 300 would understate the rate five-fold.

    Args:
        metrics_data: The whole parsed metrics.json, or None.

    Returns:
        Seconds per datapoint. DEFAULT_RESOLUTION_SECONDS when absent or
        unusable, with a warning, since a missing field is a Stage 2 change
        worth noticing rather than silently accommodating.
    """
    if isinstance(metrics_data, dict):
        raw = (metrics_data.get("metadata") or {}).get("resolution_seconds")
        if isinstance(raw, (int, float)) and raw > 0:
            return float(raw)
        if raw is not None:
            logger.warning(
                "metrics.json metadata.resolution_seconds is %r, which is not a "
                "positive number — falling back to %ss",
                raw, DEFAULT_RESOLUTION_SECONDS,
            )
            return DEFAULT_RESOLUTION_SECONDS

    logger.warning(
        "metrics.json has no metadata.resolution_seconds — assuming %ss. Any "
        "rate derived from a per-period sum is wrong if that is not the "
        "collection period.",
        DEFAULT_RESOLUTION_SECONDS,
    )
    return DEFAULT_RESOLUTION_SECONDS


def _ecpu_maximum_per_second(inventory_data: dict | None) -> float | None:
    """Return the configured ECPU-per-second ceiling, if one is set.

    Serverless caches may carry cache_usage_limits.ecpu_per_second.maximum, a
    RATE limit in ECPUs per second. Node-based clusters have no such limit, so
    this returns None for them.

    Args:
        inventory_data: Per-cluster inventory metadata, or None.

    Returns:
        The maximum in ECPUs per second, or None when unset/unparseable.
    """
    if not isinstance(inventory_data, dict):
        return None

    limits = inventory_data.get("cache_usage_limits")
    if not isinstance(limits, dict):
        return None

    ecpu = limits.get("ecpu_per_second")
    if not isinstance(ecpu, dict):
        return None

    maximum = ecpu.get("maximum")
    if not isinstance(maximum, (int, float)) or maximum <= 0:
        return None

    return float(maximum)


def _add_ecpu_percent_series(
    metrics: dict, ecpu_max_per_second: float, resolution_seconds: float
) -> None:
    """Add an ECPUUtilizationPercent series derived from ElastiCacheProcessingUnits.

    ElastiCacheProcessingUnits is collected with the Sum statistic, so each
    datapoint is the TOTAL number of ECPUs consumed during the period — a count,
    not a percentage. The configured ceiling is a per-second rate, so the
    comparable quantity is the consumed rate:

        rate = sum_over_period / resolution_seconds
        percent = rate / ecpu_max_per_second * 100

    Without this conversion the raw Sum (tens of thousands on a busy cache) is
    compared against thresholds of 50 and 80 read as percentages, so every
    serverless cache with real traffic scores High and is classified CPU-BOUND.
    Same bytes-vs-percent class of error as BytesUsedForCache.

    Mutates `metrics` in place.

    Args:
        metrics: Nested metrics dict for one serverless cache.
        ecpu_max_per_second: Configured ECPU rate ceiling (must be > 0).
        resolution_seconds: Seconds per datapoint, from Stage 2's metadata.
    """
    raw = metrics.get("ElastiCacheProcessingUnits")
    if not isinstance(raw, dict) or ecpu_max_per_second <= 0:
        return
    if resolution_seconds <= 0:
        return

    scale = 100.0 / (ecpu_max_per_second * resolution_seconds)
    derived: dict = {}
    for statistic, series in raw.items():
        # Only the Sum is a period total. An Average or Maximum datapoint is a
        # per-datapoint statistic over a different base and would not convert
        # with this scale factor.
        if statistic != "Sum":
            continue
        if not isinstance(series, dict) or "values" not in series:
            continue
        derived[statistic] = {
            "timestamps": series.get("timestamps", []),
            "values": [
                None if v is None else round(v * scale, 2)
                for v in series["values"]
            ],
        }

    if derived:
        metrics["ECPUUtilizationPercent"] = derived


def _drop_undefined_hit_rate(metrics: dict) -> None:
    """Remove CacheHitRate when the cache served no requests at all.

    CloudWatch reports a hit rate of 0 when there were zero hits AND zero
    misses. That ratio is undefined (0/0), not a 0% hit rate, so classifying it
    against the inverted CacheHitRate thresholds reports a spurious CRITICAL on
    every idle cache. The IDLE utilization finding already covers no-traffic
    clusters, so dropping the series loses no signal.

    Mutates `metrics` in place.

    Args:
        metrics: Nested metrics dict for one node or serverless cache.
    """
    if "CacheHitRate" not in metrics:
        return

    def _total(metric_name: str) -> float:
        series = metrics.get(metric_name)
        if not isinstance(series, dict):
            return 0.0
        total = 0.0
        for stat_series in series.values():
            if isinstance(stat_series, dict):
                total += sum(
                    v for v in stat_series.get("values", []) if v is not None
                )
        return total

    # Only drop when we can positively confirm zero traffic. If the hit/miss
    # counters are absent we cannot tell idle from genuinely-zero, so keep it.
    if "CacheHits" not in metrics and "CacheMisses" not in metrics:
        return

    if _total("CacheHits") == 0 and _total("CacheMisses") == 0:
        del metrics["CacheHitRate"]


def _add_storage_percent_series(metrics: dict, storage_max_bytes: float) -> None:
    """Add a BytesUsedForCachePercent series derived from BytesUsedForCache.

    Mutates `metrics` in place, adding the derived percentage alongside the raw
    byte series so thresholds can be evaluated on a comparable scale.

    Args:
        metrics: Nested metrics dict for one node or serverless cache.
        storage_max_bytes: Configured storage ceiling in bytes (must be > 0).
    """
    raw = metrics.get("BytesUsedForCache")
    if not isinstance(raw, dict) or storage_max_bytes <= 0:
        return

    derived: dict = {}
    for statistic, series in raw.items():
        if not isinstance(series, dict) or "values" not in series:
            continue
        derived[statistic] = {
            "timestamps": series.get("timestamps", []),
            "values": [
                None if v is None else round(v / storage_max_bytes * 100, 2)
                for v in series["values"]
            ],
        }

    if derived:
        metrics["BytesUsedForCachePercent"] = derived


def _add_per_minute_series(metrics: dict, resolution_seconds: float) -> None:
    """Add "<Metric>PerMinute" series for each count metric with a threshold.

    Stage 2 collects count metrics (Evictions, NewConnections, ThrottledCmds)
    with the Sum statistic over a 300-second period, so one datapoint is the
    total for FIVE minutes. Every threshold for these metrics is documented and
    registered per minute, because that is the rate an operator reasons about.
    Comparing the two directly makes each check fire at a fifth of the rate it
    claims: NewConnections' MEDIUM boundary of 1000/min triggered at about 200
    new connections per minute, recommending connection pooling to customers
    with ordinary churn.

        per_minute = sum_over_period / (resolution_seconds / 60)

    The conversion lives here, not in `classify_value`, so that every model
    reading the series sees the same unit. A divide applied at comparison time
    would leave the percentile summary reporting a 5-minute total next to a
    per-minute threshold — the same ambiguity in a new place.

    The raw Sum series is left in place: it is the collected quantity, and
    models that want a period total (eviction pressure, breach counting) should
    read it rather than multiply this back up.

    Mutates `metrics` in place.

    Args:
        metrics: Nested metrics dict for one node or serverless cache.
        resolution_seconds: Seconds per datapoint, from Stage 2's metadata.
    """
    if resolution_seconds <= 0:
        return

    minutes_per_period = resolution_seconds / 60.0

    for metric_name in ThresholdRegistry.RATE_METRICS:
        raw = metrics.get(metric_name)
        if not isinstance(raw, dict):
            continue

        derived: dict = {}
        for statistic, series in raw.items():
            # Only a Sum is a period total. An Average or Maximum datapoint is
            # already a per-datapoint statistic and does not convert this way.
            if statistic != "Sum":
                continue
            if not isinstance(series, dict) or "values" not in series:
                continue
            derived[statistic] = {
                "timestamps": series.get("timestamps", []),
                "values": [
                    None if v is None else round(v / minutes_per_period, 2)
                    for v in series["values"]
                ],
            }

        if derived:
            metrics[f"{metric_name}PerMinute"] = derived


def normalize_cluster_data(
    cluster_data: dict,
    inventory_data: dict | None = None,
    resolution_seconds: float = DEFAULT_RESOLUTION_SECONDS,
) -> dict:
    """Convert Stage 2's compact metrics format into the nested form models read.

    fetch_metrics.py emits one shared timestamp array per cluster and bare value
    lists per metric/statistic:

        {"timestamps_5min": [...], "nodes": {node: {Metric: {Stat: [values]}}}}

    Every model in this module expects each series to carry its own timestamps:

        {"nodes": {node: {"metrics": {Metric: {Stat: {"timestamps": [...],
                                                     "values": [...]}}}}}}

    This adapter rewrites the former into the latter, pairing each value list
    with the cluster-level timestamp array. Data already in the nested form is
    passed through unchanged, so both shapes are accepted.

    It also derives the series whose units the raw CloudWatch metrics cannot
    substitute for, because a threshold is only meaningful against a value in
    the same unit:

    * BytesUsedForCachePercent, when the inventory reports a configured storage
      maximum (the raw metric is in bytes).
    * ECPUUtilizationPercent, when the inventory reports a configured ECPU rate
      maximum (the raw metric is a per-period count).
    * "<Metric>PerMinute" for each of ThresholdRegistry.RATE_METRICS (the raw
      metrics are Sums over the collection period, not per-minute rates).

    Args:
        cluster_data: A single cluster's entry from metrics.json.
        inventory_data: Per-cluster inventory metadata, used to read the
                        configured storage and ECPU maximums. Optional.
        resolution_seconds: Seconds per datapoint, from Stage 2's
                        metadata.resolution_seconds. Needed to turn per-period
                        totals into per-second and per-minute rates.

    Returns:
        A new dict in the nested form. The input is not mutated.
    """
    if not isinstance(cluster_data, dict):
        return cluster_data

    timestamps = cluster_data.get("timestamps_5min") or []
    storage_max = _storage_maximum_bytes(inventory_data)
    ecpu_max = _ecpu_maximum_per_second(inventory_data)
    normalized = dict(cluster_data)

    if "nodes" in cluster_data and isinstance(cluster_data["nodes"], dict):
        nodes: dict = {}
        for node_id, node_data in cluster_data["nodes"].items():
            metrics = _normalize_metrics(node_data, timestamps)
            _drop_undefined_hit_rate(metrics)
            _add_per_minute_series(metrics, resolution_seconds)
            if storage_max:
                _add_storage_percent_series(metrics, storage_max)
            nodes[node_id] = {"metrics": metrics}
        normalized["nodes"] = nodes
    elif "metrics" in cluster_data and isinstance(cluster_data["metrics"], dict):
        metrics = _normalize_metrics(cluster_data["metrics"], timestamps)
        _drop_undefined_hit_rate(metrics)
        _add_per_minute_series(metrics, resolution_seconds)
        if storage_max:
            _add_storage_percent_series(metrics, storage_max)
        # Only the serverless shape carries ECPUs, and only it has a rate limit.
        if ecpu_max:
            _add_ecpu_percent_series(metrics, ecpu_max, resolution_seconds)
        normalized["metrics"] = metrics

    return normalized


def _normalize_metrics(metrics: dict, timestamps: list[str]) -> dict:
    """Pair bare value lists with timestamps, leaving nested series untouched.

    Args:
        metrics: Either {Metric: {Stat: [values]}} (compact) or the already
                 nested {Metric: {Stat: {"timestamps": ..., "values": ...}}}.
                 A node dict wrapping a "metrics" key is also unwrapped.
        timestamps: Cluster-level timestamp array to attach to compact series.

    Returns:
        {Metric: {Stat: {"timestamps": [...], "values": [...]}}}
    """
    if not isinstance(metrics, dict):
        return {}

    # A node entry that already wraps its series under "metrics".
    if "metrics" in metrics and isinstance(metrics["metrics"], dict):
        metrics = metrics["metrics"]

    result: dict = {}
    for metric_name, statistics in metrics.items():
        if not isinstance(statistics, dict):
            continue

        stat_out: dict = {}
        for statistic, series in statistics.items():
            if isinstance(series, list):
                # Compact form: bare value list, timestamps live on the cluster.
                # Truncate to the shorter length so zip-based consumers stay aligned.
                stat_out[statistic] = {
                    "timestamps": timestamps[: len(series)],
                    "values": series,
                }
            elif isinstance(series, dict) and "values" in series:
                # Already nested — keep as-is.
                stat_out[statistic] = series

        if stat_out:
            result[metric_name] = stat_out

    return result


def chunk_by_day(timestamps: list[str], values: list[float]) -> list[list[float]]:
    """Split time-series into per-day chunks based on ISO timestamps.

    Groups values by their calendar date (UTC). Each chunk contains all
    values that fall on the same date.

    Args:
        timestamps: List of ISO 8601 timestamp strings.
        values: Corresponding list of float values.

    Returns:
        List of per-day value lists, ordered chronologically.
    """
    if not timestamps or not values:
        return []

    day_buckets: dict[str, list[float]] = {}
    for ts, val in zip(timestamps, values):
        # Parse ISO timestamp and extract date portion
        date_str = ts[:10]  # "YYYY-MM-DD"
        if date_str not in day_buckets:
            day_buckets[date_str] = []
        day_buckets[date_str].append(val)

    # Return in chronological order
    return [day_buckets[k] for k in sorted(day_buckets.keys())]


def extract_last_n_days(
    timestamps: list[str], values: list[float], days: int
) -> list[float]:
    """Extract values from the most recent N days of the time-series.

    Determines the latest timestamp in the series and returns all values
    whose timestamps fall within the last N days (inclusive of the end date).

    Args:
        timestamps: List of ISO 8601 timestamp strings.
        values: Corresponding list of float values.
        days: Number of recent days to extract.

    Returns:
        List of values from the most recent N days.
    """
    if not timestamps or not values:
        return []

    # Find the most recent timestamp
    max_ts = max(timestamps)
    # Parse the max date
    max_date = datetime.datetime.fromisoformat(max_ts.replace("Z", "+00:00"))
    cutoff = max_date - datetime.timedelta(days=days)

    result = []
    for ts, val in zip(timestamps, values):
        parsed = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if parsed >= cutoff:
            result.append(val)

    return result


def safe_divide(
    numerator: float, denominator: float, default=None
) -> float | None:
    """Return numerator/denominator or default if denominator is zero.

    Args:
        numerator: The dividend.
        denominator: The divisor.
        default: Value to return when denominator is zero.

    Returns:
        The division result, or default when denominator is zero.
    """
    if denominator == 0:
        return default
    return numerator / denominator


def compute_nan_ratio(values: list[float]) -> float:
    """Return fraction of NaN values in the array.

    Args:
        values: List of float values (may contain NaN).

    Returns:
        Fraction of NaN values (0.0 to 1.0). Returns 0.0 for empty lists.
    """
    if not values:
        return 0.0
    arr = np.array(values, dtype=np.float64)
    nan_count = np.sum(np.isnan(arr))
    return float(nan_count / len(arr))


# ---------------------------------------------------------------------------
# Threshold Registry
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class ThresholdLevel:
    """Threshold levels for a metric, in a declared unit.

    For most metrics (CPU, Memory, Latency, etc.), thresholds represent
    upper bounds — values ABOVE the threshold are problematic.

    For inverted metrics (CacheHitRate), thresholds represent lower bounds —
    values BELOW the threshold are problematic.

    The `low` field represents an over-provisioned / under-utilized condition.

    `unit` names what the four numbers are measured in. It exists because a
    threshold is not a scalar: "1000" is a different check per minute than per
    five-minute period, and the two are indistinguishable by inspection. Every
    unit defect in this pipeline's history was a number compared against a
    series carrying a different unit, so the unit is recorded next to the
    numbers and validated at construction — see `ThresholdRegistry.UNITS`.

    `statistic` names the CloudWatch statistic these boundaries were calibrated
    against — the suffix a finding about this metric is expected to carry. It is
    not a filter: PercentileModel grades every statistic Stage 2 collected, so
    EngineCPUUtilization is classified on both its Average and its Maximum and
    the higher severity survives deduplication. What the field pins is that the
    named statistic is one the pipeline actually produces, so a boundary written
    for a peak cannot end up documented as if it graded a mean.

    `means` gives one line of interpretation per severity band. Both fields live
    here rather than in `references/thresholds.md` because that document is
    agent-facing *input* -- SKILL.md Step 3 tells the agent to read it before
    interpreting analysis.json -- so a row it carries that this registry does
    not is a wrong instruction, not a stale comment. The doc is generated from
    these fields by `scripts/generate_thresholds_doc.py`, which is why the prose
    is here: a band cannot be described in the doc unless it exists in the code,
    and a band cannot exist without being described.

    `means` is keyed by severity. A key with no corresponding number is a
    construction error rather than a silently unused string, because that is
    exactly the defect the generation exists to prevent: the committed doc
    described a LOW band for Evictions and for ECPU utilization, and an
    intermediate MEDIUM band for throttling, none of which this registry can
    produce.
    """

    critical: float | None = None  # Value above which is CRITICAL
    high: float | None = None  # Value above which is HIGH
    medium: float | None = None  # Value above which is MEDIUM
    low: float | None = None  # Value below which is LOW (over-provisioned)
    unit: str = "percent"  # What the four numbers above are measured in
    statistic: str = "Maximum"  # Which CloudWatch statistic these were set for
    means: dict[str, str] = dataclasses.field(default_factory=dict)

    #: Severity keys `means` may carry, plus the implicit band below them all.
    BANDS = ("CRITICAL", "HIGH", "MEDIUM", "LOW", "HEALTHY")

    #: Statistics Stage 2 collects. A boundary calibrated against one the
    #: collector never requests grades a series that does not exist.
    STATISTICS = ("Maximum", "Average", "Sum", "Minimum")

    def __post_init__(self):
        if self.unit not in ThresholdRegistry.UNITS:
            raise ValueError(
                f"ThresholdLevel unit {self.unit!r} is not one of "
                f"{sorted(ThresholdRegistry.UNITS)}. A threshold in an "
                "undeclared unit cannot be checked against the series it "
                "grades."
            )
        if self.statistic not in self.STATISTICS:
            raise ValueError(
                f"ThresholdLevel statistic {self.statistic!r} is not one of "
                f"{list(self.STATISTICS)}."
            )
        unknown = set(self.means) - set(self.BANDS)
        if unknown:
            raise ValueError(
                f"ThresholdLevel.means has unknown severity {sorted(unknown)}; "
                f"expected a subset of {list(self.BANDS)}."
            )
        # A described band with no number behind it is the defect this class
        # exists to prevent: the generated doc would promise a severity that
        # classify_value can never return.
        for band in ("CRITICAL", "HIGH", "MEDIUM", "LOW"):
            if band in self.means and getattr(self, band.lower()) is None:
                raise ValueError(
                    f"ThresholdLevel.means describes a {band} band but "
                    f"{band.lower()}= is None, so classify_value can never "
                    "return it. Either set the boundary or drop the text."
                )


class ThresholdRegistry:
    """Centralizes all threshold definitions for metric classification.

    Provides lookup by metric name and severity level, and classification
    of raw values into severity strings.

    Every threshold carries the unit its numbers are expressed in, and the
    series it grades must be normalized to that unit before comparison —
    `normalize_cluster_data` does that for the rate metrics. The registry does
    not convert at comparison time on purpose: a divisor applied inside
    `classify_value` would be invisible to every other model reading the same
    series, so two models would disagree about what the number means.
    """

    # Metrics where LOWER values are worse (inverted thresholds)
    INVERTED_METRICS = {"CacheHitRate"}

    # The units a threshold may be declared in.
    #
    # "per_minute" is the unit for count metrics, and it is deliberately NOT
    # the unit CloudWatch delivers. Stage 2 collects counts as a Sum over a
    # 300-second period, so a raw datapoint is a per-five-minute total. Every
    # such threshold in references/thresholds.md is written per minute, because
    # that is the rate an operator reasons about — so Stage 3 derives a
    # per-minute series rather than restating the thresholds in a unit nobody
    # uses. See RATE_METRICS.
    UNITS = {
        "percent",       # 0-100, already normalized by CloudWatch
        "per_minute",    # Count per minute, derived from a per-period Sum
        "seconds",       # ReplicationLag
        "microseconds",  # Request latency
    }

    # Count metrics whose thresholds are per minute but whose CloudWatch series
    # is a Sum over the collection period. normalize_cluster_data adds a
    # "<Metric>PerMinute" series for each, and the thresholds key on that name.
    RATE_METRICS = ("Evictions", "NewConnections", "ThrottledCmds")

    def __init__(self):
        """Register all metric thresholds from thresholds reference."""
        self._thresholds: dict[str, ThresholdLevel] = {}

        # --- Node-based metric thresholds ---

        # EngineCPUUtilization: CRITICAL >90, HIGH >70, MEDIUM >50, LOW <20
        self._thresholds["EngineCPUUtilization"] = ThresholdLevel(
            critical=90.0, high=70.0, medium=50.0, low=20.0, unit="percent",
            statistic="Maximum",
            means={
                "CRITICAL": "Engine saturated, latency spikes likely",
                "HIGH": "Approaching saturation, scale proactively",
                "MEDIUM": "Moderate load, monitor trend",
                "LOW": "Consistently idle, right-sizing opportunity",
                "HEALTHY": "Normal operating range",
            },
        )

        # DatabaseMemoryUsagePercentage: CRITICAL >90, HIGH >80, LOW <30
        self._thresholds["DatabaseMemoryUsagePercentage"] = ThresholdLevel(
            critical=90.0, high=80.0, medium=None, low=30.0, unit="percent",
            statistic="Maximum",
            means={
                "CRITICAL": "OOM imminent, evictions aggressive",
                "HIGH": "Memory pressure, evictions likely starting",
                "LOW": "Significant headroom, right-sizing opportunity",
                "HEALTHY": "Comfortable headroom",
            },
        )

        # CacheHitRate: CRITICAL <40, HIGH <60, MEDIUM <80 (inverted — lower is worse)
        self._thresholds["CacheHitRate"] = ThresholdLevel(
            critical=40.0, high=60.0, medium=80.0, low=None, unit="percent",
            statistic="Average",
            means={
                "CRITICAL": "Cache largely ineffective",
                "HIGH": "Poor effectiveness, cache may not be serving its "
                        "purpose",
                "MEDIUM": "Below optimal, investigate TTL and key design",
                "HEALTHY": "Good to excellent cache effectiveness",
            },
        )

        # ReplicationLag: CRITICAL >5s, HIGH >1s, MEDIUM >0.1s
        self._thresholds["ReplicationLag"] = ThresholdLevel(
            critical=5.0, high=1.0, medium=0.1, low=None, unit="seconds",
            statistic="Maximum",
            means={
                "CRITICAL": "Significant data loss risk on failover",
                "HIGH": "Notable staleness, failover data loss risk",
                "MEDIUM": "Minor staleness",
                "HEALTHY": "Near-realtime replication",
            },
        )

        # EvictionsPerMinute: HIGH >200/min, MEDIUM >20/min
        #
        # Keyed on the derived per-minute series for consistency with the other
        # count metrics. Unlike NewConnections, Evictions was NOT graded in the
        # wrong unit before: references/thresholds.md documented it per 5-minute
        # period ("> 1000/period") and the code compared the raw 300s Sum
        # against 1000, which agreed. The numbers here are that same check
        # restated per minute — 1000/period is 200/min, 100/period is 20/min —
        # so the severity boundaries are unchanged and only the unit moved.
        # Restating them was necessary because leaving Evictions on the raw Sum
        # would make it the one rate metric whose threshold silently depends on
        # Stage 2's collection period.
        #
        # No LOW band. The committed doc described one ("0.2-20/min ... minor
        # eviction, may be normal with volatile-* policy"), but `low` grades
        # under-utilization, and a *few* evictions is not an over-provisioned
        # cache — there is no number that band could be. classify_value returns
        # HEALTHY there, which is what the doc's own parenthetical says it is.
        self._thresholds["EvictionsPerMinute"] = ThresholdLevel(
            critical=None, high=200.0, medium=20.0, low=None,
            unit="per_minute", statistic="Sum",
            means={
                "HIGH": "Memory critically undersized",
                "MEDIUM": "Dataset outgrowing cache",
                "HEALTHY": "Little to no memory pressure (a low eviction rate "
                           "is normal under a volatile-* policy)",
            },
        )

        # SuccessfulReadRequestLatency: HIGH >10ms, MEDIUM >5ms
        self._thresholds["SuccessfulReadRequestLatency"] = ThresholdLevel(
            critical=None, high=10000.0, medium=5000.0, low=None,
            unit="microseconds", statistic="Average",
            means={
                "HIGH": "Significant latency",
                "MEDIUM": "Elevated latency",
                "HEALTHY": "Normal to excellent read performance",
            },
        )

        # NewConnectionsPerMinute: HIGH >5000/min, MEDIUM >1000/min
        #
        # Keyed on the derived per-minute series. The raw NewConnections Sum is
        # a per-five-minute total, so grading it against these numbers flagged
        # ordinary connection churn (~200/min) as MEDIUM and recommended
        # connection pooling the customer did not need.
        self._thresholds["NewConnectionsPerMinute"] = ThresholdLevel(
            critical=None, high=5000.0, medium=1000.0, low=None,
            unit="per_minute", statistic="Sum",
            means={
                "HIGH": "Connection storm, missing pooling",
                "MEDIUM": "Elevated churn",
                "HEALTHY": "Normal connection behavior",
            },
        )

        # --- Serverless metric thresholds ---

        # ThrottledCmdsPerMinute: CRITICAL >0 (any sustained throttling)
        #
        # Keyed on the derived per-minute series for consistency with the other
        # count metrics. Unlike the two above, the unit does not change this
        # check's behaviour — the boundary is zero, and dividing by five leaves
        # zero where it was. It is normalized anyway so that the number reported
        # in the finding is the rate the threshold claims to be about.
        #
        # One band, not three. The committed doc split this into CRITICAL for
        # "> 0 sustained (3+ minutes)" and MEDIUM for "sporadic (< 3 consecutive
        # minutes)", but classify_value counts no consecutive minutes: the
        # boundary is zero, so a single throttled command in a fortnight returns
        # CRITICAL and MEDIUM is unreachable.
        #
        # The sustained/sporadic split the doc described does exist in the
        # pipeline — UtilizationModel grades throttling by the fraction of
        # windows carrying any (>5% High, >0 Medium) — just not here. Reconciling
        # the two is D15; until then the doc states this band because a
        # documented band must be one classify_value can return.
        self._thresholds["ThrottledCmdsPerMinute"] = ThresholdLevel(
            critical=0.0, high=None, medium=None, low=None, unit="per_minute",
            statistic="Sum",
            means={
                "CRITICAL": "Actively losing requests — any throttling at all",
                "HEALTHY": "Within capacity",
            },
        )

        # ECPUUtilizationPercent (ECPU utilization): HIGH >80%, MEDIUM >50%
        #
        # Keyed on the derived percentage, NOT the raw ElastiCacheProcessingUnits
        # metric. That metric is collected with Sum, so a datapoint is the count
        # of ECPUs consumed during the period; comparing it against 80 and 50 as
        # percentages flags any cache consuming more than 80 ECPUs in five
        # minutes, which is nearly idle. The percentage is only defined relative
        # to a configured ECPU rate maximum, which normalize_cluster_data derives
        # when the inventory reports one.
        #
        # No LOW band, though the committed doc described one ("< 20% of max
        # sustained ... possible over-provisioning of minimum"). Utilization of
        # the *maximum* says nothing about whether the minimum is set too high;
        # the over-provisioned-serverless check belongs against
        # ecpu_per_second.minimum, which this metric is not measured against.
        self._thresholds["ECPUUtilizationPercent"] = ThresholdLevel(
            critical=None, high=80.0, medium=50.0, low=None, unit="percent",
            statistic="Sum",
            means={
                "HIGH": "Approaching the configured ECPU ceiling, throttling "
                        "risk",
                "MEDIUM": "Moderate utilization of the configured ceiling",
                "HEALTHY": "Comfortable headroom below the ceiling",
            },
        )

        # BytesUsedForCachePercent: CRITICAL >90%, HIGH >70%
        #
        # Keyed on the derived percentage, NOT the raw BytesUsedForCache metric.
        # CloudWatch emits BytesUsedForCache in bytes (see
        # references/metrics-catalog.md), so comparing it against 90/70 directly
        # flags any cluster holding >90 bytes. The percentage is only defined
        # relative to a configured storage maximum, which normalize_cluster_data
        # derives when the inventory reports one.
        self._thresholds["BytesUsedForCachePercent"] = ThresholdLevel(
            critical=90.0, high=70.0, medium=None, low=None, unit="percent",
            statistic="Maximum",
            means={
                "CRITICAL": "Storage limit imminent",
                "HIGH": "Approaching the configured storage limit",
                "HEALTHY": "Comfortable headroom",
            },
        )

    def get_threshold(self, metric_name: str) -> ThresholdLevel | None:
        """Return threshold levels for a metric, or None if not defined.

        Args:
            metric_name: The metric name (e.g., "EngineCPUUtilization").

        Returns:
            ThresholdLevel with defined severity boundaries, or None.
        """
        return self._thresholds.get(metric_name)

    def get_unit(self, metric_name: str) -> str | None:
        """Return the unit a metric's thresholds are expressed in.

        Args:
            metric_name: The metric name.

        Returns:
            One of UNITS, or None if the metric has no threshold. Callers that
            report a threshold to a human should state this alongside the
            number, since the number alone is ambiguous.
        """
        threshold = self._thresholds.get(metric_name)
        if threshold is None:
            return None
        return threshold.unit

    def get_critical(self, metric_name: str) -> float | None:
        """Return the CRITICAL threshold for a metric.

        Args:
            metric_name: The metric name.

        Returns:
            The critical threshold value, or None if not defined.
        """
        threshold = self._thresholds.get(metric_name)
        if threshold is None:
            return None
        return threshold.critical

    def classify_value(self, metric_name: str, value: float) -> str:
        """Classify a metric value into a severity string.

        For standard metrics (higher is worse): checks value against thresholds
        from most severe (CRITICAL) to least severe (LOW/over-provisioned).

        For inverted metrics like CacheHitRate (lower is worse): checks if value
        is BELOW the threshold levels.

        Args:
            metric_name: The metric name.
            value: The observed metric value.

        Returns:
            One of "CRITICAL", "HIGH", "MEDIUM", "LOW", or "HEALTHY".
        """
        threshold = self._thresholds.get(metric_name)
        if threshold is None:
            return "HEALTHY"

        if metric_name in self.INVERTED_METRICS:
            # Inverted: lower values are worse
            # CRITICAL if value < critical threshold
            if threshold.critical is not None and value < threshold.critical:
                return "CRITICAL"
            if threshold.high is not None and value < threshold.high:
                return "HIGH"
            if threshold.medium is not None and value < threshold.medium:
                return "MEDIUM"
            return "HEALTHY"
        else:
            # Standard: higher values are worse
            if threshold.critical is not None and value > threshold.critical:
                return "CRITICAL"
            if threshold.high is not None and value > threshold.high:
                return "HIGH"
            if threshold.medium is not None and value > threshold.medium:
                return "MEDIUM"
            # Check for LOW (over-provisioned) — value below the low threshold
            if threshold.low is not None and value < threshold.low:
                return "LOW"
            return "HEALTHY"


# ---------------------------------------------------------------------------
# PercentileModel
# ---------------------------------------------------------------------------


class PercentileModel:
    """Model 1: Computes distribution statistics for each metric time-series.

    For each metric time-series in the cluster:
    1. Filter NaN values, skip if < 10 valid points
    2. Compute p50, p95, p99, max via numpy.nanpercentile
    3. Compute spike_ratio = max / p95 (default 1.0 if p95 == 0)
    4. Classify spike severity: stable/notable/significant/severe

    Returns: {metric_key: {p50, p95, p99, max, spike_ratio, spike_class}}
    """

    def compute(self, cluster_data: dict, inventory_data: dict | None) -> dict:
        """Compute percentile statistics for all metric time-series in a cluster.

        Args:
            cluster_data: Per-cluster data from metrics.json containing nodes
                          and their metric time-series.
            inventory_data: Per-cluster inventory metadata (optional, not used
                           by this model but accepted for interface consistency).

        Returns:
            Dictionary keyed by '{MetricName}_{Statistic}' with values containing
            p50, p95, p99, max, spike_ratio, and spike_class for each metric.
            Metrics with insufficient data are keyed with status 'insufficient_data'.
        """
        results: dict = {}

        # Collect all metric time-series from the cluster data.
        # Node-based clusters have a "nodes" dict; serverless clusters have
        # metrics directly or under a different structure.
        all_series = self._extract_all_series(cluster_data)

        for metric_key, values in all_series.items():
            arr = np.array(values, dtype=np.float64)

            # Filter out NaN values to check valid point count
            valid_count = int(np.sum(~np.isnan(arr)))

            if valid_count < 10:
                results[metric_key] = {
                    "status": "insufficient_data",
                    "valid_points": valid_count,
                }
                logger.debug(
                    "Metric %s: insufficient data (%d valid points)",
                    metric_key,
                    valid_count,
                )
                continue

            # Compute percentiles using NaN-aware functions
            p50 = float(np.nanpercentile(arr, 50))
            p95 = float(np.nanpercentile(arr, 95))
            p99 = float(np.nanpercentile(arr, 99))
            max_val = float(np.nanmax(arr))

            # Compute spike_ratio: max / p95, default 1.0 when p95 == 0
            if p95 == 0:
                spike_ratio = 1.0
            else:
                spike_ratio = max_val / p95

            # Classify spike severity
            spike_class = self._classify_spike(spike_ratio)

            results[metric_key] = {
                "p50": round(p50, 2),
                "p95": round(p95, 2),
                "p99": round(p99, 2),
                "max": round(max_val, 2),
                "spike_ratio": round(spike_ratio, 2),
                "spike_class": spike_class,
            }

        return results

    def _extract_all_series(self, cluster_data: dict) -> dict[str, list[float]]:
        """Extract all metric time-series from cluster data.

        Handles both node-based clusters (with 'nodes' key containing
        per-node metrics) and serverless clusters (with metrics at the
        top level or under 'metrics' key directly).

        For node-based clusters, we aggregate values from all nodes into
        a single combined time-series per metric/statistic combination.

        Args:
            cluster_data: The cluster's data from the metrics file.

        Returns:
            Dictionary keyed by '{MetricName}_{Statistic}' with lists of
            float values representing the combined time-series.
        """
        all_series: dict[str, list[float]] = {}

        if "nodes" in cluster_data:
            # Node-based cluster: iterate each node's metrics
            for node_id, node_data in cluster_data["nodes"].items():
                metrics = node_data.get("metrics", {})
                self._collect_metrics(metrics, all_series)
        elif "metrics" in cluster_data:
            # Serverless or flat structure: metrics at cluster level
            metrics = cluster_data["metrics"]
            self._collect_metrics(metrics, all_series)

        return all_series

    def _collect_metrics(
        self, metrics: dict, all_series: dict[str, list[float]]
    ) -> None:
        """Collect metric values from a metrics dictionary into all_series.

        Args:
            metrics: Dictionary of metric_name -> statistic -> {timestamps, values}.
            all_series: Accumulator dictionary to extend with values.
        """
        for metric_name, statistics in metrics.items():
            for statistic, series_data in statistics.items():
                metric_key = f"{metric_name}_{statistic}"
                values = series_data.get("values", [])

                if metric_key not in all_series:
                    all_series[metric_key] = []
                all_series[metric_key].extend(values)

    @staticmethod
    def _classify_spike(spike_ratio: float) -> str:
        """Classify spike severity based on spike_ratio value.

        Args:
            spike_ratio: The ratio of max / p95.

        Returns:
            One of: 'stable', 'notable', 'significant', 'severe'.
        """
        if spike_ratio < 2:
            return "stable"
        elif spike_ratio <= 3:
            return "notable"
        elif spike_ratio <= 5:
            return "significant"
        else:
            return "severe"


# ---------------------------------------------------------------------------
# WorkloadModel
# ---------------------------------------------------------------------------


class WorkloadModel:
    """Classifies workload type from command-mix distribution.

    Analyzes the 14-day Sum totals of command family metrics to determine
    the dominant workload pattern (cache-aside, session-store, leaderboard,
    etc.) and compute the read/write ratio.
    """

    # Command family metrics to extract from cluster data
    COMMAND_FAMILIES = [
        "StringBasedCmds",
        "HashBasedCmds",
        "SortedSetBasedCmds",
        "ListBasedCmds",
        "SetBasedCmds",
        "StreamBasedCmds",
        "PubSubBasedCmds",
    ]

    def _sum_metric_across_nodes(
        self, cluster_data: dict, metric_name: str
    ) -> float:
        """Sum all values in the Sum statistic time-series across all nodes.

        Args:
            cluster_data: The cluster data dict containing nodes and metrics.
            metric_name: The metric name to sum (e.g., "StringBasedCmds").

        Returns:
            Total sum of all values across all nodes. Returns 0.0 if the
            metric is unavailable.
        """
        total = 0.0
        nodes = cluster_data.get("nodes", {})
        for node_id, node_data in nodes.items():
            metrics = node_data.get("metrics", {})
            metric = metrics.get(metric_name, {})
            sum_stat = metric.get("Sum", {})
            values = sum_stat.get("values", [])
            for v in values:
                if v is not None and not math.isnan(v):
                    total += v
        return total

    def compute(self, cluster_data: dict, inventory_data: dict | None) -> dict:
        """Classify workload based on command-mix distribution.

        1. Sum 14-day totals for each command family metric across all nodes
        2. Compute percentage distribution
        3. Compute R/W ratio (GetTypeCmds / max(SetTypeCmds, 1))
        4. Apply ordered classification rules (first match wins)

        Args:
            cluster_data: Cluster data dict with nodes containing metrics.
            inventory_data: Optional inventory data (not used by this model).

        Returns:
            Dict with keys: workload_class, command_profile, read_write_ratio,
            dominant_family.
        """
        # 5.1: Extract 14-day Sum totals for each command family
        family_totals: dict[str, float] = {}
        for family in self.COMMAND_FAMILIES:
            family_totals[family] = self._sum_metric_across_nodes(
                cluster_data, family
            )

        # 5.6: Handle all-zero or unavailable command data
        total_cmds = sum(family_totals.values())
        if total_cmds == 0:
            return {
                "workload_class": "unknown",
                "command_profile": {},
                "read_write_ratio": 0.0,
                "dominant_family": "none",
            }

        # 5.2: Compute percentage distribution
        command_profile: dict[str, float] = {}
        for family, count in family_totals.items():
            command_profile[family] = round((count / total_cmds) * 100, 2)

        # 5.3: Compute read_write_ratio as GetTypeCmds / max(SetTypeCmds, 1)
        get_cmds = self._sum_metric_across_nodes(cluster_data, "GetTypeCmds")
        set_cmds = self._sum_metric_across_nodes(cluster_data, "SetTypeCmds")
        read_write_ratio = round(get_cmds / max(set_cmds, 1.0), 2)

        # Determine dominant family (highest percentage)
        dominant_family = max(family_totals, key=family_totals.get)

        # Convenience percentages for classification logic
        string_pct = command_profile.get("StringBasedCmds", 0.0)
        hash_pct = command_profile.get("HashBasedCmds", 0.0)
        sorted_set_pct = command_profile.get("SortedSetBasedCmds", 0.0)
        list_pct = command_profile.get("ListBasedCmds", 0.0)
        stream_pct = command_profile.get("StreamBasedCmds", 0.0)
        pubsub_pct = command_profile.get("PubSubBasedCmds", 0.0)

        # 5.4: Apply ordered classification rules (first match wins)
        workload_class = "general-purpose"  # fallback

        if string_pct > 60 and read_write_ratio > 4:
            workload_class = "cache-aside"
        elif hash_pct > 35 and 1 <= read_write_ratio <= 4:
            workload_class = "session-store"
        elif sorted_set_pct > 25:
            workload_class = "leaderboard"
        elif string_pct > 60 and read_write_ratio < 2:
            workload_class = "rate-limiter"
        elif stream_pct > 15:
            workload_class = "event-stream"
        elif pubsub_pct > 10:
            workload_class = "real-time-messaging"
        elif list_pct > 20:
            workload_class = "queue"

        # 5.5: Return result
        return {
            "workload_class": workload_class,
            "command_profile": command_profile,
            "read_write_ratio": read_write_ratio,
            "dominant_family": dominant_family,
        }


# ---------------------------------------------------------------------------
# TrendModel
# ---------------------------------------------------------------------------


class TrendModel:
    """Model 2: Identifies meaningful trends over the 14-day window.

    Uses linear regression on daily averages with R² gating to detect
    metrics that are growing or declining, signaling capacity needs or
    degradation.
    """

    TRENDABLE_METRICS = [
        "DatabaseMemoryUsagePercentage",
        "BytesUsedForCache",
        "CurrItems",
        "CurrConnections",
        "Evictions",
        "CacheHitRate",
        "ReplicationLag",
    ]

    # Thresholds for operationally meaningful slopes (per week)
    MEANINGFUL_SLOPE_THRESHOLDS = {
        "DatabaseMemoryUsagePercentage": 2.0,  # >2%/wk
        "BytesUsedForCache": 0.0,  # any positive trend (absolute bytes)
        "CurrItems": 0.0,  # any positive trend
        "CurrConnections": 0.0,  # any positive trend
        "Evictions": 0.0,  # any positive trend
        "CacheHitRate": 5.0,  # declining >5%/wk
        "ReplicationLag": 0.0,  # any positive trend
    }

    def compute(self, cluster_data: dict, inventory_data: dict | None) -> dict:
        """Compute trend analysis for trendable metrics.

        For each trendable metric:
        1. Aggregate into daily averages (14 points)
        2. Skip if < 7 valid days
        3. Apply numpy.polyfit(degree=1) on daily averages
        4. Compute R² = 1 - (SS_res / SS_tot)
        5. Only report if R² > 0.4 AND slope is meaningful

        Args:
            cluster_data: Per-cluster data from metrics.json.
            inventory_data: Per-cluster inventory metadata (optional).

        Returns:
            Dictionary keyed by metric name with slope_per_day,
            slope_per_week, r_squared, direction, and is_finding.
        """
        results: dict = {}

        for metric_name in self.TRENDABLE_METRICS:
            # Extract timestamps and values for this metric across all nodes
            timestamps, values = self._extract_metric_series(
                cluster_data, metric_name
            )

            if not timestamps or not values:
                results[metric_name] = {"status": "insufficient_data"}
                continue

            # Aggregate into daily averages using chunk_by_day
            daily_chunks = chunk_by_day(timestamps, values)

            # Compute daily averages, filtering NaN
            daily_avgs = []
            for chunk in daily_chunks:
                arr = np.array(chunk, dtype=np.float64)
                valid = arr[~np.isnan(arr)]
                if len(valid) > 0:
                    daily_avgs.append(float(np.mean(valid)))

            # Skip if fewer than 7 valid daily averages
            if len(daily_avgs) < 7:
                results[metric_name] = {
                    "status": "insufficient_data",
                    "valid_days": len(daily_avgs),
                }
                continue

            # Apply linear regression
            x = np.arange(len(daily_avgs), dtype=np.float64)
            y = np.array(daily_avgs, dtype=np.float64)

            coeffs = np.polyfit(x, y, 1)
            slope_per_day = float(coeffs[0])
            slope_per_week = slope_per_day * 7

            # Compute R²
            predicted = np.polyval(coeffs, x)
            ss_res = float(np.sum((y - predicted) ** 2))
            ss_tot = float(np.sum((y - np.mean(y)) ** 2))
            r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

            # Classify direction
            if r_squared > 0.4 and slope_per_day > 0:
                direction = "rising"
            elif r_squared > 0.4 and slope_per_day < 0:
                direction = "declining"
            else:
                direction = "stable"

            # Determine if this is an operationally meaningful finding
            is_finding = self._is_meaningful(
                metric_name, slope_per_week, r_squared, direction
            )

            results[metric_name] = {
                "slope_per_day": round(slope_per_day, 4),
                "slope_per_week": round(slope_per_week, 2),
                "r_squared": round(r_squared, 4),
                "direction": direction,
                "is_finding": is_finding,
            }

        return results

    def _extract_metric_series(
        self, cluster_data: dict, metric_name: str
    ) -> tuple[list[str], list[float]]:
        """Extract combined timestamps and values for a metric across nodes.

        Uses the primary statistic for each metric (Average for rates,
        Maximum for peaks, Sum for counts).

        Args:
            cluster_data: Cluster data dict.
            metric_name: The metric name to extract.

        Returns:
            Tuple of (timestamps, values) lists. Empty lists if unavailable.
        """
        # Determine the preferred statistic for this metric
        stat_preference = ["Average", "Maximum", "Sum"]

        all_timestamps: list[str] = []
        all_values: list[float] = []

        if "nodes" in cluster_data:
            for node_id, node_data in cluster_data["nodes"].items():
                metrics = node_data.get("metrics", {})
                metric = metrics.get(metric_name, {})

                # Try statistics in preference order
                for stat in stat_preference:
                    if stat in metric:
                        series = metric[stat]
                        ts = series.get("timestamps", [])
                        vals = series.get("values", [])
                        all_timestamps.extend(ts)
                        all_values.extend(vals)
                        break
        elif "metrics" in cluster_data:
            metrics = cluster_data["metrics"]
            metric = metrics.get(metric_name, {})
            for stat in stat_preference:
                if stat in metric:
                    series = metric[stat]
                    all_timestamps = series.get("timestamps", [])
                    all_values = series.get("values", [])
                    break

        return all_timestamps, all_values

    def _is_meaningful(
        self,
        metric_name: str,
        slope_per_week: float,
        r_squared: float,
        direction: str,
    ) -> bool:
        """Determine if a trend is operationally meaningful.

        Args:
            metric_name: The metric being evaluated.
            slope_per_week: Slope in units per week.
            r_squared: R² confidence value.
            direction: "rising", "declining", or "stable".

        Returns:
            True if trend should be reported as a finding.
        """
        if r_squared <= 0.4:
            return False

        threshold = self.MEANINGFUL_SLOPE_THRESHOLDS.get(metric_name, 0.0)

        # CacheHitRate is special: declining is bad
        if metric_name == "CacheHitRate":
            return direction == "declining" and abs(slope_per_week) > threshold

        # For all other metrics, rising is concerning
        return direction == "rising" and abs(slope_per_week) > threshold


# ---------------------------------------------------------------------------
# UtilizationModel
# ---------------------------------------------------------------------------


class UtilizationModel:
    """Model 3: Classifies cluster provisioning state on a 3-axis spectrum.

    Computes CPU, Memory, and Network utilization over the most recent 7 days
    and classifies the cluster into one of: IDLE, OVER-PROVISIONED, CPU-BOUND,
    MEMORY-BOUND, NETWORK-BOUND, BALANCED, HEAVY, or SATURATED.
    """

    # Classification recommendations
    RECOMMENDATIONS = {
        "IDLE": "Decommission or consolidate",
        "OVER-PROVISIONED": "Scale down node type",
        "CPU-BOUND": "Scale up node type or add shards",
        "MEMORY-BOUND": "Scale up to larger memory node",
        "NETWORK-BOUND": "Scale to larger instance with more bandwidth",
        "BALANCED": "Well-sized, no immediate action needed",
        "HEAVY": "Monitor closely, plan scaling",
        "SATURATED": "Immediate scaling needed",
    }

    def compute(self, cluster_data: dict, inventory_data: dict | None) -> dict:
        """Compute 3-axis utilization classification.

        1. Compute CPU axis: p95 of EngineCPUUtilization (Max) over last 7 days
        2. Compute Memory axis: max of DatabaseMemoryUsagePercentage over last 7 days
        3. Compute Network axis: p95 of NetworkBaseline% (busier of In/Out) over
           the last 7 days
        4. Map each axis to Low/Medium/High
        5. Classify combination

        Args:
            cluster_data: Per-cluster data from metrics.json.
            inventory_data: Per-cluster inventory metadata (optional).

        Returns:
            Dict with cpu_level, memory_level, network_level, classification,
            recommendation, and utilization_scores.
        """
        # Check if this is a serverless cluster
        is_serverless = self._is_serverless(cluster_data, inventory_data)

        if is_serverless:
            return self._compute_serverless(cluster_data, inventory_data)

        return self._compute_node_based(cluster_data, inventory_data)

    def _compute_node_based(
        self, cluster_data: dict, inventory_data: dict | None
    ) -> dict:
        """Compute utilization for node-based clusters.

        Args:
            cluster_data: Cluster data dict.
            inventory_data: Inventory metadata.

        Returns:
            Utilization result dictionary.
        """
        # Extract last 7 days of data for each axis
        cpu_values = self._extract_last_7_days(
            cluster_data, "EngineCPUUtilization", "Maximum"
        )
        # "Maximum", not "Average": fetch_metrics.py collects
        # DatabaseMemoryUsagePercentage with Maximum only (see its
        # MetricDefinition), so asking for Average silently returned an empty
        # list and every cluster scored memory_max 0.0 -- which classified a
        # cluster at 87% memory as IDLE. The score is documented as a maximum
        # anyway, so Maximum is both what exists and what was intended.
        memory_values = self._extract_last_7_days(
            cluster_data, "DatabaseMemoryUsagePercentage", "Maximum"
        )
        network_in = self._extract_last_7_days(
            cluster_data, "NetworkBaselineUsageInPercentage", "Average"
        )
        network_out = self._extract_last_7_days(
            cluster_data, "NetworkBaselineUsageOutPercentage", "Average"
        )

        # Compute axis scores
        cpu_score = self._compute_cpu_score(cpu_values)
        memory_score = self._compute_memory_score(memory_values)
        network_score = self._compute_network_score(network_in, network_out)

        # Map to levels
        cpu_level = self._classify_cpu_level(cpu_score)
        memory_level = self._classify_memory_level(memory_score)
        network_level = self._classify_network_level(network_score)

        # Classify combination. IDLE is gated on traffic, so a lightly-loaded
        # cache that is actually serving requests is OVER-PROVISIONED (right-size)
        # rather than IDLE (decommission).
        classification = self._classify_combination(
            cpu_level, memory_level, network_level,
            has_traffic=self._has_traffic(cluster_data),
        )
        recommendation = self.RECOMMENDATIONS.get(classification, "")

        return {
            "cpu_level": cpu_level,
            "memory_level": memory_level,
            "network_level": network_level,
            "classification": classification,
            "recommendation": recommendation,
            # Each key names the statistic it holds, because "the network
            # number" is ambiguous and was wrong for a release: network_max held
            # a 7-day maximum that a single burst could set, and the axis is
            # graded on p95 now. A key that says p95 cannot quietly become a max.
            "utilization_scores": {
                "cpu_p95": round(cpu_score, 2),
                "memory_max": round(memory_score, 2),
                "network_p95": round(network_score, 2),
            },
        }

    def _compute_serverless(
        self, cluster_data: dict, inventory_data: dict | None
    ) -> dict:
        """Compute utilization for serverless clusters.

        Substitutes ECPU utilization, storage utilization, and throttling
        presence for the three axes.

        Args:
            cluster_data: Cluster data dict.
            inventory_data: Inventory metadata.

        Returns:
            Utilization result dictionary.
        """
        # ECPU utilization as CPU axis.
        #
        # ECPUUtilizationPercent, not ElastiCacheProcessingUnits: the raw metric
        # is collected with Sum, so a datapoint is the COUNT of ECPUs consumed in
        # the period -- tens of thousands on a busy cache. Feeding that to
        # _classify_cpu_level, whose bands are 20 and 70 as percentages, made
        # every serverless cache with real traffic score High and land on
        # CPU-BOUND ("Scale up node type or add shards", which is not even an
        # action available on serverless). The derived percentage is added by
        # normalize_cluster_data when the cache has a configured ECPU rate
        # maximum. Same count-vs-percent class as BytesUsedForCache.
        #
        # "Sum" here is the statistic of the SOURCE series the percentage was
        # derived from, preserved by _add_ecpu_percent_series.
        ecpu_values = self._extract_last_7_days(
            cluster_data, "ECPUUtilizationPercent", "Sum"
        )
        # Storage utilization as Memory axis.
        #
        # BytesUsedForCachePercent, not BytesUsedForCache: the raw metric is in
        # BYTES, and feeding it to _classify_memory_level -- which compares
        # against 30 and 70 as percentages -- makes any cache holding more than
        # 70 bytes score "High". The derived percent series is added by
        # normalize_cluster_data when the cache has a configured data_storage
        # maximum. Same bytes-vs-percent confusion that produced a false
        # CRITICAL earlier in this pipeline's history.
        #
        # "Maximum", not "Average", for the same reason as the node path: that
        # is the statistic actually collected.
        storage_values = self._extract_last_7_days(
            cluster_data, "BytesUsedForCachePercent", "Maximum"
        )
        # Throttling as Network axis
        throttle_values = self._extract_last_7_days(
            cluster_data, "ThrottledCmds", "Sum"
        )

        # Compute scores.
        #
        # As with storage below: a cache with no configured ecpu_per_second
        # maximum has no percentage to measure against, so the axis is Unknown
        # rather than 0.0/"Low". A default-limits serverless cache is the common
        # case, and reporting it as OVER-PROVISIONED on an unmeasured axis is the
        # bug this avoids.
        if ecpu_values:
            cpu_score = self._compute_cpu_score(ecpu_values)
            cpu_level_override = None
        else:
            cpu_score = None
            cpu_level_override = "Unknown"

        # A serverless cache with no configured data_storage maximum has no
        # percentage to measure against, so normalize_cluster_data derives no
        # BytesUsedForCachePercent series and storage_values is empty.
        #
        # _compute_memory_score would return 0.0 there, which _classify_memory_
        # level reads as "Low" -- asserting ample headroom on a cache whose
        # headroom was never measured. Unmeasured is recorded as "Unknown"
        # instead, and _classify_combination treats it as not-Low so a cache
        # cannot be called IDLE or OVER-PROVISIONED on the strength of a missing
        # metric.
        if storage_values:
            memory_score = self._compute_memory_score(storage_values)
            memory_level_override = None
        else:
            memory_score = None
            memory_level_override = "Unknown"

        # Throttling: presence of any sustained throttling is High
        if throttle_values:
            arr = np.array(throttle_values, dtype=np.float64)
            valid = arr[~np.isnan(arr)]
            # Check if sustained throttling (>5% of windows have throttling)
            if len(valid) > 0:
                throttle_ratio = float(np.sum(valid > 0)) / len(valid)
                if throttle_ratio > 0.05:
                    network_score = 90.0  # High
                elif throttle_ratio > 0:
                    network_score = 50.0  # Medium
                else:
                    network_score = 0.0  # Low
            else:
                network_score = 0.0
        else:
            network_score = 0.0

        cpu_level = (cpu_level_override
                     or self._classify_cpu_level(cpu_score))
        memory_level = (memory_level_override
                        or self._classify_memory_level(memory_score))
        network_level = self._classify_network_level(network_score)

        # A serverless cache with Unknown axes and no traffic is IDLE, not
        # BALANCED: BALANCED reads as "well-sized, no action", which is wrong for
        # a cache serving nothing while still billing its storage minimum.
        classification = self._classify_combination(
            cpu_level, memory_level, network_level,
            has_traffic=self._has_traffic(cluster_data),
        )
        recommendation = self.RECOMMENDATIONS.get(classification, "")

        return {
            "cpu_level": cpu_level,
            "memory_level": memory_level,
            "network_level": network_level,
            "classification": classification,
            "recommendation": recommendation,
            "utilization_scores": {
                # None, not 0.0, when the corresponding ceiling is unconfigured.
                # A report reading these must show "not measured" rather than a
                # figure, which is why they are null and not zero.
                "cpu_p95": (None if cpu_score is None
                            else round(cpu_score, 2)),
                "memory_max": (None if memory_score is None
                               else round(memory_score, 2)),
                # Not a percentile and not a percentage of anything: the
                # serverless network axis is a throttling proxy (90/50/0 by the
                # share of windows with any ThrottledCmds), so it is named for
                # what it is rather than borrowing the node-based key.
                "network_throttle_score": round(network_score, 2),
            },
        }

    # Counters that prove the cluster served requests. CacheHits/CacheMisses are
    # tier-1 on every cluster type; the command and ECPU sums are fallbacks.
    _TRAFFIC_COUNTERS = ("CacheHits", "CacheMisses", "GetTypeCmds",
                         "SetTypeCmds", "ElastiCacheProcessingUnits")

    def _has_traffic(self, cluster_data: dict) -> bool:
        """Whether the cluster served any traffic over the window.

        A cluster can sit at very low CPU and memory while actively serving
        cache traffic -- a light rate-limiter is the canonical case. Resource
        levels alone cannot tell that apart from an unused cache, so the IDLE
        verdict (which recommends *decommission*) must rest on traffic, not on
        the axes. Returns True unless the traffic counters are present AND sum to
        zero: absent counters cannot positively confirm idleness, so we do not
        claim it -- the same discipline ``_drop_undefined_hit_rate`` applies to
        the hit-rate ratio.

        Args:
            cluster_data: Per-cluster data (node-based or serverless).

        Returns:
            True if traffic was observed or cannot be ruled out; False only when
            the counters were collected and every one summed to zero.
        """
        present: list[float] = []
        for name in self._TRAFFIC_COUNTERS:
            present.extend(
                v for v in self._extract_last_7_days(cluster_data, name, "Sum")
                if v is not None
            )
        if not present:
            return True
        return sum(present) > 0

    def _extract_last_7_days(
        self, cluster_data: dict, metric_name: str, statistic: str
    ) -> list[float]:
        """Extract values from the last 7 days for a specific metric/statistic.

        Args:
            cluster_data: Cluster data dict.
            metric_name: The metric name.
            statistic: The statistic (Maximum, Average, Sum).

        Returns:
            List of values from the last 7 days.
        """
        all_timestamps: list[str] = []
        all_values: list[float] = []

        if "nodes" in cluster_data:
            for node_id, node_data in cluster_data["nodes"].items():
                metrics = node_data.get("metrics", {})
                metric = metrics.get(metric_name, {})
                stat_data = metric.get(statistic, {})
                ts = stat_data.get("timestamps", [])
                vals = stat_data.get("values", [])
                all_timestamps.extend(ts)
                all_values.extend(vals)
        elif "metrics" in cluster_data:
            metrics = cluster_data["metrics"]
            metric = metrics.get(metric_name, {})
            stat_data = metric.get(statistic, {})
            all_timestamps = stat_data.get("timestamps", [])
            all_values = stat_data.get("values", [])

        if not all_timestamps or not all_values:
            return []

        return extract_last_n_days(all_timestamps, all_values, 7)

    @staticmethod
    def _compute_cpu_score(values: list[float]) -> float:
        """Compute CPU axis score as p95 of values.

        Args:
            values: CPU utilization values from last 7 days.

        Returns:
            p95 value, or 0.0 if no valid data.
        """
        if not values:
            return 0.0
        arr = np.array(values, dtype=np.float64)
        valid = arr[~np.isnan(arr)]
        if len(valid) == 0:
            return 0.0
        return float(np.nanpercentile(arr, 95))

    @staticmethod
    def _compute_memory_score(values: list[float]) -> float:
        """Compute Memory axis score as maximum of values.

        Args:
            values: Memory usage values from last 7 days.

        Returns:
            Maximum value, or 0.0 if no valid data.
        """
        if not values:
            return 0.0
        arr = np.array(values, dtype=np.float64)
        valid = arr[~np.isnan(arr)]
        if len(valid) == 0:
            return 0.0
        return float(np.nanmax(arr))

    @staticmethod
    def _compute_network_score(
        in_values: list[float], out_values: list[float]
    ) -> float:
        """Compute Network axis score as p95 of In or Out, whichever is busier.

        p95, not maximum. The bands this feeds (`_classify_network_level`) put
        anything above 80% in "High", and `_classify_combination` treats
        Network=High as NETWORK-BOUND regardless of CPU and memory. Keyed on the
        maximum over seven days, that meant one five-minute burst above 80%
        relabelled an otherwise idle cluster as network-bound and recommended
        scaling it — the same single-datapoint defect as
        `_compute_network_burst_risk`, but with a louder consequence, since the
        classification is what a reader acts on first.

        The CPU axis has always used p95 for this reason; this brings the network
        axis in line. The memory axis stays on maximum deliberately: memory does
        not burst and recover the way traffic does, so its peak is the
        operationally meaningful number.

        Args:
            in_values: NetworkBaselineUsageInPercentage values.
            out_values: NetworkBaselineUsageOutPercentage values.

        Returns:
            p95 of the busier direction, or 0.0 if no valid data.
        """
        def _p95(values: list[float]) -> float:
            if not values:
                return 0.0
            arr = np.array(values, dtype=np.float64)
            if len(arr[~np.isnan(arr)]) == 0:
                return 0.0
            return float(np.nanpercentile(arr, 95))

        return max(_p95(in_values), _p95(out_values))

    @staticmethod
    def _classify_cpu_level(score: float) -> str:
        """Map CPU score to level: Low (<20%), Medium (20-70%), High (>70%).

        Args:
            score: CPU p95 percentage.

        Returns:
            "Low", "Medium", or "High".
        """
        if score < 20:
            return "Low"
        elif score <= 70:
            return "Medium"
        else:
            return "High"

    @staticmethod
    def _classify_memory_level(score: float) -> str:
        """Map Memory score to level: Low (<30%), Medium (30-70%), High (>70%).

        Args:
            score: Memory max percentage.

        Returns:
            "Low", "Medium", or "High".
        """
        if score < 30:
            return "Low"
        elif score <= 70:
            return "Medium"
        else:
            return "High"

    @staticmethod
    def _classify_network_level(score: float) -> str:
        """Map Network score to level: Low (<40%), Medium (40-80%), High (>80%).

        Args:
            score: Network score -- the p95 of the busier direction's baseline
                usage percentage, not its maximum. The bands below are only
                meaningful against a sustained level: a burst allowance exists
                to put single datapoints above 80%, so grading a maximum here
                made "High" mean "spiked once in 14 days".

        Returns:
            "Low", "Medium", or "High".
        """
        if score < 40:
            return "Low"
        elif score <= 80:
            return "Medium"
        else:
            return "High"

    @staticmethod
    def _classify_combination(
        cpu_level: str, memory_level: str, network_level: str,
        has_traffic: bool = True,
    ) -> str:
        """Classify cluster based on combination of axis levels.

        Classification rules (evaluated in order):
        - SATURATED: CPU=High AND Memory=High
        - IDLE: no traffic served in the window (regardless of axis levels)
        - OVER-PROVISIONED: serving traffic, CPU=Low AND Memory=Low AND Network≠High
        - CPU-BOUND: CPU=High, Memory≠High
        - MEMORY-BOUND: Memory=High, CPU≠High
        - NETWORK-BOUND: Network=High
        - HEAVY: two axes High
        - BALANCED: all Medium or remaining combinations

        IDLE rests on *traffic*, not on the axes. A cluster can sit at low CPU
        and memory while actively serving requests (a light rate-limiter), and
        calling that IDLE recommended decommissioning a cache in use. So a
        low-resource cluster that is *serving traffic* is OVER-PROVISIONED (a
        right-sizing candidate), and only one that served no traffic is IDLE
        (a decommission candidate). ``has_traffic`` defaults True, so a caller
        that cannot establish traffic never triggers a decommission verdict.

        An axis may also be "Unknown" when the metric backing it was never
        collected: on a serverless cache, the memory axis with no configured
        data_storage maximum, and the CPU axis with no configured
        ecpu_per_second maximum. "Unknown" is deliberately neither Low nor High,
        so it satisfies no rule that would claim spare capacity (OVER-PROVISIONED)
        or pressure (SATURATED, CPU-BOUND, MEMORY-BOUND). A serverless cache with
        Unknown axes that served no traffic is IDLE (not BALANCED, which reads as
        "well-sized"); one that served traffic falls through to BALANCED.

        Args:
            cpu_level: "Low", "Medium", "High", or "Unknown".
            memory_level: "Low", "Medium", "High", or "Unknown".
            network_level: "Low", "Medium", or "High".
            has_traffic: Whether the cluster served any traffic; only a
                confirmed-no-traffic cluster is classified IDLE.

        Returns:
            Classification string.
        """
        # SATURATED: CPU=High AND Memory=High. Checked before the traffic gate
        # so a pathological loaded-but-not-serving state is still surfaced.
        if cpu_level == "High" and memory_level == "High":
            return "SATURATED"

        # IDLE: served no traffic. This is the only path to IDLE now -- axis
        # levels alone never imply it -- so 'decommission' rests on the cluster
        # being unused, not merely lightly loaded. Covers a node-based cache at
        # all-Low with zero hits, and a serverless cache with Unknown axes and
        # no ECPU/hit activity (which used to read as BALANCED / "well-sized").
        if not has_traffic:
            return "IDLE"

        # OVER-PROVISIONED: serving traffic on a lightly-loaded node -- a
        # right-sizing candidate, not a decommission one. Network not saturated:
        # baseline bandwidth is a property of the node type, so a cluster idle on
        # compute and memory while sustaining >80% of its bandwidth allowance is
        # network-bound on a node chosen for its bandwidth, not over-provisioned.
        # Returning OVER-PROVISIONED there recommended "Scale down node type",
        # cutting the very allowance the cluster is exhausting.
        if cpu_level == "Low" and memory_level == "Low" and network_level != "High":
            return "OVER-PROVISIONED"

        # NETWORK-BOUND: Network=High
        if network_level == "High":
            # Check if it's HEAVY (two High)
            high_count = sum(
                1 for lvl in [cpu_level, memory_level, network_level]
                if lvl == "High"
            )
            if high_count >= 2:
                return "HEAVY"
            return "NETWORK-BOUND"

        # CPU-BOUND: CPU=High, Memory≠High
        if cpu_level == "High" and memory_level != "High":
            return "CPU-BOUND"

        # MEMORY-BOUND: Memory=High, CPU≠High
        if memory_level == "High" and cpu_level != "High":
            return "MEMORY-BOUND"

        # BALANCED: all Medium or remaining Medium/Low combinations
        if cpu_level == "Medium" and memory_level == "Medium":
            return "BALANCED"

        return "BALANCED"

    @staticmethod
    def _is_serverless(
        cluster_data: dict, inventory_data: dict | None
    ) -> bool:
        """Determine if a cluster is serverless.

        Checks inventory_data for serverless indicators or cluster_data
        structure (no 'nodes' key, has 'metrics' directly).

        Args:
            cluster_data: Cluster data dict.
            inventory_data: Inventory metadata.

        Returns:
            True if serverless cluster.
        """
        if inventory_data:
            if inventory_data.get("serverless", False):
                return True
            if inventory_data.get("cache_type") == "serverless":
                return True

        # Heuristic: if no 'nodes' key but has 'metrics' directly
        if "nodes" not in cluster_data and "metrics" in cluster_data:
            return True

        return False


# ---------------------------------------------------------------------------
# BreachModel
# ---------------------------------------------------------------------------


class BreachModel:
    """Model 4: Identifies threshold violations and characterizes severity.

    Computes spike ratio, breach duration, incident clustering, and
    currently-breaching detection for each metric with defined CRITICAL
    thresholds.
    """

    def __init__(self, threshold_registry: ThresholdRegistry):
        """Initialize with reference to threshold registry.

        Args:
            threshold_registry: The shared ThresholdRegistry instance.
        """
        self._registry = threshold_registry

    def compute(self, cluster_data: dict, inventory_data: dict | None) -> dict:
        """Compute breach analysis for metrics with CRITICAL thresholds.

        For each metric with a defined CRITICAL threshold:
        1. Compute spike_ratio (max / p95)
        2. Count breach windows (value > CRITICAL threshold)
        3. Cluster consecutive breaches into incidents (top 3 by duration)
        4. Check currently_breaching (p95 of last 288 points > threshold)
        5. Classify severity

        Args:
            cluster_data: Per-cluster data from metrics.json.
            inventory_data: Per-cluster inventory metadata (optional).

        Returns:
            Dictionary keyed by metric name with breach analysis results.
        """
        results: dict = {}

        # Metrics with CRITICAL thresholds to evaluate. Each name must be the
        # one the threshold is registered under, so the series and the boundary
        # carry the same unit — hence the Percent and PerMinute suffixes.
        metrics_to_check = [
            "EngineCPUUtilization",
            "DatabaseMemoryUsagePercentage",
            "CacheHitRate",
            "ReplicationLag",
            "BytesUsedForCachePercent",
            "ThrottledCmdsPerMinute",
        ]

        for metric_name in metrics_to_check:
            critical_threshold = self._registry.get_critical(metric_name)
            if critical_threshold is None:
                continue

            # Extract time-series data
            timestamps, values = self._extract_metric_series(
                cluster_data, metric_name
            )

            if not values or len(values) < 10:
                continue

            arr = np.array(values, dtype=np.float64)
            valid = arr[~np.isnan(arr)]

            if len(valid) < 10:
                continue

            # Compute spike_ratio
            p95 = float(np.nanpercentile(arr, 95))
            max_val = float(np.nanmax(arr))
            spike_ratio = max_val / p95 if p95 != 0 else 1.0

            # Determine if metric is inverted (CacheHitRate: lower is worse)
            is_inverted = metric_name in ThresholdRegistry.INVERTED_METRICS

            # Count breach windows
            breach_count = 0
            for v in values:
                if v is None or (isinstance(v, float) and math.isnan(v)):
                    continue
                if is_inverted:
                    if v < critical_threshold:
                        breach_count += 1
                else:
                    if v > critical_threshold:
                        breach_count += 1

            breach_minutes = breach_count * 5
            total_minutes = len(values) * 5
            breach_percent = round(
                (breach_minutes / total_minutes) * 100, 2
            ) if total_minutes > 0 else 0.0

            # Cluster consecutive breach windows into incidents
            incidents = self._cluster_incidents(
                timestamps, values, critical_threshold, is_inverted
            )

            # Top 3 incidents by duration
            top_incidents = sorted(
                incidents, key=lambda x: x["duration_minutes"], reverse=True
            )[:3]

            # Evaluate currently_breaching: p95 of last 288 data points
            last_288 = values[-288:] if len(values) >= 288 else values
            last_arr = np.array(last_288, dtype=np.float64)
            last_valid = last_arr[~np.isnan(last_arr)]

            currently_breaching = False
            if len(last_valid) > 0:
                last_p95 = float(np.nanpercentile(last_arr, 95))
                if is_inverted:
                    currently_breaching = last_p95 < critical_threshold
                else:
                    currently_breaching = last_p95 > critical_threshold

            # Classify severity
            severity = self._classify_severity(
                currently_breaching, breach_minutes, spike_ratio
            )

            results[metric_name] = {
                "spike_ratio": round(spike_ratio, 2),
                "breach_minutes": breach_minutes,
                "breach_percent": breach_percent,
                "incidents": top_incidents,
                "currently_breaching": currently_breaching,
                "severity": severity,
            }

        return results

    def _extract_metric_series(
        self, cluster_data: dict, metric_name: str
    ) -> tuple[list[str], list[float]]:
        """Extract timestamps and values for a metric across all nodes.

        Uses the appropriate statistic for breach analysis (Maximum for CPU,
        Average for rates, Sum for counts).

        Args:
            cluster_data: Cluster data dict.
            metric_name: The metric name to extract.

        Returns:
            Tuple of (timestamps, values) lists.
        """
        stat_preference = ["Maximum", "Average", "Sum"]

        all_timestamps: list[str] = []
        all_values: list[float] = []

        if "nodes" in cluster_data:
            for node_id, node_data in cluster_data["nodes"].items():
                metrics = node_data.get("metrics", {})
                metric = metrics.get(metric_name, {})
                for stat in stat_preference:
                    if stat in metric:
                        series = metric[stat]
                        ts = series.get("timestamps", [])
                        vals = series.get("values", [])
                        all_timestamps.extend(ts)
                        all_values.extend(vals)
                        break
        elif "metrics" in cluster_data:
            metrics = cluster_data["metrics"]
            metric = metrics.get(metric_name, {})
            for stat in stat_preference:
                if stat in metric:
                    series = metric[stat]
                    all_timestamps = series.get("timestamps", [])
                    all_values = series.get("values", [])
                    break

        return all_timestamps, all_values

    @staticmethod
    def _cluster_incidents(
        timestamps: list[str],
        values: list[float],
        threshold: float,
        is_inverted: bool,
    ) -> list[dict]:
        """Cluster consecutive breach windows into distinct incidents.

        Args:
            timestamps: List of ISO 8601 timestamp strings.
            values: Corresponding metric values.
            threshold: The CRITICAL threshold value.
            is_inverted: If True, breach is when value < threshold.

        Returns:
            List of incident dicts with start_time, duration_minutes,
            peak_value.
        """
        incidents: list[dict] = []
        incident_start_idx: int | None = None
        incident_peak: float = 0.0

        for i, v in enumerate(values):
            if v is None or (isinstance(v, float) and math.isnan(v)):
                # Treat NaN as non-breach, close current incident if any
                if incident_start_idx is not None:
                    duration = (i - incident_start_idx) * 5
                    start_time = (
                        timestamps[incident_start_idx]
                        if incident_start_idx < len(timestamps)
                        else ""
                    )
                    incidents.append({
                        "start_time": start_time,
                        "duration_minutes": duration,
                        "peak_value": round(incident_peak, 2),
                    })
                    incident_start_idx = None
                    incident_peak = 0.0
                continue

            is_breaching = (
                v < threshold if is_inverted else v > threshold
            )

            if is_breaching:
                if incident_start_idx is None:
                    incident_start_idx = i
                    incident_peak = v
                else:
                    # Update peak: for inverted, peak is the lowest value
                    if is_inverted:
                        incident_peak = min(incident_peak, v)
                    else:
                        incident_peak = max(incident_peak, v)
            else:
                if incident_start_idx is not None:
                    duration = (i - incident_start_idx) * 5
                    start_time = (
                        timestamps[incident_start_idx]
                        if incident_start_idx < len(timestamps)
                        else ""
                    )
                    incidents.append({
                        "start_time": start_time,
                        "duration_minutes": duration,
                        "peak_value": round(incident_peak, 2),
                    })
                    incident_start_idx = None
                    incident_peak = 0.0

        # Close any open incident at the end
        if incident_start_idx is not None:
            duration = (len(values) - incident_start_idx) * 5
            start_time = (
                timestamps[incident_start_idx]
                if incident_start_idx < len(timestamps)
                else ""
            )
            incidents.append({
                "start_time": start_time,
                "duration_minutes": duration,
                "peak_value": round(incident_peak, 2),
            })

        return incidents

    @staticmethod
    def _classify_severity(
        currently_breaching: bool, breach_minutes: int, spike_ratio: float
    ) -> str:
        """Classify breach severity based on conditions.

        Severity rules:
        - CRITICAL: currently breaching AND total breach > 4 hours (240 min)
        - HIGH: currently breaching OR total breach > 2 hours (120 min)
        - MEDIUM: total breach > 30 min but recovered
        - LOW: spike_ratio > 5 but no sustained breach
        - HEALTHY: no breach and spike_ratio < 3

        Args:
            currently_breaching: Whether metric is currently above threshold.
            breach_minutes: Total minutes in breach over 14 days.
            spike_ratio: max / p95 ratio.

        Returns:
            Severity string: CRITICAL, HIGH, MEDIUM, LOW, or HEALTHY.
        """
        if currently_breaching and breach_minutes > 240:
            return "CRITICAL"
        if currently_breaching or breach_minutes > 120:
            return "HIGH"
        if breach_minutes > 30:
            return "MEDIUM"
        if spike_ratio > 5:
            return "LOW"
        if breach_minutes == 0 and spike_ratio < 3:
            return "HEALTHY"
        return "HEALTHY"


# ---------------------------------------------------------------------------
# EfficiencyModel
# ---------------------------------------------------------------------------


class EfficiencyModel:
    """Model 5: Computes derived efficiency ratios from raw time-series.

    Computes hit rate, TTL coverage, eviction pressure, connection utilization,
    network burst risk, write amplification, and read/write ratio. All ratios
    use safe_divide() to handle zero denominators gracefully.
    """

    def _sum_metric_across_nodes(
        self, cluster_data: dict, metric_name: str, statistic: str = "Sum"
    ) -> float:
        """Sum all values for a metric/statistic across all nodes.

        Args:
            cluster_data: Cluster data dict containing nodes and metrics.
            metric_name: The metric name to sum.
            statistic: The statistic to use (default "Sum").

        Returns:
            Total sum of all values across all nodes. Returns 0.0 if unavailable.
        """
        total = 0.0
        nodes = cluster_data.get("nodes", {})
        for node_id, node_data in nodes.items():
            metrics = node_data.get("metrics", {})
            metric = metrics.get(metric_name, {})
            stat_data = metric.get(statistic, {})
            values = stat_data.get("values", [])
            for v in values:
                if v is not None and not math.isnan(v):
                    total += v
        return total

    def _max_metric_across_nodes(
        self, cluster_data: dict, metric_name: str, statistic: str = "Maximum"
    ) -> float:
        """Get the maximum value for a metric/statistic across all nodes.

        Args:
            cluster_data: Cluster data dict.
            metric_name: The metric name.
            statistic: The statistic to use (default "Maximum").

        Returns:
            Maximum value across all nodes. Returns 0.0 if unavailable.
        """
        max_val = 0.0
        nodes = cluster_data.get("nodes", {})
        for node_id, node_data in nodes.items():
            metrics = node_data.get("metrics", {})
            metric = metrics.get(metric_name, {})
            stat_data = metric.get(statistic, {})
            values = stat_data.get("values", [])
            for v in values:
                if v is not None and not math.isnan(v):
                    max_val = max(max_val, v)
        return max_val

    def _values_across_nodes(
        self, cluster_data: dict, metric_name: str, statistic: str = "Maximum"
    ) -> list[float]:
        """Collect every valid datapoint for a metric/statistic across nodes.

        The distribution, not just its extreme. A single max cannot distinguish
        one transient spike from a sustained condition, and several ratios here
        are graded on whether the condition persists.

        Args:
            cluster_data: Cluster data dict.
            metric_name: The metric name.
            statistic: The statistic to use.

        Returns:
            All non-null, non-NaN values, unordered. Empty if unavailable.
        """
        values: list[float] = []
        nodes = cluster_data.get("nodes", {})
        for node_id, node_data in nodes.items():
            metrics = node_data.get("metrics", {})
            metric = metrics.get(metric_name, {})
            stat_data = metric.get(statistic, {})
            for v in stat_data.get("values", []):
                if v is not None and not math.isnan(v):
                    values.append(float(v))
        return values

    def compute(self, cluster_data: dict, inventory_data: dict | None) -> dict:
        """Compute all efficiency ratios for a cluster.

        Computes: hit_rate, ttl_coverage, eviction_pressure,
        connection_utilization, network_burst_risk, write_amplification,
        and read_write_ratio. Uses safe_divide() for zero-denominator safety.

        Args:
            cluster_data: Per-cluster data from metrics.json.
            inventory_data: Per-cluster inventory metadata (optional).

        Returns:
            Dictionary with each ratio containing value, assessment,
            and recommendation.
        """
        results: dict = {}

        # 8.2: Hit Rate
        results["hit_rate"] = self._compute_hit_rate(cluster_data)

        # 8.3: TTL Coverage
        results["ttl_coverage"] = self._compute_ttl_coverage(cluster_data)

        # 8.4: Eviction Pressure
        results["eviction_pressure"] = self._compute_eviction_pressure(
            cluster_data
        )

        # 8.5: Connection Utilization
        results["connection_utilization"] = (
            self._compute_connection_utilization(cluster_data, inventory_data)
        )

        # 8.6: Network Burst Risk
        results["network_burst_risk"] = self._compute_network_burst_risk(
            cluster_data
        )

        # 8.7: Write Amplification
        results["write_amplification"] = self._compute_write_amplification(
            cluster_data
        )

        # 8.8: Read/Write Ratio
        results["read_write_ratio"] = self._compute_read_write_ratio(
            cluster_data
        )

        return results

    def _compute_hit_rate(self, cluster_data: dict) -> dict:
        """Compute Hit Rate: Sum(CacheHits) / (Sum(CacheHits) + Sum(CacheMisses)).

        Classification: HEALTHY (>80%), MEDIUM (60-80%), HIGH (<60%).

        Args:
            cluster_data: Cluster data dict.

        Returns:
            Dict with value, assessment, recommendation.
        """
        cache_hits = self._sum_metric_across_nodes(
            cluster_data, "CacheHits", "Sum"
        )
        cache_misses = self._sum_metric_across_nodes(
            cluster_data, "CacheMisses", "Sum"
        )
        denominator = cache_hits + cache_misses
        value = safe_divide(cache_hits, denominator)

        if value is None:
            return {
                "value": None,
                "assessment": "not_applicable",
                "recommendation": "No cache hit/miss data available",
            }

        # Classify
        if value > 0.80:
            assessment = "HEALTHY"
            recommendation = None
        elif value >= 0.60:
            assessment = "MEDIUM"
            recommendation = (
                "Hit rate is moderate; review caching strategy and TTL settings"
            )
        else:
            assessment = "HIGH"
            recommendation = (
                "Low hit rate indicates cache ineffectiveness; "
                "review key patterns and pre-warming strategy"
            )

        return {
            "value": round(value, 2),
            "assessment": assessment,
            "recommendation": recommendation,
        }

    def _compute_ttl_coverage(self, cluster_data: dict) -> dict:
        """Compute TTL Coverage: Max(CurrVolatileItems) / Max(CurrItems).

        Classification: HEALTHY (>90%), MEDIUM (50-90%), HIGH (<50%).

        Args:
            cluster_data: Cluster data dict.

        Returns:
            Dict with value, assessment, recommendation.
        """
        volatile_items = self._max_metric_across_nodes(
            cluster_data, "CurrVolatileItems", "Maximum"
        )
        curr_items = self._max_metric_across_nodes(
            cluster_data, "CurrItems", "Maximum"
        )
        value = safe_divide(volatile_items, curr_items)

        if value is None:
            return {
                "value": None,
                "assessment": "not_applicable",
                "recommendation": "No item count data available",
            }

        if value > 0.90:
            assessment = "HEALTHY"
            recommendation = None
        elif value >= 0.50:
            assessment = "MEDIUM"
            recommendation = (
                "Some keys lack TTL; review for unbounded growth risk"
            )
        else:
            assessment = "HIGH"
            recommendation = (
                "Most keys lack TTL; risk of unbounded memory growth. "
                "Add TTL to keys or use volatile eviction policies"
            )

        return {
            "value": round(value, 2),
            "assessment": assessment,
            "recommendation": recommendation,
        }

    def _compute_eviction_pressure(self, cluster_data: dict) -> dict:
        """Compute Eviction Pressure: Sum(Evictions) / Max(CurrItems).

        Classification: HEALTHY (<0.1%), MEDIUM (0.1-1%), HIGH (>1%).

        Args:
            cluster_data: Cluster data dict.

        Returns:
            Dict with value, assessment, recommendation.
        """
        evictions = self._sum_metric_across_nodes(
            cluster_data, "Evictions", "Sum"
        )
        curr_items = self._max_metric_across_nodes(
            cluster_data, "CurrItems", "Maximum"
        )
        value = safe_divide(evictions, curr_items)

        if value is None:
            return {
                "value": None,
                "assessment": "not_applicable",
                "recommendation": "No eviction or item count data available",
            }

        # Convert to percentage for classification
        pct = value * 100

        if pct < 0.1:
            assessment = "HEALTHY"
            recommendation = None
        elif pct <= 1.0:
            assessment = "MEDIUM"
            recommendation = (
                "Moderate eviction pressure; monitor memory usage trends"
            )
        else:
            assessment = "HIGH"
            recommendation = (
                "High eviction pressure indicates memory undersizing; "
                "scale up memory or reduce dataset size"
            )

        return {
            "value": round(value, 4),
            "assessment": assessment,
            "recommendation": recommendation,
        }

    def _compute_connection_utilization(
        self, cluster_data: dict, inventory_data: dict | None
    ) -> dict:
        """Compute Connection Utilization: Max(CurrConnections) / maxclients.

        maxclients defaults to 65000, overridden from inventory parameter data.
        Classification: HEALTHY (<80%), HIGH (>80%).

        Args:
            cluster_data: Cluster data dict.
            inventory_data: Inventory metadata with optional maxclients override.

        Returns:
            Dict with value, assessment, recommendation.
        """
        curr_connections = self._max_metric_across_nodes(
            cluster_data, "CurrConnections", "Maximum"
        )

        # Determine maxclients: default 65000, override from inventory
        maxclients = 65000
        if inventory_data:
            params = inventory_data.get("parameters", {})
            if "maxclients" in params:
                try:
                    maxclients = int(params["maxclients"])
                except (ValueError, TypeError):
                    pass

        value = safe_divide(curr_connections, maxclients)

        if value is None:
            return {
                "value": None,
                "assessment": "not_applicable",
                "recommendation": "No connection data available",
            }

        if value < 0.80:
            assessment = "HEALTHY"
            recommendation = None
        else:
            assessment = "HIGH"
            recommendation = (
                "Connection utilization near maxclients limit; "
                "implement connection pooling or increase maxclients"
            )

        return {
            "value": round(value, 2),
            "assessment": assessment,
            "recommendation": recommendation,
        }

    # Share of datapoints above the band boundary required to call a network
    # condition sustained. At 300s resolution over 14 days (~4,032 points per
    # node), 1% is roughly 40 datapoints — about 3.4 hours of elevated usage,
    # which is a pattern rather than a burst.
    #
    # This exists because grading on the 14-day maximum alone made a single
    # datapoint decide the verdict: on the example fleet, one 80.55% spike
    # against a p95 of 3.54% produced a MEDIUM "monitor for throttling" on a
    # cluster using 3.5% of its baseline. Network allowances are *designed* to be
    # burst-absorbed; one spike touching the band is the feature working.
    SUSTAINED_BREACH_FRACTION = 0.01

    def _compute_network_burst_risk(self, cluster_data: dict) -> dict:
        """Compute Network Burst Risk from sustained baseline-allowance usage.

        Grades the p95 of NetworkBaselineMaxUsageInPercentage /
        OutPercentage (whichever axis is busier), not the 14-day maximum, and
        reports the peak alongside it:

          HEALTHY  — p95 < 80%
          MEDIUM   — p95 80-100%, or >1% of datapoints over 100%
          HIGH     — p95 > 100%

        A lone spike no longer sets the assessment, but it is not discarded
        either: `peak` and `breach_fraction` are returned so a reader can see a
        one-off burst without it being reported as a risk. An exhausted
        allowance shows up as a sustained condition, which is what the
        MEDIUM-by-breach-fraction branch catches.

        Args:
            cluster_data: Cluster data dict.

        Returns:
            Dict with value (p95), peak, breach_fraction, assessment,
            recommendation.
        """
        in_values = self._values_across_nodes(
            cluster_data, "NetworkBaselineMaxUsageInPercentage", "Maximum"
        )
        out_values = self._values_across_nodes(
            cluster_data, "NetworkBaselineMaxUsageOutPercentage", "Maximum"
        )

        # Whichever direction is busier, judged by its typical level rather than
        # its worst datapoint — the same reason this function no longer grades on
        # the max.
        def _p95(values):
            return float(np.percentile(values, 95)) if values else 0.0

        values = in_values if _p95(in_values) >= _p95(out_values) else out_values

        if not values:
            return {
                "value": None,
                "peak": None,
                "breach_fraction": None,
                "assessment": "not_applicable",
                "recommendation": "No network baseline data available",
            }

        arr = np.array(values, dtype=np.float64)
        p95 = float(np.percentile(arr, 95))
        peak = float(np.max(arr))
        breach_fraction = float(np.sum(arr > 100.0)) / len(arr)

        # An all-zero series means the metric is published but the cluster moved
        # no traffic. That is a measurement of idleness, not missing data, so it
        # is HEALTHY rather than not_applicable — the absent-metric case is the
        # empty-list branch above.
        if p95 < 80.0 and breach_fraction <= self.SUSTAINED_BREACH_FRACTION:
            assessment = "HEALTHY"
            recommendation = None
        elif p95 > 100.0:
            assessment = "HIGH"
            recommendation = (
                "Network usage sustained above the baseline allowance; "
                "scale to a larger instance with more bandwidth"
            )
        else:
            assessment = "MEDIUM"
            recommendation = (
                "Network utilization approaching baseline limits; "
                "monitor for throttling"
            )

        return {
            "value": round(p95, 2),
            "peak": round(peak, 2),
            "breach_fraction": round(breach_fraction, 4),
            "assessment": assessment,
            "recommendation": recommendation,
        }

    def _compute_write_amplification(self, cluster_data: dict) -> dict:
        """Compute Write Amplification: Sum(ReplicationBytes) / Sum(NetworkBytesIn).

        Classification: HEALTHY (<3x), MEDIUM (3-5x), HIGH (>5x).

        Args:
            cluster_data: Cluster data dict.

        Returns:
            Dict with value, assessment, recommendation.
        """
        replication_bytes = self._sum_metric_across_nodes(
            cluster_data, "ReplicationBytes", "Sum"
        )
        network_bytes_in = self._sum_metric_across_nodes(
            cluster_data, "NetworkBytesIn", "Sum"
        )
        value = safe_divide(replication_bytes, network_bytes_in)

        if value is None:
            return {
                "value": None,
                "assessment": "not_applicable",
                "recommendation": "No replication or network bytes data available",
            }

        if value < 3.0:
            assessment = "HEALTHY"
            recommendation = None
        elif value <= 5.0:
            assessment = "MEDIUM"
            recommendation = (
                "Moderate write amplification from replication; "
                "consider reducing replica count if not needed"
            )
        else:
            assessment = "HIGH"
            recommendation = (
                "High write amplification; replication overhead is significant. "
                "Review replica count and large-value writes"
            )

        return {
            "value": round(value, 2),
            "assessment": assessment,
            "recommendation": recommendation,
        }

    # A workload is read-heavy or write-heavy only when one side clears this
    # share of the traffic; otherwise it is balanced. 66% (~2:1) is the point at
    # which the mix is lopsided enough to change a sizing or engine decision.
    _READ_WRITE_MAJORITY = 66.0

    def _compute_read_write_ratio(self, cluster_data: dict) -> dict:
        """Read/write mix: the split between reads (GetTypeCmds) and writes
        (SetTypeCmds), as a ratio, a percentage, and a read-heavy/write-heavy
        label.

        Informational only — no severity. Falls back to the serverless ECPU
        command metrics (``*CmdsECPUs``) when a cluster publishes no raw command
        counts, and reports "not_measured" (never 50/50) when there is no traffic
        at all — an idle cache has no read/write character to describe, and
        rendering one as balanced would be a measurement of something that did
        not happen.

        Args:
            cluster_data: Cluster data dict.

        Returns:
            Dict with value (ratio, or None for all-reads), read_pct, write_pct,
            class, basis, assessment, recommendation.
        """
        # Prefer raw command counts; fall back to the serverless ECPU command
        # metrics, which are what a serverless cache publishes instead.
        get_cmds = self._sum_metric_across_nodes(
            cluster_data, "GetTypeCmds", "Sum"
        )
        set_cmds = self._sum_metric_across_nodes(
            cluster_data, "SetTypeCmds", "Sum"
        )
        basis = "commands"
        if get_cmds + set_cmds == 0:
            get_cmds = self._sum_metric_across_nodes(
                cluster_data, "GetTypeCmdsECPUs", "Sum"
            )
            set_cmds = self._sum_metric_across_nodes(
                cluster_data, "SetTypeCmdsECPUs", "Sum"
            )
            basis = "ecpus"

        total = get_cmds + set_cmds
        if total == 0:
            return {
                "value": None,
                "read_pct": None,
                "write_pct": None,
                "class": "not_measured",
                "basis": None,
                "assessment": "not_applicable",
                "recommendation": "No command data available",
            }

        read_pct = round(get_cmds / total * 100, 1)
        write_pct = round(100.0 - read_pct, 1)
        # None, not infinity, when there are no writes at all: the ratio is
        # undefined, but the percentage split still describes the mix.
        ratio = round(get_cmds / set_cmds, 2) if set_cmds > 0 else None

        if read_pct >= self._READ_WRITE_MAJORITY:
            klass = "read-heavy"
        elif write_pct >= self._READ_WRITE_MAJORITY:
            klass = "write-heavy"
        else:
            klass = "balanced"

        return {
            "value": ratio,
            "read_pct": read_pct,
            "write_pct": write_pct,
            "class": klass,
            "basis": basis,
            "assessment": "informational",
            "recommendation": None,
        }



# ---------------------------------------------------------------------------
# ShardBalanceModel
# ---------------------------------------------------------------------------


class ShardBalanceModel:
    """Model 6: Detects uneven load distribution across shards.

    Computes Coefficient of Variation across primary nodes for CPU, Memory,
    and CacheHits. Only applicable to cluster-mode-enabled clusters with
    2+ shards.
    """

    # Metrics to check for shard balance
    BALANCE_METRICS = [
        "EngineCPUUtilization",
        "DatabaseMemoryUsagePercentage",
        "CacheHits",
    ]

    # CV classification thresholds
    CV_WELL_BALANCED = 0.15
    CV_MINOR = 0.30
    CV_SIGNIFICANT = 0.50

    def compute(
        self, cluster_data: dict, inventory_data: dict | None
    ) -> dict | None:
        """Compute shard balance analysis for cluster-mode-enabled clusters.

        Gating: returns None if not cluster_mode_enabled or num_shards < 2.

        For applicable clusters:
        1. Extract per-primary-node metrics (CPU, Memory, CacheHits)
        2. Compute CV_sustained = std(p95_values) / mean(p95_values)
        3. Compute CV_peak = std(max_values) / mean(max_values)
        4. Classify: well-balanced / minor / significant / severe
        5. Determine imbalance_type: structural / intermittent / none
        6. Identify hottest_node_id

        Args:
            cluster_data: Per-cluster data from metrics.json.
            inventory_data: Per-cluster inventory metadata.

        Returns:
            Shard balance result dict, or None if not applicable.
        """
        # 9.1: Gating — skip if not cluster-mode or < 2 shards
        if not inventory_data:
            return None

        cluster_mode_enabled = inventory_data.get(
            "cluster_mode_enabled", False
        )
        num_shards = inventory_data.get("num_shards", 0)

        if not cluster_mode_enabled or num_shards < 2:
            return None

        # Identify primary nodes from inventory
        primary_node_ids = self._get_primary_node_ids(
            cluster_data, inventory_data
        )

        if len(primary_node_ids) < 2:
            return None

        # 9.2-9.4: Compute per-metric CV values
        cv_results: dict = {}
        for metric_name in self.BALANCE_METRICS:
            p95_values, max_values = self._extract_per_node_stats(
                cluster_data, primary_node_ids, metric_name
            )

            cv_sustained = self._compute_cv(p95_values)
            cv_peak = self._compute_cv(max_values)

            cv_results[metric_name] = {
                "cv_sustained": round(cv_sustained, 4),
                "cv_peak": round(cv_peak, 4),
            }

        # 9.5: Classify using the highest CV across all metrics
        max_cv_sustained = max(
            r["cv_sustained"] for r in cv_results.values()
        )
        max_cv_peak = max(r["cv_peak"] for r in cv_results.values())
        overall_cv = max(max_cv_sustained, max_cv_peak)
        classification = self._classify_balance(overall_cv)

        # 9.6: Determine imbalance type
        imbalance_type = self._determine_imbalance_type(
            max_cv_sustained, max_cv_peak
        )

        # 9.7: Identify hottest node (highest p95 EngineCPUUtilization)
        hottest_node_id = self._find_hottest_node(
            cluster_data, primary_node_ids
        )

        return {
            "cpu_cv_sustained": cv_results["EngineCPUUtilization"][
                "cv_sustained"
            ],
            "cpu_cv_peak": cv_results["EngineCPUUtilization"]["cv_peak"],
            "memory_cv_sustained": cv_results[
                "DatabaseMemoryUsagePercentage"
            ]["cv_sustained"],
            "memory_cv_peak": cv_results["DatabaseMemoryUsagePercentage"][
                "cv_peak"
            ],
            "hits_cv_sustained": cv_results["CacheHits"]["cv_sustained"],
            "hits_cv_peak": cv_results["CacheHits"]["cv_peak"],
            "classification": classification,
            "imbalance_type": imbalance_type,
            "hottest_node_id": hottest_node_id,
        }

    def _get_primary_node_ids(
        self, cluster_data: dict, inventory_data: dict | None
    ) -> list[str]:
        """Get list of primary node IDs from inventory or cluster data.

        Args:
            cluster_data: Cluster data dict.
            inventory_data: Inventory metadata with node roles.

        Returns:
            List of primary node IDs.
        """
        primary_nodes: list[str] = []

        # Try to get from inventory_data nodes with role info
        if inventory_data and "nodes" in inventory_data:
            inv_nodes = inventory_data["nodes"]
            for node_id, node_info in inv_nodes.items():
                if isinstance(node_info, dict):
                    role = node_info.get("role", "").lower()
                    if role == "primary":
                        primary_nodes.append(node_id)

        # If no primary nodes found from inventory, use all nodes from
        # cluster_data as a fallback (assume all are primaries in a
        # cluster-mode setup)
        if not primary_nodes and "nodes" in cluster_data:
            primary_nodes = list(cluster_data["nodes"].keys())

        return primary_nodes

    def _extract_per_node_stats(
        self,
        cluster_data: dict,
        primary_node_ids: list[str],
        metric_name: str,
    ) -> tuple[list[float], list[float]]:
        """Extract p95 and max values per primary node for a metric.

        Args:
            cluster_data: Cluster data dict.
            primary_node_ids: List of primary node IDs.
            metric_name: Metric to extract.

        Returns:
            Tuple of (p95_values, max_values) — one value per node.
        """
        p95_values: list[float] = []
        max_values: list[float] = []

        nodes = cluster_data.get("nodes", {})
        stat_preference = ["Maximum", "Average", "Sum"]

        for node_id in primary_node_ids:
            node_data = nodes.get(node_id)
            if not node_data:
                continue

            metrics = node_data.get("metrics", {})
            metric = metrics.get(metric_name, {})

            # Find the best available statistic
            values: list[float] = []
            for stat in stat_preference:
                if stat in metric:
                    series = metric[stat]
                    values = series.get("values", [])
                    break

            if not values:
                p95_values.append(0.0)
                max_values.append(0.0)
                continue

            arr = np.array(values, dtype=np.float64)
            valid = arr[~np.isnan(arr)]

            if len(valid) == 0:
                p95_values.append(0.0)
                max_values.append(0.0)
            else:
                p95_values.append(float(np.nanpercentile(arr, 95)))
                max_values.append(float(np.nanmax(arr)))

        return p95_values, max_values

    @staticmethod
    def _compute_cv(values: list[float]) -> float:
        """Compute Coefficient of Variation: std / mean.

        Returns 0 when mean is 0 to avoid division by zero.

        Args:
            values: List of numeric values.

        Returns:
            CV value (>= 0).
        """
        if not values:
            return 0.0
        arr = np.array(values, dtype=np.float64)
        mean_val = float(np.mean(arr))
        if mean_val == 0:
            return 0.0
        std_val = float(np.std(arr))
        return std_val / mean_val

    @classmethod
    def _classify_balance(cls, cv: float) -> str:
        """Classify shard balance based on CV value.

        Args:
            cv: Coefficient of Variation value.

        Returns:
            Classification string.
        """
        if cv < cls.CV_WELL_BALANCED:
            return "well-balanced"
        elif cv < cls.CV_MINOR:
            return "minor_imbalance"
        elif cv < cls.CV_SIGNIFICANT:
            return "significant_imbalance"
        else:
            return "severe_imbalance"

    @classmethod
    def _determine_imbalance_type(
        cls, cv_sustained: float, cv_peak: float
    ) -> str:
        """Determine the type of imbalance.

        - "structural": CV_sustained high AND CV_peak high
        - "intermittent": CV_sustained low AND CV_peak high
        - "none": both low

        Args:
            cv_sustained: CV of p95 values.
            cv_peak: CV of max values.

        Returns:
            Imbalance type string.
        """
        sustained_high = cv_sustained >= cls.CV_WELL_BALANCED
        peak_high = cv_peak >= cls.CV_WELL_BALANCED

        if sustained_high and peak_high:
            return "structural"
        elif not sustained_high and peak_high:
            return "intermittent"
        else:
            return "none"

    def _find_hottest_node(
        self, cluster_data: dict, primary_node_ids: list[str]
    ) -> str | None:
        """Find the node with the highest p95 EngineCPUUtilization.

        Args:
            cluster_data: Cluster data dict.
            primary_node_ids: List of primary node IDs.

        Returns:
            Node ID of the hottest node, or None if no data.
        """
        hottest_node: str | None = None
        hottest_p95: float = -1.0

        nodes = cluster_data.get("nodes", {})
        for node_id in primary_node_ids:
            node_data = nodes.get(node_id)
            if not node_data:
                continue

            metrics = node_data.get("metrics", {})
            cpu_metric = metrics.get("EngineCPUUtilization", {})
            max_stat = cpu_metric.get("Maximum", {})
            values = max_stat.get("values", [])

            if not values:
                continue

            arr = np.array(values, dtype=np.float64)
            valid = arr[~np.isnan(arr)]
            if len(valid) == 0:
                continue

            p95 = float(np.nanpercentile(arr, 95))
            if p95 > hottest_p95:
                hottest_p95 = p95
                hottest_node = node_id

        return hottest_node



# ---------------------------------------------------------------------------
# TrafficPatternModel
# ---------------------------------------------------------------------------


class TrafficPatternModel:
    """Model 7: Characterizes cyclical traffic patterns.

    Aggregates command throughput by hour-of-day, computes peak-to-trough
    ratio, idle hours, weekend factor, and serverless fit score to inform
    scaling strategy decisions.
    """

    def compute(self, cluster_data: dict, inventory_data: dict | None) -> dict:
        """Compute traffic pattern analysis for a cluster.

        1. Aggregate command throughput by hour-of-day (24 buckets)
        2. Compute peak_to_trough_ratio, idle_hours, weekend_factor
        3. Identify peak_hours (4-hour contiguous block)
        4. Classify pattern
        5. Compute serverless_fit_score

        Args:
            cluster_data: Per-cluster data from metrics.json.
            inventory_data: Per-cluster inventory metadata (optional).

        Returns:
            Traffic pattern result dictionary.
        """
        # 10.1: Extract command throughput timestamps and values
        timestamps, values = self._extract_throughput(cluster_data)

        # 10.9: Handle unavailable or all-zero data
        if not timestamps or not values:
            return self._unknown_result()

        # Check if all values are zero
        arr = np.array(values, dtype=np.float64)
        valid = arr[~np.isnan(arr)]
        if len(valid) == 0 or float(np.sum(valid)) == 0:
            return self._unknown_result()

        # 10.2: Aggregate by hour-of-day (24 buckets)
        hourly_avgs = self._aggregate_by_hour(timestamps, values)

        # Check if hourly aggregation produced useful data
        if not hourly_avgs or max(hourly_avgs) == 0:
            return self._unknown_result()

        # 10.3: peak_to_trough_ratio
        peak_val = max(hourly_avgs)
        trough_val = max(min(hourly_avgs), 1)
        peak_to_trough_ratio = peak_val / trough_val

        # 10.4: idle_hours
        threshold_20pct = 0.20 * peak_val
        idle_hours = sum(1 for h in hourly_avgs if h < threshold_20pct)

        # 10.5: weekend_factor
        weekend_factor = self._compute_weekend_factor(timestamps, values)

        # 10.6: peak_hours (4-hour contiguous block)
        peak_hours = self._find_peak_hours(hourly_avgs)

        # 10.7: Classify pattern
        pattern_classification = self._classify_pattern(
            peak_to_trough_ratio, idle_hours, weekend_factor
        )

        # 10.8: serverless_fit_score
        serverless_fit_score = self._compute_serverless_fit_score(
            peak_to_trough_ratio, idle_hours
        )

        return {
            "peak_to_trough_ratio": round(peak_to_trough_ratio, 2),
            "idle_hours": idle_hours,
            "weekend_factor": round(weekend_factor, 2),
            "peak_hours": peak_hours,
            "pattern_classification": pattern_classification,
            "serverless_fit_score": serverless_fit_score,
        }

    def _extract_throughput(
        self, cluster_data: dict
    ) -> tuple[list[str], list[float]]:
        """Extract command throughput (GetTypeCmds + SetTypeCmds) time-series.

        Combines both command types by aligning on timestamps.

        Args:
            cluster_data: Cluster data dict.

        Returns:
            Tuple of (timestamps, combined_values).
        """
        get_ts, get_vals = self._extract_metric(cluster_data, "GetTypeCmds")
        set_ts, set_vals = self._extract_metric(cluster_data, "SetTypeCmds")

        # If both metrics available, combine them
        if get_ts and get_vals and set_ts and set_vals:
            # Build a timestamp-to-value map for alignment
            combined: dict[str, float] = {}
            for ts, v in zip(get_ts, get_vals):
                if v is not None and not math.isnan(v):
                    combined[ts] = combined.get(ts, 0.0) + v
            for ts, v in zip(set_ts, set_vals):
                if v is not None and not math.isnan(v):
                    combined[ts] = combined.get(ts, 0.0) + v

            if combined:
                sorted_ts = sorted(combined.keys())
                return sorted_ts, [combined[ts] for ts in sorted_ts]
        elif get_ts and get_vals:
            return get_ts, get_vals
        elif set_ts and set_vals:
            return set_ts, set_vals

        return [], []

    def _extract_metric(
        self, cluster_data: dict, metric_name: str
    ) -> tuple[list[str], list[float]]:
        """Extract timestamps and values for a metric across all nodes.

        Args:
            cluster_data: Cluster data dict.
            metric_name: Metric name to extract.

        Returns:
            Tuple of (timestamps, values).
        """
        all_timestamps: list[str] = []
        all_values: list[float] = []

        if "nodes" in cluster_data:
            for node_id, node_data in cluster_data["nodes"].items():
                metrics = node_data.get("metrics", {})
                metric = metrics.get(metric_name, {})
                stat_data = metric.get("Sum", {})
                ts = stat_data.get("timestamps", [])
                vals = stat_data.get("values", [])
                all_timestamps.extend(ts)
                all_values.extend(vals)
        elif "metrics" in cluster_data:
            metrics = cluster_data["metrics"]
            metric = metrics.get(metric_name, {})
            stat_data = metric.get("Sum", {})
            all_timestamps = stat_data.get("timestamps", [])
            all_values = stat_data.get("values", [])

        return all_timestamps, all_values

    @staticmethod
    def _aggregate_by_hour(
        timestamps: list[str], values: list[float]
    ) -> list[float]:
        """Aggregate values by hour-of-day (24 buckets).

        Averages all same-hour values across the entire time window.

        Args:
            timestamps: ISO 8601 timestamp strings.
            values: Corresponding values.

        Returns:
            List of 24 hourly averages (index 0 = hour 0, etc.).
        """
        hour_buckets: dict[int, list[float]] = {h: [] for h in range(24)}

        for ts, v in zip(timestamps, values):
            if v is None or (isinstance(v, float) and math.isnan(v)):
                continue
            try:
                # Parse hour from ISO timestamp
                # Handles formats like "2026-07-10T14:00:00Z" or
                # "2026-07-10T14:00:00+00:00"
                hour = int(ts[11:13])
                hour_buckets[hour].append(v)
            except (ValueError, IndexError):
                continue

        hourly_avgs: list[float] = []
        for h in range(24):
            bucket = hour_buckets[h]
            if bucket:
                hourly_avgs.append(float(np.mean(bucket)))
            else:
                hourly_avgs.append(0.0)

        return hourly_avgs

    @staticmethod
    def _compute_weekend_factor(
        timestamps: list[str], values: list[float]
    ) -> float:
        """Compute weekend_factor: mean(weekend) / mean(weekday).

        Weekday = Monday-Friday (weekday() 0-4).
        Weekend = Saturday-Sunday (weekday() 5-6).

        Args:
            timestamps: ISO 8601 timestamp strings.
            values: Corresponding values.

        Returns:
            Weekend factor ratio. Returns 1.0 if insufficient data.
        """
        weekday_values: list[float] = []
        weekend_values: list[float] = []

        for ts, v in zip(timestamps, values):
            if v is None or (isinstance(v, float) and math.isnan(v)):
                continue
            try:
                parsed = datetime.datetime.fromisoformat(
                    ts.replace("Z", "+00:00")
                )
                if parsed.weekday() < 5:
                    weekday_values.append(v)
                else:
                    weekend_values.append(v)
            except (ValueError, TypeError):
                continue

        if not weekday_values or not weekend_values:
            return 1.0

        weekday_mean = float(np.mean(weekday_values))
        weekend_mean = float(np.mean(weekend_values))

        if weekday_mean == 0:
            return 1.0

        return weekend_mean / weekday_mean

    @staticmethod
    def _find_peak_hours(hourly_avgs: list[float]) -> str:
        """Find the 4-hour contiguous block with highest average traffic.

        Uses a rolling window of size 4 over the 24 hourly averages.

        Args:
            hourly_avgs: List of 24 hourly average values.

        Returns:
            String like "09:00-13:00 UTC" describing the peak window.
        """
        best_start = 0
        best_sum = 0.0

        # Rolling window over 24 hours (wrapping not needed per spec:
        # rolling over hourly_avgs means we check positions 0..20)
        for i in range(21):
            window_sum = sum(hourly_avgs[i : i + 4])
            if window_sum > best_sum:
                best_sum = window_sum
                best_start = i

        end_hour = best_start + 4
        return f"{best_start:02d}:00-{end_hour:02d}:00 UTC"

    @staticmethod
    def _classify_pattern(
        peak_to_trough: float, idle_hours: int, weekend_factor: float
    ) -> str:
        """Classify the traffic pattern.

        Classification rules (evaluated in order):
        - Weekend-heavy: weekend_factor > 1.5
        - Weekday-only: weekend_factor < 0.3
        - Highly-variable: peak_to_trough > 5 OR idle_hours > 16
        - Business-hours: peak_to_trough 2-5 AND idle_hours 8-16
        - Steady: peak_to_trough < 2 AND idle_hours < 4

        Args:
            peak_to_trough: Peak to trough ratio.
            idle_hours: Number of idle hours.
            weekend_factor: Weekend to weekday ratio.

        Returns:
            Pattern classification string.
        """
        if weekend_factor > 1.5:
            return "weekend-heavy"
        if weekend_factor < 0.3:
            return "weekday-only"
        if peak_to_trough > 5 or idle_hours > 16:
            return "highly-variable"
        if 2 <= peak_to_trough <= 5 and 8 <= idle_hours <= 16:
            return "business-hours"
        if peak_to_trough < 2 and idle_hours < 4:
            return "steady"

        # Fallback for cases that don't match any specific pattern
        return "moderate-variation"

    @staticmethod
    def _compute_serverless_fit_score(
        peak_to_trough: float, idle_hours: int
    ) -> int:
        """Compute serverless fit score (0-100).

        Higher score = better fit for serverless pricing.
        Based on peak_to_trough ratio and idle hours.

        Scoring:
        - Peak-to-trough contribution (0-50): higher ratio = better serverless fit
        - Idle hours contribution (0-50): more idle hours = better serverless fit

        Args:
            peak_to_trough: Peak to trough ratio.
            idle_hours: Number of idle hours.

        Returns:
            Integer score 0-100.
        """
        # Peak-to-trough component (0-50 points)
        # Ratio of 1 = 0 points, ratio of 10+ = 50 points
        ptr_score = min(50, int((peak_to_trough - 1) * 50 / 9))
        ptr_score = max(0, ptr_score)

        # Idle hours component (0-50 points)
        # 0 idle hours = 0 points, 20+ idle hours = 50 points
        idle_score = min(50, int(idle_hours * 50 / 20))
        idle_score = max(0, idle_score)

        return min(100, ptr_score + idle_score)

    @staticmethod
    def _unknown_result() -> dict:
        """Return result for unknown/unavailable traffic pattern.

        Returns:
            Dict with pattern marked as "unknown".
        """
        return {
            "peak_to_trough_ratio": None,
            "idle_hours": None,
            "weekend_factor": None,
            "peak_hours": None,
            "pattern_classification": "unknown",
            "serverless_fit_score": None,
        }



# ---------------------------------------------------------------------------
# SteadinessModel
# ---------------------------------------------------------------------------


class SteadinessModel:
    """Characterizes how steady vs spiky a cluster's load is, from CPU, memory
    and network variability.

    This is the signal that should drive a commitment recommendation: a steady
    cluster is a sound Reserved-Node / Database-Savings-Plan candidate, a spiky
    one argues for serverless or staying on-demand. It is deliberately
    independent of command throughput — ``TrafficPatternModel`` needs
    ``GetTypeCmds``/``SetTypeCmds`` and goes ``unknown`` for every idle cluster
    and every serverless cache, which is exactly where the commitment question is
    hardest. CPU/memory/network are published even when command counts are not.

    Method: coefficient of variation (std/mean) per axis over the window. All
    three axes are computed and reported, but the label is driven by **CPU and
    memory only** — the node-capacity axes a Reserved-Node / Savings-Plan
    commitment is actually about. Network variability is reported (``network_cov``)
    but does not drive the label: a bursty network is a NETWORK-BOUND / burst-risk
    concern, already flagged elsewhere, and moving to serverless does not make it
    cheaper — so it must not by itself argue a steady-capacity cluster out of a
    sound commitment. ``driver`` names the capacity axis behind the label.

    An idle cluster — no command traffic and CPU below an activity floor — has no
    workload character to describe and returns ``not_measured`` rather than being
    labelled from background replication or backup chatter (the same
    unmeasured-≠-zero rule the rest of the pipeline follows).
    """

    # Below this mean CPU (%) with no command traffic, the cluster is idle and
    # its steadiness is moot — do not label it from replication/backup bursts.
    ACTIVITY_CPU_FLOOR = 5.0
    # CoV bands: < 0.30 is tight (steady); >= 0.75 swings widely (spiky).
    STEADY_MAX_COV = 0.30
    SPIKY_MIN_COV = 0.75

    # (axis, metric_name, statistic) — CPU on Average (sustained load, not the
    # Maximum that a single spike inflates), memory and network on what Stage 2
    # collects for each.
    _AXES = (
        ("cpu", "EngineCPUUtilization", "Average"),
        ("memory", "DatabaseMemoryUsagePercentage", "Maximum"),
        ("network", "NetworkBytesIn", "Sum"),
    )

    def compute(self, cluster_data: dict, inventory_data: dict | None) -> dict:
        """Compute the steadiness label and per-axis coefficients of variation."""
        covs = {axis: self._cov(cluster_data, metric, stat)
                for axis, metric, stat in self._AXES}

        reads = (self._sum(cluster_data, "GetTypeCmds")
                 + self._sum(cluster_data, "SetTypeCmds"))
        ecpus = (self._sum(cluster_data, "GetTypeCmdsECPUs")
                 + self._sum(cluster_data, "SetTypeCmdsECPUs"))
        cpu_mean = self._mean(cluster_data, "EngineCPUUtilization", "Average")
        active = reads > 0 or ecpus > 0 or cpu_mean > self.ACTIVITY_CPU_FLOOR

        base = {"cpu_cov": covs["cpu"], "memory_cov": covs["memory"],
                "network_cov": covs["network"]}
        # Label from the capacity axes only; network is reported, not a driver.
        capacity = {a: covs[a] for a in ("cpu", "memory")
                    if covs[a] is not None}

        if not active or not capacity:
            return {**base, "label": "not_measured", "measured": False,
                    "driver": None,
                    "reason": "no workload activity to characterise"}

        driver = max(capacity, key=capacity.get)
        worst = capacity[driver]
        if worst < self.STEADY_MAX_COV:
            label = "steady"
        elif worst >= self.SPIKY_MIN_COV:
            label = "spiky"
        else:
            label = "variable"

        return {**base, "label": label, "measured": True, "driver": driver}

    def _series(self, cluster_data: dict, metric_name: str,
                statistic: str) -> list:
        vals = []
        for node in cluster_data.get("nodes", {}).values():
            stat = node.get("metrics", {}).get(metric_name, {}).get(
                statistic, {})
            for v in stat.get("values", []):
                if v is not None and not math.isnan(v):
                    vals.append(float(v))
        return vals

    def _cov(self, cluster_data: dict, metric_name: str,
             statistic: str) -> float | None:
        vals = self._series(cluster_data, metric_name, statistic)
        if not vals:
            return None
        arr = np.array(vals, dtype=np.float64)
        mean = float(np.mean(arr))
        if mean <= 0:
            return None
        return round(float(np.std(arr)) / mean, 3)

    def _mean(self, cluster_data: dict, metric_name: str,
              statistic: str) -> float:
        vals = self._series(cluster_data, metric_name, statistic)
        return float(np.mean(np.array(vals, dtype=np.float64))) if vals else 0.0

    def _sum(self, cluster_data: dict, metric_name: str,
             statistic: str = "Sum") -> float:
        total = 0.0
        for node in cluster_data.get("nodes", {}).values():
            stat = node.get("metrics", {}).get(metric_name, {}).get(
                statistic, {})
            for v in stat.get("values", []):
                if v is not None and not math.isnan(v):
                    total += v
        return total


# ---------------------------------------------------------------------------
# CorrelationModel
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class MetricPair:
    """One correlatable metric pair and what a correlation between them means.

    Severity belongs here rather than at the finding site, because how much a
    correlation matters is a property of the pair, not of r. A cache whose CPU
    tracks its traffic is a cache working correctly, and r=0.97 makes that
    *more* obviously true -- yet grading on |r| alone reported it HIGH, above a
    genuine r=0.85 latency problem. Strength answers "is this relationship
    real"; only the pair can answer "is it a problem".

    Attributes:
        metric_a: First metric name.
        metric_b: Second metric name.
        interpretation: What a correlation in the expected direction means.
        max_severity: The severity a confirmed correlation in the expected
            direction earns. "INFO" means the relationship is worth showing but
            is not a defect -- healthy coupling, reported and never escalated.
        expected_sign: +1 if the metrics should move together for the
            interpretation to hold, -1 if inversely. A correlation with the
            opposite sign does not support the interpretation, so it is not
            reported under it.
        inverse_interpretation: What the opposite sign means, when that is
            itself informative. None means an unexpected sign is simply not
            reported -- silence beats a confident sentence about the wrong
            mechanism.
    """

    metric_a: str
    metric_b: str
    interpretation: str
    max_severity: str
    expected_sign: int = 1
    inverse_interpretation: str | None = None

    def __post_init__(self):
        if self.max_severity not in ("INFO", "LOW", "MEDIUM", "HIGH"):
            raise ValueError(
                f"MetricPair max_severity {self.max_severity!r} is not one of "
                "INFO, LOW, MEDIUM, HIGH"
            )
        if self.expected_sign not in (1, -1):
            raise ValueError(
                f"MetricPair expected_sign must be +1 or -1, got "
                f"{self.expected_sign!r}"
            )


class CorrelationModel:
    """Model 8: Detects strong correlations between metric pairs.

    Computes Pearson correlation coefficients between defined metric pairs
    and reports strong relationships (|r| > 0.7) with interpretations.

    Two things this model deliberately does not do. It does not treat
    correlation strength as severity -- see `MetricPair`. And it does not use
    |r| to decide whether an interpretation applies: r is signed, the
    interpretations are causal sentences, and a sign opposite to the mechanism
    described is evidence against that sentence, not for it. Snapshots that
    *reduce* replication lag are not "snapshots interfering with replication".
    """

    # Metric pairs to correlate, with the severity each earns and the direction
    # its interpretation requires.
    METRIC_PAIRS = [
        MetricPair(
            "EngineCPUUtilization",
            "SuccessfulReadRequestLatency",
            "CPU saturation causing latency spikes",
            max_severity="MEDIUM",
        ),
        MetricPair(
            "EngineCPUUtilization",
            "NewConnections",
            "Connection churn driving CPU utilization",
            max_severity="LOW",
        ),
        MetricPair(
            "SaveInProgress",
            "ReplicationLag",
            "Snapshots interfering with replication",
            max_severity="MEDIUM",
            # Lag rising while a snapshot runs is the interference. Lag falling
            # during snapshots describes a cache whose quiet periods are when
            # backups are scheduled -- good practice, not a finding.
            expected_sign=1,
        ),
        MetricPair(
            "Evictions",
            "CacheMisses",
            "Evictions causing cascading cache misses",
            max_severity="MEDIUM",
            # Evictions up, misses up. The inverse (evicting more while missing
            # less) is not this mechanism.
            expected_sign=1,
        ),
        MetricPair(
            "NetworkBytesIn",
            "EngineCPUUtilization",
            "Traffic-proportional CPU load (workload-driven)",
            # INFO, not HIGH. This is the pair that exposed the defect: CPU
            # tracking traffic is a cache behaving exactly as intended, and the
            # tighter the correlation the more clearly so.
            max_severity="INFO",
        ),
        MetricPair(
            "CurrConnections",
            "EngineCPUUtilization",
            "Connection overhead driving CPU utilization",
            max_severity="LOW",
        ),
        MetricPair(
            "TrafficManagementActive",
            "SuccessfulReadRequestLatency",
            "Throttling causing latency degradation",
            max_severity="HIGH",
        ),
    ]

    # Minimum correlation strength to report
    CORRELATION_THRESHOLD = 0.7

    def compute(self, cluster_data: dict, inventory_data: dict | None) -> dict:
        """Compute correlation analysis for defined metric pairs.

        For each metric pair:
        1. Extract both time-series
        2. Skip if either metric is all-zero, constant, or >20% NaN
        3. Align to same timestamp grid
        4. Compute Pearson r via numpy.corrcoef
        5. Only include if |r| > 0.7

        Args:
            cluster_data: Per-cluster data from metrics.json.
            inventory_data: Per-cluster inventory metadata (optional).

        Returns:
            Dict with 'correlations' key containing list of strong
            correlations found.
        """
        correlations: list[dict] = []

        for pair in self.METRIC_PAIRS:
            metric_a_name = pair.metric_a
            metric_b_name = pair.metric_b
            # Extract time-series for both metrics
            ts_a, vals_a = self._extract_metric_series(
                cluster_data, metric_a_name
            )
            ts_b, vals_b = self._extract_metric_series(
                cluster_data, metric_b_name
            )

            # Skip if either is empty
            if not ts_a or not vals_a or not ts_b or not vals_b:
                continue

            # 11.4: Align time-series to same timestamp grid
            aligned_a, aligned_b = self._align_series(
                ts_a, vals_a, ts_b, vals_b
            )

            if len(aligned_a) < 10:
                continue

            # 11.3: Skip if either has >20% NaN
            arr_a = np.array(aligned_a, dtype=np.float64)
            arr_b = np.array(aligned_b, dtype=np.float64)

            nan_ratio_a = compute_nan_ratio(aligned_a)
            nan_ratio_b = compute_nan_ratio(aligned_b)

            if nan_ratio_a > 0.20 or nan_ratio_b > 0.20:
                continue

            # Remove NaN pairs (both must be valid for correlation)
            valid_mask = ~np.isnan(arr_a) & ~np.isnan(arr_b)
            clean_a = arr_a[valid_mask]
            clean_b = arr_b[valid_mask]

            if len(clean_a) < 10:
                continue

            # 11.3: Skip if either is all-zero or constant
            if np.all(clean_a == 0) or np.all(clean_b == 0):
                continue
            if np.std(clean_a) == 0 or np.std(clean_b) == 0:
                continue

            # 11.5: Compute Pearson r
            r_matrix = np.corrcoef(clean_a, clean_b)
            r_value = float(r_matrix[0, 1])

            # Handle NaN from corrcoef (can happen with edge cases)
            if math.isnan(r_value):
                continue

            # 11.6: Only include if |r| > 0.7 -- strength gates reporting at
            # all. Direction then decides which interpretation, if any, the
            # relationship actually supports.
            if abs(r_value) <= self.CORRELATION_THRESHOLD:
                continue

            observed_sign = 1 if r_value > 0 else -1
            if observed_sign == pair.expected_sign:
                interpretation = pair.interpretation
                severity = pair.max_severity
            elif pair.inverse_interpretation is not None:
                interpretation = pair.inverse_interpretation
                # An inverse relationship is reported as an observation, not as
                # the problem the pair was defined to catch.
                severity = "INFO"
            else:
                # Strong but in the direction the interpretation contradicts.
                # Reporting it would attach a causal sentence to evidence
                # against that cause.
                continue

            correlations.append({
                "metric_a": metric_a_name,
                "metric_b": metric_b_name,
                "r_value": round(r_value, 4),
                "interpretation": interpretation,
                # Carried so the finding site does not have to re-derive
                # severity from r, which is the defect this replaced.
                "severity": severity,
            })

        # 11.8: Always return correlations key (empty list if none found)
        return {"correlations": correlations}

    def _extract_metric_series(
        self, cluster_data: dict, metric_name: str
    ) -> tuple[list[str], list[float]]:
        """Extract timestamps and values for a metric across all nodes.

        Uses the first available statistic in preference order:
        Maximum, Average, Sum.

        Args:
            cluster_data: Cluster data dict.
            metric_name: The metric name to extract.

        Returns:
            Tuple of (timestamps, values) lists.
        """
        stat_preference = ["Maximum", "Average", "Sum"]

        all_timestamps: list[str] = []
        all_values: list[float] = []

        if "nodes" in cluster_data:
            for node_id, node_data in cluster_data["nodes"].items():
                metrics = node_data.get("metrics", {})
                metric = metrics.get(metric_name, {})
                for stat in stat_preference:
                    if stat in metric:
                        series = metric[stat]
                        ts = series.get("timestamps", [])
                        vals = series.get("values", [])
                        all_timestamps.extend(ts)
                        all_values.extend(vals)
                        break
        elif "metrics" in cluster_data:
            metrics = cluster_data["metrics"]
            metric = metrics.get(metric_name, {})
            for stat in stat_preference:
                if stat in metric:
                    series = metric[stat]
                    all_timestamps = series.get("timestamps", [])
                    all_values = series.get("values", [])
                    break

        return all_timestamps, all_values

    @staticmethod
    def _align_series(
        ts_a: list[str],
        vals_a: list[float],
        ts_b: list[str],
        vals_b: list[float],
    ) -> tuple[list[float], list[float]]:
        """Align two time-series to the same timestamp grid.

        Only includes timestamps that exist in both series.

        Args:
            ts_a: Timestamps for metric A.
            vals_a: Values for metric A.
            ts_b: Timestamps for metric B.
            vals_b: Values for metric B.

        Returns:
            Tuple of (aligned_values_a, aligned_values_b).
        """
        # Build timestamp-to-value maps
        map_a: dict[str, float] = {}
        for ts, v in zip(ts_a, vals_a):
            map_a[ts] = v

        map_b: dict[str, float] = {}
        for ts, v in zip(ts_b, vals_b):
            map_b[ts] = v

        # Find common timestamps
        common_ts = sorted(set(map_a.keys()) & set(map_b.keys()))

        aligned_a: list[float] = []
        aligned_b: list[float] = []

        for ts in common_ts:
            aligned_a.append(map_a[ts])
            aligned_b.append(map_b[ts])

        return aligned_a, aligned_b


# ---------------------------------------------------------------------------
# FindingsGenerator
# ---------------------------------------------------------------------------


class FindingsGenerator:
    """Aggregates all model outputs into severity-classified findings.

    Runs LAST after all models complete. Consumes outputs from all 9 models,
    generates findings with severity classifications, applies workload context
    adjustments, and deduplicates findings from multiple models flagging the
    same underlying issue.
    """

    # Severity ordering for deduplication (higher index = higher severity)
    SEVERITY_ORDER = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}

    # Valid severity values
    VALID_SEVERITIES = {"CRITICAL", "HIGH", "MEDIUM", "LOW"}

    def __init__(self, threshold_registry: ThresholdRegistry):
        """Store threshold registry for severity classification.

        Args:
            threshold_registry: ThresholdRegistry instance for looking up
                metric thresholds and classifying values.
        """
        self._threshold_registry = threshold_registry
        self._finding_counter: int = 0
        self._cluster_id: str | None = None

    def generate(self, model_outputs: dict, workload_class: str,
                 cluster_id: str | None = None) -> list[dict]:
        """Consume all model outputs and produce a deduplicated findings list.

        Args:
            model_outputs: Dict with keys:
                - "percentiles": output from PercentileModel
                - "trends": output from TrendModel
                - "utilization": output from UtilizationModel
                - "breaches": output from BreachModel
                - "efficiency": output from EfficiencyModel
                - "shard_balance": output from ShardBalanceModel (or None)
                - "correlations": output from CorrelationModel
            workload_class: The workload classification string from WorkloadModel.
            cluster_id: The cluster these findings belong to. Namespaces
                finding_id so it is unique across the whole fleet rather than
                only within one cluster -- the counter resets per call, so
                without this three clusters each produced "percentile-001" and
                an id could not identify a finding. Optional so the generator
                stays usable standalone in tests; when omitted the ids are
                bare and unique only within the returned list.

        Returns:
            List of finding dicts, each containing: finding_id, model_source,
            severity, title, description, metric_name, current_value, threshold,
            recommendation.
        """
        self._finding_counter = 0
        self._cluster_id = cluster_id
        findings: list[dict] = []

        # 12.2: Generate percentile findings
        percentiles = model_outputs.get("percentiles")
        if percentiles:
            findings.extend(self._generate_percentile_findings(percentiles))

        # 12.3: Generate trend findings
        trends = model_outputs.get("trend", model_outputs.get("trends"))
        if trends:
            findings.extend(self._generate_trend_findings(trends))

        # 12.4: Generate utilization findings
        utilization = model_outputs.get("utilization")
        if utilization:
            findings.extend(self._generate_utilization_findings(utilization))

        # 12.5: Generate breach findings
        breaches = model_outputs.get("breach", model_outputs.get("breaches"))
        if breaches:
            findings.extend(self._generate_breach_findings(breaches))

        # 12.6: Generate efficiency findings
        efficiency = model_outputs.get("efficiency")
        if efficiency:
            findings.extend(self._generate_efficiency_findings(efficiency))

        # 12.7: Generate shard balance findings
        shard_balance = model_outputs.get("shard_balance")
        if shard_balance:
            findings.extend(
                self._generate_shard_balance_findings(shard_balance)
            )

        # 12.8: Generate correlation findings
        correlations = model_outputs.get("correlation", model_outputs.get("correlations"))
        if correlations:
            findings.extend(
                self._generate_correlation_findings(correlations)
            )

        # 12.9: Apply workload context adjustments
        findings = self._apply_workload_context(findings, workload_class)

        # 12.10: Deduplicate findings (same metric from multiple models)
        findings = self._deduplicate_findings(findings)

        return findings

    def _next_finding_id(self, prefix: str) -> str:
        """Generate a finding ID unique across the fleet, not just the cluster.

        The counter resets on every generate() call, which is what keeps ids
        deterministic -- but it also meant the id identified a finding only
        within one cluster. On the example fleet three clusters each
        carried "percentile-001", so anything keyed on finding_id alone (an agent
        note, a deep link, a suppression list) would attach to whichever cluster
        was read first. Stage 3.5 already namespaces its ids; this matches it.

        Args:
            prefix: Model source prefix (e.g., "percentile", "breach").

        Returns:
            Finding ID string like "prod-api-cache-percentile-001", or
            "percentile-001" when no cluster id was supplied.
        """
        self._finding_counter += 1
        bare = f"{prefix}-{self._finding_counter:03d}"
        return f"{self._cluster_id}-{bare}" if self._cluster_id else bare

    def _generate_percentile_findings(
        self, percentiles: dict
    ) -> list[dict]:
        """Generate findings where p95 exceeds threshold levels.

        Args:
            percentiles: Output from PercentileModel.

        Returns:
            List of finding dicts for metrics with p95 above thresholds.
        """
        findings: list[dict] = []

        for metric_key, data in percentiles.items():
            # Skip insufficient data entries
            if not isinstance(data, dict) or "p95" not in data:
                continue

            p95 = data["p95"]

            # Extract the base metric name (strip statistic suffix like
            # "_Maximum", "_Average", "_Sum")
            base_metric = self._extract_base_metric(metric_key)

            # Classify the p95 value against thresholds
            severity = self._threshold_registry.classify_value(
                base_metric, p95
            )

            if severity not in self.VALID_SEVERITIES or severity == "LOW":
                continue

            threshold_level = self._threshold_registry.get_threshold(
                base_metric
            )
            threshold_val = self._get_threshold_for_severity(
                threshold_level, severity, base_metric
            )

            findings.append({
                "finding_id": self._next_finding_id("percentile"),
                "model_source": "percentile",
                "severity": severity,
                "title": f"{base_metric} p95 exceeds {severity} threshold",
                "description": (
                    f"{base_metric} p95 value of {p95} exceeds the "
                    f"{severity} threshold of {threshold_val}"
                ),
                "metric_name": base_metric,
                "current_value": p95,
                "threshold": threshold_val,
                "recommendation": self._get_percentile_recommendation(
                    base_metric, severity
                ),
            })

        return findings

    def _generate_trend_findings(self, trends: dict) -> list[dict]:
        """Generate findings for metrics with meaningful trends.

        Flags metrics where R² > 0.4 and slope is operationally significant.

        Args:
            trends: Output from TrendModel.

        Returns:
            List of finding dicts for meaningful trends.
        """
        findings: list[dict] = []

        for metric_name, data in trends.items():
            if not isinstance(data, dict):
                continue
            if data.get("status") == "insufficient_data":
                continue
            if not data.get("is_finding", False):
                continue

            r_squared = data.get("r_squared", 0)
            if r_squared <= 0.4:
                continue

            direction = data.get("direction", "stable")
            if direction == "stable":
                continue

            slope_per_week = data.get("slope_per_week", 0)

            # Determine severity based on metric and slope
            severity = self._classify_trend_severity(
                metric_name, slope_per_week, direction
            )

            if severity not in self.VALID_SEVERITIES:
                continue

            findings.append({
                "finding_id": self._next_finding_id("trend"),
                "model_source": "trend",
                "severity": severity,
                "title": f"{metric_name} showing {direction} trend",
                "description": (
                    f"{metric_name} is {direction} at "
                    f"{abs(slope_per_week):.2f}/week "
                    f"(R²={r_squared:.4f})"
                ),
                "metric_name": metric_name,
                "current_value": slope_per_week,
                "threshold": None,
                "recommendation": self._get_trend_recommendation(
                    metric_name, direction
                ),
            })

        return findings

    def _generate_utilization_findings(
        self, utilization: dict
    ) -> list[dict]:
        """Generate findings for non-BALANCED utilization classifications.

        Args:
            utilization: Output from UtilizationModel.

        Returns:
            List of finding dicts for non-balanced classifications.
        """
        findings: list[dict] = []

        classification = utilization.get("classification", "BALANCED")

        if classification == "BALANCED":
            return findings

        # Map classification to severity
        severity_map = {
            "SATURATED": "CRITICAL",
            "CPU-BOUND": "HIGH",
            "MEMORY-BOUND": "HIGH",
            "NETWORK-BOUND": "HIGH",
            "HEAVY": "MEDIUM",
            "IDLE": "LOW",
            "OVER-PROVISIONED": "LOW",
        }

        severity = severity_map.get(classification, "MEDIUM")

        if severity not in self.VALID_SEVERITIES:
            return findings

        # Get the dominant axis for metric_name
        scores = utilization.get("utilization_scores", {})
        metric_name = self._get_dominant_metric(classification, scores)

        current_value = self._get_dominant_value(classification, scores)

        findings.append({
            "finding_id": self._next_finding_id("utilization"),
            "model_source": "utilization",
            "severity": severity,
            "title": f"Cluster classified as {classification}",
            "description": (
                f"Utilization analysis classifies this cluster as "
                f"{classification}. "
                f"{utilization.get('recommendation', '')}"
            ),
            "metric_name": metric_name,
            "current_value": current_value,
            "threshold": None,
            "classification": classification,
            "recommendation": utilization.get("recommendation", ""),
        })

        return findings

    def _generate_breach_findings(self, breaches: dict) -> list[dict]:
        """Generate findings for metrics with MEDIUM or worse breach severity.

        Args:
            breaches: Output from BreachModel.

        Returns:
            List of finding dicts for breach violations.
        """
        findings: list[dict] = []

        for metric_name, data in breaches.items():
            if not isinstance(data, dict):
                continue

            severity = data.get("severity", "HEALTHY")

            if severity not in self.VALID_SEVERITIES:
                continue
            if self.SEVERITY_ORDER.get(severity, -1) < self.SEVERITY_ORDER.get(
                "MEDIUM", 1
            ):
                continue

            breach_minutes = data.get("breach_minutes", 0)
            spike_ratio = data.get("spike_ratio", 1.0)
            currently_breaching = data.get("currently_breaching", False)

            threshold_val = self._threshold_registry.get_critical(metric_name)

            findings.append({
                "finding_id": self._next_finding_id("breach"),
                "model_source": "breach",
                "severity": severity,
                "title": f"{metric_name} threshold breach detected",
                "description": (
                    f"{metric_name} breached threshold for "
                    f"{breach_minutes} minutes over 14 days"
                    f"{' (currently breaching)' if currently_breaching else ''}"
                ),
                "metric_name": metric_name,
                "current_value": spike_ratio,
                "threshold": threshold_val,
                "recommendation": self._get_breach_recommendation(
                    metric_name, severity, currently_breaching
                ),
            })

        return findings

    def _generate_efficiency_findings(
        self, efficiency: dict
    ) -> list[dict]:
        """Generate findings for efficiency ratios classified as MEDIUM, HIGH, or CRITICAL.

        Args:
            efficiency: Output from EfficiencyModel.

        Returns:
            List of finding dicts for non-healthy efficiency ratios.
        """
        findings: list[dict] = []

        for ratio_name, data in efficiency.items():
            if not isinstance(data, dict):
                continue

            assessment = data.get("assessment", "HEALTHY")

            # Only flag MEDIUM, HIGH, or CRITICAL assessments
            if assessment not in ("MEDIUM", "HIGH", "CRITICAL"):
                continue

            value = data.get("value")
            recommendation = data.get("recommendation")

            # Map efficiency ratio name to a metric_name
            metric_name = self._efficiency_ratio_to_metric(ratio_name)

            findings.append({
                "finding_id": self._next_finding_id("efficiency"),
                "model_source": "efficiency",
                "severity": assessment,
                "title": f"{ratio_name} efficiency concern",
                "description": (
                    f"{ratio_name} value of {value} classified as "
                    f"{assessment}"
                ),
                "metric_name": metric_name,
                "current_value": value,
                "threshold": None,
                "recommendation": recommendation,
            })

        return findings

    def _generate_shard_balance_findings(
        self, shard_balance: dict
    ) -> list[dict]:
        """Generate findings for shard imbalance (minor_imbalance or worse).

        Args:
            shard_balance: Output from ShardBalanceModel.

        Returns:
            List of finding dicts for shard imbalance.
        """
        findings: list[dict] = []

        classification = shard_balance.get("classification", "well-balanced")

        if classification == "well-balanced":
            return findings

        # Map classification to severity
        severity_map = {
            "minor_imbalance": "LOW",
            "significant_imbalance": "MEDIUM",
            "severe_imbalance": "HIGH",
        }
        severity = severity_map.get(classification, "MEDIUM")

        if severity not in self.VALID_SEVERITIES:
            return findings

        # Use the highest CV as the current value
        cpu_cv = max(
            shard_balance.get("cpu_cv_sustained", 0),
            shard_balance.get("cpu_cv_peak", 0),
        )
        memory_cv = max(
            shard_balance.get("memory_cv_sustained", 0),
            shard_balance.get("memory_cv_peak", 0),
        )
        max_cv = max(cpu_cv, memory_cv)

        hottest_node = shard_balance.get("hottest_node_id", "unknown")
        imbalance_type = shard_balance.get("imbalance_type", "none")

        findings.append({
            "finding_id": self._next_finding_id("shard_balance"),
            "model_source": "shard_balance",
            "severity": severity,
            "title": f"Shard imbalance detected ({classification})",
            "description": (
                f"Shard balance classified as {classification} "
                f"(CV={max_cv:.4f}, type={imbalance_type}). "
                f"Hottest node: {hottest_node}"
            ),
            "metric_name": "ShardBalance",
            "current_value": round(max_cv, 4),
            "threshold": ShardBalanceModel.CV_WELL_BALANCED,
            "recommendation": self._get_shard_balance_recommendation(
                classification, imbalance_type
            ),
        })

        return findings

    def _generate_correlation_findings(
        self, correlations_output: dict
    ) -> list[dict]:
        """Generate one finding per strong correlation (|r| > 0.7).

        Severity comes from the pair, not from r. This used to read

            if abs_r > 0.9: severity = "HIGH"

        which made correlation strength stand in for consequence. The clearest
        relationship in a healthy cache is NetworkBytesIn ↔ EngineCPUUtilization
        -- CPU rising with traffic, which is the cache working -- and at r=0.97
        that healthy coupling outranked a real r=0.85 throttling-latency
        problem in the same report. See `MetricPair`.

        Args:
            correlations_output: Output from CorrelationModel.

        Returns:
            List of finding dicts for strong correlations.
        """
        findings: list[dict] = []

        correlation_list = correlations_output.get("correlations", [])

        for corr in correlation_list:
            metric_a = corr.get("metric_a", "")
            metric_b = corr.get("metric_b", "")
            r_value = corr.get("r_value", 0)
            interpretation = corr.get("interpretation", "")
            # Defaults to LOW rather than HIGH: a pair whose severity went
            # missing should not shout.
            severity = corr.get("severity", "LOW")

            # INFO correlations stay observations. A healthy coupling is not a
            # low-priority problem, it is not a problem, and a findings list is
            # a list of things to act on. It remains in the `correlations`
            # output for the narrative to draw on -- suppressing the finding
            # must not suppress the measurement.
            if severity not in self.VALID_SEVERITIES:
                continue

            findings.append({
                "finding_id": self._next_finding_id("correlation"),
                "model_source": "correlation",
                "severity": severity,
                "title": (
                    f"Strong correlation: {metric_a} ↔ {metric_b}"
                ),
                "description": (
                    f"Pearson r={r_value:.4f} between {metric_a} and "
                    f"{metric_b}. {interpretation}"
                ),
                "metric_name": metric_a,
                # A correlation is about a pair, so metric_a alone does not
                # identify it. Without this, every finding sharing a left-hand
                # metric deduplicated against the others: on the example fleet
                # that silently dropped EngineCPU ↔ NewConnections in favour of
                # EngineCPU ↔ ReadLatency on four of the six clusters the fleet
                # had at the time.
                "dedup_key": f"{metric_a}|{metric_b}",
                "current_value": r_value,
                "threshold": CorrelationModel.CORRELATION_THRESHOLD,
                "recommendation": interpretation,
            })

        return findings

    def _apply_workload_context(
        self, findings: list[dict], workload_class: str
    ) -> list[dict]:
        """Apply workload context to suppress or adjust finding severity.

        Workload context rules:
        - "rate-limiter": suppress hit_rate findings if severity is MEDIUM
          (60-80% hit rate is expected for write-heavy workloads)
        - "session-store": suppress eviction findings if severity is MEDIUM
          (evictions expected with volatile-lru policy)
        - "cache-aside": upgrade hit rate findings from MEDIUM to HIGH if
          hit rate < 70% (hit rate is more critical for cache-aside)

        Args:
            findings: List of finding dicts to adjust.
            workload_class: The workload classification string.

        Returns:
            Filtered and adjusted findings list.
        """
        if not workload_class or workload_class == "unknown":
            return findings

        adjusted: list[dict] = []

        for finding in findings:
            metric_name = finding.get("metric_name", "")
            severity = finding.get("severity", "")

            # Rate-limiter: suppress hit_rate findings at MEDIUM severity
            if workload_class == "rate-limiter":
                if (
                    metric_name == "CacheHitRate"
                    and severity == "MEDIUM"
                ):
                    continue  # Suppress — 60-80% hit rate is expected

            # Session-store: suppress eviction findings at MEDIUM severity
            if workload_class == "session-store":
                if (
                    metric_name == "EvictionsPerMinute"
                    and severity == "MEDIUM"
                ):
                    continue  # Suppress — evictions expected with volatile-lru

            # Cache-aside: upgrade hit rate from MEDIUM to HIGH
            if workload_class == "cache-aside":
                if (
                    metric_name == "CacheHitRate"
                    and severity == "MEDIUM"
                ):
                    finding = dict(finding)  # Copy before mutation
                    finding["severity"] = "HIGH"
                    finding["description"] = (
                        finding["description"]
                        + " (upgraded: cache-aside workload expects >80% hit rate)"
                    )

            adjusted.append(finding)

        return adjusted

    def _deduplicate_findings(self, findings: list[dict]) -> list[dict]:
        """Deduplicate findings where multiple models flag the same metric.

        When the same metric_name is flagged by multiple models of the same
        type, retain only the finding with the highest severity. Findings from
        different model_sources are never deduplicated against each other —
        a percentile finding and a utilization finding for the same metric
        represent different conclusions and must both be preserved.

        Args:
            findings: List of finding dicts.

        Returns:
            Deduplicated list retaining highest severity per (model_source, metric).
        """
        # Group findings by (model_source, dedup_key or metric_name) —
        # different models should never suppress each other.
        #
        # `dedup_key` exists because metric_name is not always sufficient to
        # identify a conclusion. Correlation findings are the case that proved
        # it: they set metric_name to the *first* metric of the pair, so
        # EngineCPU ↔ ReadLatency and EngineCPU ↔ NewConnections shared a key
        # and the lower-severity one was silently discarded — two distinct
        # mechanisms collapsed into one because they happened to share a
        # left-hand metric. A finding that identifies more than one metric must
        # say so here; the same principle as keying on model_source at all.
        grouped: dict[tuple[str, str], list[dict]] = {}
        for finding in findings:
            key = (
                finding.get("model_source", ""),
                finding.get("dedup_key") or finding.get("metric_name", ""),
            )
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(finding)

        deduplicated: list[dict] = []

        for _key, group in grouped.items():
            if len(group) == 1:
                deduplicated.append(group[0])
            else:
                # Keep the one with highest severity within same model_source
                best = max(
                    group,
                    key=lambda f: self.SEVERITY_ORDER.get(
                        f.get("severity", "LOW"), 0
                    ),
                )
                deduplicated.append(best)

        return deduplicated

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_base_metric(metric_key: str) -> str:
        """Extract the base metric name from a metric key.

        Strips the statistic suffix (e.g., "_Maximum", "_Average", "_Sum").

        Args:
            metric_key: Full metric key like "EngineCPUUtilization_Maximum".

        Returns:
            Base metric name like "EngineCPUUtilization".
        """
        suffixes = ["_Maximum", "_Average", "_Sum", "_Minimum"]
        for suffix in suffixes:
            if metric_key.endswith(suffix):
                return metric_key[: -len(suffix)]
        return metric_key

    @staticmethod
    def _get_threshold_for_severity(
        threshold_level: ThresholdLevel | None,
        severity: str,
        metric_name: str,
    ) -> float | None:
        """Get the threshold value corresponding to a severity level.

        Args:
            threshold_level: ThresholdLevel for the metric.
            severity: The severity string.
            metric_name: The metric name (for inverted metric handling).

        Returns:
            The threshold value, or None if not defined.
        """
        if threshold_level is None:
            return None

        if severity == "CRITICAL":
            return threshold_level.critical
        elif severity == "HIGH":
            return threshold_level.high
        elif severity == "MEDIUM":
            return threshold_level.medium
        elif severity == "LOW":
            return threshold_level.low
        return None

    @staticmethod
    def _get_percentile_recommendation(
        metric_name: str, severity: str
    ) -> str:
        """Get a recommendation string for a percentile finding.

        Args:
            metric_name: The metric name.
            severity: The finding severity.

        Returns:
            Recommendation string.
        """
        recommendations = {
            "EngineCPUUtilization": (
                "Monitor CPU trend; scale up node type if sustained"
            ),
            "DatabaseMemoryUsagePercentage": (
                "Memory pressure detected; scale up or reduce dataset"
            ),
            "CacheHitRate": (
                "Low hit rate indicates cache misses; review TTL and "
                "key patterns"
            ),
            "ReplicationLag": (
                "Replication lag detected; check write volume and "
                "replica health"
            ),
            # Keyed on the derived per-minute names, because that is what the
            # thresholds are registered under and therefore what a percentile
            # finding's metric_name says.
            "EvictionsPerMinute": (
                "Eviction rate elevated; increase memory or review TTL policy"
            ),
            "SuccessfulReadRequestLatency": (
                "Latency elevated; check CPU and connection load"
            ),
            "NewConnectionsPerMinute": (
                "High new connection rate; implement connection pooling"
            ),
            "ThrottledCmdsPerMinute": (
                "Requests are being throttled; raise the ECPU limit or reduce "
                "per-command cost"
            ),
        }
        return recommendations.get(
            metric_name,
            f"Monitor {metric_name}; investigate if degradation continues",
        )

    @staticmethod
    def _classify_trend_severity(
        metric_name: str, slope_per_week: float, direction: str
    ) -> str:
        """Classify trend severity based on metric and slope magnitude.

        Args:
            metric_name: The metric name.
            slope_per_week: Slope per week value.
            direction: Trend direction ("rising" or "declining").

        Returns:
            Severity string.
        """
        # Memory rising fast is more critical
        if metric_name == "DatabaseMemoryUsagePercentage":
            if abs(slope_per_week) > 5:
                return "HIGH"
            return "MEDIUM"

        # CacheHitRate declining is concerning
        if metric_name == "CacheHitRate":
            if abs(slope_per_week) > 10:
                return "HIGH"
            return "MEDIUM"

        # ReplicationLag rising is concerning
        if metric_name == "ReplicationLag":
            if abs(slope_per_week) > 1:
                return "HIGH"
            return "MEDIUM"

        # Other metrics with trends
        return "MEDIUM"

    @staticmethod
    def _get_trend_recommendation(metric_name: str, direction: str) -> str:
        """Get recommendation for a trend finding.

        Args:
            metric_name: The metric name.
            direction: Trend direction.

        Returns:
            Recommendation string.
        """
        if metric_name == "DatabaseMemoryUsagePercentage" and direction == "rising":
            return (
                "Memory usage growing steadily; plan capacity increase "
                "before reaching critical threshold"
            )
        if metric_name == "CacheHitRate" and direction == "declining":
            return (
                "Hit rate declining; investigate key pattern changes or "
                "TTL effectiveness"
            )
        if metric_name == "Evictions" and direction == "rising":
            return (
                "Evictions trending upward; memory may be insufficient "
                "for growing dataset"
            )
        if metric_name == "ReplicationLag" and direction == "rising":
            return (
                "Replication lag increasing; check write throughput and "
                "replica capacity"
            )
        return (
            f"{metric_name} showing {direction} trend; monitor and "
            f"investigate if operationally significant"
        )

    @staticmethod
    def _get_dominant_metric(
        classification: str, scores: dict
    ) -> str:
        """Get the primary metric name for a utilization classification.

        Args:
            classification: The utilization classification.
            scores: Utilization scores dict.

        Returns:
            The relevant metric name.
        """
        metric_map = {
            "CPU-BOUND": "EngineCPUUtilization",
            "MEMORY-BOUND": "DatabaseMemoryUsagePercentage",
            "NETWORK-BOUND": "NetworkBandwidth",
            "IDLE": "EngineCPUUtilization",
            "OVER-PROVISIONED": "EngineCPUUtilization",
            "HEAVY": "EngineCPUUtilization",
            "SATURATED": "EngineCPUUtilization",
        }
        return metric_map.get(classification, "EngineCPUUtilization")

    @staticmethod
    def _get_dominant_value(
        classification: str, scores: dict
    ) -> float | None:
        """Get the primary metric value for a utilization classification.

        Args:
            classification: The utilization classification.
            scores: Utilization scores dict with cpu_p95, memory_max, and
                    either network_p95 (node-based) or
                    network_throttle_score (serverless).

        Returns:
            The relevant metric value, or None when the axis was not measured.
        """
        if classification in ("CPU-BOUND", "IDLE", "OVER-PROVISIONED",
                              "HEAVY", "SATURATED"):
            return scores.get("cpu_p95")
        elif classification == "MEMORY-BOUND":
            return scores.get("memory_max")
        elif classification == "NETWORK-BOUND":
            # Two different quantities depending on cluster type, so ask for
            # both rather than defaulting to one and reporting a serverless
            # throttle score as a bandwidth percentage.
            value = scores.get("network_p95")
            if value is None:
                value = scores.get("network_throttle_score")
            return value
        return scores.get("cpu_p95")

    @staticmethod
    def _get_breach_recommendation(
        metric_name: str, severity: str, currently_breaching: bool
    ) -> str:
        """Get recommendation for a breach finding.

        Args:
            metric_name: The metric name.
            severity: Finding severity.
            currently_breaching: Whether the breach is active.

        Returns:
            Recommendation string.
        """
        if currently_breaching:
            prefix = "Active breach — immediate action recommended. "
        else:
            prefix = ""

        recommendations = {
            "EngineCPUUtilization": (
                f"{prefix}Scale up node type or optimize hot commands"
            ),
            "DatabaseMemoryUsagePercentage": (
                f"{prefix}Scale memory or reduce dataset size"
            ),
            "ReplicationLag": (
                f"{prefix}Check write throughput and replica capacity"
            ),
            "EvictionsPerMinute": (
                f"{prefix}Increase memory or implement TTL-based expiry"
            ),
            "ThrottledCmdsPerMinute": (
                f"{prefix}Raise the ECPU limit or reduce per-command cost"
            ),
        }
        return recommendations.get(
            metric_name,
            f"{prefix}Monitor {metric_name}; scale up if breaches increase",
        )

    @staticmethod
    def _efficiency_ratio_to_metric(ratio_name: str) -> str:
        """Map an efficiency ratio name to its primary metric.

        Args:
            ratio_name: The ratio name (e.g., "hit_rate").

        Returns:
            The associated metric name.
        """
        mapping = {
            "hit_rate": "CacheHitRate",
            "ttl_coverage": "TTLCoverage",
            "eviction_pressure": "Evictions",
            "connection_utilization": "CurrConnections",
            "network_burst_risk": "NetworkBandwidth",
            "write_amplification": "ReplicationBytes",
            "read_write_ratio": "ReadWriteRatio",
        }
        return mapping.get(ratio_name, ratio_name)

    @staticmethod
    def _get_shard_balance_recommendation(
        classification: str, imbalance_type: str
    ) -> str:
        """Get recommendation for a shard balance finding.

        Args:
            classification: Balance classification.
            imbalance_type: Type of imbalance (structural/intermittent).

        Returns:
            Recommendation string.
        """
        if imbalance_type == "structural":
            return (
                "Structural shard imbalance detected; review hash slot "
                "distribution and key patterns for hot-shard concentration"
            )
        elif imbalance_type == "intermittent":
            return (
                "Intermittent shard imbalance detected; investigate "
                "burst traffic patterns hitting specific shards"
            )
        return (
            "Shard imbalance detected; review key distribution across slots"
        )


# ---------------------------------------------------------------------------
# AnalysisOrchestrator
# ---------------------------------------------------------------------------


class AnalysisOrchestrator:
    """Coordinates the full analysis pipeline.

    Loads metrics and inventory JSON files, correlates clusters between them,
    runs all 10 models per cluster in the defined order, assembles the output
    analysis JSON, and writes it atomically to the output path.
    """

    MODELS_APPLIED = [
        "percentile",
        "trend",
        "utilization",
        "breach",
        "efficiency",
        "shard_balance",
        "traffic_pattern",
        "steadiness",
        "correlation",
        "workload",
    ]

    def __init__(self, config: AnalysisConfig):
        """Initialize with config, create ThresholdRegistry, instantiate models.

        Args:
            config: AnalysisConfig with paths and options.
        """
        self.config = config
        self.threshold_registry = ThresholdRegistry()

        # Instantiate all model classes
        self.percentile_model = PercentileModel()
        self.workload_model = WorkloadModel()
        self.trend_model = TrendModel()
        self.utilization_model = UtilizationModel()
        self.breach_model = BreachModel(self.threshold_registry)
        self.efficiency_model = EfficiencyModel()
        self.shard_balance_model = ShardBalanceModel()
        self.traffic_pattern_model = TrafficPatternModel()
        self.steadiness_model = SteadinessModel()
        self.correlation_model = CorrelationModel()
        self.findings_generator = FindingsGenerator(self.threshold_registry)

    def run(self) -> int:
        """Execute the full analysis pipeline.

        1. Load and validate metrics.json and inventory.json
        2. Correlate clusters between files (warn on mismatches)
        3. For each cluster: run models in order, catch errors
        4. Assemble analysis.json output
        5. Atomic write (temp file + rename)
        6. Return exit code (0=success, 1=failure)

        Returns:
            Exit code: 0 on success, 1 on failure.
        """
        import time as _time

        start_time = _time.time()

        # --- Step 1: Load and validate input files ---
        # metrics_data is the small manifest under the sharded format (Phase 9),
        # or the whole legacy single file. The per-cluster series are streamed
        # one shard at a time below via the shared loader, so peak raw memory is
        # a single cluster's shard rather than the whole fleet.
        metrics_data = self._load_json(self.config.metrics_path, "metrics")
        if metrics_data is None:
            return 1

        inventory_data = self._load_json(self.config.inventory_path, "inventory")
        if inventory_data is None:
            return 1

        # --- Step 2: Correlate clusters ---
        cluster_ids = iter_cluster_ids(metrics_data, self.config.metrics_path)
        resolution_seconds = _resolution_seconds(metrics_data)
        raw_inventory_clusters = inventory_data.get("clusters", {})
        # Normalize: inventory may be a list of dicts or a dict keyed by cluster_id
        if isinstance(raw_inventory_clusters, list):
            inventory_clusters = {c["cluster_id"]: c for c in raw_inventory_clusters}
        else:
            inventory_clusters = raw_inventory_clusters

        if not cluster_ids:
            logger.error("No clusters found in metrics file")
            return 1

        # Log mismatches between files
        metrics_ids = set(cluster_ids)
        inventory_ids = set(inventory_clusters.keys())

        metrics_only = metrics_ids - inventory_ids
        inventory_only = inventory_ids - metrics_ids

        for cluster_id in sorted(metrics_only):
            logger.warning(
                "Cluster '%s' present in metrics but absent from inventory — "
                "will analyze without inventory context",
                cluster_id,
            )

        for cluster_id in sorted(inventory_only):
            logger.warning(
                "Cluster '%s' present in inventory but absent from metrics — "
                "skipping (no metric data)",
                cluster_id,
            )

        # --- Step 3: Per-cluster model loop ---
        cluster_results: dict = {}
        total_findings = 0

        for cluster_id in sorted(metrics_ids):
            if self.config.verbose:
                logger.debug("Analyzing cluster: %s", cluster_id)

            inv_data = inventory_clusters.get(cluster_id)
            # Stream one shard at a time: load the raw per-cluster series, analyze
            # it, keep only the small result, and let the raw dict be freed before
            # the next cluster. Peak raw memory is one shard, not the whole fleet.
            raw_cluster = load_cluster_metrics(
                metrics_data, self.config.metrics_path, cluster_id
            )
            # Stage 2 writes a compact format; models read a nested one.
            cluster_data = normalize_cluster_data(
                raw_cluster, inv_data, resolution_seconds
            )

            result = self._analyze_cluster(cluster_id, cluster_data, inv_data)
            cluster_results[cluster_id] = result
            total_findings += len(result.get("findings", []))

        # --- Step 4: Assemble output ---
        end_time = _time.time()
        duration = round(end_time - start_time, 2)

        output = {
            "metadata": {
                "pipeline_version": PIPELINE_VERSION,
                "source_metrics": self.config.metrics_path,
                "source_inventory": self.config.inventory_path,
                "analysis_timestamp": datetime.datetime.now(
                    datetime.timezone.utc
                ).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "models_applied": self.MODELS_APPLIED,
                "clusters_analyzed": len(cluster_results),
                "total_findings": total_findings,
                "analysis_duration_seconds": duration,
            },
            "clusters": cluster_results,
        }

        # --- Step 5: Atomic write ---
        try:
            self._atomic_write(output, self.config.output_path)
        except Exception as e:
            logger.error("Failed to write output file: %s", e)
            return 1

        logger.info(
            "Analysis complete: %d clusters, %d findings, %.2fs",
            len(cluster_results),
            total_findings,
            duration,
        )
        return 0

    def _load_json(self, path: str, label: str) -> dict | None:
        """Load and parse a JSON file.

        Args:
            path: File path to load.
            label: Human-readable label for error messages.

        Returns:
            Parsed dict, or None on failure.
        """
        if not os.path.exists(path):
            logger.error("%s file not found: %s", label.capitalize(), path)
            return None

        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            logger.error(
                "%s file is not valid JSON: %s — %s",
                label.capitalize(),
                path,
                e,
            )
            return None
        except OSError as e:
            logger.error(
                "Cannot read %s file: %s — %s", label, path, e
            )
            return None

        return data

    def _analyze_cluster(
        self, cluster_id: str, cluster_data: dict, inventory_data: dict | None
    ) -> dict:
        """Run all models for a single cluster.

        Executes models in the required order:
        1. PercentileModel (first — others depend on p95/max)
        2. WorkloadModel (second — contextualizes thresholds)
        3. Remaining models (any order)
        4. FindingsGenerator (last — aggregates all model outputs)

        Per-model errors are caught, logged, and recorded in the errors list.
        If PercentileModel fails, dependent models are skipped.

        Args:
            cluster_id: The cluster identifier.
            cluster_data: Per-cluster data from the metrics file.
            inventory_data: Per-cluster inventory metadata, or None.

        Returns:
            Dict with all model results and errors list.
        """
        import time as _time

        errors: list[dict] = []
        model_outputs: dict = {}
        percentile_failed = False

        # --- Run PercentileModel (FIRST) ---
        model_start = _time.time()
        try:
            percentiles = self.percentile_model.compute(
                cluster_data, inventory_data
            )
            model_outputs["percentiles"] = percentiles
            if self.config.verbose:
                elapsed = _time.time() - model_start
                logger.debug(
                    "  [%s] PercentileModel completed in %.3fs",
                    cluster_id,
                    elapsed,
                )
        except Exception as e:
            percentile_failed = True
            err = {
                "model": "percentile",
                "error": str(e),
                "impact": "Dependent models skipped",
            }
            errors.append(err)
            logger.error(
                "PercentileModel failed for cluster '%s': %s", cluster_id, e
            )
            model_outputs["percentiles"] = {}

        # --- Run WorkloadModel (SECOND) ---
        model_start = _time.time()
        try:
            workload = self.workload_model.compute(
                cluster_data, inventory_data
            )
            model_outputs["workload"] = workload
            if self.config.verbose:
                elapsed = _time.time() - model_start
                logger.debug(
                    "  [%s] WorkloadModel completed in %.3fs — class=%s",
                    cluster_id,
                    elapsed,
                    workload.get("workload_class", "unknown"),
                )
        except Exception as e:
            err = {"model": "workload", "error": str(e)}
            errors.append(err)
            logger.error(
                "WorkloadModel failed for cluster '%s': %s", cluster_id, e
            )
            model_outputs["workload"] = {
                "workload_class": "unknown",
                "command_profile": {},
                "read_write_ratio": 0.0,
                "dominant_family": "none",
            }

        # --- Run remaining models (skip if prerequisite PercentileModel failed) ---
        remaining_models = [
            ("trend", self.trend_model),
            ("utilization", self.utilization_model),
            ("breach", self.breach_model),
            ("efficiency", self.efficiency_model),
            ("shard_balance", self.shard_balance_model),
            ("traffic_pattern", self.traffic_pattern_model),
            ("steadiness", self.steadiness_model),
            ("correlation", self.correlation_model),
        ]

        for model_name, model_instance in remaining_models:
            if percentile_failed:
                err = {
                    "model": model_name,
                    "error": "Skipped — prerequisite PercentileModel failed",
                }
                errors.append(err)
                model_outputs[model_name] = None
                continue

            model_start = _time.time()
            try:
                result = model_instance.compute(cluster_data, inventory_data)
                model_outputs[model_name] = result
                if self.config.verbose:
                    elapsed = _time.time() - model_start
                    logger.debug(
                        "  [%s] %s completed in %.3fs",
                        cluster_id,
                        model_name,
                        elapsed,
                    )
            except Exception as e:
                err = {"model": model_name, "error": str(e)}
                errors.append(err)
                logger.error(
                    "%s failed for cluster '%s': %s", model_name, cluster_id, e
                )
                model_outputs[model_name] = None

        # --- Run FindingsGenerator (LAST) ---
        workload_class = model_outputs.get("workload", {}).get(
            "workload_class", "unknown"
        )
        try:
            findings = self.findings_generator.generate(
                model_outputs, workload_class, cluster_id
            )
            if self.config.verbose:
                logger.debug(
                    "  [%s] FindingsGenerator produced %d findings",
                    cluster_id,
                    len(findings),
                )
        except Exception as e:
            findings = []
            err = {"model": "findings", "error": str(e)}
            errors.append(err)
            logger.error(
                "FindingsGenerator failed for cluster '%s': %s",
                cluster_id,
                e,
            )

        # --- Assemble per-cluster output ---
        return {
            "workload_class": workload_class,
            "utilization": model_outputs.get("utilization"),
            "percentiles": model_outputs.get("percentiles", {}),
            "trends": model_outputs.get("trend"),
            "breaches": model_outputs.get("breach"),
            "efficiency": model_outputs.get("efficiency"),
            "shard_balance": model_outputs.get("shard_balance"),
            "traffic_pattern": model_outputs.get("traffic_pattern"),
            "steadiness": model_outputs.get("steadiness"),
            "correlations": (
                model_outputs.get("correlation", {}).get("correlations", [])
                if isinstance(model_outputs.get("correlation"), dict)
                else []
            ),
            "findings": findings,
            "errors": errors,
        }

    def _atomic_write(self, data: dict, output_path: str) -> None:
        """Write JSON data atomically via temp file + os.replace().

        Creates a temporary file in the same directory as the output path,
        writes the JSON content, then atomically renames to the final path.

        Args:
            data: The dict to serialize as JSON.
            output_path: Final output file path.

        Raises:
            OSError: If file operations fail.
        """
        output_dir = os.path.dirname(os.path.abspath(output_path))
        os.makedirs(output_dir, exist_ok=True)

        fd, tmp_path = tempfile.mkstemp(
            suffix=".tmp", prefix="analysis_", dir=output_dir
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, default=str)
            os.replace(tmp_path, output_path)
        except Exception:
            # Clean up temp file on failure
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: Argument list (defaults to sys.argv[1:]).

    Returns:
        Parsed namespace with metrics, inventory, output, profile, verbose.
    """
    parser = argparse.ArgumentParser(
        description=(
            "ElastiCache Metrics Analysis — Stage 3. Applies 9 mathematical "
            "models to characterize cluster health and produce findings."
        ),
    )
    parser.add_argument(
        "--metrics",
        required=True,
        help="Path to the metrics JSON file from Stage 2 (fetch_metrics.py)",
    )
    parser.add_argument(
        "--inventory",
        required=True,
        help="Path to the inventory JSON file from Stage 1 (discover_inventory.py)",
    )
    parser.add_argument(
        "--output",
        default="analysis.json",
        help="Output path for the analysis JSON (default: analysis.json)",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="AWS profile name (accepted for CLI consistency, not used for analysis)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Enable detailed logging with per-cluster progress and model timings",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Main entry point for the metrics analysis script.

    1. Parse CLI arguments
    2. Build AnalysisConfig
    3. Validate numpy import
    4. Run orchestrator
    5. Log summary
    6. Return exit code

    Args:
        argv: Argument list (defaults to sys.argv[1:]).

    Returns:
        Exit code: 0 on success, 1 on failure.
    """
    args = parse_args(argv)

    # Configure logging based on verbosity
    setup_logging(args.verbose)

    # Validate numpy is available (should be imported at top, but verify)
    try:
        import numpy  # noqa: F401
    except ImportError:
        logger.error(
            "numpy is not installed. Install it with: pip install numpy"
        )
        return 1

    # Build config
    config = AnalysisConfig(
        metrics_path=args.metrics,
        inventory_path=args.inventory,
        output_path=args.output,
        profile=args.profile,
        verbose=args.verbose,
    )

    logger.info(
        "Starting metrics analysis: metrics=%s, inventory=%s, output=%s",
        config.metrics_path,
        config.inventory_path,
        config.output_path,
    )

    # Run the orchestrator
    orchestrator = AnalysisOrchestrator(config)
    exit_code = orchestrator.run()

    if exit_code == 0:
        logger.info("Analysis completed successfully → %s", config.output_path)
    else:
        logger.error("Analysis failed with exit code %d", exit_code)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
