"""Unit tests for analyze_metrics utility functions and scaffolding (Task 1).

Tests the AnalysisConfig dataclass, setup_logging, interpolate_small_gaps,
chunk_by_day, extract_last_n_days, safe_divide, and compute_nan_ratio.
"""

import math
import os
import sys

# Add scripts to path so we can import the module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from analyze_metrics import (
    AnalysisConfig,
    chunk_by_day,
    compute_nan_ratio,
    extract_last_n_days,
    interpolate_small_gaps,
    safe_divide,
    setup_logging,
)

# ---------------------------------------------------------------------------
# AnalysisConfig
# ---------------------------------------------------------------------------


class TestAnalysisConfig:
    def test_required_fields(self):
        config = AnalysisConfig(
            metrics_path="metrics.json", inventory_path="inventory.json"
        )
        assert config.metrics_path == "metrics.json"
        assert config.inventory_path == "inventory.json"

    def test_defaults(self):
        config = AnalysisConfig(
            metrics_path="m.json", inventory_path="i.json"
        )
        assert config.output_path == "analysis.json"
        assert config.profile is None
        assert config.verbose is False

    def test_all_fields(self):
        config = AnalysisConfig(
            metrics_path="m.json",
            inventory_path="i.json",
            output_path="out.json",
            profile="prod",
            verbose=True,
        )
        assert config.output_path == "out.json"
        assert config.profile == "prod"
        assert config.verbose is True


# ---------------------------------------------------------------------------
# setup_logging
# ---------------------------------------------------------------------------


class TestSetupLogging:
    def test_verbose_does_not_raise(self):
        setup_logging(verbose=True)

    def test_non_verbose_does_not_raise(self):
        setup_logging(verbose=False)


# ---------------------------------------------------------------------------
# interpolate_small_gaps
# ---------------------------------------------------------------------------


class TestInterpolateSmallGaps:
    def test_empty_list(self):
        assert interpolate_small_gaps([]) == []

    def test_no_nans(self):
        vals = [1.0, 2.0, 3.0, 4.0]
        assert interpolate_small_gaps(vals) == vals

    def test_small_gap_interpolated(self):
        # Gap of 2 NaN with max_gap=3 → should interpolate
        vals = [1.0, float("nan"), float("nan"), 4.0]
        result = interpolate_small_gaps(vals, max_gap=3)
        assert abs(result[1] - 2.0) < 1e-9
        assert abs(result[2] - 3.0) < 1e-9

    def test_large_gap_preserved(self):
        # Gap of 3 NaN with max_gap=3 → should NOT interpolate
        vals = [1.0, float("nan"), float("nan"), float("nan"), 5.0]
        result = interpolate_small_gaps(vals, max_gap=3)
        assert math.isnan(result[1])
        assert math.isnan(result[2])
        assert math.isnan(result[3])

    def test_gap_at_start(self):
        # NaN at start, no left boundary → backward fill
        vals = [float("nan"), 2.0, 3.0]
        result = interpolate_small_gaps(vals, max_gap=3)
        assert abs(result[0] - 2.0) < 1e-9

    def test_gap_at_end(self):
        # NaN at end, no right boundary → forward fill
        vals = [1.0, 2.0, float("nan")]
        result = interpolate_small_gaps(vals, max_gap=3)
        assert abs(result[2] - 2.0) < 1e-9

    def test_boundary_gap_size(self):
        # Gap of exactly 1 with max_gap=2 → should interpolate
        vals = [0.0, float("nan"), 4.0]
        result = interpolate_small_gaps(vals, max_gap=2)
        assert abs(result[1] - 2.0) < 1e-9


# ---------------------------------------------------------------------------
# chunk_by_day
# ---------------------------------------------------------------------------


class TestChunkByDay:
    def test_empty_input(self):
        assert chunk_by_day([], []) == []

    def test_single_day(self):
        ts = ["2024-01-01T00:00:00Z", "2024-01-01T12:00:00Z"]
        vals = [1.0, 2.0]
        chunks = chunk_by_day(ts, vals)
        assert len(chunks) == 1
        assert chunks[0] == [1.0, 2.0]

    def test_multiple_days(self):
        ts = [
            "2024-01-01T00:00:00Z",
            "2024-01-01T05:00:00Z",
            "2024-01-02T00:00:00Z",
            "2024-01-03T08:00:00Z",
        ]
        vals = [1.0, 2.0, 3.0, 4.0]
        chunks = chunk_by_day(ts, vals)
        assert len(chunks) == 3
        assert chunks[0] == [1.0, 2.0]
        assert chunks[1] == [3.0]
        assert chunks[2] == [4.0]

    def test_chronological_order(self):
        # Provide timestamps out of order within same day
        ts = ["2024-01-02T00:00:00Z", "2024-01-01T00:00:00Z"]
        vals = [2.0, 1.0]
        chunks = chunk_by_day(ts, vals)
        # Days should be sorted chronologically
        assert len(chunks) == 2
        assert chunks[0] == [1.0]  # Jan 1
        assert chunks[1] == [2.0]  # Jan 2


# ---------------------------------------------------------------------------
# extract_last_n_days
# ---------------------------------------------------------------------------


class TestExtractLastNDays:
    def test_empty_input(self):
        assert extract_last_n_days([], [], 7) == []

    def test_extracts_recent_days(self):
        ts = [
            "2024-01-10T00:00:00Z",
            "2024-01-11T00:00:00Z",
            "2024-01-12T00:00:00Z",
            "2024-01-13T00:00:00Z",
            "2024-01-14T00:00:00Z",
        ]
        vals = [10.0, 20.0, 30.0, 40.0, 50.0]
        result = extract_last_n_days(ts, vals, days=2)
        # Cutoff: Jan 14 - 2 days = Jan 12. Values from Jan 12 onward.
        assert 30.0 in result
        assert 40.0 in result
        assert 50.0 in result
        assert 10.0 not in result

    def test_all_days_included(self):
        ts = ["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"]
        vals = [1.0, 2.0]
        result = extract_last_n_days(ts, vals, days=30)
        assert result == [1.0, 2.0]


# ---------------------------------------------------------------------------
# safe_divide
# ---------------------------------------------------------------------------


class TestSafeDivide:
    def test_normal_division(self):
        assert safe_divide(10, 2) == 5.0

    def test_zero_denominator_default_none(self):
        assert safe_divide(10, 0) is None

    def test_zero_denominator_custom_default(self):
        assert safe_divide(10, 0, default=0.0) == 0.0

    def test_zero_numerator(self):
        assert safe_divide(0, 5) == 0.0

    def test_negative_values(self):
        assert safe_divide(-10, 2) == -5.0


# ---------------------------------------------------------------------------
# compute_nan_ratio
# ---------------------------------------------------------------------------


class TestComputeNanRatio:
    def test_empty_list(self):
        assert compute_nan_ratio([]) == 0.0

    def test_no_nans(self):
        assert compute_nan_ratio([1.0, 2.0, 3.0]) == 0.0

    def test_all_nans(self):
        assert compute_nan_ratio([float("nan"), float("nan")]) == 1.0

    def test_mixed(self):
        ratio = compute_nan_ratio([1.0, float("nan"), 3.0])
        assert abs(ratio - 1 / 3) < 1e-9

    def test_half_nans(self):
        ratio = compute_nan_ratio([1.0, float("nan"), 2.0, float("nan")])
        assert abs(ratio - 0.5) < 1e-9
