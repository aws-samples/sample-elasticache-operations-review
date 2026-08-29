"""Pins every threshold to the unit of the series it grades.

Four of the defects found in this pipeline were the same mistake: a number read
with the wrong statistic or the wrong unit. `NewConnections` was the eighth
instance — `references/thresholds.md` documents it as "Sum per minute" and
`fetch_metrics.py` collects it as a Sum over a 300-second period, so the MEDIUM
boundary of 1000/min fired at about 200/min. `Evictions` and `ThrottledCmds`
had the same shape.

The lesson from those four was: **assert the unit and the statistic, not merely
that a value came back.** A test that checks `classify_value` returns MEDIUM at
1200 passes whether 1200 means per minute or per five minutes, which is why the
existing threshold tests were green throughout.

So these tests do not check individual numbers. They check the property that
makes the numbers meaningful:

1. Every registered threshold declares a unit from a closed set.
2. Every threshold whose unit is not what CloudWatch delivers is keyed on a
   DERIVED series name, never on the raw metric — because the raw series is
   what every model reads by default.
3. The derived series that those names refer to are actually produced by
   `normalize_cluster_data`, with the right conversion factor, at whatever
   resolution Stage 2 reports.
4. Nothing looks up a raw rate metric in the registry, since `classify_value`
   answers HEALTHY for an unknown name rather than raising.

Point 4 is the one a correctness test cannot cover: the failure is silent.
"""
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from analyze_metrics import (  # noqa: E402
    PercentileModel,
    ThresholdLevel,
    ThresholdRegistry,
    normalize_cluster_data,
)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
ANALYZE_SRC = os.path.join(REPO_ROOT, "scripts", "analyze_metrics.py")

# What CloudWatch actually delivers per metric, from fetch_metrics.py's
# MetricDefinition list. A count collected as a Sum over `period` seconds is a
# per-period total, which is a per-minute rate only when period == 60.
COUNT_METRICS_COLLECTED_AS_PERIOD_SUM = ("Evictions", "NewConnections",
                                         "ThrottledCmds")


def _registry():
    return ThresholdRegistry()


def _all_thresholds(reg):
    """Every (name, ThresholdLevel) pair, reaching past the private dict.

    The registry exposes only per-name lookup, and the property under test is
    about the whole set — a new entry added without a unit is exactly the
    regression this file exists to catch, and it is invisible to a lookup-based
    test that does not know the new name.
    """
    return dict(reg._thresholds)


def _cluster_with(metric_name, values, resolution=300):
    """A node-based cluster in Stage 2's compact shape, carrying one metric."""
    return {
        "cluster_type": "node-based",
        "region": "us-east-1",
        "timestamps_5min": [
            f"2026-08-01T{i // 12:02d}:{(i % 12) * 5:02d}:00Z"
            for i in range(len(values))
        ],
        "nodes": {"cluster-0001-001": {metric_name: {"Sum": list(values)}}},
        "errors": [],
    }


class TestEveryThresholdDeclaresItsUnit:
    def test_all_units_are_from_the_closed_set(self):
        for name, level in _all_thresholds(_registry()).items():
            assert level.unit in ThresholdRegistry.UNITS, (
                f"{name}'s threshold declares unit {level.unit!r}, which is "
                f"not in ThresholdRegistry.UNITS. Add it there deliberately, "
                "or fix the typo -- an undeclared unit is how a per-period sum "
                "gets compared against a per-minute boundary."
            )

    def test_an_undeclared_unit_is_rejected_at_construction(self):
        """The guard has to fail loudly, at import, not at comparison time.

        A threshold with a nonsense unit that constructs fine produces findings
        that look exactly like correct ones.
        """
        with pytest.raises(ValueError, match="not one of"):
            ThresholdLevel(critical=1.0, unit="per_fortnight")

    def test_the_default_unit_is_percent(self):
        """Most metrics are percentages, so the default is the safe majority.

        Stated as a test because the default is what an author who forgets to
        pass `unit` will get, and it should be the option least likely to be
        silently wrong.
        """
        assert ThresholdLevel(critical=90.0).unit == "percent"


class TestRateThresholdsAreKeyedOnDerivedSeries:
    """The core of D11: the threshold name and the series name must agree."""

    @pytest.mark.parametrize("raw", COUNT_METRICS_COLLECTED_AS_PERIOD_SUM)
    def test_the_raw_count_metric_has_no_threshold(self, raw):
        """Grading the raw Sum is the bug. Registering it is how it comes back.

        The raw series is a total over the collection period; every documented
        threshold for these metrics is per minute. Leaving the raw name
        unregistered means a lookup returns None instead of quietly grading a
        5-minute total against a 1-minute boundary.
        """
        assert _registry().get_threshold(raw) is None, (
            f"{raw} has a threshold registered under its raw name. It is "
            f"collected as a Sum over the collection period, so its p95 is a "
            f"per-period total; the thresholds are per minute. Register "
            f"{raw}PerMinute instead."
        )

    @pytest.mark.parametrize("raw", COUNT_METRICS_COLLECTED_AS_PERIOD_SUM)
    def test_the_derived_name_is_registered_per_minute(self, raw):
        level = _registry().get_threshold(f"{raw}PerMinute")
        assert level is not None, (
            f"{raw}PerMinute has no threshold, so nothing grades {raw} at all "
            "-- the metric is collected and then ignored."
        )
        assert level.unit == "per_minute"

    def test_the_registry_lists_exactly_these_rate_metrics(self):
        """Pins RATE_METRICS against this file's own list.

        normalize_cluster_data derives a series for each name in RATE_METRICS.
        If one is added there without a threshold, or a threshold is added
        without the derivation, the two halves drift apart silently.
        """
        assert set(ThresholdRegistry.RATE_METRICS) == set(
            COUNT_METRICS_COLLECTED_AS_PERIOD_SUM)


class TestTheDerivedSeriesExistAndConvertCorrectly:
    """A threshold keyed on a series nobody produces grades nothing."""

    @pytest.mark.parametrize("raw", COUNT_METRICS_COLLECTED_AS_PERIOD_SUM)
    def test_normalize_produces_the_series_the_threshold_names(self, raw):
        cluster = _cluster_with(raw, [600.0] * 20)
        metrics = normalize_cluster_data(
            cluster, None, 300.0)["nodes"]["cluster-0001-001"]["metrics"]
        assert f"{raw}PerMinute" in metrics, (
            f"{raw}PerMinute is registered as a threshold but "
            "normalize_cluster_data does not derive it, so the check never "
            "fires no matter how bad the metric gets."
        )

    @pytest.mark.parametrize("raw", COUNT_METRICS_COLLECTED_AS_PERIOD_SUM)
    def test_the_conversion_divides_by_the_period_in_minutes(self, raw):
        """600 per 5-minute period is 120 per minute, not 600.

        This is the arithmetic the whole defect reduces to.
        """
        cluster = _cluster_with(raw, [600.0] * 20)
        metrics = normalize_cluster_data(
            cluster, None, 300.0)["nodes"]["cluster-0001-001"]["metrics"]
        assert metrics[f"{raw}PerMinute"]["Sum"]["values"][0] == 120.0

    def test_the_divisor_follows_stage_2s_resolution(self):
        """Not hardcoded /5. A run collected at 60s needs no division at all.

        The cheap version of this fix divided by a constant 5, which is right
        only while Stage 2's period stays 300 and fails silently otherwise.
        """
        for resolution, expected in ((60.0, 600.0), (300.0, 120.0),
                                     (900.0, 40.0)):
            cluster = _cluster_with("Evictions", [600.0] * 20)
            metrics = normalize_cluster_data(
                cluster, None, resolution)["nodes"][
                    "cluster-0001-001"]["metrics"]
            got = metrics["EvictionsPerMinute"]["Sum"]["values"][0]
            assert got == expected, (
                f"at {resolution}s resolution, 600 per period is {expected}/min, "
                f"got {got}"
            )

    def test_the_raw_series_survives_alongside_the_derived_one(self):
        """Eviction pressure and breach counting want the period total.

        The conversion adds a series; it must not replace one, or the models
        reading a count get a rate without knowing it.
        """
        cluster = _cluster_with("Evictions", [600.0] * 20)
        metrics = normalize_cluster_data(
            cluster, None, 300.0)["nodes"]["cluster-0001-001"]["metrics"]
        assert metrics["Evictions"]["Sum"]["values"][0] == 600.0

    def test_nulls_stay_null_through_the_conversion(self):
        """Unmeasured is not zero, and 0/5 is a measurement of zero."""
        cluster = _cluster_with("Evictions", [None, 600.0] + [300.0] * 18)
        metrics = normalize_cluster_data(
            cluster, None, 300.0)["nodes"]["cluster-0001-001"]["metrics"]
        values = metrics["EvictionsPerMinute"]["Sum"]["values"]
        assert values[0] is None
        assert values[1] == 120.0

    def test_a_zero_resolution_derives_nothing_rather_than_dividing_by_zero(self):
        cluster = _cluster_with("Evictions", [600.0] * 20)
        metrics = normalize_cluster_data(
            cluster, None, 0.0)["nodes"]["cluster-0001-001"]["metrics"]
        assert "EvictionsPerMinute" not in metrics


class TestTheDefectItselfDoesNotRecur:
    """The end-to-end shape of D11, stated as the number a customer would see."""

    def test_ordinary_connection_churn_is_not_a_medium_finding(self):
        """1200 new connections per 5-minute period is 240/min -- healthy.

        Before the fix, that 1200 was compared against the per-minute MEDIUM
        boundary of 1000 and produced "implement connection pooling" for a fleet
        with unremarkable churn. The threshold numbers did not change; the unit
        of the value they grade did.
        """
        cluster = _cluster_with("NewConnections", [1200.0] * 20)
        normalized = normalize_cluster_data(cluster, None, 300.0)
        percentiles = PercentileModel().compute(normalized, None)

        reg = _registry()
        graded = {}
        for key, data in percentiles.items():
            if "p95" not in data:
                continue
            base = key.rsplit("_", 1)[0]
            level = reg.get_threshold(base)
            if level is not None:
                graded[base] = reg.classify_value(base, data["p95"])

        assert graded == {"NewConnectionsPerMinute": "HEALTHY"}, (
            f"expected the per-minute series to be the only one graded, and to "
            f"be healthy at 240/min; got {graded}"
        )

    @pytest.mark.parametrize("per_period", [
        0.0, 50.0, 99.0, 100.0, 101.0, 500.0, 999.0, 1000.0, 1001.0, 5000.0,
    ])
    def test_evictions_severity_is_unchanged_by_the_unit_move(self, per_period):
        """Evictions was NOT a unit defect, and the fix must not pretend it was.

        Its doc said "per 5-min period" and the code compared the raw 300s Sum
        against those numbers, so the two already agreed. Moving it onto the
        shared per-minute series is a consistency change only: 1000/period became
        200/min, 100/period became 20/min.

        The old verdict is reconstructed here rather than tabulated by hand,
        because hand-tabulating it is how this test first went wrong -- the
        boundaries are exclusive (`>`), so 100/period was HEALTHY, not MEDIUM,
        and an expectation table encodes that off-by-one silently.

        Written after the first attempt at this fix folded Evictions in with the
        two genuinely-broken metrics and left its numbers at 1000 and 100 per
        MINUTE -- making the check five times less sensitive and suppressing a
        correct MEDIUM on the example fleet's prod-api-cache.
        """
        # The check exactly as it stood before: the raw 300s Sum against 1000/100.
        if per_period > 1000.0:
            old_verdict = "HIGH"
        elif per_period > 100.0:
            old_verdict = "MEDIUM"
        else:
            old_verdict = "HEALTHY"

        cluster = _cluster_with("Evictions", [per_period] * 20)
        normalized = normalize_cluster_data(cluster, None, 300.0)
        p95 = PercentileModel().compute(
            normalized, None)["EvictionsPerMinute_Sum"]["p95"]
        new_verdict = _registry().classify_value("EvictionsPerMinute", p95)

        assert new_verdict == old_verdict, (
            f"{per_period}/period is {p95}/min and now grades {new_verdict}; "
            f"before the unit move it graded {old_verdict}. Evictions' "
            "strictness was not supposed to change."
        )

    def test_real_connection_storms_still_fire(self):
        """The fix must not simply make the check unreachable.

        6000/min is a genuine connection storm: 30000 per 5-minute period.
        """
        cluster = _cluster_with("NewConnections", [30000.0] * 20)
        normalized = normalize_cluster_data(cluster, None, 300.0)
        percentiles = PercentileModel().compute(normalized, None)
        p95 = percentiles["NewConnectionsPerMinute_Sum"]["p95"]
        assert p95 == 6000.0
        assert _registry().classify_value(
            "NewConnectionsPerMinute", p95) == "HIGH"


class TestTheDocsAgreeWithTheRegistry:
    """references/thresholds.md is what the agent reads. It must match."""

    def test_the_rate_metric_sections_state_per_minute(self):
        """The doc's unit label and the registry's declared unit are one fact.

        Recorded in two places, so a test has to hold them together -- this is
        the same documentation-to-code agreement class as
        tests/test_wa_mapping_accuracy.py.

        The heading names the DERIVED series, not the raw metric. When this test
        was written the doc said "### Evictions (Sum per minute, sustained)",
        which is a true statement about a series that is not the one graded: the
        threshold is registered under EvictionsPerMinute, and a reader who greps
        the doc's own heading in analysis.json finds the ungraded five-minute
        total. Naming the derived series and the raw source separately is what
        the generated heading does now -- see tests/test_thresholds_doc.py, which
        pins the whole table rather than this one property.
        """
        path = os.path.join(REPO_ROOT, "references", "thresholds.md")
        with open(path, encoding="utf-8") as handle:
            text = handle.read()

        for raw in COUNT_METRICS_COLLECTED_AS_PERIOD_SUM:
            heading = re.search(rf"^###\s+{raw}PerMinute\b(.*)$", text, re.M)
            assert heading, (
                f"references/thresholds.md has no section for {raw}PerMinute, "
                f"which is the name {raw}'s threshold is registered under."
            )
            assert "per minute" in heading.group(1).lower(), (
                f"{raw}'s section heading in references/thresholds.md is "
                f"{heading.group(0)!r}. The registry grades it per minute; a "
                "heading that says otherwise is the ambiguity that caused D11."
            )
            assert f"`{raw}`" in heading.group(1), (
                f"{raw}PerMinute's heading does not say which raw metric it is "
                f"derived from, so a reader cannot connect it to the {raw} "
                "series CloudWatch actually delivers."
            )

    def test_the_analysis_module_names_the_conversion(self):
        """The 'why' has to survive in the code, not only in PLAN.md.

        The next author to see a division by resolution_seconds needs to find
        the reason next to it, or the cheap fix looks like an improvement.
        """
        with open(ANALYZE_SRC, encoding="utf-8") as handle:
            source = handle.read()
        assert "_add_per_minute_series" in source
        assert "RATE_METRICS" in source
