"""Pins the rules that must require a sustained condition, not one datapoint.

`network_burst_risk` was graded on the maximum of a 14-day series. Over 4,032
datapoints per node, "max > 80" means "one bad five-minute bucket, ever" — and
on the live fleet a single 80.55% spike against a p95 of 3.54% produced a MEDIUM
"monitor for throttling" on a cluster using 3.5% of its baseline allowance. Burst
allowances exist to absorb exactly that spike, so the finding described the
feature working.

The same statistic drove `_compute_network_score`, where the consequence is
worse: `_classify_network_level` puts anything over 80% in "High", and
`_classify_combination` makes Network=High mean NETWORK-BOUND regardless of CPU
and memory. One burst relabelled an idle cluster as network-bound and recommended
scaling it — the first thing a reader acts on.

This is the defect family the plan calls **a statistic used as if it were a
condition**: `max` answers "did this ever happen", the threshold asks "is this
happening". Neither site had a single test, which is how both shipped. The fix is
not a higher threshold — that hides the class — but grading on p95 and reporting
the peak separately, so a one-off burst is visible without being a verdict.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from analyze_metrics import (  # noqa: E402
    EfficiencyModel,
    UtilizationModel,
)

# 14 days at 300s resolution. The point of the defect is that it needs a long
# series to bite: one spike in twenty datapoints is 5% of the window, but one
# spike in 4,032 is noise that `max` promotes to a verdict.
FOURTEEN_DAYS = 4032


def _node_cluster(metrics):
    """A node-based cluster carrying pre-nested metric series."""
    return {
        "cluster_type": "node-based",
        "region": "us-east-1",
        "nodes": {"cluster-0001-001": {"metrics": metrics}},
    }


def _series(values):
    return {"Maximum": {"timestamps": [], "values": list(values)}}


def _mostly(baseline, spikes, total=FOURTEEN_DAYS):
    """A quiet series with a few spikes in it."""
    return [baseline] * (total - len(spikes)) + list(spikes)


class TestNetworkBurstRisk:
    """EfficiencyModel.network_burst_risk — the ratio the plan named."""

    def _risk(self, in_values=None, out_values=None):
        metrics = {}
        if in_values is not None:
            metrics["NetworkBaselineMaxUsageInPercentage"] = _series(in_values)
        if out_values is not None:
            metrics["NetworkBaselineMaxUsageOutPercentage"] = _series(out_values)
        return EfficiencyModel()._compute_network_burst_risk(
            _node_cluster(metrics))

    def test_one_spike_on_a_quiet_cluster_is_not_a_finding(self):
        """The exact shape of the shipped false positive: 80.55 over p95 3.54."""
        result = self._risk(in_values=_mostly(3.5, [80.55]))
        assert result["assessment"] == "HEALTHY", (
            f"a single 80.55% spike against a p95 of {result['value']}% is still "
            "producing a finding; burst allowances are designed to absorb it"
        )
        assert result["recommendation"] is None

    def test_the_spike_is_reported_even_though_it_is_not_a_finding(self):
        """Suppressing the verdict must not suppress the observation.

        A reader looking for "did we ever come close" needs the peak. Dropping it
        would trade a false positive for a blind spot.
        """
        result = self._risk(in_values=_mostly(3.5, [80.55]))
        assert result["peak"] == 80.55
        assert result["value"] < 10.0, "value must be the sustained level, not the peak"

    def test_sustained_high_usage_still_fires(self):
        """The fix must not make the check unreachable."""
        result = self._risk(in_values=[85.0] * FOURTEEN_DAYS)
        assert result["assessment"] == "MEDIUM"
        assert result["recommendation"]

    def test_sustained_over_allowance_is_high(self):
        result = self._risk(in_values=[120.0] * FOURTEEN_DAYS)
        assert result["assessment"] == "HIGH"
        assert "scale" in result["recommendation"].lower()

    def test_repeated_breaches_on_an_otherwise_quiet_cluster_are_medium(self):
        """A cluster that exceeds its allowance regularly has a real problem.

        p95 alone would call this healthy: 2% of datapoints over 100% leaves the
        95th percentile in the quiet band. The breach fraction is what catches a
        cluster whose allowance is genuinely exhausted in bursts, which is the
        case a pure-percentile fix would have lost.
        """
        breaches = [130.0] * int(FOURTEEN_DAYS * 0.02)
        result = self._risk(in_values=_mostly(4.0, breaches))
        assert result["assessment"] == "MEDIUM", (
            f"p95={result['value']}, breach_fraction={result['breach_fraction']}"
        )
        assert result["breach_fraction"] > 0.01

    def test_a_single_breach_is_below_the_sustained_fraction(self):
        """One datapoint over 100% in 14 days is not a sustained condition."""
        result = self._risk(in_values=_mostly(4.0, [130.0]))
        assert result["assessment"] == "HEALTHY"
        assert 0 < result["breach_fraction"] <= 0.01

    def test_the_busier_direction_is_the_one_graded(self):
        """In and Out are separate allowances; the worse one decides."""
        result = self._risk(in_values=[2.0] * FOURTEEN_DAYS,
                            out_values=[90.0] * FOURTEEN_DAYS)
        assert result["assessment"] == "MEDIUM"
        assert result["value"] == pytest.approx(90.0)

    def test_a_missing_metric_is_not_applicable_not_healthy(self):
        """Unmeasured is not a pass. Tier 2 metrics are not always collected."""
        result = self._risk()
        assert result["assessment"] == "not_applicable"
        assert result["value"] is None
        assert result["peak"] is None

    def test_an_all_zero_series_is_healthy_not_not_applicable(self):
        """Published-but-idle is a measurement of zero traffic.

        The old code returned not_applicable whenever the max was 0.0, conflating
        "no data" with "no traffic". Only the absent metric is unmeasured.
        """
        result = self._risk(in_values=[0.0] * FOURTEEN_DAYS)
        assert result["assessment"] == "HEALTHY"
        assert result["value"] == 0.0


class TestNetworkAxisOfTheUtilizationMatrix:
    """The louder instance: a burst must not relabel the cluster."""

    def test_the_axis_is_a_percentile_not_a_maximum(self):
        score = UtilizationModel()._compute_network_score(
            _mostly(3.5, [95.0]), [])
        assert score < 10.0, (
            f"network score is {score}; keyed on the maximum, one burst makes "
            "the axis High and the whole cluster NETWORK-BOUND"
        )

    def test_one_burst_does_not_make_a_cluster_network_bound(self):
        """End to end through the classifier, which is what a reader sees."""
        model = UtilizationModel()
        score = model._compute_network_score(_mostly(3.5, [95.0]), [])
        assert model._classify_network_level(score) == "Low"

    def test_sustained_saturation_still_classifies_as_high(self):
        model = UtilizationModel()
        score = model._compute_network_score([88.0] * FOURTEEN_DAYS, [])
        assert model._classify_network_level(score) == "High"
        assert model._classify_combination("Low", "Low", "High") == "NETWORK-BOUND"

    def test_a_network_saturated_idle_cluster_is_not_told_to_scale_down(self):
        """The inverted recommendation this file's first run uncovered.

        OVER-PROVISIONED is evaluated before the network rule and read only two
        axes, so a cluster idle on CPU and memory while sustaining >80% of its
        bandwidth allowance was classified OVER-PROVISIONED -> "Scale down node
        type". Baseline bandwidth is a property of the node type, so following
        that advice cuts the allowance the cluster is already exhausting.

        This is the worst failure mode in the report: not a missing finding but
        a confident recommendation pointing the wrong way.
        """
        model = UtilizationModel()
        classification = model._classify_combination("Low", "Low", "High")
        assert classification == "NETWORK-BOUND", (
            f"a bandwidth-saturated cluster is classified {classification}, whose "
            f"recommendation is {model.RECOMMENDATIONS[classification]!r}"
        )
        assert "scale down" not in model.RECOMMENDATIONS[classification].lower()

    def test_a_genuinely_idle_cluster_is_still_over_provisioned(self):
        """The network guard must not swallow the case it sits in front of.

        A cluster serving traffic at Low/Low/Medium is OVER-PROVISIONED. IDLE now
        requires confirmed no traffic (has_traffic=False), not merely all-Low
        axes -- a lightly-loaded but serving cache is a right-size candidate, not
        a decommission one.
        """
        model = UtilizationModel()
        assert model._classify_combination("Low", "Low", "Medium") == \
            "OVER-PROVISIONED"
        assert model._classify_combination(
            "Low", "Low", "Low", has_traffic=False) == "IDLE"
        # Same low axes, but serving traffic -> right-size, not decommission.
        assert model._classify_combination(
            "Low", "Low", "Low", has_traffic=True) == "OVER-PROVISIONED"

    def test_the_busier_direction_wins(self):
        score = UtilizationModel()._compute_network_score(
            [5.0] * FOURTEEN_DAYS, [85.0] * FOURTEEN_DAYS)
        assert score == pytest.approx(85.0)

    def test_no_data_scores_zero_rather_than_raising(self):
        assert UtilizationModel()._compute_network_score([], []) == 0.0

    def test_the_memory_axis_deliberately_stays_on_maximum(self):
        """Not every max is a defect, and this one is intentional.

        Memory does not burst and recover the way traffic does: a cluster that
        reached 95% memory was at 95% memory, and its peak is the number that
        matters for OOM risk. Recorded as a test so a future sweep for
        max-keyed rules does not "fix" it by symmetry.
        """
        score = UtilizationModel()._compute_memory_score(_mostly(30.0, [95.0]))
        assert score == pytest.approx(95.0)


class TestTheScoreKeysNameTheirStatistic:
    """A key called network_max holding a p95 is the next bug.

    The rename is part of the fix, not cosmetics: the previous key promised a
    maximum and the report printed it as one.
    """

    def _scores(self, cluster):
        return UtilizationModel().compute(
            cluster, None)["utilization_scores"]

    def test_node_based_publishes_network_p95(self):
        cluster = _node_cluster({
            "EngineCPUUtilization": _series([40.0] * 100),
            "DatabaseMemoryUsagePercentage": _series([50.0] * 100),
            "NetworkBaselineUsageInPercentage": _series([20.0] * 100),
        })
        scores = self._scores(cluster)
        assert "network_p95" in scores
        assert "network_max" not in scores, (
            "network_max is back, and it now holds a percentile -- the exact "
            "mislabel the rename removed"
        )
