"""Unit tests for the Well-Architected pillar scoring (generate_html_report).

The score is a graded number a customer will quote, so it must reproduce the
skill's published formula exactly and be recomputable by hand. These tests pin:
the penalty table, pillar assignment (config pillar vs analysis model_source),
band thresholds, per-cluster weighting, and the score-then-average fleet rule
(never pool penalties fleet-wide).
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from generate_html_report import (  # noqa: E402
    _PILLAR_WEIGHTS,
    _analysis_pillar,
    _build_scores,
    _score_band,
    _score_cluster,
)


def _pillars(result):
    """{pillar_key: score} from a _score_cluster/_build_scores cluster result."""
    return {p["key"]: p["score"] for p in result["pillars"]}


class TestScoreBand:
    @pytest.mark.parametrize("score,band", [
        (100, "Excellent"), (90, "Excellent"), (89.9, "Good"), (70, "Good"),
        (69.9, "Needs Improvement"), (50, "Needs Improvement"),
        (49.9, "At Risk"), (0, "At Risk"),
    ])
    def test_band_thresholds(self, score, band):
        assert _score_band(score) == band


class TestPillarAssignment:
    def test_utilization_idle_is_sustainability(self):
        f = {"model_source": "utilization", "classification": "IDLE"}
        assert _analysis_pillar(f) == "sustainability"

    def test_utilization_over_provisioned_is_sustainability(self):
        f = {"model_source": "utilization", "classification": "OVER-PROVISIONED"}
        assert _analysis_pillar(f) == "sustainability"

    def test_utilization_saturated_is_performance(self):
        # A non-waste utilization verdict is a performance problem, not waste.
        f = {"model_source": "utilization", "classification": "SATURATED"}
        assert _analysis_pillar(f) == "performance"

    def test_correlation_is_excluded(self):
        assert _analysis_pillar({"model_source": "correlation"}) is None

    @pytest.mark.parametrize("ms", ["percentile", "breach", "efficiency",
                                    "shard_balance", "trend", "workload"])
    def test_other_analysis_models_are_performance(self, ms):
        assert _analysis_pillar({"model_source": ms}) == "performance"


class TestScoreCluster:
    def test_penalties_and_weighted_overall(self):
        # One CRITICAL security + one HIGH reliability config finding.
        # security = 100-25 = 75; reliability = 100-15 = 85; others 100.
        config = [
            {"pillar": "security", "severity": "CRITICAL"},
            {"pillar": "reliability", "severity": "HIGH"},
        ]
        result = _score_cluster([], config)
        p = _pillars(result)
        assert p["security"] == 75
        assert p["reliability"] == 85
        assert p["performance"] == 100
        assert p["cost_optimization"] == 100
        assert p["operational_excellence"] == 100
        assert p["sustainability"] == 100
        # Weighted: .25*85 + .25*100 + .20*75 + .15*100 + .10*100 + .05*100
        expected = (0.25 * 85 + 0.25 * 100 + 0.20 * 75 + 0.15 * 100
                    + 0.10 * 100 + 0.05 * 100)
        assert result["overall"] == pytest.approx(expected, abs=0.05)
        assert result["band"] == "Excellent"

    def test_penalties_accumulate_within_a_pillar(self):
        # Two CRITICALs on one pillar drive it to zero (100 - 50), floored at 0
        # only if it would go negative.
        config = [
            {"pillar": "reliability", "severity": "HIGH"},   # 15
            {"pillar": "reliability", "severity": "HIGH"},   # 15
            {"pillar": "reliability", "severity": "MEDIUM"},  # 8
            {"pillar": "reliability", "severity": "LOW"},     # 3
        ]
        assert _pillars(_score_cluster([], config))["reliability"] == 59

    def test_pillar_score_never_negative(self):
        config = [{"pillar": "security", "severity": "CRITICAL"} for _ in range(5)]
        assert _pillars(_score_cluster([], config))["security"] == 0

    def test_analysis_and_config_findings_both_count(self):
        analysis = [{"model_source": "utilization", "classification": "IDLE",
                     "severity": "LOW"}]  # sustainability -3
        config = [{"pillar": "security", "severity": "CRITICAL"}]  # security -25
        p = _pillars(_score_cluster(analysis, config))
        assert p["sustainability"] == 97
        assert p["security"] == 75

    def test_correlation_finding_costs_nothing(self):
        analysis = [{"model_source": "correlation", "severity": "HIGH"}]
        result = _score_cluster(analysis, [])
        assert result["overall"] == 100
        assert result["band"] == "Excellent"

    def test_weights_sum_to_one(self):
        assert sum(_PILLAR_WEIGHTS.values()) == pytest.approx(1.0)


class TestBuildScores:
    def _inv(self, ids):
        return {cid: {"cluster_id": cid} for cid in ids}

    def test_fleet_is_mean_of_cluster_scores_not_pooled(self):
        # A: clean (all 100). B: one CRITICAL security (security 75).
        analysis = {"a": {"findings": []}, "b": {"findings": []}}
        config = {"clusters": {
            "a": {"findings": []},
            "b": {"findings": [{"pillar": "security", "severity": "CRITICAL"}]},
        }}
        scores = _build_scores(analysis, config, self._inv(["a", "b"]))
        fleet_pillars = {p["key"]: p["score"] for p in scores["fleet"]["pillars"]}
        # Pooling would give 100-25=75; the correct score-then-average is
        # mean(100, 75) = 87.5. This is the whole point of the rule.
        assert fleet_pillars["security"] == pytest.approx(87.5, abs=0.05)
        # Fleet overall = mean of the two cluster overalls.
        a = scores["clusters"]["a"]["overall"]
        b = scores["clusters"]["b"]["overall"]
        assert scores["fleet"]["overall"] == pytest.approx((a + b) / 2, abs=0.05)

    def test_empty_fleet_scores_100(self):
        scores = _build_scores({}, {"clusters": {}}, {})
        assert scores["fleet"]["overall"] == 100.0
        assert scores["fleet"]["band"] == "Excellent"
        assert scores["clusters"] == {}

    def test_every_inventory_cluster_is_scored(self):
        scores = _build_scores({}, None, self._inv(["x", "y", "z"]))
        assert set(scores["clusters"]) == {"x", "y", "z"}
        # No findings anywhere -> every cluster is a clean 100.
        assert all(c["overall"] == 100 for c in scores["clusters"].values())

    def test_pillars_are_in_weight_order(self):
        scores = _build_scores({}, None, self._inv(["x"]))
        keys = [p["key"] for p in scores["clusters"]["x"]["pillars"]]
        assert keys == list(_PILLAR_WEIGHTS)
