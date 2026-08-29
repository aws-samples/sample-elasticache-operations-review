"""Pins severity as a property of the metric pair, not of |r|.

The finding site graded correlations on strength alone:

    if abs_r > 0.9: severity = "HIGH"

which makes "how certain is this relationship" stand in for "how much does it
matter". Those come apart hardest on healthy caches. The tightest correlation a
well-behaved cache produces is NetworkBytesIn ↔ EngineCPUUtilization -- CPU
rising with the traffic that causes it, the cache doing its job -- and the better
it behaves the higher r goes. At r=0.97 that healthy coupling was reported HIGH,
ranked above a genuine r=0.85 throttling-latency problem in the same report. The
rule was strongest where it was most wrong.

The second half of the defect is `abs()`. The interpretations are causal
sentences ("Snapshots interfering with replication"), and a correlation whose
sign is opposite to the described mechanism is evidence *against* that sentence.
Reporting it anyway attaches a confident explanation to data that contradicts it.

Both halves are one mistake: a signed, dimensionless measure of association was
used as if it were a graded measure of harm.
"""
import dataclasses
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from analyze_metrics import (  # noqa: E402
    CorrelationModel,
    FindingsGenerator,
    MetricPair,
    ThresholdRegistry,
)


def _pair(metric_a, metric_b):
    return next(p for p in CorrelationModel.METRIC_PAIRS
                if p.metric_a == metric_a and p.metric_b == metric_b)


def _findings(correlations):
    generator = FindingsGenerator(ThresholdRegistry())
    return generator._generate_correlation_findings(
        {"correlations": correlations})


def _corr(pair, r_value, severity):
    return {
        "metric_a": pair.metric_a,
        "metric_b": pair.metric_b,
        "r_value": r_value,
        "interpretation": pair.interpretation,
        "severity": severity,
    }


class TestSeverityComesFromThePairNotFromR:

    def test_a_near_perfect_healthy_correlation_produces_no_finding(self):
        """The defect, stated as a test.

        r=0.97 between traffic and CPU is the strongest signal in the report and
        the least actionable thing in it.
        """
        pair = _pair("NetworkBytesIn", "EngineCPUUtilization")
        assert pair.max_severity == "INFO"
        assert _findings([_corr(pair, 0.97, "INFO")]) == []

    def test_a_weaker_throttling_correlation_outranks_it(self):
        """Ordering is the point: the weaker signal is the bigger problem."""
        throttling = _pair("TrafficManagementActive",
                           "SuccessfulReadRequestLatency")
        findings = _findings([_corr(throttling, 0.85, throttling.max_severity)])
        assert len(findings) == 1
        assert findings[0]["severity"] == "HIGH"

    def test_severity_does_not_move_with_r(self):
        """The same pair at three strengths earns the same severity.

        If severity tracked r at all, this is where it would show.
        """
        pair = _pair("EngineCPUUtilization", "SuccessfulReadRequestLatency")
        severities = {
            _findings([_corr(pair, r, pair.max_severity)])[0]["severity"]
            for r in (0.72, 0.85, 0.99)
        }
        assert severities == {"MEDIUM"}

    def test_a_missing_severity_defaults_low_not_high(self):
        """A pair added without a severity should not shout."""
        findings = _findings([{
            "metric_a": "EngineCPUUtilization",
            "metric_b": "NewConnections",
            "r_value": 0.95,
            "interpretation": "Connection churn driving CPU utilization",
        }])
        assert findings[0]["severity"] == "LOW"

    def test_info_findings_are_suppressed_but_the_correlation_is_not(self):
        """Suppressing the finding must not suppress the measurement.

        The correlations list is what the narrative reads; dropping healthy
        couplings from it would hide that the cache is traffic-driven, which is
        useful context even though it is not a finding.
        """
        pair = _pair("NetworkBytesIn", "EngineCPUUtilization")
        model_output = {"correlations": [_corr(pair, 0.97, "INFO")]}
        assert _findings(model_output["correlations"]) == []
        assert len(model_output["correlations"]) == 1

    def test_no_pair_claims_a_severity_the_pipeline_cannot_rank(self):
        """INFO is deliberately outside VALID_SEVERITIES; nothing else may be.

        A severity string the dedup ordering does not know would sort to the
        bottom silently.
        """
        rankable = FindingsGenerator.SEVERITY_ORDER
        for pair in CorrelationModel.METRIC_PAIRS:
            assert pair.max_severity == "INFO" or pair.max_severity in rankable, (
                f"{pair.metric_a} ↔ {pair.metric_b} claims severity "
                f"{pair.max_severity!r}, which SEVERITY_ORDER cannot rank"
            )


def _cluster(series):
    """A single-node cluster carrying the named metric series."""
    timestamps = [f"2026-01-01T{h:02d}:00:00Z" for h in range(24)]
    metrics = {
        name: {"Maximum": {"timestamps": timestamps, "values": values}}
        for name, values in series.items()
    }
    return {"cluster_type": "node-based", "region": "us-east-1",
            "nodes": {"n-001": {"metrics": metrics}}}


def _correlate(pair, sign):
    """Run the real model over data correlated in the given direction.

    Reimplementing the sign rule in the test would only assert that I can write
    the same three lines twice -- and hand-reconstructing production logic is how
    an earlier test in this suite encoded its own off-by-one. So this builds a
    series pair whose Pearson r is close to +1 or -1 and asks compute() what it
    makes of it. Returns the correlation entry, or None if the model dropped it.
    """
    rising = [float(i) for i in range(24)]
    other = rising if sign > 0 else [23.0 - v for v in rising]
    output = CorrelationModel().compute(
        _cluster({pair.metric_a: rising, pair.metric_b: other}), None)
    matches = [c for c in output["correlations"]
               if c["metric_a"] == pair.metric_a
               and c["metric_b"] == pair.metric_b]
    return matches[0] if matches else None


class TestDirectionIsCheckedNotDiscarded:

    def test_snapshots_that_reduce_replication_lag_are_not_interference(self):
        """A strong negative r is evidence against the pair's own sentence.

        A cache whose replication lag falls while snapshots run has its backups
        scheduled in its quiet window -- good practice. Under abs() that was
        reported as "Snapshots interfering with replication" at HIGH, since the
        magnitude alone cleared 0.9.
        """
        pair = _pair("SaveInProgress", "ReplicationLag")
        assert pair.expected_sign == 1
        result = _correlate(pair, -1)
        assert result is None, (
            "a strong negative correlation is still reported as "
            f"{result['interpretation']!r} at {result['severity']}"
            if result else ""
        )

    def test_the_expected_direction_still_reports(self):
        """The sign check must not make the pair unreachable."""
        pair = _pair("SaveInProgress", "ReplicationLag")
        result = _correlate(pair, 1)
        assert result is not None
        assert result["interpretation"] == pair.interpretation
        assert result["severity"] == "MEDIUM"
        assert result["r_value"] > 0

    def test_evictions_falling_while_misses_rise_is_not_the_cascade(self):
        assert _correlate(_pair("Evictions", "CacheMisses"), -1) is None

    def test_every_pair_reports_in_its_expected_direction(self):
        """Swept across the whole registry, not just the two pairs above.

        A pair whose expected_sign was set backwards would never fire at all --
        a silent loss of coverage, which is harder to notice than a false
        positive.
        """
        for pair in CorrelationModel.METRIC_PAIRS:
            result = _correlate(pair, pair.expected_sign)
            assert result is not None, (
                f"{pair.metric_a} ↔ {pair.metric_b} reports nothing even in its "
                "own expected direction; expected_sign may be inverted"
            )

    def test_no_pair_reports_against_its_expected_direction(self):
        for pair in CorrelationModel.METRIC_PAIRS:
            if pair.inverse_interpretation is not None:
                continue
            assert _correlate(pair, -pair.expected_sign) is None, (
                f"{pair.metric_a} ↔ {pair.metric_b} still reports when the sign "
                "contradicts its interpretation"
            )


class TestThePairDefinitionsAreValidated:

    def test_an_unrankable_severity_is_rejected_at_construction(self):
        with pytest.raises(ValueError, match="max_severity"):
            MetricPair("A", "B", "why", max_severity="URGENT")

    def test_a_nonsense_sign_is_rejected_at_construction(self):
        with pytest.raises(ValueError, match="expected_sign"):
            MetricPair("A", "B", "why", max_severity="LOW", expected_sign=0)

    def test_pairs_are_immutable(self):
        """The registry is shared across clusters; a mutable pair would let one
        cluster's analysis change the next cluster's severities."""
        pair = CorrelationModel.METRIC_PAIRS[0]
        with pytest.raises(dataclasses.FrozenInstanceError):
            pair.max_severity = "HIGH"

    def test_no_duplicate_pairs(self):
        keys = [(p.metric_a, p.metric_b) for p in CorrelationModel.METRIC_PAIRS]
        assert len(keys) == len(set(keys))


class TestTwoPairsSharingAMetricAreTwoFindings:
    """A correlation is about a pair, so one metric cannot identify it.

    Dedup keys on (model_source, metric_name), and every correlation finding set
    metric_name to metric_a. Four of the seven pairs start with
    EngineCPUUtilization, so they all shared a key and only the highest-severity
    one survived -- on the example fleet, EngineCPU ↔ NewConnections vanished on
    four of the six clusters it had at the time, in favour of
    EngineCPU ↔ ReadLatency.

    This is the same defect as the (model_source, metric_name) fix one level
    down: that change was made because "different models represent different
    conclusions", and two different metric pairs are also different conclusions.
    """

    def _deduplicate(self, findings):
        return FindingsGenerator(ThresholdRegistry())._deduplicate_findings(
            findings)

    def test_both_survive_deduplication(self):
        latency = _pair("EngineCPUUtilization", "SuccessfulReadRequestLatency")
        churn = _pair("EngineCPUUtilization", "NewConnections")
        assert latency.metric_a == churn.metric_a, (
            "this test is only meaningful while the two pairs share a left-hand "
            "metric")

        findings = _findings([
            _corr(latency, 0.93, latency.max_severity),
            _corr(churn, 0.98, churn.max_severity),
        ])
        assert len(findings) == 2

        survivors = self._deduplicate(findings)
        titles = {f["title"] for f in survivors}
        assert len(survivors) == 2, (
            "deduplication dropped one of two unrelated correlations that "
            f"happen to share a metric; kept only {titles}")

    def test_the_lower_severity_one_is_the_one_that_would_be_lost(self):
        """Names the asymmetry: dedup keeps the loudest, so the quiet finding
        disappears -- the failure mode you do not notice."""
        latency = _pair("EngineCPUUtilization", "SuccessfulReadRequestLatency")
        churn = _pair("EngineCPUUtilization", "NewConnections")
        survivors = self._deduplicate(_findings([
            _corr(latency, 0.93, latency.max_severity),
            _corr(churn, 0.98, churn.max_severity),
        ]))
        assert any("NewConnections" in f["title"] for f in survivors)

    def test_genuine_duplicates_of_the_same_pair_still_collapse(self):
        """The fix must not disable deduplication for correlations."""
        pair = _pair("EngineCPUUtilization", "SuccessfulReadRequestLatency")
        survivors = self._deduplicate(_findings([
            _corr(pair, 0.93, "LOW"),
            _corr(pair, 0.93, "MEDIUM"),
        ]))
        assert len(survivors) == 1
        assert survivors[0]["severity"] == "MEDIUM"

    def test_findings_without_a_dedup_key_still_key_on_metric_name(self):
        """Every other model relies on the metric_name fallback."""
        survivors = self._deduplicate([
            {"model_source": "percentile", "metric_name": "M", "severity": "LOW"},
            {"model_source": "percentile", "metric_name": "M", "severity": "HIGH"},
        ])
        assert len(survivors) == 1
        assert survivors[0]["severity"] == "HIGH"


class TestTheDocAgreesWithTheRegistry:
    """references/mathematical-models.md publishes the severity per pair.

    The doc had said NetworkBytesIn ↔ EngineCPU was "Workload-driven, not
    pathological" all along while the code reported it HIGH -- the documentation
    was right and nothing checked it against the code. Parsing the table rather
    than restating it is what makes that impossible to repeat.
    """

    DOC = os.path.join(os.path.dirname(__file__), "..", "references",
                       "mathematical-models.md")

    def _rows(self):
        """(severity, sign) keyed by the doc's abbreviated pair label."""
        import re
        with open(self.DOC, encoding="utf-8") as handle:
            text = handle.read()
        rows = {}
        for line in text.splitlines():
            if "↔" not in line or not line.startswith("|"):
                continue
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if len(cells) < 5:
                continue
            sign = 1 if "+" in cells[1] else -1
            severity = re.sub(r"[^A-Z]", "", cells[2].split("(")[0])
            if severity:
                rows[cells[0]] = (severity, sign)
        return rows

    def test_the_table_is_actually_parsed(self):
        """Guards the guard: a reformat must not silently pass everything."""
        assert len(self._rows()) == len(CorrelationModel.METRIC_PAIRS), (
            f"parsed {len(self._rows())} pair rows for "
            f"{len(CorrelationModel.METRIC_PAIRS)} registered pairs"
        )

    def test_every_pair_matches_its_documented_severity_and_sign(self):
        rows = self._rows()
        mismatched = []
        for pair in CorrelationModel.METRIC_PAIRS:
            # The doc abbreviates: EngineCPUUtilization -> EngineCPU,
            # SuccessfulReadRequestLatency -> SuccessfulReadLatency.
            match = next(
                (v for label, v in rows.items()
                 if pair.metric_a.replace("Utilization", "") in label
                 and pair.metric_b.replace("Request", "")
                     .replace("Utilization", "") in label),
                None)
            if match is None:
                mismatched.append(
                    f"{pair.metric_a} ↔ {pair.metric_b}: no row in the doc")
                continue
            severity, sign = match
            if severity != pair.max_severity:
                mismatched.append(
                    f"{pair.metric_a} ↔ {pair.metric_b}: code says "
                    f"{pair.max_severity}, doc says {severity}")
            if sign != pair.expected_sign:
                mismatched.append(
                    f"{pair.metric_a} ↔ {pair.metric_b}: code expects sign "
                    f"{pair.expected_sign:+d}, doc says {sign:+d}")
        assert not mismatched, "\n  ".join([""] + mismatched)


class TestTheModelEndToEnd:
    """A correlation must survive from compute() to a finding with the right
    severity, because the two halves of the fix live in different methods."""

    def test_a_healthy_traffic_cpu_coupling_reaches_no_finding(self):
        rising = [float(i) for i in range(24)]
        cluster = _cluster({
            "NetworkBytesIn": [v * 1000.0 for v in rising],
            "EngineCPUUtilization": [v * 2.0 + 1.0 for v in rising],
        })
        output = CorrelationModel().compute(cluster, None)
        found = [c for c in output["correlations"]
                 if c["metric_a"] == "NetworkBytesIn"]
        assert found, "the correlation itself must still be measured"
        assert found[0]["severity"] == "INFO"
        assert _findings(output["correlations"]) == []

    def test_an_inverted_snapshot_correlation_is_dropped_by_compute(self):
        rising = [float(i) for i in range(24)]
        cluster = _cluster({
            "SaveInProgress": rising,
            "ReplicationLag": [24.0 - v for v in rising],
        })
        output = CorrelationModel().compute(cluster, None)
        assert not [c for c in output["correlations"]
                    if c["metric_a"] == "SaveInProgress"], (
            "a strong negative r is being reported under an interpretation that "
            "requires a positive one"
        )
