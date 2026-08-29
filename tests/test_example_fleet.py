"""Tests for the synthetic example fleet generator.

``examples/metrics.json`` is ~113 MB -- above GitHub's file size limit -- so the
example fleet ships as a generator rather than as committed JSON. That only works
if running the generator twice produces the same fleet, which is what these tests
pin.

They also pin the fixture's internal coherence. The fleet's job is to be the
input that catches cross-stage bugs, and it has already caught two (a memory
statistic that was never collected, and an ECPU count compared against percentage
thresholds). A fixture whose own numbers contradict each other cannot do that job:
if the ECPU total disagrees with the request counts that produced it, a
rate-vs-count bug downstream looks plausible.

The generator is invoked as a subprocess because that is how it runs in practice,
and because it shells out to the real Stage 3 and Stage 3.5 itself.
"""
import json
import os
import subprocess
import sys
from collections import Counter

import pytest

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
GENERATOR = os.path.join(REPO_ROOT, "scripts", "make_example_fleet.py")

sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))
from _metrics_store import iter_cluster_ids, load_cluster_metrics  # noqa: E402

# Metadata that is expected to differ between two runs: when the run happened,
# how long it took, and where its inputs lived.
VOLATILE_METADATA = {
    "analysis_timestamp",
    "analysis_duration_seconds",
    "collection_timestamp",
    "collection_duration_seconds",
    "check_timestamp",
    "duration_seconds",
    "source_metrics",
    "source_inventory",
}


def _generate(out_dir):
    result = subprocess.run(
        [sys.executable, GENERATOR, "--output", str(out_dir)],
        capture_output=True, text=True, cwd=REPO_ROOT,
    )
    assert result.returncode == 0, result.stderr
    docs = {
        name: json.load(open(os.path.join(out_dir, name)))
        for name in ("inventory.json", "metrics.json", "analysis.json",
                     "config_findings.json", "pricing.json",
                     "report_data.json")
    }
    # Phase 9: metrics.json is now a KB-scale manifest and the per-cluster series
    # live in one shard each under metrics/. Load every shard through the shared
    # loader -- both so the coherence tests can reach a cluster's series and so
    # reproducibility covers the shards, not only the manifest.
    manifest_path = os.path.join(out_dir, "metrics.json")
    shards = {
        cid: load_cluster_metrics(docs["metrics.json"], manifest_path, cid)
        for cid in iter_cluster_ids(docs["metrics.json"], manifest_path)
    }
    docs["metrics_shards"] = shards
    return docs


def _strip_volatile(doc):
    """Drop run-specific metadata, leaving only what must be reproducible."""
    if isinstance(doc, dict) and isinstance(doc.get("metadata"), dict):
        doc = dict(doc)
        doc["metadata"] = {k: v for k, v in doc["metadata"].items()
                           if k not in VOLATILE_METADATA}
    return doc


@pytest.fixture(scope="module")
def fleet(tmp_path_factory):
    return _generate(tmp_path_factory.mktemp("fleet"))


class TestReproducibility:
    """Two runs must agree, or the committed generator is not a golden fixture."""

    def test_two_runs_produce_the_same_fleet(self, tmp_path_factory, fleet):
        second = _generate(tmp_path_factory.mktemp("fleet2"))
        for name, doc in fleet.items():
            assert _strip_volatile(doc) == _strip_volatile(second[name]), name

    def test_security_group_ids_are_stable_across_processes(
            self, tmp_path_factory, fleet):
        # These were built with the builtin hash(), which is salted per process,
        # so every run produced different group IDs. Two subprocesses is the only
        # way to catch that -- within one process it looks deterministic.
        second = _generate(tmp_path_factory.mktemp("fleet3"))
        ids = [c["security_groups"][0]["group_id"]
               for c in fleet["inventory.json"]["clusters"]
               if c.get("security_groups")]
        ids2 = [c["security_groups"][0]["group_id"]
                for c in second["inventory.json"]["clusters"]
                if c.get("security_groups")]
        assert ids and ids == ids2


class TestFixtureCoherence:
    """The fixture's own numbers must agree with each other."""

    def test_serverless_ecpu_total_agrees_with_request_counts(self, fleet):
        # Per the ElastiCache pricing docs a simple <=1 KB GET/SET costs 1 ECPU
        # and larger payloads cost proportionally more, so ECPUs per request
        # should sit in a plausible band -- not orders of magnitude away, which
        # is what an independently-chosen number would give.
        metrics = fleet["metrics_shards"]["prod-serverless-events"]
        m = metrics["metrics"]
        ecpu = m["ElastiCacheProcessingUnits"]["Sum"][0]
        requests = m["CacheHits"]["Sum"][0] + m["CacheMisses"]["Sum"][0]

        assert requests > 0
        assert 1.0 <= ecpu / requests <= 10.0

    def test_serverless_ecpu_rate_is_a_meaningful_fraction_of_its_limit(
            self, fleet):
        # A cache pinned near 0% of its ECPU ceiling cannot distinguish a working
        # percentage derivation from a broken one, which is how the raw-count bug
        # survived. This is the fixture guarding its own diagnostic value.
        scores = (fleet["analysis.json"]["clusters"]["prod-serverless-events"]
                  ["utilization"]["utilization_scores"])
        assert 20.0 < scores["cpu_p95"] < 80.0

    def test_no_two_clusters_share_memory_and_network_scores(self, fleet):
        # Memory and network levels were keyed on the load profile alone, so any
        # two clusters with the same label reported identical maxima to two
        # decimal places. That reads as a copy-paste bug in an example report and
        # would mask a cluster-keying error in any stage downstream.
        #
        # The network key is whichever the cluster type publishes: node-based
        # clusters carry network_p95 (a bandwidth percentile), serverless carries
        # network_throttle_score (a throttling proxy). They are different
        # quantities and are named separately for that reason.
        seen = {}
        for cluster_id, data in fleet["analysis.json"]["clusters"].items():
            scores = data["utilization"]["utilization_scores"]
            network = scores.get("network_p95")
            if network is None:
                network = scores["network_throttle_score"]
            key = (scores["memory_max"], network)
            assert key not in seen, f"{cluster_id} duplicates {seen.get(key)}"
            seen[key] = cluster_id

    def test_every_utilization_axis_is_measured(self, fleet):
        # Both serverless usage limits are configured, so no axis should be
        # Unknown. An Unknown here means a derived percentage series went missing.
        for cluster_id, data in fleet["analysis.json"]["clusters"].items():
            util = data["utilization"]
            for axis in ("cpu_level", "memory_level", "network_level"):
                assert util[axis] != "Unknown", f"{cluster_id}.{axis}"

    def test_fleet_spans_the_conditions_the_live_fleet_could_not(self, fleet):
        classifications = {
            cluster_id: data["utilization"]["classification"]
            for cluster_id, data in fleet["analysis.json"]["clusters"].items()
        }
        # The live fleet was three idle clusters, so no report path was ever
        # exercised with a busy or pressured cluster. Exactly one idle cluster
        # keeps that path covered without letting "everything is idle" prose --
        # the bug this fixture was built to expose -- look correct.
        assert list(classifications.values()).count("IDLE") == 1
        assert "SATURATED" in classifications.values()
        assert "BALANCED" in classifications.values()

    def test_a_cluster_is_network_bound_on_low_cpu_and_memory(self, fleet):
        """The Low/Low/High combination, end to end (D13).

        This is the one axis combination where the two plausible verdicts are
        opposites: OVER-PROVISIONED recommends scaling the node down, and
        scaling down cuts the very bandwidth allowance the cluster is
        exhausting. The guard against that was added to _classify_combination
        with a unit test over synthetic levels -- which cannot catch a fixture
        that never produces the combination, so the corrected boundary was never
        exercised through the real Stage 3 or rendered in a report.

        Asserting the three axes and not just the label: NETWORK-BOUND reached
        by any other route (a High CPU cluster that also saturates its network)
        would satisfy the classification while leaving this boundary uncovered.
        """
        util = fleet["analysis.json"]["clusters"]["prod-fanout-relay"][
            "utilization"]
        assert util["cpu_level"] == "Low"
        assert util["memory_level"] == "Low"
        assert util["network_level"] == "High"
        assert util["classification"] == "NETWORK-BOUND"

    def test_the_network_bound_cluster_is_not_at_the_band_edge(self, fleet):
        """A fixture that lands on 80.4% is one RNG change from silent loss.

        The network band edge is 80. If this cluster's p95 drifted below it the
        classification would become OVER-PROVISIONED and every assertion above
        would fail loudly -- but the *fixture* would have stopped covering D13
        for a reason no error message would name. The margin is the fixture
        guarding its own diagnostic value.
        """
        scores = (fleet["analysis.json"]["clusters"]["prod-fanout-relay"]
                  ["utilization"]["utilization_scores"])
        assert scores["network_p95"] > 85.0
        # Not pegged at the fixture's own 100 ceiling either: a series clamped
        # flat at 100 for two weeks reads as a broken generator rather than a
        # measured saturation, and would hide a percentage that overflowed.
        assert scores["network_p95"] < 99.0
        # The other two axes have their own margin from their own edges (CPU Low
        # below 20, memory Low below 30), for the same reason.
        assert scores["cpu_p95"] < 17.0
        assert scores["memory_max"] < 25.0

    def test_fleet_spans_multiple_regions_including_a_special_billing_code(
            self, fleet):
        regions = {c["region"] for c in fleet["inventory.json"]["clusters"]}
        assert len(regions) >= 3
        # eu-west-1 bills as EU, not EUW1. A single-region us-east-1 fleet made
        # that bug invisible.
        assert "eu-west-1" in regions

    def test_idle_cluster_has_no_hit_rate_finding(self, fleet):
        # Zero hits and zero misses is an undefined ratio, not a 0% hit rate.
        findings = fleet["analysis.json"]["clusters"]["dev-scratch-idle"][
            "findings"]
        assert not [f for f in findings if "HitRate" in (f.get("title") or "")]

    def test_config_findings_exist_for_the_noncompliant_cluster(self, fleet):
        findings = fleet["config_findings.json"]
        subjects = json.dumps(findings)
        assert "staging-redis-legacy" in subjects

    def test_the_fleet_has_both_a_steady_and_a_variable_active_cluster(
            self, fleet):
        # Phase 7c gates commitment savings on steadiness: an active+steady
        # cluster earns a Reserved-Node / Savings-Plan recommendation stated as
        # sound; an active+variable one earns it with a risk annotation. Both
        # paths only render, and can only be tested end to end, if the fleet
        # produces at least one of each -- and the live idle fleet produces
        # neither, which is what made this fixture change necessary.
        labels = {}
        for cid, data in fleet["analysis.json"]["clusters"].items():
            classification = data["utilization"]["classification"]
            label = (data.get("steadiness") or {}).get("label")
            if classification and classification != "IDLE":
                labels[cid] = label
        assert "steady" in labels.values(), labels
        assert ({"variable", "spiky"} & set(labels.values())), labels

    def test_pricing_is_emitted_for_every_cluster(self, fleet):
        # pricing.json feeds the savings layer; a cluster missing from it would
        # silently drop its cost-options block. Serverless is priced with a null
        # node_type and an empty option list, not omitted.
        priced = fleet["pricing.json"]["clusters"]
        inv_ids = {c["cluster_id"]
                   for c in fleet["inventory.json"]["clusters"]}
        assert set(priced) == inv_ids
        serverless = priced["prod-serverless-events"]
        assert serverless["node_type"] is None
        assert serverless["current_monthly"] is None
        assert serverless["options"] == []


class TestExpectedFindingSet:
    """The finding set itself, pinned -- the hole TestReproducibility leaves open.

    Byte-stability proves two runs agree; it cannot prove they agree on the
    *right* answer. A change that silently stopped emitting SEC-03 fleet-wide,
    or dropped a whole metric model, would keep both runs identical and this
    suite green -- exactly the class of regression the golden fleet exists to
    catch (PLAN.md D7).

    So this is a golden change-detector: it pins counts by severity and
    check_id, never finding text, so wording stays free to change (titles,
    recommendations and reasoning are the agent's and the models' to reword).
    When a deliberate change to finding logic moves a number here, update the
    expectation *after* confirming the diff is the change you intended -- never
    to turn a red suite green without reading what moved.

    The config half (Stage 3.5) is pinned exactly per cluster: it is a pure
    function of a fixed synthetic inventory against a 16-check registry, so any
    drift is a real regression. The metric half (Stage 3) is pinned by
    fleet-wide totals and the one CRITICAL, which catches a model that stops
    firing without pinning per-cluster metric counts that legitimately shift as
    models gain outputs.
    """

    # Stage 3.5 output is deterministic given the fixed inventory, so the exact
    # set of check ids per cluster is knowable and worth pinning. SEC-03 (no
    # authentication) is the finding D7 names: it must stay present, and on the
    # one non-compliant cluster only.
    EXPECTED_CONFIG_CHECKS = {
        # OE-06a (slow-log) / OE-06b (engine-log) fire wherever the log type is
        # not actively delivered and the engine supports it. session-store has
        # both active (silent); api-cache has only slow-log active (OE-06b
        # fires); the rest have neither, so both fire.
        "dev-scratch-idle": ["OE-04", "OE-06a", "OE-06b", "REL-01", "REL-02",
                             "REL-03", "REL-04b", "SEC-04"],
        "prod-api-cache": ["OE-06b", "REL-03", "REL-04b"],
        "prod-eu-catalog": ["OE-06a", "OE-06b"],
        "prod-fanout-relay": ["OE-06a", "OE-06b", "REL-03"],
        "prod-serverless-events": [],
        "prod-session-store": [],
        "staging-redis-legacy": ["COST-01", "OE-04", "OE-06a", "OE-06b",
                                 "REL-01", "REL-02", "REL-03", "REL-04",
                                 "SEC-01", "SEC-02", "SEC-03", "SEC-05",
                                 "SEC-06"],
    }

    # Fleet-wide severity distributions. The config counts follow from the
    # per-cluster sets above; the metric counts are the change-detector floor
    # for Stage 3.
    EXPECTED_CONFIG_SEVERITY = {"CRITICAL": 3, "HIGH": 6, "MEDIUM": 5, "LOW": 15}
    EXPECTED_METRIC_SEVERITY = {"CRITICAL": 1, "HIGH": 8, "MEDIUM": 15, "LOW": 11}

    # Every metric model must still produce findings on this fleet. A model that
    # silently stopped firing would drop out of this map, and byte-stability
    # would not notice.
    EXPECTED_METRIC_MODEL_SOURCES = {
        "trend", "utilization", "percentile", "breach",
        "efficiency", "shard_balance", "correlation",
    }

    def _config(self, fleet):
        return {cid: (d.get("findings") or [])
                for cid, d in fleet["config_findings.json"]["clusters"].items()}

    def _metric(self, fleet):
        return {cid: (d.get("findings") or [])
                for cid, d in fleet["analysis.json"]["clusters"].items()}

    def test_config_check_ids_per_cluster_are_exactly_as_expected(self, fleet):
        actual = {cid: sorted(f["check_id"] for f in fs)
                  for cid, fs in self._config(fleet).items()}
        expected = {cid: sorted(ids)
                    for cid, ids in self.EXPECTED_CONFIG_CHECKS.items()}
        assert actual == expected

    def test_config_severity_totals_hold(self, fleet):
        counts = Counter(f["severity"]
                         for fs in self._config(fleet).values() for f in fs)
        assert dict(counts) == self.EXPECTED_CONFIG_SEVERITY

    def test_sec03_fires_once_on_the_noncompliant_cluster_only(self, fleet):
        # The specific regression D7 names: authentication is the highest-stakes
        # check, and it must stay present and attributed to the right cluster.
        hits = [cid for cid, fs in self._config(fleet).items()
                if any(f["check_id"] == "SEC-03" for f in fs)]
        assert hits == ["staging-redis-legacy"]

    def test_the_noncompliant_cluster_keeps_its_three_criticals(self, fleet):
        fs = self._config(fleet)["staging-redis-legacy"]
        criticals = sorted(f["check_id"] for f in fs
                           if f["severity"] == "CRITICAL")
        assert criticals == ["SEC-01", "SEC-03", "SEC-05"]

    def test_metric_severity_totals_hold(self, fleet):
        counts = Counter(f["severity"]
                         for fs in self._metric(fleet).values() for f in fs)
        assert dict(counts) == self.EXPECTED_METRIC_SEVERITY

    def test_the_single_metric_critical_is_on_the_saturated_cluster(self, fleet):
        # Exactly one metric CRITICAL on the whole fleet, on the hot-shard
        # cluster. A second CRITICAL appearing elsewhere, or this one moving,
        # is a finding-logic change to acknowledge deliberately.
        criticals = [(cid, f) for cid, fs in self._metric(fleet).items()
                     for f in fs if f["severity"] == "CRITICAL"]
        assert len(criticals) == 1
        assert criticals[0][0] == "prod-api-cache"

    def test_every_metric_model_still_produces_findings(self, fleet):
        sources = {f.get("model_source")
                   for fs in self._metric(fleet).values() for f in fs}
        assert sources == self.EXPECTED_METRIC_MODEL_SOURCES

    def test_the_merged_finding_count_is_sixty_four(self, fleet):
        # The number the report renders and the verification block cites. Sum of
        # the two severity maps above; pinned here so the merged total cannot
        # drift while both halves happen to offset each other.
        metric = sum(len(fs) for fs in self._metric(fleet).values())
        config = sum(len(fs) for fs in self._config(fleet).values())
        assert (metric, config, metric + config) == (35, 29, 64)


class TestProvenance:
    """Every output stamps the pipeline version, and they agree (Phase 4).

    Provenance is what the agent quotes instead of recalling: a report states
    which pipeline produced it, so two reports a week apart are comparable and a
    diff between them is attributable. A field the four stages could disagree on
    would be worse than absent -- it would read as authoritative while being
    ambiguous -- so the version comes from one shared module, and this pins that
    all four outputs carry it and that it matches the module.
    """

    def _version(self):
        sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))
        import _pipeline_version
        return _pipeline_version.PIPELINE_VERSION

    def test_every_output_carries_the_shared_pipeline_version(self, fleet):
        version = self._version()
        assert version  # a blank version is not provenance
        for name in ("inventory.json", "metrics.json", "analysis.json",
                     "config_findings.json"):
            assert fleet[name]["metadata"].get("pipeline_version") == version, (
                f"{name} does not stamp the shared pipeline version")


class TestFindingIdsIdentifyAFinding:
    """An id that repeats across clusters does not identify anything.

    Stage 3's counter resets on every generate() call -- which is what keeps the
    ids deterministic -- so it numbered findings per cluster. On this example
    fleet three clusters each carried "percentile-001" and two each carried
    "efficiency-001". Anything keyed on finding_id alone (an agent note, a deep
    link, a suppression list) would attach to whichever cluster happened to be
    read first, silently and with no error.

    Only a multi-cluster fixture can catch this: on the three-cluster live fleet
    the collision existed too, and on any single-cluster unit fixture it cannot.
    Stage 3.5 already had this test; Stage 3 did not, which is why the ids
    diverged.
    """

    def _ids(self, fleet):
        return [(cluster_id, f["finding_id"])
                for name in ("analysis.json", "config_findings.json")
                for cluster_id, data in fleet[name]["clusters"].items()
                for f in (data.get("findings") or [])]

    def test_ids_are_unique_across_the_whole_fleet(self, fleet):
        pairs = self._ids(fleet)
        ids = [fid for _, fid in pairs]
        collisions = {fid: sorted(c for c, f in pairs if f == fid)
                      for fid in ids if ids.count(fid) > 1}
        assert not collisions, (
            "finding_id repeats across clusters, so it cannot identify a "
            f"finding: {collisions}")

    def test_the_fixture_actually_has_findings_on_several_clusters(self, fleet):
        """Guards the guard: uniqueness over one cluster proves nothing."""
        pairs = self._ids(fleet)
        assert len({c for c, _ in pairs}) >= 3
        assert len(pairs) > 20

    def test_ids_are_unique_across_both_stages_together(self, fleet):
        """The two stages number independently, so overlap is possible.

        Stage 3 uses a model-source prefix and Stage 3.5 uses "config-", which
        keeps them apart today -- but the report merges both lists into one
        table (D4), so a collision between stages would be as damaging as one
        within a stage and is not otherwise tested anywhere.
        """
        stage3 = {f["finding_id"]
                  for data in fleet["analysis.json"]["clusters"].values()
                  for f in (data.get("findings") or [])}
        stage35 = {f["finding_id"]
                   for data in fleet["config_findings.json"]["clusters"].values()
                   for f in (data.get("findings") or [])}
        assert stage3 and stage35
        assert not (stage3 & stage35)

    def test_the_id_names_the_cluster_it_belongs_to(self, fleet):
        """Not required for uniqueness, but it is what makes an id readable in
        a note or a bug report -- and it is how the fix works, so pin it."""
        for cluster_id, data in fleet["analysis.json"]["clusters"].items():
            for f in (data.get("findings") or []):
                assert f["finding_id"].startswith(cluster_id + "-"), (
                    f"{f['finding_id']} does not name its cluster {cluster_id}")
