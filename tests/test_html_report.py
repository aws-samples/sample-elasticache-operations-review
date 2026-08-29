"""Tests for the HTML report generator.

Two things matter here and neither is about appearance:

1. **Reproducibility.** The same three JSON inputs must produce a
   byte-identical document, so two reports can be diffed and any change
   attributed to the data rather than the renderer.
2. **Derived prose.** The narrative callouts assert specific facts (a
   volatile-* policy with no TTL keys, a serverless cache billed while
   empty, spend in an unscanned region). Each must come from the input
   data, and must disappear when the data does not support it -- a
   hardcoded claim that outlives its evidence is how a report starts
   lying.
"""
import json
import os
import re
import shutil
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from check_configuration import CHECKS  # noqa: E402
from generate_html_report import (  # noqa: E402
    BILLING_CODE_FOR_REGION,
    CITABLE_SOURCES,
    MAX_CITE_NUMBERS,
    PANEL_METRICS,
    REGION_BILLING_CODES,
    NotesError,
    _config_coverage,
    _derive_facts,
    _emit_report_data,
    _gate_options,
    _region_for_billing_code,
    build_payload,
    build_report_data,
    cluster_series,
    downsample,
    main,
    prose_figures,
    render,
    verify_notes,
)

STAMP = "2026-08-11 10:00 UTC"


def _script(html):
    """The report's single inline script block, unescaped."""
    blocks = re.findall(r"<script>(.*?)</script>", html, re.S)
    assert len(blocks) == 1, (
        f"expected one inline script block, found {len(blocks)}; the callers "
        "here assume one and would silently check the wrong thing")
    # render() escapes </ so the payload cannot close the tag early.
    return blocks[0].replace("<\\/", "</")


def _strip_comments(script):
    """Drop // comments so a test can ban a string the source explains.

    Several fixes in the template are commented with the exact wording they
    exist to prevent ("0 from configuration checks", "across all pillars"), and
    a test asserting that string is absent then fails on its own rationale. A
    comment is not a claim to the reader, so it is not what these tests are
    about.
    """
    return re.sub(r"^\s*//.*$", "", script, flags=re.M)


def _inventory(clusters=None):
    """Minimal inventory.json in the shape discover_inventory.py writes."""
    if clusters is None:
        clusters = [{
            "cluster_id": "node-1",
            "cluster_type": "node-based",
            "region": "us-east-1",
            "engine": "redis",
            "engine_version": "7.1.0",
            "node_type": "cache.m5.large",
            "total_nodes": 2,
            "tls_enabled": True,
            "encryption_at_rest": True,
            "auth_mode": "none",
            "multi_az": True,
            "automatic_failover": True,
            "snapshot_retention_days": 1,
            "tags": {},
            "parameters": {"maxmemory-policy": "volatile-lru"},
        }]
    return {
        "metadata": {
            "account_id": "111122223333",
            "regions_scanned": ["us-east-1"],
            "total_clusters": len(clusters),
        },
        "clusters": clusters,
    }


def _metrics(values=None, usage_types=None):
    """Minimal metrics.json in the compact shape fetch_metrics.py writes."""
    if values is None:
        values = [1.0] * 24
    if usage_types is None:
        usage_types = {"NodeUsage:cache.m5.large": 7.49}
    return {
        "metadata": {
            "period_start": "2026-07-28T00:00:00Z",
            "period_end": "2026-08-11T00:00:00Z",
            "total_datapoints": 1234,
        },
        "clusters": {
            "node-1": {
                "cluster_type": "node-based",
                "timestamps_5min": [
                    f"2026-08-01T{i // 12:02d}:{(i % 12) * 5:02d}:00Z"
                    for i in range(len(values))
                ],
                "nodes": {
                    "node-1-001": {
                        "EngineCPUUtilization": {"Maximum": values},
                        "NetworkBytesIn": {"Sum": [1048576.0] * len(values)},
                    }
                },
                "errors": [],
            }
        },
        "cost": {"daily": [{"date": "2026-07-28", "total_usd": 11.64,
                            "by_usage_type": usage_types}]},
    }


def _analysis(percentiles=None, findings=None):
    """Minimal analysis.json in the shape analyze_metrics.py writes."""
    return {
        "metadata": {"clusters_analyzed": 1},
        "clusters": {
            "node-1": {
                "percentiles": percentiles if percentiles is not None else {
                    "EngineCPUUtilization_Maximum": {"p95": 0.4, "max": 0.67},
                    "CurrItems_Maximum": {"max": 101608.0},
                    "CurrVolatileItems_Maximum": {"max": 0.0},
                },
                "utilization": {"classification": "IDLE"},
                "findings": findings or [],
                "errors": [],
            }
        },
    }


class TestReproducibility:
    def test_same_inputs_produce_byte_identical_output(self):
        inv, met, an = _inventory(), _metrics(), _analysis()
        first = render(build_payload(inv, met, an, STAMP))
        second = render(build_payload(inv, met, an, STAMP))
        assert first == second

    def test_timestamp_is_the_only_injected_nondeterminism(self):
        inv, met, an = _inventory(), _metrics(), _analysis()
        a = render(build_payload(inv, met, an, "2026-01-01 00:00 UTC"))
        b = render(build_payload(inv, met, an, "2026-12-31 23:59 UTC"))
        assert a != b
        assert a.replace("2026-01-01 00:00 UTC", "X") == \
            b.replace("2026-12-31 23:59 UTC", "X")


class TestSelfContained:
    def test_no_external_resource_references(self):
        html = render(build_payload(_inventory(), _metrics(), _analysis(), STAMP))
        # A report that fetches anything at open time stops working offline
        # and leaks which account it describes.
        assert ' src="http' not in html
        assert ' href="http' not in html

    def test_script_close_tag_in_data_cannot_break_out(self):
        # A cluster named with a closing script tag would otherwise terminate
        # the host <script> element early and inject markup.
        inv = _inventory()
        inv["clusters"][0]["cluster_id"] = "</script><img src=x onerror=alert(1)>"
        html = render(build_payload(inv, _metrics(), _analysis(), STAMP))
        assert "</script><img" not in html
        assert "<\\/script>" in html


class TestDownsample:
    def test_max_aggregation_preserves_spikes(self):
        values = [1.0] * 24
        values[7] = 99.0
        out, _ = downsample(values, [""] * 24, bucket=12, agg="max")
        assert out == [99.0, 1.0]

    def test_bucket_timestamps_align_with_bucket_starts(self):
        stamps = [f"t{i}" for i in range(24)]
        _, out_ts = downsample([1.0] * 24, stamps, bucket=12)
        assert out_ts == ["t0", "t12"]

    def test_all_null_bucket_is_dropped_not_zeroed(self):
        # Reporting a gap as 0 would understate a metric; dropping is honest.
        out, _ = downsample([None] * 12 + [5.0] * 12, [""] * 24, bucket=12)
        assert out == [5.0]


class TestClusterSeries:
    def test_hottest_node_wins_at_each_point(self):
        # Averaging would dilute a single hot shard, which is the thing a
        # reviewer most needs to see.
        cluster = {
            "timestamps_5min": ["a", "b"],
            "nodes": {
                "n1": {"EngineCPUUtilization": {"Maximum": [10.0, 1.0]}},
                "n2": {"EngineCPUUtilization": {"Maximum": [2.0, 50.0]}},
            },
        }
        values, _ = cluster_series(cluster, "EngineCPUUtilization", "Maximum",
                                   agg="max")
        assert values == [50.0]

    def test_absent_metric_yields_empty_series(self):
        cluster = {"timestamps_5min": ["a"], "nodes": {"n1": {}}}
        assert cluster_series(cluster, "Missing", "Maximum") == ([], [])


def _bundle_charts(bundle):
    """Flatten a panel/report_data bundle's grouped charts to {metric: chart}."""
    return {c["metric"]: c
            for g in bundle["groups"] for c in g["charts"]}


class TestReportDataDecoupling:
    """Phase 9a: rendering from report_data.json must be byte-identical to
    rendering from the raw metrics.json it was reduced from.

    ``build_report_data`` moves the chart reduction out of the render path so
    the report never loads the multi-GB metrics.json. This is the guard that
    the *move* changed nothing: the panels (chart series, flat_zero,
    not_reported, metrics_reported) must match exactly whichever source the
    render used. It reuses the same downsample/_series functions, so they
    should -- if this ever fails, fix the reducer, do not loosen the test.
    """

    # An inventory with a cluster that has metrics and one that does not, so the
    # missing-bundle fallback is exercised alongside real chart building.
    def _inventory(self):
        base = _inventory()["clusters"][0]
        full = dict(base, cluster_id="c-full")
        absent = dict(base, cluster_id="c-absent")
        return _inventory([full, absent])

    def _metrics(self):
        # c-full: two nodes (hottest-node), a real CPU series (charted), a real
        # memory-in-bytes series (exercises the /1048576 divisor), an
        # all-zero Evictions series (flat_zero), and every other panel metric
        # absent (not_reported). c-absent is deliberately not in the map.
        stamps = [f"2026-08-01T{i:02d}:00:00Z" for i in range(14)]
        cpu_a = [float(i) for i in range(14)]
        cpu_b = [float(13 - i) for i in range(14)]  # so the hotter node varies
        return {
            "metadata": {"period_start": "2026-08-01T00:00:00Z",
                         "period_end": "2026-08-14T00:00:00Z",
                         "total_datapoints": 99},
            "clusters": {
                "c-full": {
                    "cluster_type": "node-based",
                    "timestamps_5min": stamps,
                    "nodes": {
                        "c-full-001": {
                            "EngineCPUUtilization": {"Maximum": cpu_a},
                            "BytesUsedForCache": {"Maximum":
                                                  [2097152.0] * 14},
                            "Evictions": {"Sum": [0.0] * 14},
                        },
                        "c-full-002": {
                            "EngineCPUUtilization": {"Maximum": cpu_b},
                        },
                    },
                    "errors": [],
                },
            },
            "cost": {"daily": [{"date": "2026-08-01", "total_usd": 5.0,
                                "by_usage_type": {
                                    "NodeUsage:cache.m5.large": 5.0}}]},
        }

    def test_panels_are_identical_from_both_sources(self):
        inv, met, an = self._inventory(), self._metrics(), _analysis()
        old = build_payload(inv, met, an, STAMP)
        rd = build_report_data(met)
        new = build_payload(inv, None, an, STAMP, report_data=rd)
        # The whole render agrees, not only the panels: cost and metadata also
        # route through report_data.
        assert render(old) == render(new)
        # And the specific fields this split moved, panel by panel.
        old_p = {p["cluster"]: p for p in old["panels"]}
        new_p = {p["cluster"]: p for p in new["panels"]}
        assert old_p.keys() == new_p.keys()
        for cid in old_p:
            for key in ("groups", "chart_count", "flat_zero", "not_reported",
                        "metrics_reported"):
                assert old_p[cid][key] == new_p[cid][key], (cid, key)

    def test_the_fixture_exercises_all_three_states(self):
        # Guard the guard: if the fixture stopped covering a charted metric, a
        # flat-zero one and an absent one, the byte-identical assertion would
        # pass vacuously.
        panel = {p["cluster"]: p
                 for p in build_payload(self._inventory(), self._metrics(),
                                        _analysis(), STAMP)["panels"]}["c-full"]
        assert panel["chart_count"] > 0  # at least one charted
        assert "Evictions (per 5 min)" in panel["flat_zero"]
        assert panel["not_reported"]  # at least one absent
        assert panel["metrics_reported"] is True

    def test_every_node_gets_its_own_series(self):
        # Each chart overlays one downsampled series per node, aligned to a
        # shared timestamp axis -- not a single hottest-node merge. cpu_a rises
        # 0..13, cpu_b falls 13..0; 14 points at SAMPLES_PER_HOUR=12 is two
        # buckets. Bucket 0 covers indices 0-11, bucket 1 covers 12-13, so
        # node 001's per-bucket max is [11, 13] and node 002's is [13, 1].
        rd = build_report_data(self._metrics())
        charts = _bundle_charts(rd["clusters"]["c-full"])
        cpu = charts["EngineCPUUtilization"]
        series = {s["node"]: s["values"] for s in cpu["series"]}
        assert series == {"c-full-001": [11.0, 13.0], "c-full-002": [13.0, 1.0]}
        assert len(cpu["timestamps"]) == 2  # one shared axis for both nodes
        # BytesUsedForCache is only on node 001 and carries the " MB" divisor
        # (1048576), applied per node inside the reduction: 2 MiB -> 2.0.
        bytes_chart = charts["BytesUsedForCache"]
        assert [s["node"] for s in bytes_chart["series"]] == ["c-full-001"]
        assert bytes_chart["series"][0]["values"] == [2.0, 2.0]

    def test_report_data_carries_cost_and_metadata_verbatim(self):
        met = self._metrics()
        rd = build_report_data(met)
        assert rd["cost"] == met["cost"]
        assert rd["metadata"] == met["metadata"]

    def test_absent_cluster_gets_a_not_reported_panel_either_way(self):
        inv, met, an = self._inventory(), self._metrics(), _analysis()
        old = {p["cluster"]: p
               for p in build_payload(inv, met, an, STAMP)["panels"]}
        rd = build_report_data(met)
        new = {p["cluster"]: p
               for p in build_payload(inv, None, an, STAMP,
                                      report_data=rd)["panels"]}
        for panels in (old, new):
            assert panels["c-absent"]["metrics_reported"] is False
            assert panels["c-absent"]["chart_count"] == 0
            assert panels["c-absent"]["groups"] == []
            # Every panel metric is "not reported" for a cluster with no data.
            assert len(panels["c-absent"]["not_reported"]) == len(PANEL_METRICS)

    def test_build_payload_needs_metrics_or_report_data(self):
        with pytest.raises(ValueError):
            build_payload(self._inventory(), None, _analysis(), STAMP)

    def test_render_is_byte_identical_through_the_real_emit_file(self, tmp_path):
        # The end-to-end guard: emit report_data.json exactly as the CLI does,
        # load it back, and render. This must match a render off raw metrics
        # byte-for-byte. The in-memory comparison above cannot catch a
        # serialisation choice in the emit step (e.g. sort_keys reordering the
        # chart dict keys) because it never round-trips through the file.
        inv, met, an = self._inventory(), self._metrics(), _analysis()
        metrics_path = tmp_path / "metrics.json"
        metrics_path.write_text(json.dumps(met))
        rd_path = tmp_path / "report_data.json"
        rc = _emit_report_data(str(metrics_path), str(rd_path))
        assert rc == 0
        rd = json.loads(rd_path.read_text())
        from_rd = render(build_payload(inv, None, an, STAMP, report_data=rd))
        from_metrics = render(build_payload(inv, met, an, STAMP))
        assert from_rd == from_metrics

    def test_the_emit_file_is_byte_stable_across_runs(self, tmp_path):
        met = self._metrics()
        metrics_path = tmp_path / "metrics.json"
        metrics_path.write_text(json.dumps(met))
        a = tmp_path / "a.json"
        b = tmp_path / "b.json"
        assert _emit_report_data(str(metrics_path), str(a)) == 0
        assert _emit_report_data(str(metrics_path), str(b)) == 0
        assert a.read_bytes() == b.read_bytes()


class TestDerivedFacts:
    def test_ttl_risk_detected_for_volatile_policy_with_no_volatile_items(self):
        facts = _derive_facts(
            {"node-1": _inventory()["clusters"][0]},
            _analysis()["clusters"],
            {},
        )
        assert facts["ttl_risk"]["cluster"] == "node-1"
        assert facts["ttl_risk"]["policy"] == "volatile-lru"
        assert facts["ttl_risk"]["items"] == 101608

    def test_no_ttl_risk_when_keys_carry_ttls(self):
        an = _analysis(percentiles={
            "CurrItems_Maximum": {"max": 100.0},
            "CurrVolatileItems_Maximum": {"max": 100.0},
        })
        facts = _derive_facts({"node-1": _inventory()["clusters"][0]},
                             an["clusters"], {})
        assert "ttl_risk" not in facts

    def test_no_ttl_risk_for_allkeys_policy(self):
        inv = _inventory()["clusters"][0]
        inv["parameters"] = {"maxmemory-policy": "allkeys-lru"}
        facts = _derive_facts({"node-1": inv}, _analysis()["clusters"], {})
        assert "ttl_risk" not in facts

    def test_no_ttl_risk_for_empty_cluster(self):
        # Zero items with zero volatile items is not a TTL problem.
        an = _analysis(percentiles={"CurrItems_Maximum": {"max": 0.0},
                                    "CurrVolatileItems_Maximum": {"max": 0.0}})
        facts = _derive_facts({"node-1": _inventory()["clusters"][0]},
                             an["clusters"], {})
        assert "ttl_risk" not in facts

    def test_serverless_minimum_flagged_when_storing_nothing(self):
        inv = {"sl": {"cluster_id": "sl", "cluster_type": "serverless",
                      "engine": "redis", "region": "us-east-1"}}
        an = {"sl": {"percentiles": {"BytesUsedForCache_Maximum": {"max": 0.0}}}}
        facts = _derive_facts(inv, an, {"USE1-CachedData:Redis": 41.62})
        assert facts["serverless_minimum"]["cost"] == 41.62

    def test_serverless_minimum_not_flagged_when_storing_data(self):
        inv = {"sl": {"cluster_id": "sl", "cluster_type": "serverless",
                      "engine": "redis", "region": "us-east-1"}}
        an = {"sl": {"percentiles":
                     {"BytesUsedForCache_Maximum": {"max": 5_000_000.0}}}}
        facts = _derive_facts(inv, an, {"USE1-CachedData:Redis": 41.62})
        assert "serverless_minimum" not in facts

    def test_foreign_region_spend_detected_from_usage_type_prefix(self):
        facts = _derive_facts(
            {"node-1": _inventory()["clusters"][0]},
            _analysis()["clusters"],
            {"NodeUsage:cache.m5.large": 104.55,
             "USE2-NodeUsage:cache.t4g.micro": 5.31},
        )
        assert facts["foreign_region"] == {"us-east-2": 5.31}

    def test_home_region_usage_type_is_not_foreign(self):
        # USE1- prefixed types are in the scanned region and must not be
        # reported as unaccounted spend.
        facts = _derive_facts(
            {"node-1": _inventory()["clusters"][0]},
            _analysis()["clusters"],
            {"USE1-CachedData:Redis": 41.62},
        )
        assert "foreign_region" not in facts


class TestRegionBillingCodes:
    """The map is data transcribed from AWS docs, so test it as data.

    The previous version listed six regions and was wrong twice: it omitted
    every other region (whose spend was silently read as home-region), and it
    mapped eu-west-1 to EUW1 when the billing code is EU. Both failures are
    invisible on a single-region us-east-1 fleet, which is exactly why this
    class tests regions the live fleet does not use.
    """

    def test_code_and_region_maps_agree(self):
        assert len(BILLING_CODE_FOR_REGION) == len(REGION_BILLING_CODES)
        for code, region in REGION_BILLING_CODES.items():
            assert BILLING_CODE_FOR_REGION[region] == code

    def test_eu_west_1_is_EU_not_EUW1(self):
        # Not derivable from the pattern -- eu-west-1 predates the numbering.
        assert REGION_BILLING_CODES["EU"] == "eu-west-1"
        assert "EUW1" not in REGION_BILLING_CODES

    def test_ap_south_codes_do_not_follow_the_region_string(self):
        # APS3 is ap-south-1 while APS1/APS2 are ap-southeast-*. Any attempt to
        # derive the code from the region name gets these wrong.
        assert REGION_BILLING_CODES["APS3"] == "ap-south-1"
        assert REGION_BILLING_CODES["APS5"] == "ap-south-2"
        assert REGION_BILLING_CODES["APS1"] == "ap-southeast-1"

    def test_resolves_a_region_outside_the_original_six(self):
        # ca-central-1, eu-central-1 and ap-southeast-2 were all unlisted before
        # and had their entire spend misattributed.
        assert _region_for_billing_code("CAN1-NodeUsage:cache.r7g.large") \
            == "ca-central-1"
        assert _region_for_billing_code("EUC1-CachedData:Valkey") == "eu-central-1"
        assert _region_for_billing_code("APS2-NodeUsage:cache.m5.large") \
            == "ap-southeast-2"

    def test_unprefixed_usage_type_resolves_to_unknown(self):
        # This form appears in real Cost Explorer output alongside prefixed
        # ones. Attributing it to the home region would invent a fact.
        assert _region_for_billing_code("NodeUsage:cache.m5.large") is None

    def test_unrecognised_code_resolves_to_unknown_not_home_region(self):
        # A region newer than the table, GovCloud, or a sovereign partition.
        assert _region_for_billing_code("UGW1-NodeUsage:cache.m5.large") is None

    def test_foreign_spend_found_for_customer_outside_the_original_six(self):
        # A ca-central-1 customer with stray eu-central-1 spend: under the old
        # six-entry map neither code was known, so this reported nothing.
        inv = _inventory()["clusters"][0]
        inv["region"] = "ca-central-1"
        facts = _derive_facts(
            {"node-1": inv},
            _analysis()["clusters"],
            {"CAN1-NodeUsage:cache.m5.large": 104.55,
             "EUC1-NodeUsage:cache.t4g.micro": 5.31},
        )
        assert facts["foreign_region"] == {"eu-central-1": 5.31}

    def test_home_region_spend_not_foreign_for_eu_west_1(self):
        # The EUW1/EU bug made an eu-west-1 customer's own node hours either
        # unattributable or, worse, foreign.
        inv = _inventory()["clusters"][0]
        inv["region"] = "eu-west-1"
        facts = _derive_facts(
            {"node-1": inv},
            _analysis()["clusters"],
            {"EU-NodeUsage:cache.m5.large": 104.55},
        )
        assert "foreign_region" not in facts

    def test_unattributable_spend_is_not_reported_as_foreign(self):
        facts = _derive_facts(
            {"node-1": _inventory()["clusters"][0]},
            _analysis()["clusters"],
            {"NodeUsage:cache.m5.large": 104.55},
        )
        assert "foreign_region" not in facts

    def test_govcloud_only_fleet_warns_rather_than_claiming_no_foreign_spend(
            self, caplog):
        inv = _inventory()["clusters"][0]
        inv["region"] = "us-gov-west-1"
        with caplog.at_level("WARNING"):
            facts = _derive_facts({"node-1": inv}, _analysis()["clusters"],
                                  {"UGW1-NodeUsage:cache.m5.large": 104.55})
        assert "foreign_region" not in facts
        assert "billing code" in caplog.text


class TestPayload:
    def test_cost_projections_scale_from_observed_window(self):
        payload = build_payload(_inventory(), _metrics(), _analysis(), STAMP)
        meta = payload["meta"]
        assert meta["cost_total"] == 11.64
        assert meta["cost_days"] == 1
        assert meta["cost_monthly"] == round(11.64 * 30.4, 2)

    def test_findings_sorted_most_severe_first(self):
        an = _analysis(findings=[
            {"severity": "LOW", "title": "l"},
            {"severity": "CRITICAL", "title": "c"},
            {"severity": "MEDIUM", "title": "m"},
        ])
        payload = build_payload(_inventory(), _metrics(), an, STAMP)
        assert [f["severity"] for f in payload["findings"]] == \
            ["CRITICAL", "MEDIUM", "LOW"]

    def test_embedded_payload_is_valid_json(self):
        html = render(build_payload(_inventory(), _metrics(), _analysis(), STAMP))
        start = html.index('id="reportData" type="application/json">')
        start = html.index(">", start) + 1
        end = html.index("</script>", start)
        # render() escapes </ so the payload cannot close the tag early
        data = json.loads(html[start:end].replace("<\\/", "</"))
        assert data["meta"]["account"] == "111122223333"
        assert data["clusters"][0]["cluster"] == "node-1"


class TestUncollectedCostIsNotZero:
    """`--skip-cost` must not produce a report claiming the fleet is free.

    fetch_metrics.py omits the "cost" key entirely under --skip-cost -- a
    documented flag, and the only option for a customer without Cost Explorer
    permissions -- and also when the Cost Explorer call raises. Every projection
    then computed to 0.0 and rendered as "$0.00" monthly and annual spend.

    That is the same defect class as reading a metric with the wrong statistic:
    absence arriving as a confident number. So it is tested the same way -- not
    "a value came back", but *which* value, and that it is distinguishable from a
    real measurement of zero.
    """

    def test_absent_cost_yields_null_projections_not_zero(self):
        met = _metrics()
        del met["cost"]
        meta = build_payload(_inventory(), met, _analysis(), STAMP)["meta"]
        assert meta["cost_collected"] is False
        # None, not 0.0. A zero reads as a measurement; a null forces the
        # renderer to branch.
        assert meta["cost_total"] is None
        assert meta["cost_monthly"] is None
        assert meta["cost_annual"] is None
        # Also None: 1 was a fabricated window over a window never observed.
        assert meta["cost_days"] is None

    def test_empty_daily_list_is_also_uncollected(self):
        # Cost Explorer returning no rows is not the same as spending nothing.
        met = _metrics()
        met["cost"] = {"daily": []}
        meta = build_payload(_inventory(), met, _analysis(), STAMP)["meta"]
        assert meta["cost_collected"] is False
        assert meta["cost_monthly"] is None

    def test_genuine_zero_spend_is_distinguishable_from_uncollected(self):
        # The distinction the whole fix exists to preserve: a fleet that really
        # cost $0 over the window reports a measured zero, and says so.
        met = _metrics()
        met["cost"] = {"daily": [{"date": "2026-07-28", "total_usd": 0.0,
                                  "by_usage_type": {}}]}
        meta = build_payload(_inventory(), met, _analysis(), STAMP)["meta"]
        assert meta["cost_collected"] is True
        assert meta["cost_total"] == 0.0
        assert meta["cost_monthly"] == 0.0

    def test_collected_cost_still_sets_the_flag(self):
        meta = build_payload(_inventory(), _metrics(), _analysis(), STAMP)["meta"]
        assert meta["cost_collected"] is True
        assert meta["cost_total"] == 11.64

    def test_serverless_minimum_callout_quotes_no_figure_when_uncollected(self):
        # The finding rests on the bytes metric, so it must still fire -- but
        # summing an empty by_type gave 0.0, and the callout then said the cache
        # "accrued $0.00 in storage charges", contradicting its own point.
        inv = {"sl": {"cluster_id": "sl", "cluster_type": "serverless",
                      "engine": "redis", "region": "us-east-1"}}
        an = {"sl": {"percentiles": {"BytesUsedForCache_Maximum": {"max": 0.0}}}}
        facts = _derive_facts(inv, an, {})
        assert facts["serverless_minimum"]["cluster"] == "sl"
        assert facts["serverless_minimum"]["cost"] is None

    def test_no_dollar_zero_anywhere_in_the_rendered_document(self):
        # The end-to-end guard. Checks the JSON payload the JS renders from,
        # since the figures reach the reader through it -- a "$0.00" in a source
        # comment is not a claim, but a 0.0 in the payload becomes one.
        met = _metrics()
        del met["cost"]
        html = render(build_payload(_inventory(), met, _analysis(), STAMP))
        start = html.index('id="reportData" type="application/json">')
        start = html.index(">", start) + 1
        data = json.loads(
            html[start:html.index("</script>", start)].replace("<\\/", "</"))
        for key in ("cost_total", "cost_monthly", "cost_annual", "cost_days"):
            assert data["meta"][key] is None, key
        # The renderer must have something to branch on, and the reason must be
        # stated rather than left as an empty frame.
        assert data["meta"]["cost_collected"] is False
        assert "not collected" in html


def _config(clusters=None, **meta):
    """Minimal config_findings.json in the shape check_configuration.py writes."""
    if clusters is None:
        clusters = {
            "node-1": {
                "cluster_type": "node-based",
                "findings": [{
                    "severity": "CRITICAL",
                    "finding_id": "config-node-1-sec-03",
                    "title": "No authentication configured",
                    "description": "auth_mode is none",
                    "recommendation": "Enable Redis AUTH or IAM auth",
                    "check_id": "SEC-03",
                    "pillar": "Security",
                    "metric_name": None,
                    "current_value": False,
                }],
                "checks_skipped": [{"check_id": "COST-06",
                                    "reason": "not applicable to node-based"}],
                "checks_evaluated": 13,
            }
        }
    base = {
        "account_id": "111122223333",
        "regions_scanned": ["us-east-1"],
        "checks_in_registry": 14,
        "check_ids": ["SEC-01", "SEC-03", "COST-06"],
        "clusters_checked": 1,
        "review_date": "2026-08-11",
        "policy": {"min_replicas_per_shard": 2,
                   "min_snapshot_retention_days": 7,
                   "required_tags": ["Environment", "Owner"]},
    }
    base.update(meta)
    return {"metadata": base, "clusters": clusters}


class TestConfigFindingsReachTheReport:
    """Stage 3.5's output was written and never read (PLAN.md D4).

    Three CRITICAL `auth_mode: none` findings existed only in
    config_findings.json, which nothing downstream opened. A finding nobody sees
    is close to a finding that does not exist -- and it is worse than a missing
    finding, because the pipeline reported success.
    """

    def test_a_critical_config_finding_reaches_the_payload(self):
        payload = build_payload(_inventory(), _metrics(), _analysis(), STAMP,
                                _config())
        titles = [f["title"] for f in payload["findings"]]
        assert "No authentication configured" in titles

    def test_it_reaches_the_rendered_document(self):
        html = render(build_payload(_inventory(), _metrics(), _analysis(),
                                    STAMP, _config()))
        assert "No authentication configured" in html
        assert "SEC-03" in html

    def test_config_findings_carry_their_check_id_and_pillar(self):
        """The check id is how a reader traces a row to its mapping."""
        payload = build_payload(_inventory(), _metrics(), _analysis(), STAMP,
                                _config())
        row = next(f for f in payload["findings"]
                   if f["source"] == "configuration")
        assert row["check_id"] == "SEC-03"
        assert row["pillar"] == "Security"

    def test_metric_findings_are_labelled_too(self):
        """Both sources must be distinguishable, not just the new one."""
        an = _analysis(findings=[{"severity": "HIGH", "title": "hot"}])
        payload = build_payload(_inventory(), _metrics(), an, STAMP, _config())
        sources = {f["title"]: f["source"] for f in payload["findings"]}
        assert sources["hot"] == "metrics"
        assert sources["No authentication configured"] == "configuration"

    def test_a_config_critical_outranks_a_metric_high(self):
        """Merged into one table, so severity must win over provenance.

        A separate second table would have ranked every metric finding above
        every configuration one regardless of severity, which is the reading
        error the merge exists to prevent: an open security group is not less
        urgent than a MEDIUM eviction rate because a different script found it.
        """
        an = _analysis(findings=[{"severity": "HIGH", "title": "hot"}])
        payload = build_payload(_inventory(), _metrics(), an, STAMP, _config())
        assert payload["findings"][0]["title"] == "No authentication configured"

    def test_a_severity_tie_breaks_on_the_cluster_not_on_which_file_was_read(
            self):
        """Python's sort is stable, so severity alone left ties in input order.

        Every metric finding then preceded every configuration finding of the
        same severity, and the report's rows were grouped by the accident of
        which file the generator opened first -- which reads as a pattern
        ("all the metric problems, then all the config problems") that is an
        artefact of the loop order. Ties break on the cluster instead, so one
        cluster's problems read together.
        """
        inv = _inventory(clusters=[
            dict(_inventory()["clusters"][0], cluster_id="node-1"),
        ])
        an = _analysis(findings=[{"severity": "HIGH", "title": "zzz-metric"}])
        cfg = _config(clusters={
            "aaa-cluster": {
                "cluster_type": "node-based",
                "findings": [{"severity": "HIGH", "title": "aaa-config",
                              "check_id": "SEC-01", "pillar": "Security"}],
                "checks_skipped": [],
                "checks_evaluated": 13,
            }
        })
        payload = build_payload(inv, _metrics(), an, STAMP, cfg)
        highs = [f["cluster"] for f in payload["findings"]
                 if f["severity"] == "HIGH"]
        assert highs == ["aaa-cluster", "node-1"], (
            "a HIGH on aaa-cluster sorts after a HIGH on node-1, so ordering "
            "still follows which file was read rather than the data")

    def test_a_bool_current_value_is_not_coerced(self):
        """Configuration values are settings, not measurements.

        Rendering False as 0 would read as a metric that measured zero -- the
        same absence-as-a-number confusion as $0.00 for uncollected cost.
        """
        payload = build_payload(_inventory(), _metrics(), _analysis(), STAMP,
                                _config())
        row = next(f for f in payload["findings"]
                   if f["source"] == "configuration")
        assert row["value"] is False
        assert row["metric"] is None


class TestTheReportStillRendersWithoutStage35:
    """Stages 1-3 are the floor.

    A customer whose Stage 3.5 run failed, or who has no elasticache:Describe*
    on some clusters, still gets the metric sections. The optional input must not
    become a required one.
    """

    def test_payload_builds_with_no_config_findings(self):
        payload = build_payload(_inventory(), _metrics(), _analysis(), STAMP)
        assert payload["config"] is None
        assert all(f["source"] == "metrics" for f in payload["findings"])

    def test_the_document_still_renders(self):
        html = render(build_payload(_inventory(), _metrics(), _analysis(), STAMP))
        assert "Configuration checks" in html

    def test_the_findings_kpi_does_not_claim_pillar_coverage_it_lacks(self):
        """The count's meaning depends on whether Stage 3.5 ran.

        The tile's footer read "across all pillars" unconditionally, so while the
        configuration findings went unread it claimed Security and Reliability
        coverage for a number that held only metric findings. Both strings live
        in the template; the branch is what makes the claim honest.
        """
        without = render(build_payload(_inventory(), _metrics(), _analysis(),
                                       STAMP))
        assert "no configuration check ran" in without
        assert "across all pillars" not in _strip_comments(_script(without))

    def test_reproducibility_survives_the_new_input(self):
        args = (_inventory(), _metrics(), _analysis(), STAMP, _config())
        assert render(build_payload(*args)) == render(build_payload(*args))


class TestUncheckedIsNotPassing:
    """The section exists to say what was *not* examined.

    A findings table lists failures, so silence in it has two readings: the check
    passed, or the check never ran. That ambiguity is the reason coverage is
    reported at all, and it is why absent Stage 3.5 output renders as absent
    rather than as a clean bill of health.
    """

    def test_no_config_means_a_stated_absence_not_an_empty_section(self):
        html = render(build_payload(_inventory(), _metrics(), _analysis(), STAMP))
        # The renderer needs something to branch on...
        assert '"config":null' in html
        # ...and the reason must be in the document, not implied by a blank.
        assert "no configuration check" in html
        assert "not as a clean result" in html

    def test_the_findings_note_does_not_count_config_findings_as_zero(self):
        """"0 from configuration checks" says the checks ran and found nothing.

        Same defect as $0.00 for uncollected spend: with no Stage 3.5 output the
        configuration count is not zero, it is undefined.

        The banned sentence is assembled at runtime from a count and a literal,
        so it never appears in the source and a grep for it passes no matter what
        the code does -- the first version of this test was vacuous for exactly
        that reason. What is greppable is the branch's own wording, which
        disappears the moment the branch does.
        """
        script = _strip_comments(_script(render(build_payload(
            _inventory(), _metrics(), _analysis(), STAMP))))
        assert "all from measured metrics" in script, (
            "the findings note no longer has a no-configuration branch, so it "
            "reports the configuration count as 0 for a run where no "
            "configuration check happened")
        with_cfg = render(build_payload(_inventory(), _metrics(), _analysis(),
                                        STAMP, _config()))
        assert "from configuration checks" in with_cfg

    def test_the_method_section_states_which_stages_ran(self):
        """Method is where a reader decides how much of the report to trust.

        A report built from metrics alone and one that also graded configuration
        support different conclusions, and the difference must not be discoverable
        only by noticing that no check id appears in the findings table.
        """
        script = _strip_comments(_script(render(build_payload(
            _inventory(), _metrics(), _analysis(), STAMP))))
        assert "No configuration check ran for this report" in script
        assert "regardless of the observation window" in script

    def test_coverage_reports_the_registry_size_and_the_evaluated_ids(self):
        cov = _config_coverage(_config())
        assert cov["checks_in_registry"] == 14
        assert cov["check_ids"] == ["SEC-01", "SEC-03", "COST-06"]
        assert cov["clusters_checked"] == 1

    def test_a_cluster_stage_35_never_saw_is_named(self):
        """The real coverage gap, since every check runs against every cluster.

        check_configuration.py evaluates its whole registry per cluster, so a
        check is only ever evaluated or skipped-with-a-reason -- there is no
        third "not evaluated" state to report. What can go missing is a cluster:
        a record with no cluster_id is dropped with a warning, and a cluster
        discovered after Stage 3.5 ran is absent too. Its posture is then
        completely ungraded, and the findings table shows that as no rows, which
        reads as clean.
        """
        inv = _inventory(clusters=[
            dict(_inventory()["clusters"][0], cluster_id="node-1"),
            dict(_inventory()["clusters"][0], cluster_id="ghost"),
        ])
        payload = build_payload(inv, _metrics(), _analysis(), STAMP, _config())
        assert payload["config"]["unchecked_clusters"] == ["ghost"]
        html = render(payload)
        assert "Not checked at all" in html
        assert "ghost" in html

    def test_a_fully_checked_fleet_reports_no_gap(self):
        """The claim must be falsifiable, or it is decoration."""
        cov = _config_coverage(_config(), ["node-1"])
        assert cov["unchecked_clusters"] == []

    def test_omitting_the_inventory_does_not_invent_a_gap(self):
        """_config_coverage is also called directly in tests and by future
        callers; an absent inventory means "unknown", not "everything missing"."""
        assert _config_coverage(_config())["unchecked_clusters"] == []

    def test_skips_are_grouped_by_check_not_repeated_per_cluster(self):
        clusters = {
            f"c{i}": {"cluster_type": "node-based", "findings": [],
                      "checks_skipped": [
                          {"check_id": "COST-06",
                           "reason": "not applicable to node-based"}],
                      "checks_evaluated": 13}
            for i in range(5)
        }
        cov = _config_coverage(_config(clusters=clusters))
        assert len(cov["skipped"]) == 1
        assert cov["skipped"][0]["check_id"] == "COST-06"
        assert len(cov["skipped"][0]["clusters"]) == 5

    def test_skip_reasons_survive_to_the_document(self):
        html = render(build_payload(_inventory(), _metrics(), _analysis(),
                                    STAMP, _config()))
        assert "COST-06" in html
        assert "not applicable to node-based" in html

    def test_a_skip_is_not_reported_as_a_finding(self):
        """"Did not apply" and "failed" must not merge into one count."""
        payload = build_payload(_inventory(), _metrics(), _analysis(), STAMP,
                                _config())
        assert not any("COST-06" == f["check_id"]
                       for f in payload["findings"])

    def test_the_graded_policy_is_published(self):
        """A finding against a standard the customer does not use is a finding
        to override, and they cannot tell without seeing the threshold."""
        cov = _config_coverage(_config())
        assert cov["policy"]["min_snapshot_retention_days"] == 7
        assert cov["policy"]["required_tags"] == ["Environment", "Owner"]
        html = render(build_payload(_inventory(), _metrics(), _analysis(),
                                    STAMP, _config()))
        assert "Graded against" in html

    def test_a_zero_threshold_policy_value_is_still_published(self):
        """0 is a real policy (no snapshots required); falsy is not absent."""
        cfg = _config()
        cfg["metadata"]["policy"]["min_replicas_per_shard"] = 0
        cov = _config_coverage(cfg)
        assert cov["policy"]["min_replicas_per_shard"] == 0

    def test_missing_metadata_does_not_crash_the_render(self):
        """An older or partial Stage 3.5 file must degrade, not raise."""
        cfg = {"clusters": {}}
        cov = _config_coverage(cfg)
        assert cov["checks_in_registry"] is None
        assert cov["check_ids"] == []
        assert cov["skipped"] == []
        render(build_payload(_inventory(), _metrics(), _analysis(), STAMP, cfg))

    def test_an_empty_dict_is_treated_as_no_config_at_all(self):
        assert _config_coverage({}) is None
        assert _config_coverage(None) is None


class TestTheCheckGlossaryAgreesWithTheRegistry:
    """A reader who meets "SEC-03" in a finding row can look up what it means.

    The glossary is derived from check_configuration.CHECKS, so this is a
    doc-to-code agreement pin in both directions: a check the registry defines
    must appear with its own title and pillar, and the glossary may not carry an
    id the registry does not. It is imported here rather than duplicated, so the
    test cannot quietly agree with a stale copy.
    """

    def test_the_glossary_id_set_matches_the_registry(self):
        p = build_payload(_inventory(), _metrics(), _analysis(), STAMP, _config())
        glossary_ids = {g["check_id"] for g in p["check_glossary"]}
        assert glossary_ids == {c.check_id for c in CHECKS}

    def test_each_entry_carries_the_registry_title_and_pillar(self):
        p = build_payload(_inventory(), _metrics(), _analysis(), STAMP, _config())
        by_id = {g["check_id"]: g for g in p["check_glossary"]}
        for check in CHECKS:
            assert by_id[check.check_id]["title"] == check.title
            assert by_id[check.check_id]["pillar"] == check.pillar

    def test_every_check_id_and_its_meaning_reach_the_rendered_document(self):
        # The glossary renders regardless of whether Stage 3.5 ran, so a passed
        # check with no finding row still has its id explained here.
        html = render(build_payload(_inventory(), _metrics(), _analysis(), STAMP))
        for check in CHECKS:
            assert check.check_id in html
            assert check.title in html


class TestTheEmbeddedScriptParses:
    """The report's body is built entirely by its own inline JS.

    Every other test here checks the payload -- the data the JS reads. But the
    template is a Python string holding ~1,200 lines of JavaScript that nothing
    executes during the suite, and one syntax error in it renders a document with
    a complete <body> and nothing inside it. The failure is total and silent:
    valid HTML, valid JSON payload, no content, and every existing assertion
    still passes because they all read the payload rather than the page.

    `node --check` is a cheap floor against that. It is not a runtime test -- it
    would not catch a null dereference in the coverage branch -- but it makes the
    one failure mode that empties the whole report impossible to commit.
    """

    @pytest.mark.skipif(shutil.which("node") is None,
                        reason="node not available to parse the inline script")
    def test_the_inline_script_is_syntactically_valid(self, tmp_path):
        html = render(build_payload(_inventory(), _metrics(), _analysis(),
                                    STAMP, _config()))
        path = tmp_path / "report.js"
        path.write_text(_script(html), encoding="utf-8")
        result = subprocess.run(["node", "--check", str(path)],
                                capture_output=True, text=True)
        assert result.returncode == 0, (
            "the report's inline script does not parse, so the document would "
            f"render an empty body:\n{result.stderr}")

    @pytest.mark.skipif(shutil.which("node") is None,
                        reason="node not available to parse the inline script")
    def test_it_also_parses_without_config_findings(self, tmp_path):
        """Both branches of the coverage section are in the same script."""
        html = render(build_payload(_inventory(), _metrics(), _analysis(),
                                    STAMP))
        path = tmp_path / "report.js"
        path.write_text(_script(html), encoding="utf-8")
        result = subprocess.run(["node", "--check", str(path)],
                                capture_output=True, text=True)
        assert result.returncode == 0, result.stderr


# ---------------------------------------------------------------------------
# Agent notes (Phase 5b)
# ---------------------------------------------------------------------------

def _notes(**over):
    """A notes.json whose every figure is present under the paths it cites.

    Written against _analysis()'s default percentiles: p95 0.4, max 0.67.
    """
    base = {
        "assessment": {
            "prose": "The cluster peaks at 0.67% CPU, so it is idle.",
            "cites": ["clusters.node-1.percentiles.EngineCPUUtilization_Maximum"],
        },
        "context": {"environment": "confirmed non-production", "source": "user"},
        "finding_notes": [],
        "priorities": [],
    }
    base.update(over)
    return base


def _finding(finding_id="node-1-percentile-001", **over):
    """A Stage 3 finding, in the shape analyze_metrics.py writes it."""
    row = {
        "finding_id": finding_id,
        "severity": "HIGH",
        "title": "CPU sustained above threshold",
        "description": "p95 is 0.4",
        "recommendation": "Scale up",
        "metric_name": "EngineCPUUtilization",
        "current_value": 0.4,
    }
    row.update(over)
    return row


class TestProseFiguresSeparatesClaimsFromNames:
    """A figure is a claim about the fleet; a name that contains a digit is not.

    This distinction is the whole usability of the check. If "p95" or "SEC-06"
    counted as figures, an author would satisfy the check by never naming a
    metric or a check id -- making the prose less traceable in order to pass a
    traceability check.
    """

    @pytest.mark.parametrize("text,expected", [
        ("CPU peaked at 80.55%", ["80.55"]),
        ("about 1,024 items", ["1,024"]),
        ("held 4.5GB", ["4.5"]),
        ("~12% of the window", ["12"]),
        ("spend was $47.20", ["47.20"]),
        (">90% memory", ["90"]),
        ("2 of 7 clusters", ["2", "7"]),
        ("latency of 250ms", ["250"]),
    ])
    def test_figures_are_found(self, text, expected):
        assert prose_figures(text) == expected

    @pytest.mark.parametrize("text", [
        "the p95 is thin",
        "SEC-06 applies",
        "on cache.m5.large",
        "redis 7.1.0 is current",  # a version is a name, not a measurement
        "in us-east-1",
        "EngineCPUUtilization is flat",
        "the CacheHitRate2 metric",
    ])
    def test_names_containing_digits_are_not_figures(self, text):
        assert prose_figures(text) == []

    def test_a_figure_is_returned_as_written(self):
        # The caller reports these back to a human who will search the file for
        # them, so normalising "80.6" to "80.60" would name a figure that is
        # not there.
        assert prose_figures("peaked at 80.60% and 0.4%") == ["80.60", "0.4"]

    def test_a_unit_is_stripped_longest_first(self):
        # "mib" must not be stripped as "b", leaving "4.5mi" to fail the
        # numeric test and pass the check as a name.
        assert prose_figures("4.5MiB of data") == ["4.5"]


class TestFabricatedFiguresAreRejected:
    """The check that makes agent prose safe to embed.

    Not "the agent was asked to be careful" -- a figure that is not in the data
    fails the render and is named in the error.
    """

    def _verify(self, notes, findings=None, config=None):
        return verify_notes(
            notes,
            {"inventory": _inventory(), "analysis": _analysis(findings=findings),
             "config": config or {}},
        )

    def test_a_figure_present_under_the_cite_passes(self):
        self._verify(_notes())  # does not raise

    def test_a_plausible_but_absent_figure_is_rejected_by_name(self):
        notes = _notes(assessment={
            "prose": "The cluster peaks at 73.42% CPU.",
            "cites": ["clusters.node-1.percentiles.EngineCPUUtilization_Maximum"],
        })
        with pytest.raises(NotesError, match="73.42"):
            self._verify(notes)

    def test_a_figure_may_be_rounded_to_the_precision_it_was_written_at(self):
        # Prose rounds; that is what prose is for. "0.7" is an honest reading of
        # 0.67 and a note forced to write 0.6700000000000001 would be worse.
        notes = _notes(assessment={
            "prose": "It peaks around 0.7%.",
            "cites": ["clusters.node-1.percentiles.EngineCPUUtilization_Maximum"],
        })
        self._verify(notes)

    def test_rounding_does_not_extend_to_a_different_number(self):
        notes = _notes(assessment={
            "prose": "It peaks around 0.9%.",
            "cites": ["clusters.node-1.percentiles.EngineCPUUtilization_Maximum"],
        })
        with pytest.raises(NotesError, match="0.9"):
            self._verify(notes)

    def test_more_precision_than_the_source_is_rejected(self):
        # 0.6712 asserts digits the source does not have, which is the subtle
        # form of fabrication: right magnitude, invented precision.
        notes = _notes(assessment={
            "prose": "It peaks at 0.6712%.",
            "cites": ["clusters.node-1.percentiles.EngineCPUUtilization_Maximum"],
        })
        with pytest.raises(NotesError, match="0.6712"):
            self._verify(notes)

    def test_every_problem_is_reported_not_just_the_first(self):
        notes = _notes(assessment={
            "prose": "It peaks at 73.42% having held 999.9GB.",
            "cites": ["clusters.node-1.percentiles.EngineCPUUtilization_Maximum"],
        })
        with pytest.raises(NotesError) as err:
            self._verify(notes)
        assert "73.42" in str(err.value) and "999.9" in str(err.value)

    def test_prose_with_no_figures_needs_no_cite(self):
        # Judgement without numbers is still the agent's job, and demanding a
        # citation for "this looks like a sizing problem" would push the agent
        # to invent one.
        self._verify(_notes(assessment={
            "prose": "This reads as a sizing problem rather than a spike.",
            "cites": [],
        }))

    def test_a_figure_with_no_cite_at_all_is_rejected(self):
        with pytest.raises(NotesError, match="nothing"):
            self._verify(_notes(assessment={
                "prose": "It peaks at 0.67%.", "cites": []}))

    def test_the_reasoning_on_a_finding_note_is_checked_too(self):
        notes = _notes(finding_notes=[{
            "finding_id": "node-1-percentile-001", "verdict": "confirmed",
            "reasoning": "The 55.5% peak is real.",
            "cites": ["clusters.node-1.percentiles.EngineCPUUtilization_Maximum"],
        }])
        with pytest.raises(NotesError, match="55.5"):
            self._verify(notes, findings=[_finding()])

    def test_priority_prose_is_checked_too(self):
        notes = _notes(priorities=[{
            "rank": 1, "action": "Resize to 12.5GB", "why": "It is small",
            "cites": ["clusters.node-1.percentiles.EngineCPUUtilization_Maximum"],
        }])
        with pytest.raises(NotesError, match="12.5"):
            self._verify(notes)


class TestTheCheckIsScopedToTheCitedPath:
    """Scoping is the check. Without it, it is a formality.

    metrics.json for the seven-cluster example fleet holds ~1.9 million distinct
    numbers, which covers every one of the 10,001 two-decimal values between 0
    and 100 -- so "the figure appears somewhere in the source" would pass any
    fabricated percentage a note could write. These tests pin the scope rather
    than the mechanism.
    """

    def _verify(self, notes):
        return verify_notes(
            notes,
            {"inventory": _inventory(),
             "analysis": _analysis(percentiles={
                 "EngineCPUUtilization_Maximum": {"p95": 0.4, "max": 0.67},
                 "BytesUsedForCache_Maximum": {"max": 88.25},
             }),
             "config": {}},
        )

    def test_a_real_figure_from_an_uncited_path_is_still_rejected(self):
        # 88.25 is genuinely in the data -- under a path this note does not
        # cite. Citing one metric does not license quoting another.
        notes = _notes(assessment={
            "prose": "It reached 88.25.",
            "cites": ["clusters.node-1.percentiles.EngineCPUUtilization_Maximum"],
        })
        with pytest.raises(NotesError, match="88.25"):
            self._verify(notes)

    def test_the_same_figure_passes_once_its_own_path_is_cited(self):
        self._verify(_notes(assessment={
            "prose": "It reached 88.25.",
            "cites": ["clusters.node-1.percentiles.BytesUsedForCache_Maximum"],
        }))

    def test_raw_metrics_are_not_a_citable_source(self):
        # Deliberate, and not a matter of degree: one raw metric series on the
        # example fleet is 8,064 datapoints admitting 46% of the plausible
        # percentage grid. The summary of a series is citable; the series is not.
        assert "metrics" not in CITABLE_SOURCES

    def test_a_cite_into_raw_metrics_fails_as_unresolvable(self):
        notes = _notes(assessment={
            "prose": "It peaks at 0.67%.",
            "cites": ["clusters.node-1.nodes.node-1-001.EngineCPUUtilization"],
        })
        with pytest.raises(NotesError, match="no citable source"):
            verify_notes(notes, {"inventory": _inventory(),
                                 "metrics": _metrics(),
                                 "analysis": _analysis(), "config": {}})

    def test_a_cite_that_resolves_nowhere_is_an_error(self):
        # A citation to nothing looks like evidence to every reader who does
        # not try to follow it, so it fails even before any figure is checked.
        notes = _notes(assessment={
            "prose": "It peaks at 0.67%.",
            "cites": ["clusters.node-1.percentiles.NoSuchMetric"],
        })
        with pytest.raises(NotesError, match="NoSuchMetric"):
            self._verify(notes)

    def test_a_cite_broad_enough_to_authorise_anything_is_rejected(self):
        big = {f"Metric{i}_Maximum": {"max": i / 100} for i in range(600)}
        notes = _notes(assessment={"prose": "It peaks at 3.33%.",
                                   "cites": ["clusters.node-1.percentiles"]})
        with pytest.raises(NotesError, match="authorises almost any figure"):
            verify_notes(notes, {"inventory": _inventory(),
                                 "analysis": _analysis(percentiles=big),
                                 "config": {}})

    def test_a_too_broad_cite_does_not_also_report_every_figure(self):
        # The harvest is truncated at the cap, so continuing would report each
        # figure as fabricated and bury the one problem worth fixing.
        big = {f"Metric{i}_Maximum": {"max": i / 100} for i in range(600)}
        notes = _notes(assessment={"prose": "It peaks at 3.33%.",
                                   "cites": ["clusters.node-1.percentiles"]})
        with pytest.raises(NotesError) as err:
            verify_notes(notes, {"inventory": _inventory(),
                                 "analysis": _analysis(percentiles=big),
                                 "config": {}})
        assert "3.33" not in str(err.value)

    def test_narrow_cites_cannot_be_stacked_to_the_same_breadth(self):
        # One budget for the note, not one per cite. Each cite below is narrow
        # enough to be legitimate on its own -- 10 numbers, the size of a real
        # percentiles entry -- but 60 of them reach past the cap together.
        # Per-cite budgets would admit this, which is the same hole by a longer
        # route.
        pct = {f"M{i}_Maximum": {f"p{j}": i * 100 + j for j in range(10)}
               for i in range(60)}
        notes = _notes(assessment={
            "prose": "It peaks at 3.33%.",
            "cites": [f"clusters.node-1.percentiles.M{i}_Maximum"
                      for i in range(60)],
        })
        with pytest.raises(NotesError, match="authorises almost any figure"):
            verify_notes(notes, {"inventory": _inventory(),
                                 "analysis": _analysis(percentiles=pct),
                                 "config": {}})

    def test_one_of_those_cites_alone_is_perfectly_legitimate(self):
        """Guards the guard above: the cap must reject the stack, not the cite.

        A test that passed because each individual cite was already too broad
        would prove nothing about the shared budget.
        """
        pct = {f"M{i}_Maximum": {f"p{j}": i * 100 + j for j in range(10)}
               for i in range(60)}
        verify_notes(
            _notes(assessment={"prose": "It peaks at 3.", "cites":
                               ["clusters.node-1.percentiles.M0_Maximum"]}),
            {"inventory": _inventory(), "analysis": _analysis(percentiles=pct),
             "config": {}})

    def test_the_error_names_a_few_cites_rather_than_all_of_them(self):
        """An error longer than the notes file is an error nobody reads."""
        notes = _notes(assessment={
            "prose": "It peaks at 73.42%.",
            "cites": [f"clusters.node-1.percentiles.M{i}_Maximum"
                      for i in range(30)]})
        pct = {f"M{i}_Maximum": {"max": i} for i in range(30)}
        with pytest.raises(NotesError) as err:
            verify_notes(notes, {"inventory": _inventory(),
                                 "analysis": _analysis(percentiles=pct),
                                 "config": {}})
        assert "other cites" in str(err.value)
        assert "M29_Maximum" not in str(err.value)

    def test_the_cap_leaves_room_for_a_whole_cluster(self):
        # Sized from the example fleet: a cluster's entire analysis entry is
        # ~229 numbers. A claim about one cluster must stay checkable against
        # that cluster, or the check pushes authors toward citing nothing.
        assert MAX_CITE_NUMBERS >= 229

    def test_a_path_in_two_documents_resolves_against_both(self):
        # clusters.node-1 exists in analysis.json and config_findings.json.
        # Resolving to whichever was listed first made the answer depend on
        # dict insertion order, and reported a real figure as fabricated.
        notes = _notes(assessment={
            "prose": "13 checks were evaluated.",
            "cites": ["clusters.node-1.checks_evaluated"],
        })
        verify_notes(notes, {"inventory": _inventory(),
                             "analysis": _analysis(), "config": _config()})

    def test_a_boolean_does_not_authorise_the_figure_one(self):
        # True is not the measurement 1. Admitting it would let a note quote
        # "1" against a cite of a TLS flag.
        notes = _notes(assessment={"prose": "Exactly 1 replica is configured.",
                                   "cites": ["clusters.0.tls_enabled"]})
        with pytest.raises(NotesError):
            verify_notes(notes, {"inventory": _inventory(),
                                 "analysis": _analysis(), "config": {}})


class TestNotesReachTheReport:
    def _payload(self, notes, findings=None):
        return build_payload(_inventory(), _metrics(),
                             _analysis(findings=findings or [_finding()]),
                             STAMP, _config(), notes)

    def test_the_assessment_is_in_the_payload(self):
        p = self._payload(_notes())
        assert p["notes"]["assessment"] == \
            "The cluster peaks at 0.67% CPU, so it is idle."

    def test_the_environment_carries_where_it_came_from(self):
        # "the user told us" and "we inferred it from a tag" carry different
        # weight, and every recommendation leans on it.
        p = self._payload(_notes())
        assert p["notes"]["environment"] == "confirmed non-production"
        assert p["notes"]["environment_source"] == "user"

    def test_a_verdict_attaches_to_the_finding_it_names(self):
        notes = _notes(finding_notes=[{
            "finding_id": "node-1-percentile-001", "verdict": "confirmed",
            "reasoning": "Real and sustained.", "cites": []}])
        p = self._payload(notes)
        row = [f for f in p["findings"]
               if f["finding_id"] == "node-1-percentile-001"][0]
        assert row["verdict"] == "confirmed"
        assert row["note"] == "Real and sustained."

    def test_a_note_does_not_leak_onto_a_neighbouring_finding(self):
        notes = _notes(finding_notes=[{
            "finding_id": "node-1-percentile-002", "verdict": "confirmed",
            "reasoning": "Only this one.", "cites": []}])
        p = self._payload(notes, findings=[
            _finding("node-1-percentile-001"),
            _finding("node-1-percentile-002", title="Other finding")])
        by_id = {f["finding_id"]: f for f in p["findings"]}
        assert by_id["node-1-percentile-002"]["note"] == "Only this one."
        assert by_id["node-1-percentile-001"].get("note") is None

    def test_a_configuration_finding_can_be_annotated_too(self):
        # Both stages' findings merge into one table, so a note must be able to
        # reach either. Stage 3.5's ids are namespaced differently.
        notes = _notes(finding_notes=[{
            "finding_id": "config-node-1-sec-03", "verdict": "confirmed",
            "reasoning": "Auth really is off.", "cites": []}])
        p = self._payload(notes)
        row = [f for f in p["findings"]
               if f["finding_id"] == "config-node-1-sec-03"][0]
        assert row["note"] == "Auth really is off."
        assert row["source"] == "configuration"

    def test_priorities_are_ordered_by_rank_not_by_file_order(self):
        notes = _notes(priorities=[
            {"rank": 2, "action": "Second", "why": "b", "cites": []},
            {"rank": 1, "action": "First", "why": "a", "cites": []}])
        p = self._payload(notes)
        assert [x["action"] for x in p["notes"]["priorities"]] == \
            ["First", "Second"]

    def test_a_note_naming_an_unknown_finding_fails_the_render(self):
        # The failure this prevents: Stage 3 numbered findings per cluster, so
        # three clusters each had a "percentile-001". A note keyed on the bare
        # id would have annotated whichever was read first, with no error.
        notes = _notes(finding_notes=[{
            "finding_id": "percentile-001", "verdict": "confirmed",
            "reasoning": "x", "cites": []}])
        with pytest.raises(NotesError, match="annotate nothing"):
            self._payload(notes)

    def test_build_payload_verifies_rather_than_trusting_its_caller(self):
        # Checked inside build_payload so no code path can embed unverified
        # agent prose by forgetting a call.
        notes = _notes(assessment={
            "prose": "It peaks at 73.42%.",
            "cites": ["clusters.node-1.percentiles.EngineCPUUtilization_Maximum"]})
        with pytest.raises(NotesError):
            self._payload(notes)

    def test_findings_are_still_counted_in_full_alongside_a_dismissal(self):
        # A false positive is annotated, never removed: the row is still what
        # the pipeline produced, and deleting it would hide a Stage 3 bug
        # behind the agent's opinion.
        notes = _notes(finding_notes=[{
            "finding_id": "node-1-percentile-001", "verdict": "false_positive",
            "reasoning": "Not a real problem.", "cites": []}])
        p = self._payload(notes)
        ids = [f["finding_id"] for f in p["findings"]]
        assert "node-1-percentile-001" in ids
        assert p["notes"]["annotated"] == 1
        assert p["notes"]["findings_total"] == len(p["findings"])

    def test_an_unrecognised_verdict_keeps_the_reasoning_and_drops_the_label(self):
        notes = _notes(finding_notes=[{
            "finding_id": "node-1-percentile-001", "verdict": "probably_fine",
            "reasoning": "Still worth reading.", "cites": []}])
        p = self._payload(notes)
        row = [f for f in p["findings"]
               if f["finding_id"] == "node-1-percentile-001"][0]
        assert row["verdict"] is None
        assert row["note"] == "Still worth reading."


class TestTheReportRendersWithoutNotes:
    """The data sections are the floor. Agent prose is additive."""

    def test_the_payload_says_none_rather_than_an_empty_structure(self):
        p = build_payload(_inventory(), _metrics(), _analysis(), STAMP,
                          _config())
        assert p["notes"] is None

    def test_no_analyst_box_but_a_no_ai_review_notice_leads_the_section(self):
        # 7b: the AI review leads "What the data shows" when a pass ran; without
        # one, the slot renders an explicit notice rather than the analyst box or
        # a vanished section. Assert the guard (box only with notes) AND the else
        # branch's notice. Render-path, not payload; jsdom is not a committed dep
        # (D10), so this pins the fallback's presence and
        # TestTheEmbeddedScriptParses covers that it is valid JS.
        html = render(build_payload(_inventory(), _metrics(), _analysis(),
                                    STAMP, _config()))
        script = _strip_comments(_script(html))
        assert "NOTES && (NOTES.assessment || NOTES.environment)" in script
        assert "No AI review was generated for this run" in script

    def test_the_findings_table_has_no_analyst_column(self):
        script = _strip_comments(_script(render(build_payload(
            _inventory(), _metrics(), _analysis(), STAMP, _config()))))
        assert 'if (NOTES) findingHeaders.push' in script

    def test_the_method_section_states_that_no_analyst_pass_ran(self):
        # Absence stated as absence. An unreviewed findings table and a
        # reviewed one with no dismissals look identical, so the report has to
        # say which it is -- the same rule as unmeasured-is-not-zero.
        script = _strip_comments(_script(render(build_payload(
            _inventory(), _metrics(), _analysis(), STAMP, _config()))))
        assert "No AI review pass reviewed these findings" in script

    def test_the_findings_note_does_not_imply_a_review_that_did_not_happen(self):
        p = build_payload(_inventory(), _metrics(), _analysis(), STAMP,
                          _config())
        assert p["notes"] is None
        script = _strip_comments(_script(render(p)))
        # reviewNote is empty when NOTES is null; the count sentence must be
        # inside that branch rather than unconditional.
        assert 'const reviewNote = NOTES' in script


class TestActionsDoNotAssertAFleetWideIdleState:
    """6b: the recommended-actions prose is classification-derived, never a blanket
    "all clusters are idle / serve zero traffic". The action strings are JS-rendered
    (not payload-visible) and jsdom is not a committed dep (D10), so this pins the
    removal of the old unconditional claims and the presence of the classification
    gates in the embedded script; TestTheEmbeddedScriptParses covers validity.
    """

    def _script(self):
        html = render(build_payload(_inventory(), _metrics(), _analysis(),
                                    STAMP, _config()))
        return _strip_comments(_script(html))

    def test_no_blanket_fleet_wide_idle_or_history_claims(self):
        s = self._script()
        for banned in ("serve zero traffic", "zero cache commands",
                       "how this fleet got here", "Four pipeline defects"):
            assert banned not in s, f"fleet-specific claim resurfaced: {banned!r}"

    def test_the_decommission_and_scale_actions_are_gated_on_classification(self):
        s = self._script()
        assert 'classification === "IDLE"' in s
        assert 'classification === "SATURATED"' in s

    def test_an_empty_action_set_is_stated_not_left_blank(self):
        # A compliant, busy fleet with no idle/saturated cluster and no gaps must
        # say "no fleet-level actions", never render an empty section.
        assert "No fleet-level actions" in self._script()


class TestNotesAreAutoIncluded:
    """Every agent-run report carries the AI review with no flag: the render
    auto-picks output/notes.json (SKILL Step 4 requires the agent to write it).
    This is the mechanic behind "every report has an AI review" — the wording may
    vary run to run, but it is present whenever the agent step ran.
    """

    def _populate(self, tmp_path, with_notes):
        out = tmp_path / "output"
        out.mkdir()
        (out / "inventory.json").write_text(json.dumps(_inventory()))
        (out / "analysis.json").write_text(json.dumps(_analysis()))
        (out / "config_findings.json").write_text(json.dumps(_config()))
        (out / "report_data.json").write_text(
            json.dumps(build_report_data(_metrics())))
        if with_notes:
            (out / "notes.json").write_text(json.dumps(_notes(assessment={
                # AUTOPICKMARKER is a non-numeric token (ignored by the figure
                # check); 0.67 matches the cited max, so verify_notes passes.
                "prose": "The cluster peaks at 0.67% CPU. AUTOPICKMARKER.",
                "cites": [
                    "clusters.node-1.percentiles.EngineCPUUtilization_Maximum"],
            })))

    def test_notes_json_is_auto_included_without_a_flag(self, tmp_path,
                                                        monkeypatch):
        self._populate(tmp_path, with_notes=True)
        monkeypatch.chdir(tmp_path)
        assert main(["--output", "output/report.html"]) == 0
        html = (tmp_path / "output" / "report.html").read_text()
        assert "AUTOPICKMARKER" in html  # embedded with no --notes flag passed

    def test_absent_notes_leaves_the_review_out(self, tmp_path, monkeypatch):
        self._populate(tmp_path, with_notes=False)
        monkeypatch.chdir(tmp_path)
        assert main(["--output", "output/report.html"]) == 0
        html = (tmp_path / "output" / "report.html").read_text()
        assert "AUTOPICKMARKER" not in html


class TestNotesReproducibility:
    def test_the_same_notes_produce_byte_identical_output(self):
        args = (_inventory(), _metrics(), _analysis(findings=[_finding()]),
                STAMP, _config())
        first = render(build_payload(*args, _notes()))
        second = render(build_payload(*args, _notes()))
        assert first == second

    def test_changing_the_notes_changes_the_output(self):
        # Guards the guard above: byte-identical output would also be produced
        # if the notes were silently ignored.
        args = (_inventory(), _metrics(), _analysis(findings=[_finding()]),
                STAMP, _config())
        with_notes = render(build_payload(*args, _notes()))
        without = render(build_payload(*args))
        assert with_notes != without

    @pytest.mark.skipif(shutil.which("node") is None,
                        reason="node not available to parse the inline script")
    def test_the_inline_script_still_parses_with_notes(self, tmp_path):
        html = render(build_payload(
            _inventory(), _metrics(), _analysis(findings=[_finding()]),
            STAMP, _config(),
            _notes(finding_notes=[{"finding_id": "node-1-percentile-001",
                                   "verdict": "false_positive",
                                   "reasoning": "x", "cites": []}],
                   priorities=[{"rank": 1, "action": "a", "why": "b",
                                "cites": []}])))
        path = tmp_path / "report.js"
        path.write_text(_script(html), encoding="utf-8")
        result = subprocess.run(["node", "--check", str(path)],
                                capture_output=True, text=True)
        assert result.returncode == 0, result.stderr


# ---------------------------------------------------------------------------
# Per-cluster panels (Phase 5a)
# ---------------------------------------------------------------------------

def _multi_metrics(cpu_by_cluster, net_by_cluster=None, points=24):
    """metrics.json for several node-based clusters at once.

    The per-cluster panels need more than one cluster to exist -- panel ordering
    and coverage cannot be expressed by a single-cluster fixture.
    """
    stamps = [f"2026-08-01T{i // 12:02d}:{(i % 12) * 5:02d}:00Z"
              for i in range(points)]
    clusters = {}
    for cid, cpu in cpu_by_cluster.items():
        node = {}
        if cpu is not None:
            node["EngineCPUUtilization"] = {"Maximum": [cpu] * points}
        net = (net_by_cluster or {}).get(cid, 1048576.0)
        if net is not None:
            node["NetworkBytesIn"] = {"Sum": [net] * points}
        clusters[cid] = {"cluster_type": "node-based",
                         "timestamps_5min": stamps,
                         "nodes": {f"{cid}-001": node},
                         "errors": []}
    return {
        "metadata": {"period_start": "2026-07-28T00:00:00Z",
                     "period_end": "2026-08-11T00:00:00Z",
                     "total_datapoints": 999},
        "clusters": clusters,
        "cost": {"daily": [{"date": "2026-07-28", "total_usd": 1.0,
                            "by_usage_type": {"NodeUsage:cache.m5.large": 1.0}}]},
    }


def _multi_inventory(cluster_ids):
    return _inventory([{
        "cluster_id": cid,
        "cluster_type": "node-based",
        "region": "us-east-1",
        "engine": "valkey",
        "engine_version": "8.0",
        "node_type": "cache.m5.large",
        "total_nodes": 1,
        "tls_enabled": True,
        "encryption_at_rest": True,
        "auth_mode": "RBAC",
        "multi_az": True,
        "automatic_failover": True,
        "snapshot_retention_days": 7,
        "tags": {},
        "parameters": {"maxmemory-policy": "allkeys-lru"},
    } for cid in cluster_ids])


def _multi_analysis(cluster_ids, classifications=None, findings=None,
                    steadiness=None):
    return {
        "metadata": {"clusters_analyzed": len(cluster_ids)},
        "clusters": {cid: {
            "percentiles": {"EngineCPUUtilization_Maximum": {"p95": 1.0,
                                                             "max": 2.0}},
            "utilization": {
                "classification": (classifications or {}).get(cid, "BALANCED"),
                "recommendation": "Well-sized, no immediate action needed",
            },
            # label defaults to None (not measured), which the savings gate
            # treats as "no steadiness signal" -- listed without a risk note.
            "steadiness": {"label": (steadiness or {}).get(cid)},
            "findings": (findings or {}).get(cid, []),
            "errors": [],
        } for cid in cluster_ids},
    }


class TestPanelCharacterizationLabels:
    """7e: the per-cluster panel carries the steadiness label and read/write mix.

    Both are the honest-absence kind of signal: an idle or command-less cluster
    carries no measured steadiness and no read/write split, and the panel must
    pass that through as such rather than as a default (the JS renders it as
    "not measured", never a 50/50).
    """

    def _payload(self):
        ids = ["c-steady", "c-idle"]
        met = _multi_metrics({"c-steady": 1.0, "c-idle": 2.0})
        an = _multi_analysis(
            ids,
            classifications={"c-steady": "BALANCED", "c-idle": "IDLE"},
            steadiness={"c-steady": "steady"},
        )
        an["clusters"]["c-steady"]["efficiency"] = {"read_write_ratio": {
            "class": "read-heavy", "read_pct": 82.0, "write_pct": 18.0,
            "value": 4.56}}
        return build_payload(_multi_inventory(ids), met, an, STAMP)

    def _panel(self, payload, cid):
        return [p for p in payload["panels"] if p["cluster"] == cid][0]

    def test_steadiness_label_is_carried_on_the_panel(self):
        assert self._panel(self._payload(), "c-steady")["steadiness"] == "steady"

    def test_idle_cluster_carries_no_measured_steadiness(self):
        assert self._panel(self._payload(), "c-idle")["steadiness"] in (
            None, "not_measured")

    def test_read_write_mix_is_carried_on_the_panel(self):
        rw = self._panel(self._payload(), "c-steady")["read_write"]
        assert rw["class"] == "read-heavy" and rw["read_pct"] == 82.0

    def test_a_command_less_cluster_has_no_fabricated_split(self):
        rw = self._panel(self._payload(), "c-idle")["read_write"]
        assert rw.get("class") in (None, "not_measured")
        assert rw.get("read_pct") is None


class TestEveryClusterGetsItsOwnPanel:
    """Every cluster in the inventory gets a panel, worst-in-need-first."""

    def _payload(self, count=6):
        ids = [f"c{i}" for i in range(count)]
        met = _multi_metrics({cid: float(i + 1) for i, cid in enumerate(ids)})
        return build_payload(_multi_inventory(ids), met,
                             _multi_analysis(ids), STAMP)

    def test_a_panel_exists_for_every_inventory_cluster(self):
        payload = self._payload()
        assert ({p["cluster"] for p in payload["panels"]} ==
                {f"c{i}" for i in range(6)})

    def test_panels_are_ordered_by_attention_worst_severity_first(self):
        # Panels are ordered by _attention_key: worst finding severity first,
        # then classification, then cluster id. A cluster with a CRITICAL must
        # sort ahead of quiet ones, and the quiet ones fall back to the id
        # tiebreak -- which is what keeps the order stable between two runs.
        ids = ["quiet-a", "quiet-b", "hot"]
        met = _multi_metrics({cid: 5.0 for cid in ids})
        an = _multi_analysis(ids, findings={
            "hot": [{"severity": "CRITICAL", "title": "x",
                     "finding_id": "hot-percentile-001"}]})
        order = [p["cluster"] for p in
                 build_payload(_multi_inventory(ids), met, an, STAMP)["panels"]]
        assert order[0] == "hot"
        assert order[1:] == ["quiet-a", "quiet-b"]

    def test_a_cluster_with_no_metrics_still_gets_a_panel_saying_so(self):
        """Silently omitting it would make a monitoring gap look like a cluster
        that does not exist."""
        ids = ["seen", "unmonitored"]
        met = _multi_metrics({"seen": 5.0})           # no entry for the other
        payload = build_payload(_multi_inventory(ids), met,
                                _multi_analysis(ids), STAMP)
        panel = [p for p in payload["panels"]
                 if p["cluster"] == "unmonitored"][0]
        assert panel["metrics_reported"] is False
        assert panel["chart_count"] == 0

    def test_a_panel_carries_the_worst_severity_across_both_sources(self):
        ids = ["c0"]
        met = _multi_metrics({"c0": 5.0})
        an = _multi_analysis(ids, findings={
            "c0": [{"severity": "MEDIUM", "title": "m",
                    "finding_id": "c0-percentile-001"}]})
        cfg = _config(clusters={"c0": {
            "cluster_type": "node-based",
            "findings": [{"severity": "CRITICAL",
                          "finding_id": "config-c0-sec-01", "title": "t",
                          "description": "d", "recommendation": "r",
                          "check_id": "SEC-01", "pillar": "Security",
                          "metric_name": None, "current_value": False}],
            "checks_skipped": [], "checks_evaluated": 14}})
        payload = build_payload(_multi_inventory(ids), met, an, STAMP, cfg)
        panel = payload["panels"][0]
        assert panel["worst_severity"] == "CRITICAL"
        assert panel["finding_count"] == 2

    def test_a_panel_with_no_findings_says_none_rather_than_a_severity(self):
        payload = self._payload(count=1)
        panel = payload["panels"][0]
        assert panel["worst_severity"] is None
        assert panel["finding_count"] == 0


class TestAPanelChartCarriesItsOwnUnitsAndStatistic:
    """A number is a scalar plus a unit plus a statistic, so the chart says all
    three. A Maximum and an Average of one metric are different facts, and a
    reader checking a figure against CloudWatch has to know which to ask for."""

    def _panel(self):
        ids = ["c0"]
        met = _multi_metrics({"c0": 5.0})
        met["clusters"]["c0"]["nodes"]["c0-001"].update({
            "BytesUsedForCache": {"Maximum": [1073741824.0] * 24},
            "CacheHitRate": {"Average": [92.5] * 24},
        })
        payload = build_payload(_multi_inventory(ids), met,
                                _multi_analysis(ids), STAMP)
        return payload["panels"][0]

    def test_every_chart_names_its_statistic(self):
        for chart in _bundle_charts(self._panel()).values():
            assert chart["statistic"] in ("Maximum", "Average", "Sum")

    def test_the_statistic_is_the_one_the_series_was_read_with(self):
        charts = _bundle_charts(self._panel())
        assert charts["CacheHitRate"]["statistic"] == "Average"
        assert charts["BytesUsedForCache"]["statistic"] == "Maximum"

    def test_a_byte_metric_is_converted_and_labelled_in_mb(self):
        # 1 GiB read as "1073741824 MB" is the unit half of the recurring bug.
        chart = _bundle_charts(self._panel())["BytesUsedForCache"]
        assert chart["unit"].strip() == "MB"
        assert chart["series"][0]["values"][0] == 1024.0

    def test_a_percentage_metric_is_left_as_a_percentage(self):
        # CacheHitRate is published 0-100 despite a catalog description reading
        # like a ratio. Divided to 0-1 it would draw flat along the axis and read
        # as a cache that never hits.
        chart = _bundle_charts(self._panel())["CacheHitRate"]
        assert chart["unit"] == "%"
        assert chart["series"][0]["values"][0] == 92.5

    def test_every_chart_has_a_timestamp_per_value(self):
        # One shared timestamp axis per chart; every node series aligns to it.
        for chart in _bundle_charts(self._panel()).values():
            for s in chart["series"]:
                assert len(chart["timestamps"]) == len(s["values"])


class TestAnAbsentPanelChartIsDistinguishedFromAZeroOne:
    """Omitting both without saying which is the absence-as-measurement bug
    inverted: the reader assumes the metric was fine."""

    def _panel(self):
        ids = ["c0"]
        met = _multi_metrics({"c0": 5.0})
        # Reported, and zero for the whole window.
        met["clusters"]["c0"]["nodes"]["c0-001"]["Evictions"] = {
            "Sum": [0.0] * 24}
        payload = build_payload(_multi_inventory(ids), met,
                                _multi_analysis(ids), STAMP)
        return payload["panels"][0]

    def test_a_flat_zero_metric_is_listed_as_measured_zero(self):
        panel = self._panel()
        assert any("Eviction" in label for label in panel["flat_zero"])

    def test_an_unreported_metric_is_listed_separately(self):
        panel = self._panel()
        assert panel["not_reported"]
        assert not set(panel["flat_zero"]) & set(panel["not_reported"])

    def test_neither_list_holds_a_charted_metric(self):
        panel = self._panel()
        charted = {c["label"] for c in _bundle_charts(panel).values()}
        assert not charted & set(panel["flat_zero"])
        assert not charted & set(panel["not_reported"])


class TestThePanelSectionRenders:
    """The payload is only half of it: the panels are built by the inline JS."""

    def _html(self):
        ids = [f"c{i}" for i in range(6)]
        met = _multi_metrics({cid: float(i + 1) for i, cid in enumerate(ids)})
        return render(build_payload(_multi_inventory(ids), met,
                                    _multi_analysis(ids), STAMP))

    def test_the_section_and_its_mount_point_exist(self):
        html = self._html()
        assert 'id="panels"' in html
        assert "Cluster detail" in html

    def test_no_chart_colour_is_chosen_by_series_index(self):
        """The bug was `colors[si % colors.length]`. Banning the modulo keeps it
        from coming back through a new call site."""
        script = _strip_comments(_script(self._html()))
        assert "% colors.length" not in script
        assert "% slots.length" not in script

    @pytest.mark.skipif(shutil.which("node") is None,
                        reason="node not available to parse the inline script")
    def test_the_inline_script_still_parses_with_panels(self, tmp_path):
        path = tmp_path / "report.js"
        path.write_text(_script(self._html()), encoding="utf-8")
        result = subprocess.run(["node", "--check", str(path)],
                                capture_output=True, text=True)
        assert result.returncode == 0, result.stderr


# ---------------------------------------------------------------------------
# Priced savings (Phase 7c)
# ---------------------------------------------------------------------------

def _priced_cluster(current, engine="valkey", gen7=True):
    """One priced cluster with the full keep-and-optimize lever set.

    Gen7 Valkey, so the Database Savings Plan lever is eligible; Graviton is not
    (already Graviton). Redis clusters get a Valkey lever instead of DSP.
    """
    options = []
    if engine in ("redis", "redis-oss"):
        options.append({"key": "valkey",
                        "label": "Migrate to Valkey (on-demand)",
                        "monthly": round(current * 0.80, 2),
                        "saving_monthly": round(current * 0.20, 2),
                        "eligible": True, "commitment": False})
    elif gen7:
        options.append({"key": "dsp",
                        "label": "Database Savings Plan (Valkey, Gen7+, 1yr)",
                        "monthly": round(current * 0.80, 2),
                        "saving_monthly": round(current * 0.20, 2),
                        "eligible": True, "commitment": True, "ratio": 0.20})
    else:
        options.append({"key": "dsp",
                        "label": "Database Savings Plan (Valkey, Gen7+, 1yr)",
                        "monthly": None, "saving_monthly": None,
                        "eligible": False, "commitment": True, "ratio": 0.20,
                        "note": "Requires a Gen7+ node."})
    options += [
        {"key": "reserved_1yr", "label": "Reserved Nodes -- 1yr, no upfront",
         "monthly": round(current * 0.68, 2),
         "saving_monthly": round(current * 0.32, 2),
         "eligible": True, "commitment": True},
        {"key": "reserved_3yr", "label": "Reserved Nodes -- 3yr, no upfront",
         "monthly": round(current * 0.52, 2),
         "saving_monthly": round(current * 0.48, 2),
         "eligible": True, "commitment": True},
        {"key": "graviton", "label": "Graviton upgrade",
         "monthly": None, "saving_monthly": None, "eligible": False,
         "commitment": False, "note": "Already on Graviton."},
    ]
    return {"node_type": "cache.r7g.large", "nodes": 2, "engine": engine,
            "generation": 7 if gen7 else 5, "current_monthly": current,
            "options": options}


def _pricing(clusters, metadata=None):
    return {
        "metadata": metadata or {
            "source": "synthetic", "region": "us-east-1",
            "retrieved_at": "2026-01-15", "guard": "synthetic figures"},
        "clusters": clusters,
    }


def _priced_payload():
    """A three-cluster fleet spanning the three gating states.

    idle -> decommission only; steady -> commitments plain; spiky -> commitments
    flagged risky. One payload so the states are exercised side by side.
    """
    ids = ["c-idle", "c-spiky", "c-steady"]
    met = _multi_metrics({cid: 5.0 for cid in ids})
    an = _multi_analysis(
        ids,
        classifications={"c-idle": "IDLE", "c-spiky": "SATURATED",
                         "c-steady": "BALANCED"},
        steadiness={"c-spiky": "spiky", "c-steady": "steady"})
    pricing = _pricing({
        "c-idle": _priced_cluster(100.0),
        "c-spiky": _priced_cluster(200.0),
        "c-steady": _priced_cluster(400.0),
    })
    return build_payload(_multi_inventory(ids), met, an, STAMP,
                         pricing=pricing)


def _panel(payload, cid):
    return [p for p in payload["panels"] if p["cluster"] == cid][0]


def _cost_options(payload, cid):
    # Cost options are no longer rendered per panel; the gated options live in
    # the fleet savings payload. The gating logic (_gate_options) is unchanged,
    # so these tests assert it there.
    return payload["savings"]["clusters"][cid]["options"]


class TestIdleClusterShowsDecommissionOnly:
    """An IDLE cluster's only saving is to switch it off. Offering a multi-year
    commitment on a cluster the same report says to decommission is advice that
    locks spend onto waste (PLAN.md 7c, the owner's catch)."""

    def test_the_only_option_is_decommission(self):
        opts = _cost_options(_priced_payload(), "c-idle")
        assert len(opts) == 1
        assert opts[0]["is_decommission"] is True

    def test_no_reserved_or_savings_plan_on_an_idle_cluster(self):
        opts = _cost_options(_priced_payload(), "c-idle")
        labels = " ".join(o["label"] for o in opts)
        assert "Reserved" not in labels
        assert "Savings Plan" not in labels
        assert "Valkey" not in labels
        assert "Graviton" not in labels

    def test_decommission_saving_is_the_current_spend(self):
        opts = _cost_options(_priced_payload(), "c-idle")
        assert opts[0]["saving"] == 100.0


class TestActiveSteadyShowsCommitmentsPlainly:
    """A steady cluster is a sound Reserved-Node / Savings-Plan candidate, shown
    without a risk annotation."""

    def test_commitments_are_present(self):
        opts = _cost_options(_priced_payload(), "c-steady")
        commitments = [o for o in opts if o["commitment"]]
        assert {o["label"] for o in commitments} >= {
            "Reserved Nodes -- 1yr, no upfront",
            "Reserved Nodes -- 3yr, no upfront",
            "Database Savings Plan (Valkey, Gen7+, 1yr)"}

    def test_no_commitment_carries_a_risk_note(self):
        opts = _cost_options(_priced_payload(), "c-steady")
        assert all(o["risk_note"] is None for o in opts)

    def test_decommission_is_not_offered_on_an_active_cluster(self):
        opts = _cost_options(_priced_payload(), "c-steady")
        assert not any(o["is_decommission"] for o in opts)


class TestActiveSpikyFlagsCommitmentRisk:
    """A spiky/variable cluster fits serverless; a multi-year commitment is
    listed but annotated as risky, not sold as a clean saving."""

    def test_each_commitment_carries_the_risk_note(self):
        opts = _cost_options(_priced_payload(), "c-spiky")
        commitments = [o for o in opts if o["commitment"]]
        assert commitments
        for o in commitments:
            assert o["risk_note"]
            assert "spiky" in o["risk_note"]
            assert "serverless" in o["risk_note"]

    def test_a_variable_cluster_is_flagged_the_same_way(self):
        ids = ["c-var"]
        met = _multi_metrics({"c-var": 5.0})
        an = _multi_analysis(ids, classifications={"c-var": "BALANCED"},
                             steadiness={"c-var": "variable"})
        payload = build_payload(_multi_inventory(ids), met, an, STAMP,
                                pricing=_pricing({"c-var": _priced_cluster(50.0)}))
        opts = _cost_options(payload, "c-var")
        risky = [o for o in opts if o["risk_note"]]
        assert risky and all("variable" in o["risk_note"] for o in risky)

    def test_not_measured_steadiness_lists_commitments_without_a_note(self):
        # No steadiness signal is not a licence to warn: list the commitment,
        # but do not invent a risk the data does not show.
        ids = ["c-nm"]
        met = _multi_metrics({"c-nm": 5.0})
        an = _multi_analysis(ids, classifications={"c-nm": "BALANCED"})
        payload = build_payload(_multi_inventory(ids), met, an, STAMP,
                                pricing=_pricing({"c-nm": _priced_cluster(50.0)}))
        opts = _cost_options(payload, "c-nm")
        assert any(o["commitment"] for o in opts)
        assert all(o["risk_note"] is None for o in opts)


class TestGatingRendersIneligibleAndGravitonHonestly:
    def test_an_ineligible_option_renders_as_its_note_with_no_dollar(self):
        # DSP on a non-Gen7 Valkey cluster: the note is the whole line.
        price = _priced_cluster(50.0, gen7=False)
        opts = _gate_options(price, "BALANCED", "steady")
        dsp = [o for o in opts if "Savings Plan" in o["label"]][0]
        assert dsp["monthly"] is None and dsp["saving"] is None
        assert dsp["note"]

    def test_a_graviton_option_with_no_saving_says_so_not_a_negative(self):
        # An eligible Graviton upgrade priced higher than the current node has a
        # negative saving; it must read "no on-demand saving", never "-$2.92".
        price = {"node_type": "cache.m5.large", "nodes": 2, "engine": "redis",
                 "current_monthly": 100.0, "options": [
                     {"key": "graviton", "label": "Graviton3 (cache.m7g.large)",
                      "monthly": 101.3, "saving_monthly": -1.3,
                      "eligible": True, "commitment": False}]}
        opts = _gate_options(price, "BALANCED", "steady")
        grav = [o for o in opts if "Graviton" in o["label"]][0]
        assert grav["saving"] is None
        assert "no on-demand saving" in grav["note"].lower()


class TestFleetSavingsKpis:
    def test_idle_recoverable_sums_idle_current_spend(self):
        sv = _priced_payload()["savings"]
        assert sv["idle_recoverable"] == 100.0

    def test_best_committed_saving_is_the_max_over_kept_steady_clusters(self):
        # Only c-steady is active AND steady; its best commitment is reserved_3yr
        # at 48% of 400. c-spiky (bigger, but spiky) and c-idle are excluded --
        # the exclusion is what encodes "no commitment on idle or spiky".
        sv = _priced_payload()["savings"]
        assert sv["best_committed_saving"] == round(400.0 * 0.48, 2)

    def test_best_committed_saving_is_none_on_an_all_idle_fleet(self):
        ids = ["a", "b"]
        met = _multi_metrics({cid: 5.0 for cid in ids})
        an = _multi_analysis(ids, classifications={"a": "IDLE", "b": "IDLE"})
        sv = build_payload(_multi_inventory(ids), met, an, STAMP,
                           pricing=_pricing({"a": _priced_cluster(30.0),
                                             "b": _priced_cluster(20.0)}))["savings"]
        assert sv["idle_recoverable"] == 50.0
        assert sv["best_committed_saving"] is None

    def test_the_savings_kpi_labels_render(self):
        html = render(_priced_payload())
        assert "Idle recoverable" in html
        assert "Best committed saving" in html


class TestTheReportRendersWithoutPricing:
    """Pricing is additive. With none, the savings KPIs and cost blocks are
    absent and every other section is unchanged -- the null-branch discipline
    config and notes already follow."""

    def _no_pricing(self):
        ids = ["c0", "c1"]
        met = _multi_metrics({cid: 5.0 for cid in ids})
        return build_payload(_multi_inventory(ids), met,
                             _multi_analysis(ids), STAMP)

    def test_savings_is_none_without_pricing(self):
        assert self._no_pricing()["savings"] is None

    def test_no_panel_carries_cost_options_without_pricing(self):
        for p in self._no_pricing()["panels"]:
            assert "cost_options" not in p

    def test_the_report_still_renders(self):
        html = render(self._no_pricing())
        assert 'id="panels"' in html
        assert "Cluster detail" in html

    @pytest.mark.skipif(shutil.which("node") is None,
                        reason="node not available to parse the inline script")
    def test_the_inline_script_parses_both_with_and_without_pricing(
            self, tmp_path):
        for name, payload in (("with", _priced_payload()),
                              ("without", self._no_pricing())):
            path = tmp_path / f"report-{name}.js"
            path.write_text(_script(render(payload)), encoding="utf-8")
            result = subprocess.run(["node", "--check", str(path)],
                                    capture_output=True, text=True)
            assert result.returncode == 0, (name, result.stderr)


class TestAPricingFigureCanBeCited:
    """pricing.json is a CITABLE_SOURCE, so the AI review may quote a savings
    figure and have it verified like any other -- the check that makes agent
    prose about money safe to embed."""

    def test_pricing_is_a_citable_source(self):
        assert "pricing" in CITABLE_SOURCES

    def test_a_note_citing_a_pricing_figure_resolves(self):
        pricing = _pricing({"node-1": _priced_cluster(100.0)})
        note = {"assessment": {
            "prose": "Decommissioning node-1 recovers $100.00 each month.",
            "cites": ["clusters.node-1.current_monthly"]}}
        # No exception means the figure was found under the cited pricing path.
        assert verify_notes(note, {"pricing": pricing}) is None

    def test_a_pricing_figure_from_the_wrong_path_is_still_rejected(self):
        pricing = _pricing({"node-1": _priced_cluster(100.0)})
        note = {"assessment": {
            "prose": "Reserved nodes save $999.00 each month.",
            "cites": ["clusters.node-1.current_monthly"]}}
        with pytest.raises(NotesError):
            verify_notes(note, {"pricing": pricing})


class TestWellArchitectedScoreWidget:
    """The fleet/per-cluster WA score payload and its rendered widget."""

    def _payload(self, ids=("c0", "c1")):
        ids = list(ids)
        met = _multi_metrics({cid: float(i + 1) for i, cid in enumerate(ids)})
        return build_payload(_multi_inventory(ids), met,
                             _multi_analysis(ids), STAMP)

    def test_payload_carries_fleet_and_cluster_scores(self):
        p = self._payload()
        assert "scores" in p
        fleet = p["scores"]["fleet"]
        assert set(fleet) == {"overall", "band", "pillars"}
        assert len(fleet["pillars"]) == 6
        assert set(p["scores"]["clusters"]) == {"c0", "c1"}

    def test_each_panel_carries_its_own_score(self):
        p = self._payload()
        for panel in p["panels"]:
            assert panel["scores"] is not None
            assert "overall" in panel["scores"]

    def test_the_widget_and_band_tokens_render(self):
        html = render(self._payload())
        assert 'id="scores"' in html
        assert "function scoreWidget" in html
        assert "--band-excellent" in html
        # A pillar label the widget prints, so identity is not colour-alone.
        assert "Performance Efficiency" in html

    def test_the_cost_hero_is_gone_and_health_leads(self):
        html = render(self._payload())
        assert "heroFigure" not in html
        # Fleet health section leads the body, before "What to do".
        assert html.index("Fleet health") < html.index("What to do")
