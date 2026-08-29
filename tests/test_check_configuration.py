"""Tests for Stage 3.5 configuration checks.

Every check is tested twice: it fires on non-compliant config, and it stays
SILENT on compliant config. The silence half is the point. Four false CRITICALs
shipped in this pipeline because tests only ever asserted that findings appeared
-- a check that fires unconditionally passes a fires-only test suite.

The other thing under test is the Stage 1 contract: inventory['clusters'] is a
LIST. The removed Stage 4 assumed a dict, crashed on every real run, and had 153
passing fixture-shaped tests. So the fixtures here are built to the shape
discover_inventory.py actually writes, and the list/dict confusion has its own
test.
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from check_configuration import (  # noqa: E402
    CHECKS,
    MIN_REPLICAS_PER_SHARD,
    MIN_SNAPSHOT_RETENTION_DAYS,
    REQUIRED_TAGS,
    VALID_SEVERITIES,
    ConfigurationChecker,
    Policy,
    _policy_from_args,
    main,
    parse_args,
)


def node_cluster(**overrides):
    """A fully compliant node-based cluster: every check must stay silent."""
    cluster = {
        "cluster_id": "compliant-node",
        "cluster_type": "node-based",
        "region": "us-east-1",
        "engine": "valkey",
        "engine_version": "8.0",
        "node_type": "cache.r7g.large",
        "tls_enabled": True,
        "encryption_at_rest": True,
        "auth_mode": "RBAC",
        "multi_az": True,
        "automatic_failover": True,
        "snapshot_retention_days": 7,
        "num_shards": 1,
        "num_replicas_per_shard": 2,
        "total_nodes": 3,
        "security_groups": [{"group_id": "sg-1", "permissive_rules": []}],
        "tags": {t: "set" for t in REQUIRED_TAGS},
        "parameters": {},
        # Both log types delivered and active, so OE-06a/OE-06b stay silent on
        # the compliant fixture. Tests that exercise log delivery override this.
        "log_delivery": [
            {"log_type": "slow-log", "destination_type": "cloudwatch-logs",
             "destination_details": {}, "log_format": "json",
             "status": "active"},
            {"log_type": "engine-log", "destination_type": "cloudwatch-logs",
             "destination_details": {}, "log_format": "json",
             "status": "active"},
        ],
        "replication_group_log_delivery_enabled": True,
    }
    cluster.update(overrides)
    return cluster


def serverless_cluster(**overrides):
    """A fully compliant serverless cache: every check must stay silent."""
    cluster = {
        "cluster_id": "compliant-serverless",
        "cluster_type": "serverless",
        "region": "us-east-1",
        "engine": "valkey",
        "engine_version": "8",
        # Stage 1 hardcodes these True for serverless (platform-enforced).
        "tls_enabled": True,
        "encryption_at_rest": True,
        "multi_az": True,
        "auth_mode": "RBAC",
        "snapshot_retention_days": 7,
        "security_groups": [{"group_id": "sg-1", "permissive_rules": []}],
        "tags": {t: "set" for t in REQUIRED_TAGS},
        "cache_usage_limits": {
            "data_storage": {"maximum": 10, "unit": "GB"},
            "ecpu_per_second": {"maximum": 5000},
        },
    }
    cluster.update(overrides)
    return cluster


def inventory(clusters):
    """inventory.json in the shape discover_inventory.py writes (list!)."""
    return {
        "metadata": {
            "account_id": "111122223333",
            "regions_scanned": ["us-east-1"],
            "scan_timestamp": "2026-08-11T00:00:00Z",
            "total_clusters": len(clusters),
        },
        "clusters": clusters,
    }


def ids(findings):
    """Check IDs present in a findings list."""
    return {f["check_id"] for f in findings}


def check_one(cluster):
    """Run the registry against a single cluster, return findings."""
    findings, _ = ConfigurationChecker().check_cluster(cluster)
    return findings


# ---------------------------------------------------------------------------
# The silence half -- a compliant fleet must produce zero findings
# ---------------------------------------------------------------------------


class TestCompliantConfigIsSilent:
    def test_compliant_node_cluster_produces_no_findings(self):
        assert check_one(node_cluster()) == []

    def test_compliant_serverless_cache_produces_no_findings(self):
        assert check_one(serverless_cluster()) == []

    def test_compliant_fleet_reports_zero_across_all_severities(self):
        out = ConfigurationChecker().run(
            inventory([node_cluster(), serverless_cluster()])
        )
        assert out["metadata"]["total_findings"] == 0
        assert all(v == 0 for v in
                   out["metadata"]["findings_by_severity"].values())


# ---------------------------------------------------------------------------
# Security
# ---------------------------------------------------------------------------


class TestSecurityChecks:
    def test_sec01_fires_when_tls_disabled(self):
        f = check_one(node_cluster(tls_enabled=False))
        assert ids(f) == {"SEC-01"}
        assert f[0]["severity"] == "CRITICAL"

    def test_sec02_fires_when_at_rest_encryption_disabled(self):
        f = check_one(node_cluster(encryption_at_rest=False))
        assert ids(f) == {"SEC-02"}
        assert f[0]["severity"] == "HIGH"

    def test_sec03_fires_when_no_auth_configured(self):
        f = check_one(node_cluster(auth_mode="none"))
        assert "SEC-03" in ids(f)
        assert [x for x in f if x["check_id"] == "SEC-03"][0]["severity"] \
            == "CRITICAL"

    def test_sec03_silent_for_auth_token(self):
        # An AUTH token IS authentication -- weak, but present. SEC-04's job.
        assert "SEC-03" not in ids(check_one(node_cluster(auth_mode="AUTH-token")))

    def test_sec04_fires_for_legacy_auth_token(self):
        f = check_one(node_cluster(auth_mode="AUTH-token"))
        assert ids(f) == {"SEC-04"}
        assert f[0]["severity"] == "MEDIUM"

    def test_sec04_silent_for_rbac(self):
        assert "SEC-04" not in ids(check_one(node_cluster(auth_mode="RBAC")))

    def test_sec03_and_sec04_do_not_both_fire(self):
        # One missing-auth problem must not be counted twice.
        assert ids(check_one(node_cluster(auth_mode="none"))) == {"SEC-03"}

    def test_sec05_fires_on_permissive_cidr(self):
        f = check_one(node_cluster(security_groups=[
            {"group_id": "sg-open", "permissive_rules": ["0.0.0.0/0 on ports 6379-6379"]}
        ]))
        assert ids(f) == {"SEC-05"}
        assert f[0]["severity"] == "CRITICAL"
        assert "sg-open" in f[0]["description"]

    def test_sec05_counts_every_offending_rule(self):
        f = check_one(node_cluster(security_groups=[
            {"group_id": "sg-a", "permissive_rules": ["0.0.0.0/0 on ports 1-65535"]},
            {"group_id": "sg-b", "permissive_rules": ["::/0 on ports 6379-6379"]},
        ]))
        assert f[0]["current_value"] == 2

    def test_sec05_silent_when_security_groups_absent(self):
        # No data is not evidence of a permissive rule.
        c = node_cluster()
        del c["security_groups"]
        assert "SEC-05" not in ids(check_one(c))


class TestServerlessSkipsPlatformEnforcedChecks:
    """Stage 1 hardcodes tls/at-rest/multi-az True for serverless.

    Evaluating them would report a pass the data never established. They must be
    skipped and the skip recorded.
    """

    def test_platform_enforced_checks_are_skipped_not_passed(self):
        _, skipped = ConfigurationChecker().check_cluster(serverless_cluster())
        assert {"SEC-01", "SEC-02", "REL-01", "REL-02", "REL-03"} <= \
            {s["check_id"] for s in skipped}

    def test_skip_reason_is_recorded(self):
        _, skipped = ConfigurationChecker().check_cluster(serverless_cluster())
        reason = [s for s in skipped if s["check_id"] == "SEC-01"][0]["reason"]
        assert "serverless" in reason

    def test_tls_false_on_serverless_still_does_not_fire(self):
        # Guards against a future refactor quietly making SEC-01 serverless-wide.
        assert "SEC-01" not in ids(check_one(serverless_cluster(tls_enabled=False)))

    def test_cost06_is_serverless_only(self):
        _, skipped = ConfigurationChecker().check_cluster(node_cluster())
        assert "COST-06" in {s["check_id"] for s in skipped}


# ---------------------------------------------------------------------------
# Reliability
# ---------------------------------------------------------------------------


class TestReliabilityChecks:
    def test_rel01_fires_when_multi_az_disabled(self):
        assert ids(check_one(node_cluster(multi_az=False))) == {"REL-01"}

    def test_rel02_fires_when_failover_disabled(self):
        assert ids(check_one(node_cluster(automatic_failover=False))) == {"REL-02"}

    def test_rel03_fires_below_replica_minimum(self):
        f = check_one(node_cluster(num_replicas_per_shard=1))
        assert ids(f) == {"REL-03"}
        assert f[0]["current_value"] == 1
        assert f[0]["threshold"] == MIN_REPLICAS_PER_SHARD

    def test_rel03_silent_at_exactly_the_minimum(self):
        # Boundary: >= minimum passes. An off-by-one here flags healthy fleets.
        assert "REL-03" not in ids(
            check_one(node_cluster(num_replicas_per_shard=MIN_REPLICAS_PER_SHARD))
        )

    def test_rel03_silent_when_topology_unknown(self):
        c = node_cluster()
        del c["num_replicas_per_shard"]
        assert "REL-03" not in ids(check_one(c))

    def test_rel04_fires_when_backups_disabled(self):
        f = check_one(node_cluster(snapshot_retention_days=0))
        assert ids(f) == {"REL-04"}
        assert f[0]["severity"] == "HIGH"

    def test_rel04b_fires_when_retention_is_short(self):
        f = check_one(node_cluster(snapshot_retention_days=1))
        assert ids(f) == {"REL-04b"}
        assert f[0]["severity"] == "LOW"

    def test_backups_disabled_reports_once_not_twice(self):
        # Zero retention is REL-04's finding alone; REL-04b must not pile on.
        assert ids(check_one(node_cluster(snapshot_retention_days=0))) == {"REL-04"}

    def test_rel04b_silent_at_exactly_the_policy_floor(self):
        assert check_one(
            node_cluster(snapshot_retention_days=MIN_SNAPSHOT_RETENTION_DAYS)
        ) == []


# ---------------------------------------------------------------------------
# Operational excellence & cost
# ---------------------------------------------------------------------------


class TestTaggingAndCostChecks:
    def test_oe04_fires_on_missing_tags_and_names_them(self):
        f = check_one(node_cluster(tags={"Environment": "prod"}))
        assert ids(f) == {"OE-04"}
        assert "Owner" in f[0]["description"]
        assert f[0]["current_value"] == len(REQUIRED_TAGS) - 1

    def test_oe04_treats_empty_tag_value_as_missing(self):
        # An Owner tag set to "" attributes nothing.
        tags = {t: "set" for t in REQUIRED_TAGS}
        tags["Owner"] = ""
        assert "OE-04" in ids(check_one(node_cluster(tags=tags)))

    def test_cost01_fires_for_redis_oss(self):
        f = check_one(node_cluster(engine="redis"))
        assert ids(f) == {"COST-01"}
        assert f[0]["severity"] == "LOW"

    def test_cost01_silent_for_valkey(self):
        assert "COST-01" not in ids(check_one(node_cluster(engine="valkey")))

    def test_cost01_is_case_insensitive(self):
        assert "COST-01" in ids(check_one(node_cluster(engine="Redis")))

    def test_cost06_fires_when_no_usage_limits_set(self):
        f = check_one(serverless_cluster(cache_usage_limits={
            "data_storage": {"maximum": None},
            "ecpu_per_second": {"maximum": None},
        }))
        assert ids(f) == {"COST-06"}
        assert f[0]["current_value"] == 2

    def test_cost06_fires_when_only_one_limit_is_set(self):
        f = check_one(serverless_cluster(cache_usage_limits={
            "data_storage": {"maximum": 10},
            "ecpu_per_second": {"maximum": None},
        }))
        assert f[0]["current_value"] == 1
        assert "ECPUPerSecond" in f[0]["description"]

    def test_cost06_fires_when_limits_key_absent_entirely(self):
        c = serverless_cluster()
        del c["cache_usage_limits"]
        assert "COST-06" in ids(check_one(c))


# ---------------------------------------------------------------------------
# Log delivery (OE-06a slow-log, OE-06b engine-log)
# ---------------------------------------------------------------------------


def skips(cluster):
    """Check IDs recorded as skipped (not evaluated / not applicable)."""
    _, skipped = ConfigurationChecker().check_cluster(cluster)
    return {s["check_id"] for s in skipped}


def _log(log_type, status="active"):
    return {"log_type": log_type, "destination_type": "cloudwatch-logs",
            "destination_details": {}, "log_format": "json", "status": status}


class TestLogDeliveryChecks:
    """OE-06a/OE-06b: both log types, each gated on the engine that supports it.

    Tested in both directions (fires when absent, silent when active) and at the
    version boundaries -- the gate must skip-with-a-reason on an engine too old
    to have the log, never report a config the platform would reject as a
    failure.
    """

    def test_oe06a_fires_when_no_slow_log_delivery(self):
        f = check_one(node_cluster(log_delivery=[_log("engine-log")]))
        assert "OE-06a" in ids(f)
        oe06a = [x for x in f if x["check_id"] == "OE-06a"][0]
        assert oe06a["severity"] == "LOW"

    def test_oe06a_silent_when_slow_log_active(self):
        assert "OE-06a" not in ids(
            check_one(node_cluster(log_delivery=[_log("slow-log")])))

    def test_oe06b_fires_when_no_engine_log_delivery(self):
        f = check_one(node_cluster(log_delivery=[_log("slow-log")]))
        assert "OE-06b" in ids(f)
        assert [x for x in f if x["check_id"] == "OE-06b"][0]["severity"] == "LOW"

    def test_oe06b_silent_when_engine_log_active(self):
        assert "OE-06b" not in ids(
            check_one(node_cluster(log_delivery=[_log("engine-log")])))

    def test_both_fire_when_log_delivery_empty(self):
        assert {"OE-06a", "OE-06b"} <= ids(check_one(node_cluster(log_delivery=[])))

    def test_a_non_active_log_does_not_count_as_configured(self):
        # An entry stuck in "enabling" is not delivering; it must not silence.
        f = check_one(node_cluster(log_delivery=[_log("slow-log", "enabling")]))
        assert "OE-06a" in ids(f)

    def test_both_skip_serverless_not_fire(self):
        # applies_to=("node-based",): serverless has no log delivery at all.
        c = serverless_cluster()
        assert "OE-06a" not in ids(check_one(c))
        assert "OE-06b" not in ids(check_one(c))
        assert {"OE-06a", "OE-06b"} <= skips(c)

    def test_redis_506_skips_both_not_fire(self):
        # Below both gates (slow 6.0, engine 6.2): neither applicable.
        c = node_cluster(engine="redis", engine_version="5.0.6", log_delivery=[])
        assert "OE-06a" not in ids(check_one(c))
        assert "OE-06b" not in ids(check_one(c))
        assert {"OE-06a", "OE-06b"} <= skips(c)

    def test_redis_60_slow_log_applies_engine_log_skipped(self):
        # 6.0 gates slow-log in but engine-log (6.2) out.
        c = node_cluster(engine="redis", engine_version="6.0.0", log_delivery=[])
        assert "OE-06a" in ids(check_one(c))
        assert "OE-06b" not in ids(check_one(c))
        assert "OE-06b" in skips(c)
        assert "OE-06a" not in skips(c)

    def test_redis_62_both_apply(self):
        c = node_cluster(engine="redis", engine_version="6.2.0", log_delivery=[])
        assert {"OE-06a", "OE-06b"} <= ids(check_one(c))

    def test_valkey_72_both_apply(self):
        c = node_cluster(engine="valkey", engine_version="7.2.0", log_delivery=[])
        assert {"OE-06a", "OE-06b"} <= ids(check_one(c))

    def test_skip_reason_names_the_version(self):
        c = node_cluster(engine="redis", engine_version="5.0.6", log_delivery=[])
        _, skipped = ConfigurationChecker().check_cluster(c)
        reason = [s for s in skipped if s["check_id"] == "OE-06a"][0]["reason"]
        assert "5.0.6" in reason and "6.0+" in reason


# ---------------------------------------------------------------------------
# Finding shape -- must match Stage 3 so the report needs no special case
# ---------------------------------------------------------------------------


class TestFindingShape:
    REQUIRED_KEYS = {
        "finding_id", "model_source", "severity", "title", "description",
        "metric_name", "current_value", "threshold", "recommendation",
    }

    def test_finding_carries_every_stage3_key(self):
        f = check_one(node_cluster(tls_enabled=False))[0]
        assert self.REQUIRED_KEYS <= set(f)

    def test_model_source_identifies_config_findings(self):
        f = check_one(node_cluster(tls_enabled=False))[0]
        assert f["model_source"] == "configuration"

    def test_metric_name_is_none_because_no_metric_was_read(self):
        # A config check that claimed a metric_name would misattribute its source.
        assert check_one(node_cluster(tls_enabled=False))[0]["metric_name"] is None

    def test_every_registry_severity_is_valid(self):
        assert all(c.severity in VALID_SEVERITIES for c in CHECKS)

    def test_check_ids_are_unique(self):
        seen = [c.check_id for c in CHECKS]
        assert len(seen) == len(set(seen))

    def test_every_check_declares_at_least_one_cluster_type(self):
        assert all(c.applies_to for c in CHECKS)

    def test_finding_ids_are_unique_across_the_fleet(self):
        out = ConfigurationChecker().run(inventory([
            node_cluster(cluster_id="a", tls_enabled=False, multi_az=False),
            node_cluster(cluster_id="b", tls_enabled=False, multi_az=False),
        ]))
        all_ids = [f["finding_id"]
                   for c in out["clusters"].values() for f in c["findings"]]
        assert len(all_ids) == len(set(all_ids))

    def test_finding_ids_are_deterministic_across_runs(self):
        # The report's byte-stability guarantee depends on this.
        inv = inventory([node_cluster(tls_enabled=False, multi_az=False)])
        a = ConfigurationChecker().run(inv)
        b = ConfigurationChecker().run(inv)
        assert json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)


# ---------------------------------------------------------------------------
# Stage 1 contract -- the bug that killed the old Stage 4
# ---------------------------------------------------------------------------


class TestInventoryContract:
    def test_dict_shaped_clusters_raises_loudly(self):
        # The removed Stage 4 read inventory['clusters'] as a dict and died with
        # AttributeError on every real run. Silence here would repeat that.
        with pytest.raises(TypeError, match="must be a list"):
            ConfigurationChecker().run({"metadata": {}, "clusters": {"a": {}}})

    def test_missing_clusters_key_raises(self):
        with pytest.raises(TypeError):
            ConfigurationChecker().run({"metadata": {}})

    def test_empty_fleet_is_valid_not_an_error(self):
        out = ConfigurationChecker().run(inventory([]))
        assert out["metadata"]["clusters_checked"] == 0
        assert out["metadata"]["total_findings"] == 0

    def test_cluster_without_id_is_skipped_not_crashed(self):
        out = ConfigurationChecker().run(inventory([{"cluster_type": "node-based"}]))
        assert out["metadata"]["clusters_checked"] == 0

    def test_unknown_cluster_type_evaluates_nothing(self):
        # A new ElastiCache flavor must not be silently reported as compliant.
        out = ConfigurationChecker().run(
            inventory([{"cluster_id": "x", "cluster_type": "future-type"}])
        )
        entry = out["clusters"]["x"]
        assert entry["findings"] == []
        assert entry["checks_evaluated"] == 0
        assert len(entry["checks_skipped"]) == len(CHECKS)

    def test_predicate_error_is_recorded_not_fatal(self):
        # A malformed field must not abort the whole stage.
        out = ConfigurationChecker().run(inventory([
            node_cluster(security_groups="not-a-list"),
        ]))
        skipped = out["clusters"]["compliant-node"]["checks_skipped"]
        assert any(s["check_id"] == "SEC-05" and "error" in s["reason"]
                   for s in skipped)


# ---------------------------------------------------------------------------
# Output document & CLI
# ---------------------------------------------------------------------------


class TestOutputDocument:
    def test_severity_counts_match_the_findings(self):
        out = ConfigurationChecker().run(inventory([
            node_cluster(tls_enabled=False, snapshot_retention_days=0),
        ]))
        counts = out["metadata"]["findings_by_severity"]
        assert counts["CRITICAL"] == 1  # SEC-01
        assert counts["HIGH"] == 1      # REL-04
        assert out["metadata"]["total_findings"] == 2

    def test_policy_thresholds_are_recorded_in_metadata(self):
        # The report must be able to state the policy it judged against.
        policy = ConfigurationChecker().run(inventory([]))["metadata"]["policy"]
        assert policy["min_snapshot_retention_days"] == MIN_SNAPSHOT_RETENTION_DAYS
        assert policy["min_replicas_per_shard"] == MIN_REPLICAS_PER_SHARD
        assert policy["required_tags"] == list(REQUIRED_TAGS)

    def test_registry_contents_are_recorded(self):
        meta = ConfigurationChecker().run(inventory([]))["metadata"]
        assert meta["checks_in_registry"] == len(CHECKS)
        assert "SEC-01" in meta["check_ids"]

    def test_account_and_region_provenance_carried_from_inventory(self):
        meta = ConfigurationChecker().run(inventory([node_cluster()]))["metadata"]
        assert meta["account_id"] == "111122223333"
        assert meta["regions_scanned"] == ["us-east-1"]


class TestCLI:
    def test_writes_output_and_exits_zero(self, tmp_path):
        inv = tmp_path / "inventory.json"
        inv.write_text(json.dumps(inventory([node_cluster(tls_enabled=False)])))
        out = tmp_path / "config_findings.json"
        assert main(["--inventory", str(inv), "--output", str(out)]) == 0
        data = json.loads(out.read_text())
        assert data["metadata"]["total_findings"] == 1

    def test_missing_inventory_exits_nonzero(self, tmp_path):
        assert main(["--inventory", str(tmp_path / "nope.json"),
                     "--output", str(tmp_path / "o.json")]) == 1

    def test_malformed_json_exits_nonzero(self, tmp_path):
        inv = tmp_path / "bad.json"
        inv.write_text("{not json")
        assert main(["--inventory", str(inv),
                     "--output", str(tmp_path / "o.json")]) == 1

    def test_dict_shaped_inventory_exits_nonzero_without_traceback(self, tmp_path):
        inv = tmp_path / "inventory.json"
        inv.write_text(json.dumps({"metadata": {}, "clusters": {}}))
        assert main(["--inventory", str(inv),
                     "--output", str(tmp_path / "o.json")]) == 1


class TestPolicyOverrides:
    """D5 + D6: OE-04 required tags, REL-03 replica floor and REL-04b retention
    floor are per-customer policy, not hardcoded module constants.

    A hardcoded standard manufactures false findings for every customer who does
    not share it. Each override is tested three ways: it changes which findings
    fire, it is recorded in metadata.policy so the report can state what it
    graded, and the checker stays a pure function of (inventory, policy).
    """

    def test_default_policy_is_the_wa_constants(self):
        p = Policy()
        assert p.required_tags == REQUIRED_TAGS
        assert p.min_replicas_per_shard == MIN_REPLICAS_PER_SHARD
        assert p.min_snapshot_retention_days == MIN_SNAPSHOT_RETENTION_DAYS

    def test_no_policy_arg_matches_explicit_default(self):
        # Omitting policy must behave exactly like passing Policy(), or the
        # override plumbing changed the default behaviour.
        c = node_cluster(num_replicas_per_shard=1, snapshot_retention_days=3,
                         tags={})
        assert (ConfigurationChecker().check_cluster(c)[0]
                == ConfigurationChecker(policy=Policy()).check_cluster(c)[0])

    # --- OE-04 required tags (D5) ---

    def test_custom_required_tags_change_which_clusters_fire(self):
        c = node_cluster(tags={"team": "x"})  # has 'team', lacks WA three
        assert "OE-04" in ids(check_one(c))  # default fires
        findings, _ = ConfigurationChecker(
            policy=Policy(required_tags=("team",))
        ).check_cluster(c)
        assert "OE-04" not in ids(findings)  # their standard is met

    def test_empty_required_tags_never_fires_oe04(self):
        c = node_cluster(tags={})
        findings, _ = ConfigurationChecker(
            policy=Policy(required_tags=())
        ).check_cluster(c)
        assert "OE-04" not in ids(findings)

    def test_oe04_recommendation_names_the_effective_tags(self):
        c = node_cluster(tags={})
        findings, _ = ConfigurationChecker(
            policy=Policy(required_tags=("CostCenter", "Squad"))
        ).check_cluster(c)
        rec = next(f for f in findings if f["check_id"] == "OE-04")["recommendation"]
        assert "CostCenter" in rec and "Squad" in rec
        assert "Environment" not in rec  # not the default set

    # --- REL-03 replica floor (D6) ---

    def test_lowering_replica_floor_silences_rel03(self):
        c = node_cluster(num_replicas_per_shard=1)
        assert "REL-03" in ids(check_one(c))  # default floor 2
        findings, _ = ConfigurationChecker(
            policy=Policy(min_replicas_per_shard=1)
        ).check_cluster(c)
        assert "REL-03" not in ids(findings)

    def test_raising_replica_floor_fires_rel03_on_a_default_pass(self):
        c = node_cluster(num_replicas_per_shard=2)  # compliant by default
        assert "REL-03" not in ids(check_one(c))
        findings, _ = ConfigurationChecker(
            policy=Policy(min_replicas_per_shard=3)
        ).check_cluster(c)
        f = next(x for x in findings if x["check_id"] == "REL-03")
        assert f["threshold"] == 3 and "3 per shard" in f["recommendation"]

    # --- REL-04b retention floor (D6) ---

    def test_retention_floor_is_policy_driven(self):
        c = node_cluster(snapshot_retention_days=3)
        assert "REL-04b" in ids(check_one(c))  # 3 < default 7
        findings, _ = ConfigurationChecker(
            policy=Policy(min_snapshot_retention_days=3)
        ).check_cluster(c)
        assert "REL-04b" not in ids(findings)  # 3 meets a 3-day floor

    def test_retention_floor_recommendation_names_effective_value(self):
        c = node_cluster(snapshot_retention_days=10)
        findings, _ = ConfigurationChecker(
            policy=Policy(min_snapshot_retention_days=30)
        ).check_cluster(c)
        f = next(x for x in findings if x["check_id"] == "REL-04b")
        assert f["threshold"] == 30 and "30 day" in f["recommendation"]

    # --- metadata + reproducibility ---

    def test_metadata_records_the_effective_policy(self):
        p = Policy(required_tags=("team",), min_replicas_per_shard=1,
                   min_snapshot_retention_days=14)
        out = ConfigurationChecker(policy=p).run(inventory([node_cluster()]))
        assert out["metadata"]["policy"] == {
            "required_tags": ["team"],
            "min_replicas_per_shard": 1,
            "min_snapshot_retention_days": 14,
        }

    def test_same_inventory_same_policy_is_byte_identical(self):
        inv = inventory([node_cluster(num_replicas_per_shard=1, tags={}),
                         serverless_cluster()])
        p = Policy(required_tags=("team",), min_snapshot_retention_days=14)
        a = json.dumps(ConfigurationChecker(policy=p).run(inv), sort_keys=True)
        b = json.dumps(ConfigurationChecker(policy=p).run(inv), sort_keys=True)
        assert a == b

    # --- CLI wiring ---

    def test_policy_from_args_absent_flags_are_wa_defaults(self):
        args = parse_args(["--inventory", "x"])
        assert _policy_from_args(args) == Policy()

    def test_policy_from_args_distinguishes_unset_from_empty_tags(self):
        unset = _policy_from_args(parse_args(["--inventory", "x"]))
        empty = _policy_from_args(parse_args(["--inventory", "x", "--required-tags"]))
        assert unset.required_tags == REQUIRED_TAGS
        assert empty.required_tags == ()

    def test_cli_threads_overrides_into_output(self, tmp_path):
        inv = tmp_path / "inventory.json"
        inv.write_text(json.dumps(inventory([node_cluster(
            num_replicas_per_shard=1, snapshot_retention_days=3, tags={"team": "x"})])))
        out = tmp_path / "cf.json"
        assert main(["--inventory", str(inv), "--output", str(out),
                     "--required-tags", "team",
                     "--min-replicas-per-shard", "1",
                     "--min-snapshot-retention-days", "3"]) == 0
        data = json.loads(out.read_text())
        got = {f["check_id"] for c in data["clusters"].values()
               for f in c["findings"]}
        assert "OE-04" not in got and "REL-03" not in got and "REL-04b" not in got
        assert data["metadata"]["policy"]["required_tags"] == ["team"]

    def test_cli_rejects_negative_floor(self, tmp_path):
        inv = tmp_path / "inventory.json"
        inv.write_text(json.dumps(inventory([node_cluster()])))
        assert main(["--inventory", str(inv), "--output", str(tmp_path / "o.json"),
                     "--min-replicas-per-shard", "-1"]) == 1
        assert main(["--inventory", str(inv), "--output", str(tmp_path / "o.json"),
                     "--min-snapshot-retention-days", "-1"]) == 1
