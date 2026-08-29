#!/usr/bin/env python3
"""
Configuration Checks — Stage 3.5

Evaluates ElastiCache configuration against a fixed policy and emits findings.
Reads inventory.json only: no AWS API calls, no CloudWatch, no metrics. Runs in
milliseconds.

Why this stage exists separately from Stage 3
---------------------------------------------
Stage 3 answers questions whose correct answer depends on 14 days of timeseries
("is p95 CPU too high?"). This stage answers questions whose correct answer is a
pure function of a boolean already in the inventory ("is TLS on?"). Those are the
highest-stakes, zero-judgment checks in the whole review, and until this stage
existed they were prose in SKILL.md — meaning nothing failed if the agent forgot
to look. A deterministic script cannot forget.

Deliberately NOT here
---------------------
No pillar scores, no weighted 0-100 health number, no penalty arithmetic. A
weighted score is judgment wearing the costume of a measurement; the agent owns
judgment. This stage reports which checks failed and how severely, and stops.

Serverless caches
-----------------
Stage 1 hardcodes tls_enabled/encryption_at_rest/multi_az to True for serverless
because the platform enforces them (discover_inventory.py). Reporting those as
"passed" would imply a verification that never happened, so the relevant checks
skip serverless and say so in the skipped list.

Usage:
  python3 scripts/check_configuration.py --inventory inventory.json \
      --output config_findings.json
"""

import argparse
import dataclasses
import datetime
import json
import logging
import os
import sys
import tempfile

from _pipeline_version import PIPELINE_VERSION

logger = logging.getLogger("check_configuration")

# ---------------------------------------------------------------------------
# Policy constants
# ---------------------------------------------------------------------------

# Well-Architected recommends a minimum of 2 replicas per shard for HA.
MIN_REPLICAS_PER_SHARD = 2

# Backup retention floor, in days. 0 means backups are disabled entirely.
MIN_SNAPSHOT_RETENTION_DAYS = 7

# Tags required for operational ownership and cost attribution (OE-04).
REQUIRED_TAGS = ("Environment", "Owner", "Application")

VALID_SEVERITIES = ("CRITICAL", "HIGH", "MEDIUM", "LOW")


@dataclasses.dataclass(frozen=True)
class Policy:
    """Overridable, per-customer policy values a check grades against.

    Every field defaults to the Well-Architected recommendation (the module
    constants above). A customer whose standard differs — a different tagging
    convention, a 30-day retention floor, a single-replica topology they have
    accepted — passes overrides via the CLI (``--required-tags``,
    ``--min-replicas-per-shard``, ``--min-snapshot-retention-days``). The
    checker stays a pure function of ``(inventory, policy)``, so the
    reproducibility guarantee holds: same inventory + same policy => byte-for-byte
    the same findings.

    Why this is data and not the module constants alone: a hardcoded tuple
    manufactures false findings for every customer who does not share it — OE-04
    fires fleet-wide against a fleet tagged to a different standard. The effective
    values are recorded in ``metadata.policy`` so a reader can see which standard
    produced a finding, and each firing predicate rewrites its recommendation to
    match, so the advice never names the default when a customer set otherwise.
    """

    required_tags: tuple[str, ...] = REQUIRED_TAGS
    min_replicas_per_shard: int = MIN_REPLICAS_PER_SHARD
    min_snapshot_retention_days: int = MIN_SNAPSHOT_RETENTION_DAYS

# --- Engine support lifecycle (SEC-06) -------------------------------------
#
# End of standard support per Redis OSS major version. The day after this date,
# the cache is automatically enrolled in ElastiCache Extended Support and starts
# accruing a premium on the On-Demand node rate.
#
# Vendored deliberately, NOT fetched. references/engine-support-lifecycle.md is
# the authoritative copy with the source URLs and the reasoning;
# tests/test_engine_lifecycle.py fails if this constant and that file disagree,
# or if the file's "Last verified" date is over a year old.
#
# Only Redis OSS appears here. Valkey, Memcached, and Redis OSS 7.x have NO
# announced end of standard support, so this check must stay silent about them
# rather than extrapolate: an invented deadline makes a customer reschedule real
# work around a date AWS never published, which is worse than saying nothing.
ENGINE_SUPPORT_SCHEDULE = {
    ("redis", 4): "2026-01-31",
    ("redis", 5): "2026-01-31",
    ("redis", 6): "2027-01-31",
}

# Premium multipliers on the On-Demand node rate, by year of Extended Support.
# Reported as multipliers, never as dollars: the underlying rates are
# region-dependent and change without notice, so converting to money is the
# agent's job at presentation time, not this stage's.
EXTENDED_SUPPORT_PREMIUM = {1: 0.80, 2: 0.80, 3: 1.60}

# Days before end of standard support at which the finding escalates to MEDIUM.
# An engine upgrade needs change-management lead time; a quarter is the point
# where starting late becomes the risk rather than the schedule.
SUPPORT_END_WARNING_DAYS = 90


# ---------------------------------------------------------------------------
# Check registry
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Check:
    """One configuration check.

    Attributes:
        check_id: Stable identifier from references/well-architected-mapping.md.
        pillar: Well-Architected pillar this check belongs to.
        title: Short human-readable failure headline.
        severity: One of CRITICAL/HIGH/MEDIUM/LOW.
        applies_to: Cluster types this check evaluates. Checks that would be
            vacuously true for a type are excluded rather than auto-passed.
        predicate: Callable taking the cluster dict, returning None when the
            check passes, a dict of evidence fields when it fails, or a `_Skip`
            when the check is inapplicable to this specific cluster for a
            data-dependent reason (e.g. an engine version predating the feature).
            Evidence keys become `current_value` / `description` inputs so no
            finding ever states a number the inventory does not contain.

            A predicate may also return a "severity" key to override the
            check's fixed severity for that one finding. Needed by SEC-06,
            whose urgency is a function of the review date rather than of the
            configuration: the same Redis OSS 6 cluster is a plannable upgrade
            in 2026 and an active hourly surcharge in 2027, and one fixed
            severity cannot express both. Any override must be in
            VALID_SEVERITIES; the evaluator rejects anything else rather than
            emitting a finding with an unrankable severity.

            A predicate may also return a "title" key to override the fixed
            title, for the same reason -- "past end of support" and
            "approaching end of support" are different headlines.
        recommendation: Remediation text. Read-only skill: this is advice, and
            the agent is required to explain impact and ask before any change.
            A predicate may override it per finding with a "recommendation" key
            when the right action depends on the evidence.
        needs_review_date: True when the predicate takes the review date as a
            keyword argument (``today``). Declared here rather than discovered by
            introspection so the calling convention is readable at the registry
            entry, and so a predicate that needs the date but forgets to say so
            fails with a TypeError instead of quietly reading the wall clock.
        needs_policy: True when the predicate takes the effective Policy as a
            keyword argument (``policy``) — the checks graded against a
            customer-overridable value (OE-04 required tags, REL-03 replica
            floor, REL-04b retention floor). Declared for the same reason as
            needs_review_date: the dependency is visible at the registry entry,
            and a mismatch is a TypeError, not a silent read of a module constant.
    """

    check_id: str
    pillar: str
    title: str
    severity: str
    applies_to: tuple[str, ...]
    predicate: callable
    recommendation: str
    needs_review_date: bool = False
    needs_policy: bool = False


def _fmt_list(values) -> str:
    """Render a list for prose without leaking Python repr syntax."""
    return ", ".join(str(v) for v in values)


class _Skip:
    """Sentinel a predicate returns to skip THIS cluster with a reason.

    ``applies_to`` handles inapplicability at the *type* level -- a check that is
    vacuous for serverless is excluded there. But some checks are inapplicable
    for a *per-record* reason the type cannot express: OE-06a/OE-06b apply to
    every node-based cluster, yet the log type they check for did not exist
    before a given engine version, so a cluster running an older engine cannot
    have it and must be skipped-with-a-reason rather than reported as failing.

    Returning this from a predicate lands the check in ``checks_skipped`` (with
    the reason) instead of ``findings``, keeping "did not apply" and "failed"
    distinct -- the same rule serverless skips rely on. Returning ``None`` still
    means the check ran and passed.
    """

    __slots__ = ("reason",)

    def __init__(self, reason: str):
        self.reason = reason


# --- Security -------------------------------------------------------------


def _sec01_tls(cluster: dict):
    """SEC-01: in-transit encryption enabled."""
    if cluster.get("tls_enabled"):
        return None
    return {"value": False, "detail": "TransitEncryptionEnabled is false"}


def _sec02_at_rest(cluster: dict):
    """SEC-02: at-rest encryption enabled."""
    if cluster.get("encryption_at_rest"):
        return None
    return {"value": False, "detail": "AtRestEncryptionEnabled is false"}


def _sec03_auth(cluster: dict):
    """SEC-03: some authentication is configured."""
    if cluster.get("auth_mode", "none") != "none":
        return None
    return {"value": "none", "detail": "no RBAC user group and no AUTH token"}


def _sec04_rbac(cluster: dict):
    """SEC-04: auth uses RBAC rather than a legacy AUTH token.

    Only meaningful when auth exists at all — a cluster with no auth is SEC-03's
    finding, and reporting both would double-count one problem.
    """
    if cluster.get("auth_mode") != "AUTH-token":
        return None
    return {"value": "AUTH-token", "detail": "legacy AUTH token in use, not RBAC"}


def _sec05_security_groups(cluster: dict):
    """SEC-05: no overly broad inbound CIDR on attached security groups."""
    offenders = []
    for group in cluster.get("security_groups") or []:
        for rule in group.get("permissive_rules") or []:
            offenders.append(f"{group.get('group_id')}: {rule}")
    if not offenders:
        return None
    return {"value": len(offenders), "detail": _fmt_list(offenders)}


# --- Reliability ----------------------------------------------------------


def _rel01_multi_az(cluster: dict):
    """REL-01: Multi-AZ enabled."""
    if cluster.get("multi_az"):
        return None
    return {"value": False, "detail": "MultiAZ is not enabled"}


def _rel02_failover(cluster: dict):
    """REL-02: automatic failover enabled."""
    if cluster.get("automatic_failover"):
        return None
    return {"value": False, "detail": "AutomaticFailover is not enabled"}


def _rel03_replicas(cluster: dict, policy: "Policy"):
    """REL-03: at least policy.min_replicas_per_shard replicas per shard."""
    floor = policy.min_replicas_per_shard
    replicas = cluster.get("num_replicas_per_shard")
    if replicas is None:
        return None  # topology unknown; absence is not a failure
    if replicas >= floor:
        return None
    return {
        "value": replicas,
        "threshold": floor,
        "detail": f"{replicas} replica(s) per shard",
        "recommendation": (
            f"Add replicas to reach {floor} per shard. With fewer, a failover "
            "leaves the shard with reduced or no redundancy."
        ),
    }


def _rel04_backups(cluster: dict):
    """REL-04: daily backups retained for MIN_SNAPSHOT_RETENTION_DAYS or more.

    Zero retention means backups are off, which is a materially worse condition
    than a short retention window, so the two are reported with different
    severities via the split check IDs below.
    """
    retention = cluster.get("snapshot_retention_days") or 0
    if retention == 0:
        return {"value": 0, "detail": "SnapshotRetentionLimit is 0 (backups disabled)"}
    return None


def _rel04b_retention_short(cluster: dict, policy: "Policy"):
    """REL-04 (secondary): backups on, but retained for fewer days than policy."""
    floor = policy.min_snapshot_retention_days
    retention = cluster.get("snapshot_retention_days") or 0
    if retention == 0:
        return None  # covered by REL-04
    if retention >= floor:
        return None
    return {
        "value": retention,
        "threshold": floor,
        "detail": f"retention is {retention} day(s)",
        "recommendation": (
            f"Raise snapshot retention to at least {floor} day(s) to cover the "
            "recovery window your policy requires."
        ),
    }


def _major_version(version) -> int | None:
    """Extract the major version number from an engine_version string.

    Real inventories carry "6.2", "8.0", and bare "8" (serverless reports the
    major only), so this takes the leading integer and ignores the rest.

    Returns None when there is no leading integer to read. That case must stay
    silent rather than default to a number: guessing a major version here would
    either invent a deadline for a cluster that has none, or hide a real one.
    """
    if version is None:
        return None
    head = str(version).strip().split(".")[0]
    try:
        return int(head)
    except ValueError:
        return None


def _sec06_extended_support(cluster: dict, today: datetime.date):
    """SEC-06: engine is not in (or approaching) ElastiCache Extended Support.

    Past the end of standard support, a cache is auto-enrolled in Extended
    Support and accrues a premium on its node rate -- 80% in years 1-2, 160% in
    year 3 -- while receiving only critical security and defect fixes.

    Unlike every other check in this registry, the answer depends on *when the
    review runs*, not only on the configuration. So severity is graded by
    proximity and the finding always states the date and the day count, letting
    a reader check the grading instead of trusting it.

    Reports dates and multipliers, never dollars: the premium is a percentage of
    a region-dependent On-Demand rate this stage has no business guessing at.
    The agent converts to money at presentation time.

    Args:
        cluster: One cluster record from inventory.json.
        today: Review date, injected by the evaluator (Check.needs_review_date).
            Required, with no default: a default would let this read the wall
            clock the moment the evaluator forgot to pass it, and the resulting
            output would be non-reproducible in a way nothing would notice.

    Returns:
        None only when the engine has no announced end of standard support. A
        date that is merely far away is NOT a pass: a dated obligation is always
        reported, just at LOW.
    """
    engine = (cluster.get("engine") or "").lower()
    major = _major_version(cluster.get("engine_version"))
    if major is None:
        return None

    end_of_support = ENGINE_SUPPORT_SCHEDULE.get((engine, major))
    if end_of_support is None:
        # Valkey, Memcached, and Redis OSS 7+ have no announced date. Silence is
        # the correct output; see references/engine-support-lifecycle.md.
        return None

    eos_date = datetime.date.fromisoformat(end_of_support)
    days = (eos_date - today).days
    version_label = f"{engine} {cluster.get('engine_version')}"

    if days < 0:
        # Already enrolled. Which premium year applies depends on how long ago
        # standard support ended -- reported so the number is checkable.
        year = min(3, (-days) // 365 + 1)
        premium = EXTENDED_SUPPORT_PREMIUM[year]
        return {
            "value": end_of_support,
            "severity": "HIGH",
            "title": "Engine is in Extended Support (paid)",
            "detail": (
                f"{version_label} passed end of standard support on "
                f"{end_of_support} ({-days} days ago) and is enrolled in "
                f"Extended Support year {year}, adding a "
                f"{premium:.0%} premium to the On-Demand node rate"
            ),
            "recommendation": (
                "Upgrade to Valkey to end the Extended Support premium and pay "
                "20% less per node-hour than Redis OSS (33% less on "
                "serverless). For Multi-AZ replication groups on Redis OSS "
                "5.0.6+ the in-place upgrade is zero-downtime. Deleting the "
                "cluster also stops the charge if nothing depends on it. "
                "Estimate the exact surcharge for this node type before "
                "scheduling; the premium is a percentage of a region-specific "
                "rate."
            ),
        }

    severity = "MEDIUM" if days <= SUPPORT_END_WARNING_DAYS else "LOW"
    return {
        "value": end_of_support,
        "threshold": SUPPORT_END_WARNING_DAYS,
        "severity": severity,
        "title": "Engine approaching end of standard support",
        "detail": (
            f"{version_label} reaches end of standard support on "
            f"{end_of_support}, in {days} days; Extended Support then adds a "
            f"premium of {EXTENDED_SUPPORT_PREMIUM[1]:.0%} to the On-Demand "
            "node rate until upgraded"
        ),
        "recommendation": (
            f"Plan an upgrade before {end_of_support} to avoid Extended "
            "Support charges entirely. Valkey is the recommended target: no "
            "announced end of support, and 20% less per node-hour than Redis "
            "OSS. Verify client compatibility first."
        ),
    }


# --- Operational Excellence -----------------------------------------------


def _oe04_tags(cluster: dict, policy: "Policy"):
    """OE-04: required operational tags present (per policy.required_tags)."""
    required = policy.required_tags
    if not required:
        return None  # customer declared no required tags; nothing to check
    tags = cluster.get("tags") or {}
    missing = [t for t in required if not tags.get(t)]
    if not missing:
        return None
    return {
        "value": len(missing),
        "detail": f"missing {_fmt_list(missing)}",
        "recommendation": (
            f"Add {_fmt_list(required)} tags for ownership and cost attribution."
        ),
    }


# --- Log delivery (OE-06) --------------------------------------------------
#
# ElastiCache delivers two independent log types, gated on different engine
# versions, so OE-06 is two checks. slow-log (which commands are expensive)
# needs Redis OSS 6.0+ or Valkey; engine-log (failed sync, backup start,
# critical events) needs Redis OSS 6.2+ or Valkey. A cluster on an engine below
# the gate cannot have the log at all, so the check must skip-with-a-reason
# rather than report a config the platform would reject. Serverless has no log
# delivery whatsoever, which applies_to=("node-based",) handles.


def _engine_version_tuple(version):
    """(major, minor) ints from an engine_version string, or None.

    "7.1.0" -> (7, 1), "6.2" -> (6, 2), bare "8" -> (8, 0). Returns None when
    there is no leading integer to read, matching _major_version's rule: a
    guessed version would either invent a gate or hide one. A non-numeric minor
    (unusual, but possible in malformed data) is read as 0 rather than crashing.
    """
    if version is None:
        return None
    parts = str(version).strip().split(".")
    try:
        major = int(parts[0])
    except (ValueError, IndexError):
        return None
    minor = 0
    if len(parts) > 1:
        try:
            minor = int(parts[1])
        except ValueError:
            minor = 0
    return (major, minor)


def _supports_slow_log(engine: str, version_tuple) -> bool:
    """slow-log delivery is available on Redis OSS 6.0+ or any Valkey."""
    if engine == "valkey":
        return True
    if engine == "redis":
        return version_tuple is not None and version_tuple >= (6, 0)
    return False


def _supports_engine_log(engine: str, version_tuple) -> bool:
    """engine-log delivery is available on Redis OSS 6.2+ or any Valkey."""
    if engine == "valkey":
        return True
    if engine == "redis":
        return version_tuple is not None and version_tuple >= (6, 2)
    return False


def _has_active_log(cluster: dict, log_type: str) -> bool:
    """True when a log_delivery entry for log_type has an active Status.

    "Configured" means active, not merely present: an entry stuck in enabling,
    disabling, or error is not delivering logs, so treating it as configured
    would mask a real gap.
    """
    for entry in cluster.get("log_delivery") or []:
        if (entry.get("log_type") == log_type
                and (entry.get("status") or "").lower() == "active"):
            return True
    return False


_OE06_RECOMMENDATION = (
    "Enable log delivery with modify-replication-group (or modify-cache-cluster) "
    "so command and engine diagnostics reach CloudWatch Logs or Kinesis Data "
    "Firehose. slow-log requires Redis OSS 6.0+ or Valkey; engine-log requires "
    "Redis OSS 6.2+ or Valkey."
)


def _oe06a_slow_log(cluster: dict):
    """OE-06a: slow-log delivery configured (gated on engine support)."""
    engine = (cluster.get("engine") or "").lower()
    version = cluster.get("engine_version")
    if not _supports_slow_log(engine, _engine_version_tuple(version)):
        return _Skip(
            f"slow-log delivery requires Redis OSS 6.0+ or Valkey; "
            f"{engine or 'unknown'} {version} does not support it"
        )
    if _has_active_log(cluster, "slow-log"):
        return None
    return {"value": False, "detail": "no active slow-log delivery configured"}


def _oe06b_engine_log(cluster: dict):
    """OE-06b: engine-log delivery configured (gated on engine support)."""
    engine = (cluster.get("engine") or "").lower()
    version = cluster.get("engine_version")
    if not _supports_engine_log(engine, _engine_version_tuple(version)):
        return _Skip(
            f"engine-log delivery requires Redis OSS 6.2+ or Valkey; "
            f"{engine or 'unknown'} {version} does not support it"
        )
    if _has_active_log(cluster, "engine-log"):
        return None
    return {"value": False, "detail": "no active engine-log delivery configured"}


# --- Cost Optimization ----------------------------------------------------


def _cost01_valkey(cluster: dict):
    """COST-01: engine is Valkey.

    Valkey is materially cheaper than Redis OSS for the same workload. This is
    an opportunity, not a defect, hence LOW.
    """
    engine = (cluster.get("engine") or "").lower()
    if engine != "redis":
        return None
    return {"value": engine, "detail": "engine is Redis OSS, not Valkey"}


def _cost06_usage_limits(cluster: dict):
    """COST-06: serverless caches have usage limits set.

    Without DataStorage.Maximum or ECPUPerSecond.Maximum, serverless spend is
    unbounded.
    """
    limits = cluster.get("cache_usage_limits") or {}
    storage_max = (limits.get("data_storage") or {}).get("maximum")
    ecpu_max = (limits.get("ecpu_per_second") or {}).get("maximum")
    unset = []
    if storage_max is None:
        unset.append("DataStorage.Maximum")
    if ecpu_max is None:
        unset.append("ECPUPerSecond.Maximum")
    if not unset:
        return None
    return {"value": len(unset), "detail": f"unset: {_fmt_list(unset)}"}


# Registry order is the order findings appear before severity sorting. Kept
# grouped by pillar so a reviewer can diff it against
# references/well-architected-mapping.md row by row.
CHECKS: tuple[Check, ...] = (
    Check(
        check_id="SEC-01",
        pillar="security",
        title="In-transit encryption (TLS) disabled",
        severity="CRITICAL",
        applies_to=("node-based",),
        predicate=_sec01_tls,
        recommendation=(
            "Enable TLS. Use the preferred->required migration so existing "
            "clients keep working during rollout."
        ),
    ),
    Check(
        check_id="SEC-02",
        pillar="security",
        title="At-rest encryption disabled",
        severity="HIGH",
        applies_to=("node-based",),
        predicate=_sec02_at_rest,
        recommendation=(
            "At-rest encryption cannot be enabled on an existing cluster. "
            "Create a new encrypted cluster and migrate via backup/restore."
        ),
    ),
    Check(
        check_id="SEC-03",
        pillar="security",
        title="No authentication configured",
        severity="CRITICAL",
        applies_to=("node-based", "serverless"),
        predicate=_sec03_auth,
        recommendation=(
            "Configure RBAC with a dedicated user per application. Network "
            "isolation alone is not authentication."
        ),
    ),
    Check(
        check_id="SEC-04",
        pillar="security",
        title="Legacy AUTH token instead of RBAC",
        severity="MEDIUM",
        applies_to=("node-based", "serverless"),
        predicate=_sec04_rbac,
        recommendation=(
            "Migrate from AUTH token to RBAC for per-user, per-command "
            "granularity and rotation without downtime."
        ),
    ),
    Check(
        check_id="SEC-05",
        pillar="security",
        title="Overly permissive security group rule",
        severity="CRITICAL",
        applies_to=("node-based", "serverless"),
        predicate=_sec05_security_groups,
        recommendation=(
            "Restrict inbound rules to the application security groups that "
            "need cache access. Remove broad CIDR ranges."
        ),
    ),
    Check(
        check_id="REL-01",
        pillar="reliability",
        title="Multi-AZ not enabled",
        severity="HIGH",
        applies_to=("node-based",),
        predicate=_rel01_multi_az,
        recommendation=(
            "Enable Multi-AZ so a single AZ failure does not take the cache "
            "offline. Requires at least one replica."
        ),
    ),
    Check(
        check_id="REL-02",
        pillar="reliability",
        title="Automatic failover not enabled",
        severity="HIGH",
        applies_to=("node-based",),
        predicate=_rel02_failover,
        recommendation=(
            "Enable automatic failover so replica promotion does not require "
            "manual intervention during a primary failure."
        ),
    ),
    Check(
        check_id="REL-03",
        pillar="reliability",
        title="Fewer than two replicas per shard",
        severity="MEDIUM",
        applies_to=("node-based",),
        predicate=_rel03_replicas,
        recommendation=(
            f"Add replicas to reach {MIN_REPLICAS_PER_SHARD} per shard. With "
            "one replica, a failover leaves the shard with no redundancy."
        ),
        needs_policy=True,
    ),
    Check(
        check_id="REL-04",
        pillar="reliability",
        title="Backups disabled",
        severity="HIGH",
        applies_to=("node-based", "serverless"),
        predicate=_rel04_backups,
        recommendation=(
            f"Enable daily automatic backups with at least "
            f"{MIN_SNAPSHOT_RETENTION_DAYS} days retention."
        ),
    ),
    Check(
        check_id="REL-04b",
        pillar="reliability",
        title="Backup retention below policy",
        severity="LOW",
        applies_to=("node-based", "serverless"),
        predicate=_rel04b_retention_short,
        recommendation=(
            f"Raise snapshot retention to at least "
            f"{MIN_SNAPSHOT_RETENTION_DAYS} days to cover a full week of "
            "recovery points."
        ),
        needs_policy=True,
    ),
    Check(
        check_id="OE-04",
        pillar="operational_excellence",
        title="Required operational tags missing",
        severity="LOW",
        applies_to=("node-based", "serverless"),
        predicate=_oe04_tags,
        recommendation=(
            f"Add {_fmt_list(REQUIRED_TAGS)} tags for ownership and cost "
            "attribution."
        ),
        needs_policy=True,
    ),
    Check(
        check_id="OE-06a",
        pillar="operational_excellence",
        title="Slow-log delivery not configured",
        severity="LOW",
        applies_to=("node-based",),
        predicate=_oe06a_slow_log,
        recommendation=_OE06_RECOMMENDATION,
    ),
    Check(
        check_id="OE-06b",
        pillar="operational_excellence",
        title="Engine-log delivery not configured",
        severity="LOW",
        applies_to=("node-based",),
        predicate=_oe06b_engine_log,
        recommendation=_OE06_RECOMMENDATION,
    ),
    Check(
        check_id="COST-01",
        pillar="cost_optimization",
        title="Engine is Redis OSS rather than Valkey",
        severity="LOW",
        applies_to=("node-based", "serverless"),
        predicate=_cost01_valkey,
        recommendation=(
            "Migrate to Valkey: it is about 20% less per node-hour than Redis "
            "OSS at equivalent capacity (published on-demand ratio). Price the "
            "exact saving for this node type with price_calculator.py and verify "
            "client compatibility first."
        ),
    ),
    Check(
        check_id="COST-06",
        pillar="cost_optimization",
        title="Serverless usage limits not set",
        severity="MEDIUM",
        applies_to=("serverless",),
        predicate=_cost06_usage_limits,
        recommendation=(
            "Set DataStorage.Maximum and ECPUPerSecond.Maximum so a traffic "
            "anomaly cannot produce an unbounded bill."
        ),
    ),
    Check(
        check_id="SEC-06",
        pillar="security",
        # Filed under security because the defining consequence of running past
        # end of standard support is receiving only critical-CVE fixes. The
        # Extended Support premium is a cost consequence of the same fact, and
        # the finding states it, but the reason to act is the patch gap.
        title="Engine approaching end of standard support",
        # Overridden per finding by the predicate: LOW / MEDIUM / HIGH depending
        # on how close the date is. This value is the floor.
        severity="LOW",
        applies_to=("node-based", "serverless"),
        predicate=_sec06_extended_support,
        recommendation=(
            "Upgrade to Valkey or a Redis OSS version under standard support."
        ),
        needs_review_date=True,
    ),
)


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------


class ConfigurationChecker:
    """Runs the check registry over an inventory and emits findings.

    Findings match the shape Stage 3 emits (see analyze_metrics.py
    FindingsGenerator) so downstream consumers need no special case. The
    `model_source` field is "configuration", which is how a reader tells a
    config finding from a metric finding.
    """

    def __init__(self, checks: tuple[Check, ...] = CHECKS, today=None, policy=None):
        """Store the check registry, the review date, and the policy.

        Args:
            checks: Checks to evaluate. Injectable for testing.
            today: Review date as a `datetime.date`, defaulting to the current
                UTC date. Injectable because SEC-06's severity is a function of
                it: a test that relied on the real clock would grade a fleet
                differently over time and eventually start failing on its own.
                Resolved once here rather than per predicate call, so a run that
                straddles midnight cannot grade two clusters against two dates.
            policy: A `Policy` of overridable per-customer values (required tags,
                replica and retention floors), defaulting to the Well-Architected
                recommendations. Held once here so every cluster is graded against
                one policy, and recorded in metadata.policy for the reader.
        """
        self._checks = checks
        self._today = today or datetime.datetime.now(datetime.timezone.utc).date()
        self._policy = policy or Policy()
        self._counter = 0

    def _next_finding_id(self, check_id: str) -> str:
        """Produce a stable, ordered finding id.

        Deterministic across runs on identical input, which the report's
        byte-stability guarantee depends on.
        """
        self._counter += 1
        return f"config-{self._counter:03d}-{check_id.lower()}"

    def check_cluster(self, cluster: dict) -> tuple[list[dict], list[dict]]:
        """Evaluate every applicable check against one cluster.

        Args:
            cluster: A single cluster record from inventory.json.

        Returns:
            (findings, skipped) where findings is a list of finding dicts and
            skipped records checks that did not apply, with the reason. Skips
            are reported rather than dropped so a passing report cannot be
            confused with an unevaluated one.
        """
        cluster_type = cluster.get("cluster_type") or "unknown"
        findings: list[dict] = []
        skipped: list[dict] = []

        for check in self._checks:
            if cluster_type not in check.applies_to:
                skipped.append({
                    "check_id": check.check_id,
                    "reason": (
                        f"not applicable to {cluster_type} clusters "
                        "(platform-enforced or field absent)"
                    ),
                })
                continue

            try:
                kwargs = {}
                if check.needs_review_date:
                    kwargs["today"] = self._today
                if check.needs_policy:
                    kwargs["policy"] = self._policy
                evidence = check.predicate(cluster, **kwargs)
            except Exception as e:  # a broken predicate must not kill the stage
                logger.warning(
                    "Check %s raised on cluster %s: %s",
                    check.check_id, cluster.get("cluster_id"), e,
                )
                skipped.append({
                    "check_id": check.check_id,
                    "reason": f"check raised an error: {e}",
                })
                continue

            if evidence is None:
                continue

            # A predicate may declare the check inapplicable to THIS cluster for
            # a data-dependent reason (see _Skip): record it as a skip, not a
            # finding, so "did not apply" never reads as "passed".
            if isinstance(evidence, _Skip):
                skipped.append({
                    "check_id": check.check_id,
                    "reason": evidence.reason,
                })
                continue

            # Per-finding overrides (see Check.severity / .title). An unknown
            # severity is dropped back to the registry value rather than
            # emitted: the scoring formula and the severity counters both key on
            # VALID_SEVERITIES, so an unrankable string would either KeyError in
            # run() or silently score as nothing.
            severity = evidence.get("severity") or check.severity
            if severity not in VALID_SEVERITIES:
                logger.warning(
                    "Check %s returned severity %r for cluster %s, which is not "
                    "one of %s; using the registry severity %s instead",
                    check.check_id, evidence.get("severity"),
                    cluster.get("cluster_id"), _fmt_list(sorted(VALID_SEVERITIES)),
                    check.severity,
                )
                severity = check.severity

            findings.append({
                "finding_id": self._next_finding_id(check.check_id),
                "model_source": "configuration",
                "check_id": check.check_id,
                "pillar": check.pillar,
                "severity": severity,
                "title": evidence.get("title") or check.title,
                "description": (
                    f"{cluster.get('cluster_id')}: {evidence['detail']}"
                ),
                "metric_name": None,  # config checks are not metric-derived
                "current_value": evidence.get("value"),
                "threshold": evidence.get("threshold"),
                "recommendation": (
                    evidence.get("recommendation") or check.recommendation
                ),
            })

        return findings, skipped

    def run(self, inventory: dict) -> dict:
        """Evaluate the whole inventory.

        Args:
            inventory: Parsed inventory.json.

        Returns:
            The output document: metadata, per-cluster findings and skips.
        """
        clusters = inventory.get("clusters")
        if not isinstance(clusters, list):
            # Stage 1 writes `clusters` as a LIST. The removed Stage 4 assumed a
            # dict here and crashed on every real run while its fixture-shaped
            # tests passed. Fail loudly instead of silently producing nothing.
            raise TypeError(
                "inventory['clusters'] must be a list (as discover_inventory.py "
                f"writes), got {type(clusters).__name__}"
            )

        self._counter = 0
        results: dict[str, dict] = {}
        total_findings = 0
        by_severity = {s: 0 for s in VALID_SEVERITIES}

        for cluster in clusters:
            cluster_id = cluster.get("cluster_id")
            if not cluster_id:
                logger.warning("Skipping cluster record with no cluster_id")
                continue

            findings, skipped = self.check_cluster(cluster)
            for f in findings:
                by_severity[f["severity"]] += 1
            total_findings += len(findings)

            results[cluster_id] = {
                "cluster_type": cluster.get("cluster_type"),
                "findings": findings,
                "checks_skipped": skipped,
                "checks_evaluated": len(self._checks) - len(skipped),
            }

        inv_meta = inventory.get("metadata") or {}
        return {
            "metadata": {
                "pipeline_version": PIPELINE_VERSION,
                "account_id": inv_meta.get("account_id"),
                "regions_scanned": inv_meta.get("regions_scanned") or [],
                "source_inventory_scan": inv_meta.get("scan_timestamp"),
                "checks_in_registry": len(self._checks),
                "check_ids": [c.check_id for c in self._checks],
                # SEC-06's severity is graded against this date, so it is
                # recorded: without it a reader cannot reproduce the grading, and
                # the same fleet legitimately grades differently next quarter.
                "review_date": self._today.isoformat(),
                "clusters_checked": len(results),
                "total_findings": total_findings,
                "findings_by_severity": by_severity,
                "policy": {
                    "min_replicas_per_shard": self._policy.min_replicas_per_shard,
                    "min_snapshot_retention_days": (
                        self._policy.min_snapshot_retention_days
                    ),
                    "required_tags": list(self._policy.required_tags),
                },
            },
            "clusters": results,
        }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _atomic_write(data: dict, output_path: str) -> None:
    """Write JSON atomically via temp file + os.replace()."""
    output_dir = os.path.dirname(os.path.abspath(output_path))
    os.makedirs(output_dir, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(
        suffix=".tmp", prefix="config_findings_", dir=output_dir
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=False)
            f.write("\n")
        os.replace(tmp_path, output_path)
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


def parse_args(argv=None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Configuration Checks — Stage 3.5. Evaluates ElastiCache "
            "configuration against a fixed policy using inventory.json only. "
            "No AWS calls. Emits deterministic findings; no scores."
        ),
    )
    parser.add_argument(
        "--inventory", required=True, help="Path to inventory.json (Stage 1)."
    )
    parser.add_argument(
        "--output",
        default="./config_findings.json",
        help="Output path (default: ./config_findings.json).",
    )
    parser.add_argument(
        "--as-of",
        default=None,
        metavar="YYYY-MM-DD",
        help=(
            "Review date used to grade time-sensitive checks (SEC-06 engine "
            "end-of-support). Defaults to today (UTC). Pin it to make output "
            "reproducible, which is what the example fleet generator does."
        ),
    )
    parser.add_argument(
        "--required-tags",
        nargs="*",
        default=None,
        metavar="TAG",
        help=(
            "Tag keys OE-04 requires on every cluster. Omit to use the "
            "Well-Architected default (Environment Owner Application); pass your "
            "own set to match your tagging standard; pass the flag with no values "
            "to require none. The effective set is recorded in metadata.policy."
        ),
    )
    parser.add_argument(
        "--min-replicas-per-shard",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Replica floor REL-03 grades against (default: "
            f"{MIN_REPLICAS_PER_SHARD}). Set to your accepted HA topology."
        ),
    )
    parser.add_argument(
        "--min-snapshot-retention-days",
        type=int,
        default=None,
        metavar="DAYS",
        help=(
            "Backup-retention floor REL-04b grades against (default: "
            f"{MIN_SNAPSHOT_RETENTION_DAYS}). Set to your recovery-window policy."
        ),
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable detailed logging."
    )
    return parser.parse_args(argv)


def _policy_from_args(args) -> Policy:
    """Build the effective Policy from CLI args, falling back to WA defaults.

    A flag left unset keeps the Well-Architected default; a flag set replaces it.
    `--required-tags` distinguishes 'unset' (None -> default three) from 'set to
    none' ([] -> require no tags), which the OE-04 predicate honours by not firing.
    """
    defaults = Policy()
    return Policy(
        required_tags=(
            tuple(args.required_tags)
            if args.required_tags is not None
            else defaults.required_tags
        ),
        min_replicas_per_shard=(
            args.min_replicas_per_shard
            if args.min_replicas_per_shard is not None
            else defaults.min_replicas_per_shard
        ),
        min_snapshot_retention_days=(
            args.min_snapshot_retention_days
            if args.min_snapshot_retention_days is not None
            else defaults.min_snapshot_retention_days
        ),
    )


def main(argv=None) -> int:
    """Entry point."""
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    as_of = None
    if args.as_of:
        try:
            as_of = datetime.date.fromisoformat(args.as_of)
        except ValueError:
            logger.error(
                "--as-of must be an ISO date (YYYY-MM-DD), got %r", args.as_of
            )
            return 1

    if args.min_replicas_per_shard is not None and args.min_replicas_per_shard < 0:
        logger.error("--min-replicas-per-shard must be >= 0")
        return 1
    if (
        args.min_snapshot_retention_days is not None
        and args.min_snapshot_retention_days < 0
    ):
        logger.error("--min-snapshot-retention-days must be >= 0")
        return 1
    policy = _policy_from_args(args)

    try:
        with open(args.inventory, encoding="utf-8") as f:
            inventory = json.load(f)
    except FileNotFoundError:
        logger.error("Inventory file not found: %s", args.inventory)
        return 1
    except json.JSONDecodeError as e:
        logger.error("Inventory file is not valid JSON: %s", e)
        return 1

    try:
        output = ConfigurationChecker(today=as_of, policy=policy).run(inventory)
    except TypeError as e:
        logger.error("%s", e)
        return 1

    try:
        _atomic_write(output, args.output)
    except OSError as e:
        logger.error("Failed to write output: %s", e)
        return 1

    meta = output["metadata"]
    counts = meta["findings_by_severity"]
    logger.info(
        "Checked %d cluster(s) against %d checks: %d finding(s) "
        "(%d CRITICAL, %d HIGH, %d MEDIUM, %d LOW)",
        meta["clusters_checked"], meta["checks_in_registry"],
        meta["total_findings"], counts["CRITICAL"], counts["HIGH"],
        counts["MEDIUM"], counts["LOW"],
    )
    logger.info("Output: %s", args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
