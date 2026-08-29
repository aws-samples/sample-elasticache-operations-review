#!/usr/bin/env python3
"""Generate the synthetic example fleet (Phase 6e / Phase 4 golden fleet).

Why this exists
---------------
Two problems, one fixture:

1. **A public sample nobody can run.** Without an AWS account holding
   ElastiCache clusters there was no way to see what this tool produces. This
   writes a complete set of pipeline outputs so ``generate_html_report.py`` runs
   offline, with no credentials.

2. **Every generalization bug hid behind the live fleet.** The development fleet
   is 3 idle clusters in one region. Report prose asserted "all N clusters serve
   zero traffic" as fact and the palette silently reused a hue past the third
   series -- both invisible against that fleet, both wrong for a customer.

So this fleet is built to be *unlike* it, deliberately:

===========================  =====================================================
Property                     Why
===========================  =====================================================
7 clusters                   More than twice the 3-slot categorical palette, so
                             the fleet charts must fold rather than cycle -- and
                             the fold has to name what it folded.
3 regions                    us-east-1, eu-west-1, ap-southeast-2. eu-west-1 is
                             the region whose Cost Explorer billing code is EU
                             rather than EUW1 -- the 6a bug.
Busy, not idle               Real traffic, real hit rates, diurnal shape. Any
                             prose asserting fleet-wide idleness is now false and
                             a test can catch it.
One hot cluster              Sustained CPU above the 70% HIGH threshold, so the
                             report has a genuine finding to show.
One fully compliant cluster  Nothing to report. Proves the checks stay silent --
                             a report that always finds something is useless.
One idle cluster             Idle *is* a real condition; it must still be
                             detectable, just not assumed.
One network-bound cluster    Low CPU, low memory, network past 80%. The one
                             combination where "idle on compute" and "scale it
                             down" are opposite answers, and where the fix
                             (a bigger node) is bought for bandwidth alone.
===========================  =====================================================

How it stays honest
-------------------
Only ``inventory.json`` and ``metrics.json`` are synthesized -- the two stages
that talk to AWS. ``analysis.json`` and ``config_findings.json`` are produced by
running the **real** Stage 3 and Stage 3.5 over them.

That is deliberate and it is the whole point. All five defects in this pipeline's
history came from one cause: stage boundaries verified against hand-written
fixtures instead of the previous stage's actual output. A hand-written
``analysis.json`` would reintroduce exactly that. If the synthesized metrics are
the wrong shape, Stage 3 fails here rather than in a customer's report.

Determinism: values come from a seeded ``random.Random``, timestamps are
computed from a fixed epoch, and no wall-clock or ambient state is read. Running
this twice produces byte-identical files, which is what lets the golden fleet
detect an unintended change.

Usage::

    python3 scripts/make_example_fleet.py --output examples/
    python3 scripts/generate_html_report.py \
        --inventory examples/inventory.json \
        --metrics examples/metrics.json \
        --analysis examples/analysis.json \
        --output examples/report.html \
        --generated-at "2026-01-15 12:00 UTC"

The account id is ``123456789012`` (the AWS documentation placeholder) and every
cluster name, subnet, and security group is obviously synthetic. Nothing here
comes from a real account.
"""

import argparse
import datetime
import hashlib
import json
import logging
import math
import os
import random
import re
import subprocess
import sys
import tempfile
from pathlib import Path

from _pipeline_version import PIPELINE_VERSION

logger = logging.getLogger("make_example_fleet")

SCRIPT_DIR = Path(__file__).resolve().parent

# Documentation placeholder account. Never a real one.
ACCOUNT_ID = "123456789012"

# Fixed window so output is reproducible. 14 days at 5-minute resolution is what
# fetch_metrics.py collects by default: 12 * 24 * 14 = 4032 points.
EPOCH = datetime.datetime(2026, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
DAYS = 14
POINTS = 12 * 24 * DAYS
RESOLUTION_SECONDS = 300

# The date Stage 3.5 grades time-sensitive checks against (SEC-06 engine
# end-of-support). Derived from the metric window rather than written as a second
# constant, so the review cannot claim to have happened before the data it reads.
# Pinned because SEC-06's severity is a function of the review date: without this
# the example fleet's config_findings.json would change severity as the real
# calendar advanced, breaking the byte-for-byte reproducibility guarantee.
REVIEW_DATE = (EPOCH + datetime.timedelta(days=DAYS)).date()

# Statistic sets per metric, transcribed from real fetch_metrics.py output so the
# fixture cannot drift from what Stage 3 is given in production. Order matters
# only for readability; Stage 3 keys by name.
NODE_METRIC_STATS = {
    "EngineCPUUtilization": ["Average", "Maximum"],
    "CPUUtilization": ["Average"],
    "DatabaseMemoryUsagePercentage": ["Maximum"],
    "DatabaseCapacityUsagePercentage": ["Maximum"],
    "ReplicationLag": ["Maximum"],
    "SaveInProgress": ["Maximum"],
    "TrafficManagementActive": ["Maximum"],
    "CacheHitRate": ["Average"],
    "CacheHits": ["Sum"],
    "CacheMisses": ["Sum"],
    "Evictions": ["Sum"],
    "CurrConnections": ["Maximum"],
    "NewConnections": ["Sum"],
    "NetworkBytesIn": ["Sum"],
    "NetworkBytesOut": ["Sum"],
    "BytesUsedForCache": ["Maximum"],
    "CurrItems": ["Maximum"],
    "SuccessfulReadRequestLatency": ["Average"],
    "SuccessfulWriteRequestLatency": ["Average"],
    "NetworkBandwidthInAllowanceExceeded": ["Maximum"],
    "NetworkBandwidthOutAllowanceExceeded": ["Maximum"],
    "NetworkConntrackAllowanceExceeded": ["Maximum"],
    "NetworkPacketsPerSecondAllowanceExceeded": ["Maximum"],
    "NetworkBaselineUsageInPercentage": ["Average", "Maximum"],
    "NetworkBaselineUsageOutPercentage": ["Average", "Maximum"],
    "NetworkBaselineMaxUsageInPercentage": ["Maximum"],
    "NetworkBaselineMaxUsageOutPercentage": ["Maximum"],
    "NetworkMaxBytesIn": ["Maximum"],
    "NetworkMaxBytesOut": ["Maximum"],
    "FreeableMemory": ["Minimum"],
    "SwapUsage": ["Maximum"],
    "MemoryFragmentationRatio": ["Maximum"],
    "DatabaseMemoryUsageCountedForEvictPercentage": ["Maximum"],
    "CurrVolatileItems": ["Maximum"],
    "Reclaimed": ["Sum"],
    "BlockedConnections": ["Maximum"],
    "RejectedConnections": ["Sum"],
    "ErrorCount": ["Sum"],
    "GetTypeCmds": ["Sum"],
    "SetTypeCmds": ["Sum"],
    "StringBasedCmds": ["Sum"],
    "HashBasedCmds": ["Sum"],
    "SortedSetBasedCmds": ["Sum"],
    "ListBasedCmds": ["Sum"],
    "SetBasedCmds": ["Sum"],
    "StreamBasedCmds": ["Sum"],
    "PubSubBasedCmds": ["Sum"],
    "JsonBasedCmds": ["Sum"],
    "JsonBasedGetCmds": ["Sum"],
    "JsonBasedSetCmds": ["Sum"],
    "AuthenticationFailures": ["Sum"],
    "KeyAuthorizationFailures": ["Sum"],
    "CommandAuthorizationFailures": ["Sum"],
    "IamAuthenticationExpirations": ["Sum"],
    "IamAuthenticationThrottling": ["Sum"],
}

SERVERLESS_METRIC_STATS = {
    "ElastiCacheProcessingUnits": ["Sum"],
    "ThrottledCmds": ["Sum"],
    "CacheHitRate": ["Average"],
    "CacheHits": ["Sum"],
    "CacheMisses": ["Sum"],
    "Evictions": ["Sum"],
    "CurrConnections": ["Maximum"],
    "NewConnections": ["Sum"],
    "NetworkBytesIn": ["Sum"],
    "NetworkBytesOut": ["Sum"],
    "BytesUsedForCache": ["Maximum"],
    "CurrItems": ["Maximum"],
    "SuccessfulReadRequestLatency": ["Average"],
    "SuccessfulWriteRequestLatency": ["Average"],
    "GetTypeCmdsECPUs": ["Sum"],
    "SetTypeCmdsECPUs": ["Sum"],
    "StringBasedCmdsECPUs": ["Sum"],
    "HashBasedCmdsECPUs": ["Sum"],
    "SortedSetBasedCmdsECPUs": ["Sum"],
    "ListBasedCmdsECPUs": ["Sum"],
    "SetBasedCmdsECPUs": ["Sum"],
    "StreamBasedCmdsECPUs": ["Sum"],
    "PubSubBasedCmdsECPUs": ["Sum"],
    "AuthenticationFailures": ["Sum"],
    "KeyAuthorizationFailures": ["Sum"],
    "CommandAuthorizationFailures": ["Sum"],
    "IamAuthenticationExpirations": ["Sum"],
    "IamAuthenticationThrottling": ["Sum"],
}


# ---------------------------------------------------------------------------
# Fleet definition -- the interesting part. Each entry states what it is FOR,
# because a fixture whose purpose is undocumented gets "tidied" into uselessness.
# ---------------------------------------------------------------------------

FLEET = [
    {
        "cluster_id": "prod-session-store",
        "purpose": "Busy, healthy, and STEADY. The common case the live fleet "
                   "never covered: high traffic, good hit rate, nothing wrong "
                   "operationally -- and a tight CPU coefficient of variation, "
                   "so it is the fleet's one active+steady cluster. That is the "
                   "gate for a sound Reserved-Node / Database-Savings-Plan "
                   "recommendation (Phase 7c), which every other active cluster "
                   "here is too variable to earn. On a Gen7 Graviton node "
                   "(r7g), so its DSP lever is eligible.",
        "cluster_type": "node-based",
        "region": "us-east-1",
        "engine": "valkey",
        "engine_version": "8.0",
        "node_type": "cache.r7g.xlarge",
        "num_shards": 3,
        "num_replicas_per_shard": 2,
        "load": "busy",
        "cpu_base": 34.0,
        # Compress the CPU diurnal swing so cpu_cov lands well below the 0.30
        # steady band, without touching the traffic that drives its
        # correlations. 0.5 measures ~0.18 through the real Stage 3.
        "cpu_diurnal_scale": 0.5,
        "hit_rate": 0.968,
        "tls_enabled": True,
        "encryption_at_rest": True,
        "auth_mode": "RBAC",
        "multi_az": True,
        "automatic_failover": True,
        "snapshot_retention_days": 7,
        "maxmemory_policy": "allkeys-lru",
        "tags": {"Environment": "production", "Owner": "platform-team",
                 "Application": "web-sessions"},
        # Both log types delivered and active, so OE-06a and OE-06b both stay
        # silent here -- this is the fleet's one fully-compliant cluster and it
        # must report nothing.
        "log_delivery": [
            {"log_type": "slow-log", "destination_type": "cloudwatch-logs",
             "destination_details": {"CloudWatchLogsDetails": {
                 "LogGroup": "/elasticache/prod-session-store/slow-log"}},
             "log_format": "json", "status": "active"},
            {"log_type": "engine-log", "destination_type": "cloudwatch-logs",
             "destination_details": {"CloudWatchLogsDetails": {
                 "LogGroup": "/elasticache/prod-session-store/engine-log"}},
             "log_format": "json", "status": "active"},
        ],
        "replication_group_log_delivery_enabled": True,
    },
    {
        "cluster_id": "prod-api-cache",
        "purpose": "The hot cluster. Sustained CPU above the 70% HIGH "
                   "threshold with rising evictions, so the report has a real "
                   "performance finding to display. Also the only cluster with "
                   "a genuinely high memory percentage.",
        "cluster_type": "node-based",
        "region": "us-east-1",
        "engine": "valkey",
        "engine_version": "8.0",
        "node_type": "cache.r7g.large",
        "num_shards": 2,
        "num_replicas_per_shard": 1,
        "load": "hot",
        "cpu_base": 74.0,
        "hit_rate": 0.812,
        "tls_enabled": True,
        "encryption_at_rest": True,
        "auth_mode": "RBAC",
        "multi_az": True,
        "automatic_failover": True,
        "snapshot_retention_days": 3,
        "maxmemory_policy": "volatile-lru",
        "tags": {"Environment": "production", "Owner": "api-team",
                 "Application": "api-gateway"},
        # slow-log active but engine-log absent: OE-06a stays silent, OE-06b
        # fires. Covers the partial-coverage branch that a cluster with both
        # (session-store) or neither (the rest) does not.
        "log_delivery": [
            {"log_type": "slow-log", "destination_type": "cloudwatch-logs",
             "destination_details": {"CloudWatchLogsDetails": {
                 "LogGroup": "/elasticache/prod-api-cache/slow-log"}},
             "log_format": "json", "status": "active"},
        ],
        "replication_group_log_delivery_enabled": True,
    },
    {
        "cluster_id": "prod-eu-catalog",
        "purpose": "Second region, and specifically eu-west-1 -- whose Cost "
                   "Explorer billing code is EU, not EUW1. The 6a bug was "
                   "invisible without a fleet outside us-east-1. Moderate "
                   "steady load.",
        "cluster_type": "node-based",
        "region": "eu-west-1",
        "engine": "valkey",
        "engine_version": "7.2",
        "node_type": "cache.m7g.large",
        "num_shards": 1,
        "num_replicas_per_shard": 2,
        "load": "moderate",
        "cpu_base": 22.0,
        "mem_pct": 44.0,
        "net_pct": 6.0,
        "hit_rate": 0.943,
        "tls_enabled": True,
        "encryption_at_rest": True,
        "auth_mode": "RBAC",
        "multi_az": True,
        "automatic_failover": True,
        "snapshot_retention_days": 7,
        "maxmemory_policy": "allkeys-lru",
        "tags": {"Environment": "production", "Owner": "catalog-team",
                 "Application": "product-catalog"},
    },
    {
        "cluster_id": "staging-redis-legacy",
        "purpose": "The non-compliant one. Redis OSS rather than Valkey, TLS "
                   "off, no auth, single node, no backups, no Multi-AZ, "
                   "untagged. Exercises most of the Stage 3.5 registry at once "
                   "so the config section has content.",
        "cluster_type": "node-based",
        "region": "us-east-1",
        "engine": "redis",
        "engine_version": "6.2",
        "node_type": "cache.t4g.medium",
        "num_shards": 1,
        "num_replicas_per_shard": 0,
        "load": "moderate",
        # Deliberately above the IDLE bands (CPU 20%, memory 30%): this cluster
        # is here to carry configuration findings, and an IDLE verdict would
        # bury them under "decommission this". dev-scratch-idle is the fleet's
        # one idle cluster, and one is enough to exercise the path.
        "cpu_base": 26.0,
        "mem_pct": 35.0,
        "net_pct": 2.0,
        "hit_rate": 0.771,
        "tls_enabled": False,
        "encryption_at_rest": False,
        "auth_mode": "none",
        "multi_az": False,
        "automatic_failover": False,
        "snapshot_retention_days": 0,
        "maxmemory_policy": "volatile-lru",
        "tags": {},
        "permissive_rules": ["0.0.0.0/0 on ports 6379-6379"],
    },
    {
        "cluster_id": "dev-scratch-idle",
        "purpose": "Idle, in a third region. Idleness is a real finding and "
                   "must stay detectable -- the bug was asserting it "
                   "fleet-wide, not reporting it per cluster. ap-southeast-2 "
                   "keeps the region count above two.",
        "cluster_type": "node-based",
        "region": "ap-southeast-2",
        "engine": "valkey",
        "engine_version": "8.0",
        "node_type": "cache.t4g.micro",
        "num_shards": 1,
        "num_replicas_per_shard": 1,
        "load": "idle",
        "cpu_base": 0.4,
        "hit_rate": None,  # no traffic -> undefined, not 0%
        "tls_enabled": True,
        "encryption_at_rest": True,
        "auth_mode": "AUTH-token",
        "multi_az": False,
        "automatic_failover": False,
        "snapshot_retention_days": 1,
        "maxmemory_policy": "allkeys-lru",
        "tags": {"Environment": "development", "Owner": "data-eng"},
    },
    {
        "cluster_id": "prod-fanout-relay",
        "purpose": "Network-bound: CPU and memory both in the Low bands while "
                   "the network axis sits above 80% of baseline. The one "
                   "combination where the two plausible verdicts are opposites "
                   "-- OVER-PROVISIONED says scale the node down, NETWORK-BOUND "
                   "says scale it up, and scaling down cuts the very bandwidth "
                   "allowance the cluster is exhausting. The boundary was fixed "
                   "with a unit test and never rendered end-to-end (D13). A "
                   "pub/sub relay is the realistic shape: tiny values, enormous "
                   "fan-out, so bytes/sec saturates long before CPU does.",
        "cluster_type": "node-based",
        "region": "us-east-1",
        "engine": "valkey",
        "engine_version": "8.0",
        "node_type": "cache.m7g.large",
        "num_shards": 1,
        "num_replicas_per_shard": 1,
        # The three axes, each aimed at its own band deliberately:
        #   CPU     p95 of EngineCPUUtilization/Maximum, Low below 20
        #   Memory  max of DatabaseMemoryUsagePercentage, Low below 30
        #   Network p95 of NetworkBaselineUsage{In,Out}Percentage/Average,
        #           High above 80
        #
        # net_pct feeds the In Average at net_pct * 0.8 and the Out Average at
        # net_pct * 1.1, and the model takes whichever direction is busier. Out
        # is the binding one here, which is also the honest shape for a fan-out
        # relay: one publish in, many deliveries out. 80.0 puts Out's p95 near
        # 86% -- clear of the 80 band edge -- while its maximum stays near 95%,
        # so the series never pegs at the fixture's 100 ceiling. A flat line
        # sitting exactly on 100 for two weeks would read as a clamped series
        # rather than a measured one.
        "load": "moderate",
        "cpu_base": 11.0,
        "mem_pct": 14.0,
        "net_pct": 80.0,
        # One publish in, many deliveries out -- so egress, not command rate, is
        # what this cluster runs out of. The command counts stay at the moderate
        # profile (a relay's CPU work is small) while the out-direction byte
        # counters are scaled, which is what makes "low CPU, high network" a
        # coherent cluster rather than three numbers that disagree.
        #
        # Bounded deliberately at 14x. This fixture sets its byte counters and
        # its baseline-percentage series independently -- see the note in
        # _node_metrics -- so a factor chosen to make the bytes literally agree
        # with 88% of a cache.m7g.large's allowance would be ~11,000x, and would
        # put this one cluster four orders of magnitude above its peers in the
        # fleet-wide chart, flattening every other line to the axis. Ordering is
        # what the chart needs to be right about here; the magnitudes are the
        # fixture's existing convention.
        "net_out_scale": 14.0,
        "hit_rate": 0.921,
        "tls_enabled": True,
        "encryption_at_rest": True,
        "auth_mode": "RBAC",
        "multi_az": True,
        "automatic_failover": True,
        "snapshot_retention_days": 7,
        "maxmemory_policy": "allkeys-lru",
        "tags": {"Environment": "production", "Owner": "messaging-team",
                 "Application": "fanout-relay"},
    },
    {
        "cluster_id": "prod-serverless-events",
        "purpose": "Serverless, and busy. The live fleet's serverless cache "
                   "was empty and idle, so no report path was ever exercised "
                   "with real ECPU consumption or storage.",
        "cluster_type": "serverless",
        "region": "us-east-1",
        "engine": "valkey",
        "engine_version": "8",
        "load": "busy",
        "hit_rate": 0.955,
        "tls_enabled": True,
        "encryption_at_rest": True,
        "auth_mode": "RBAC",
        "multi_az": True,
        "snapshot_retention_days": 7,
        "tags": {"Environment": "production", "Owner": "events-team",
                 "Application": "event-bus"},
        # Both ceilings are set, which is what makes the two derived percentage
        # series exist at all: ECPUUtilizationPercent needs ecpu_per_second and
        # BytesUsedForCachePercent needs data_storage. The rate limit is sized so
        # this cache runs at a meaningful fraction of it (~3,100 req/s at ~2.4
        # ECPUs each is ~7,440 ECPU/s, so ~62% of 12,000) rather than a rounding
        # error -- a fixture pinned near 0% cannot tell a working percentage from
        # a broken one.
        "cache_usage_limits": {
            "data_storage": {"maximum": 50, "unit": "GB"},
            "ecpu_per_second": {"maximum": 12000},
        },
    },
]


# ---------------------------------------------------------------------------
# Series synthesis
# ---------------------------------------------------------------------------


def _timestamps() -> list[str]:
    """5-minute ISO-8601 timestamps across the fixed window."""
    return [
        (EPOCH + datetime.timedelta(seconds=i * RESOLUTION_SECONDS))
        .strftime("%Y-%m-%dT%H:%M:%SZ")
        for i in range(POINTS)
    ]


# The diurnal multiplier swings 0.325 either side of this mean. Named so the
# steadiness compression below can pivot the swing around the mean without
# moving the level -- a compressed series has the same average, just a tighter
# spread, which is what "steady" means.
_DIURNAL_MEAN = 0.675
_DIURNAL_AMPLITUDE = 0.325


def _diurnal(i: int, scale: float = 1.0) -> float:
    """Traffic shape over a day, peaking mid-afternoon.

    Returns a multiplier in roughly 0.35-1.0. Real cache traffic is not flat,
    and a flat fixture would let a trend or seasonality model look correct
    while doing nothing.

    ``scale`` compresses the diurnal swing around its mean without changing the
    mean: scale 1.0 is the full swing, 0.5 is half. Because the compressed
    multiplier is an affine function of the full one, a compressed series stays
    perfectly correlated (Pearson r) with a full-amplitude one -- only the
    independent jitter decorrelates them. That is what lets one cluster read as
    *steady* (a tight CPU coefficient of variation) while its CPU/latency/
    connection correlations, which are scale-invariant, still fire. See
    ``SteadinessModel`` and the ``cpu_diurnal_scale`` spec key.
    """
    hour = (i * RESOLUTION_SECONDS / 3600.0) % 24.0
    # Peak near 15:00, trough near 03:00.
    return _DIURNAL_MEAN + scale * _DIURNAL_AMPLITUDE * math.sin(
        (hour - 9.0) / 24.0 * 2 * math.pi)


def _series(rng: random.Random, base: float, load: str, jitter: float = 0.08,
            shape: bool = True, floor: float = 0.0,
            ceiling: float | None = None,
            diurnal_scale: float = 1.0) -> list[float]:
    """A plausible metric series: base level, diurnal shape, bounded noise.

    Args:
        rng: Seeded RNG. Never the module-level ``random``, so parallel
            generation stays reproducible.
        base: Level the series varies around.
        load: Load profile name; "idle" flattens the diurnal shape since an
            unused cluster has no traffic pattern to follow.
        jitter: Relative noise amplitude.
        shape: Whether to apply the diurnal multiplier.
        floor: Hard lower bound (metrics are rarely negative).
        ceiling: Hard upper bound, e.g. 100 for a percentage.

    Returns:
        POINTS floats rounded to 4dp.
    """
    out = []
    for i in range(POINTS):
        mult = (_diurnal(i, diurnal_scale) if (shape and load != "idle")
                else 1.0)
        value = base * mult * (1.0 + rng.uniform(-jitter, jitter))
        value = max(floor, value)
        if ceiling is not None:
            value = min(ceiling, value)
        out.append(round(value, 4))
    return out


def _zeros() -> list[float]:
    """A flat-zero series. Distinct from an empty one, which means no data."""
    return [0.0] * POINTS


def _node_metrics(rng: random.Random, spec: dict, is_primary: bool,
                  hot_shard: bool) -> dict:
    """Build the full node metric block for one cache node.

    Args:
        rng: Seeded RNG.
        spec: Fleet entry.
        is_primary: Primaries carry the writes, so replicas show lower CPU and
            no write latency.
        hot_shard: One shard of the hot cluster runs hotter than its siblings,
            which is what makes shard imbalance visible at all.

    Returns:
        Mapping of metric name -> statistic -> series, matching
        NODE_METRIC_STATS exactly.
    """
    load = spec["load"]
    idle = load == "idle"

    cpu = spec["cpu_base"] * (1.0 if is_primary else 0.72)
    if hot_shard:
        cpu *= 1.18
    ceiling = 100.0

    # A steady cluster's CPU has a tight coefficient of variation. Compressing
    # only the CPU diurnal swing (not the traffic that drives connections and
    # latency) is what pushes cpu_cov below SteadinessModel's 0.30 band while
    # leaving the CPU<->connection/latency correlations intact -- they key on
    # shape, and an affine amplitude change does not move Pearson r. Default 1.0
    # leaves every other cluster exactly as it was, so only the steady cluster's
    # bytes change and the RNG stream (and thus every other cluster) is
    # untouched.
    cpu_scale = spec.get("cpu_diurnal_scale", 1.0)

    # Traffic volume scales with how busy the cluster is.
    ops = {"busy": 4200.0, "hot": 5600.0, "moderate": 900.0, "idle": 0.0}[load]
    if not is_primary:
        ops *= 0.45  # replicas serve reads only

    hit_rate = spec.get("hit_rate")
    if idle or ops == 0 or hit_rate is None:
        # Zero traffic: hits and misses are both zero, and the hit rate is
        # UNDEFINED rather than 0%. Reporting 0/0 as a critical 0% hit rate was
        # one of the four false CRITICALs, so the fixture reproduces the input
        # condition to keep _drop_undefined_hit_rate() honest.
        hits = _zeros()
        misses = _zeros()
        hit_series: list[float] = []
    else:
        hits = _series(rng, ops * 300 * hit_rate, load, floor=0.0)
        misses = _series(rng, ops * 300 * (1.0 - hit_rate), load, floor=0.0)
        hit_series = _series(rng, hit_rate * 100, load, jitter=0.02,
                             shape=False, ceiling=100.0)

    # Memory and network levels are per-cluster, not per-load-profile. With
    # shape=False and 3% jitter over 4032 points the maximum converges to
    # base * 1.03, so two clusters sharing a load label would report the same
    # memory_max to two decimal places no matter what the RNG did -- which
    # reads as a copy-paste bug in an example fleet and would hide a
    # cluster-keying error in any stage downstream.
    mem_pct = spec.get(
        "mem_pct",
        {"busy": 58.0, "hot": 87.0, "moderate": 31.0, "idle": 2.0}[load])
    # r7g.large is ~13.07 GiB usable; scale bytes from the percentage so the two
    # metrics agree. A fixture whose memory percent and byte count disagree
    # would let a unit bug pass.
    node_bytes = {"cache.r7g.xlarge": 26.32, "cache.r7g.large": 13.07,
                  "cache.m7g.large": 6.38, "cache.t4g.medium": 3.09,
                  "cache.t4g.micro": 0.5}.get(spec["node_type"], 6.38)
    total_bytes = node_bytes * (1024 ** 3)

    evictions = (_series(rng, 145.0, load, jitter=0.4) if load == "hot"
                 else _zeros())
    net_in = _series(rng, ops * 1150.0, load, floor=0.0) if ops else _zeros()
    # Egress scaling for a fan-out shape: same commands in, far more bytes out.
    # Note what this fixture does NOT claim -- the byte counters and the
    # baseline-percentage series below are set independently, so they agree in
    # direction and ordering but not arithmetically. The pipeline never derives
    # one from the other (it grades the percentage series and charts the byte
    # series), so nothing downstream can be misled by that; a reader comparing
    # the two by hand should know it, which is why it is written here.
    net_out_scale = spec.get("net_out_scale", 1.0)

    metrics = {
        "EngineCPUUtilization": {
            "Average": _series(rng, cpu * 0.86, load, ceiling=ceiling,
                               diurnal_scale=cpu_scale),
            "Maximum": _series(rng, cpu, load, ceiling=ceiling,
                               diurnal_scale=cpu_scale),
        },
        "CPUUtilization": {
            "Average": _series(rng, cpu * 0.52, load, ceiling=ceiling,
                               diurnal_scale=cpu_scale),
        },
        "DatabaseMemoryUsagePercentage": {
            "Maximum": _series(rng, mem_pct, load, jitter=0.03, shape=False,
                               ceiling=ceiling),
        },
        "DatabaseCapacityUsagePercentage": {
            "Maximum": _series(rng, mem_pct * 0.97, load, jitter=0.03,
                               shape=False, ceiling=ceiling),
        },
        "ReplicationLag": {
            "Maximum": (_zeros() if is_primary
                        else _series(rng, 0.28 if load == "hot" else 0.04,
                                     load, jitter=0.5)),
        },
        "SaveInProgress": {"Maximum": _zeros()},
        "TrafficManagementActive": {"Maximum": _zeros()},
        "CacheHitRate": {"Average": hit_series},
        "CacheHits": {"Sum": hits},
        "CacheMisses": {"Sum": misses},
        "Evictions": {"Sum": evictions},
        "CurrConnections": {
            "Maximum": _series(rng, {"busy": 340.0, "hot": 520.0,
                                     "moderate": 62.0, "idle": 4.0}[load],
                               load, jitter=0.15),
        },
        "NewConnections": {
            "Sum": _series(rng, {"busy": 210.0, "hot": 390.0,
                                 "moderate": 45.0, "idle": 1.0}[load], load),
        },
        "NetworkBytesIn": {"Sum": net_in},
        "NetworkBytesOut": {
            "Sum": (_series(rng, ops * 2600.0 * net_out_scale, load)
                    if ops else _zeros()),
        },
        "BytesUsedForCache": {
            "Maximum": _series(rng, total_bytes * mem_pct / 100.0, load,
                               jitter=0.03, shape=False),
        },
        "CurrItems": {
            "Maximum": _series(rng, {"busy": 2_450_000.0, "hot": 3_900_000.0,
                                     "moderate": 384_000.0, "idle": 0.0}[load],
                               load, jitter=0.02, shape=False),
        },
        "SuccessfulReadRequestLatency": {
            "Average": (_series(rng, 0.72 if load == "hot" else 0.24, load,
                                jitter=0.2) if ops else []),
        },
        "SuccessfulWriteRequestLatency": {
            "Average": (_series(rng, 0.91 if load == "hot" else 0.31, load,
                                jitter=0.2) if (ops and is_primary) else []),
        },
        "FreeableMemory": {
            "Minimum": _series(rng, total_bytes * (1 - mem_pct / 100.0), load,
                               jitter=0.03, shape=False),
        },
        "SwapUsage": {"Maximum": _zeros()},
        "MemoryFragmentationRatio": {
            "Maximum": _series(rng, 1.14, load, jitter=0.05, shape=False),
        },
        "CurrVolatileItems": {
            # allkeys-* clusters have no TTL requirement; volatile-* ones must
            # carry TTL-bearing keys or writes eventually fail with OOM. The
            # fixture keeps them consistent so the ttl_risk callout only fires
            # where it should.
            "Maximum": (_zeros() if spec.get("maxmemory_policy",
                                             "").startswith("allkeys")
                        else _series(rng, {"busy": 2_400_000.0,
                                           "hot": 3_850_000.0,
                                           "moderate": 380_000.0,
                                           "idle": 0.0}[load],
                                     load, jitter=0.02, shape=False)),
        },
        "Reclaimed": {"Sum": evictions},
        "BlockedConnections": {"Maximum": _zeros()},
        "RejectedConnections": {"Sum": _zeros()},
        "ErrorCount": {"Sum": _zeros()},
        "DatabaseMemoryUsageCountedForEvictPercentage": {
            "Maximum": _series(rng, mem_pct * 0.94, load, jitter=0.03,
                               shape=False, ceiling=ceiling),
        },
    }

    # Network allowance and baseline metrics: present, mostly zero. The hot
    # cluster brushes its baseline so the network section is not uniformly flat.
    net_pct = spec.get(
        "net_pct",
        {"busy": 12.0, "hot": 41.0, "moderate": 3.0, "idle": 0.1}[load])
    metrics.update({
        "NetworkBandwidthInAllowanceExceeded": {"Maximum": _zeros()},
        "NetworkBandwidthOutAllowanceExceeded": {"Maximum": _zeros()},
        "NetworkConntrackAllowanceExceeded": {"Maximum": _zeros()},
        "NetworkPacketsPerSecondAllowanceExceeded": {"Maximum": _zeros()},
        "NetworkBaselineUsageInPercentage": {
            "Average": _series(rng, net_pct * 0.8, load, ceiling=ceiling),
            "Maximum": _series(rng, net_pct, load, ceiling=ceiling),
        },
        "NetworkBaselineUsageOutPercentage": {
            "Average": _series(rng, net_pct * 1.1, load, ceiling=ceiling),
            "Maximum": _series(rng, net_pct * 1.4, load, ceiling=ceiling),
        },
        "NetworkBaselineMaxUsageInPercentage": {
            "Maximum": _series(rng, net_pct * 1.2, load, ceiling=ceiling),
        },
        "NetworkBaselineMaxUsageOutPercentage": {
            "Maximum": _series(rng, net_pct * 1.6, load, ceiling=ceiling),
        },
        "NetworkMaxBytesIn": {"Maximum": _series(rng, ops * 1400.0, load)
                              if ops else _zeros()},
        "NetworkMaxBytesOut": {
            "Maximum": (_series(rng, ops * 3100.0 * net_out_scale, load)
                        if ops else _zeros()),
        },
    })

    # Command-type breakdown. Reads dominate a cache, as they should.
    cmd_base = ops * 300 if ops else 0.0
    for name, share in (("GetTypeCmds", 0.78), ("SetTypeCmds", 0.22),
                        ("StringBasedCmds", 0.61), ("HashBasedCmds", 0.24),
                        ("SortedSetBasedCmds", 0.08), ("ListBasedCmds", 0.05),
                        ("SetBasedCmds", 0.02)):
        metrics[name] = {"Sum": (_series(rng, cmd_base * share, load)
                                 if cmd_base else _zeros())}
    for name in ("StreamBasedCmds", "PubSubBasedCmds", "JsonBasedCmds",
                 "JsonBasedGetCmds", "JsonBasedSetCmds"):
        metrics[name] = {"Sum": _zeros()}

    # Auth/authorization failure counters: zero everywhere. A non-zero value
    # would imply a security incident the rest of the fixture does not tell a
    # story about.
    for name in ("AuthenticationFailures", "KeyAuthorizationFailures",
                 "CommandAuthorizationFailures", "IamAuthenticationExpirations",
                 "IamAuthenticationThrottling"):
        metrics[name] = {"Sum": _zeros()}

    # Assert the shape rather than trusting it: a missing metric here becomes a
    # Stage 3 gap that is tedious to trace back.
    missing = set(NODE_METRIC_STATS) - set(metrics)
    if missing:
        raise AssertionError(f"node metrics missing: {sorted(missing)}")
    extra = set(metrics) - set(NODE_METRIC_STATS)
    if extra:
        raise AssertionError(f"node metrics not in real output: {sorted(extra)}")
    for name, stats in NODE_METRIC_STATS.items():
        if sorted(metrics[name]) != sorted(stats):
            raise AssertionError(
                f"{name}: statistics {sorted(metrics[name])} != real "
                f"output's {sorted(stats)}"
            )
    return metrics


def _serverless_metrics(rng: random.Random, spec: dict) -> dict:
    """Build the serverless metric block.

    Serverless publishes a different metric set: ECPU consumption instead of
    CPU percentage, no per-node anything, no replication lag. Getting this wrong
    is how the ``ServerlessCacheName`` dimension bug survived.
    """
    load = spec["load"]
    ops = 3100.0  # requests per second
    hit_rate = spec["hit_rate"]

    # ECPUs are derived from the request rate, not chosen independently. Per the
    # ElastiCache pricing docs, a simple GET/SET transferring <= 1 KB consumes
    # 1 ECPU, and larger or more complex commands consume proportionally more.
    # This fixture assumes ~2.4 KB average payloads, so ~2.4 ECPUs per request.
    #
    # The metric is collected with Sum, so each datapoint is the total for the
    # 300-second period -- not a rate. A fixture that picked the ECPU number out
    # of the air would disagree with its own CacheHits/CacheMisses counts, and
    # let a rate-vs-count bug look plausible.
    ecpu_per_request = 2.4
    ecpu_per_period = ops * ecpu_per_request * RESOLUTION_SECONDS

    metrics = {
        "ElastiCacheProcessingUnits": {"Sum": _series(rng, ecpu_per_period, load)},
        "ThrottledCmds": {"Sum": _zeros()},
        "CacheHitRate": {"Average": _series(rng, hit_rate * 100, load,
                                            jitter=0.02, shape=False,
                                            ceiling=100.0)},
        "CacheHits": {"Sum": _series(rng, ops * 300 * hit_rate, load)},
        "CacheMisses": {"Sum": _series(rng, ops * 300 * (1 - hit_rate), load)},
        "Evictions": {"Sum": _zeros()},
        "CurrConnections": {"Maximum": _series(rng, 180.0, load, jitter=0.15)},
        "NewConnections": {"Sum": _series(rng, 96.0, load)},
        "NetworkBytesIn": {"Sum": _series(rng, ops * 980.0, load)},
        "NetworkBytesOut": {"Sum": _series(rng, ops * 2100.0, load)},
        # 18.4 GB stored -- a serverless cache actually holding data, which the
        # live fleet's (0 bytes) never exercised.
        "BytesUsedForCache": {"Maximum": _series(rng, 18.4 * (1024 ** 3), load,
                                                 jitter=0.04, shape=False)},
        "CurrItems": {"Maximum": _series(rng, 1_820_000.0, load, jitter=0.02,
                                         shape=False)},
        "SuccessfulReadRequestLatency": {"Average": _series(rng, 0.41, load,
                                                            jitter=0.2)},
        "SuccessfulWriteRequestLatency": {"Average": _series(rng, 0.55, load,
                                                             jitter=0.2)},
    }
    for name, share in (("GetTypeCmdsECPUs", 0.74), ("SetTypeCmdsECPUs", 0.26),
                        ("StringBasedCmdsECPUs", 0.58),
                        ("HashBasedCmdsECPUs", 0.27),
                        ("SortedSetBasedCmdsECPUs", 0.09),
                        ("ListBasedCmdsECPUs", 0.04),
                        ("SetBasedCmdsECPUs", 0.02)):
        metrics[name] = {"Sum": _series(rng, ops * 42.0 * share, load)}
    for name in ("StreamBasedCmdsECPUs", "PubSubBasedCmdsECPUs"):
        metrics[name] = {"Sum": _zeros()}
    for name in ("AuthenticationFailures", "KeyAuthorizationFailures",
                 "CommandAuthorizationFailures", "IamAuthenticationExpirations",
                 "IamAuthenticationThrottling"):
        metrics[name] = {"Sum": _zeros()}

    missing = set(SERVERLESS_METRIC_STATS) - set(metrics)
    if missing:
        raise AssertionError(f"serverless metrics missing: {sorted(missing)}")
    extra = set(metrics) - set(SERVERLESS_METRIC_STATS)
    if extra:
        raise AssertionError(f"serverless metrics unexpected: {sorted(extra)}")
    return metrics


# ---------------------------------------------------------------------------
# Documents
# ---------------------------------------------------------------------------


def _node_ids(spec: dict) -> list[str]:
    """Node ids in ElastiCache's own naming scheme.

    ``<cluster>-000<shard>-00<member>`` for a replication group, matching what
    ``describe-replication-groups`` returns.
    """
    ids = []
    for shard in range(1, spec["num_shards"] + 1):
        for member in range(1, spec["num_replicas_per_shard"] + 2):
            ids.append(f"{spec['cluster_id']}-{shard:04d}-{member:03d}")
    return ids


def build_inventory() -> dict:
    """inventory.json in the exact shape discover_inventory.py writes.

    Note ``clusters`` is a LIST. The removed Stage 4 read it as a dict and
    crashed on every real run while 153 fixture tests passed, so this shape is
    load-bearing and Stage 3.5 asserts it.
    """
    clusters = []
    for spec in FLEET:
        region = spec["region"]
        azs = [f"{region}{s}" for s in ("a", "b", "c")]
        common = {
            "cluster_id": spec["cluster_id"],
            "cluster_type": spec["cluster_type"],
            "region": region,
            "engine": spec["engine"],
            "engine_version": spec["engine_version"],
            "status": "available",
            "tls_enabled": spec["tls_enabled"],
            "auth_mode": spec["auth_mode"],
            "encryption_at_rest": spec["encryption_at_rest"],
            "multi_az": spec["multi_az"],
            "snapshot_retention_days": spec["snapshot_retention_days"],
            "tags": dict(spec["tags"]),
            "security_groups": [{
                # blake2b, not hash(): the builtin is salted per process
                # (PYTHONHASHSEED), so it produced a different group_id on every
                # run and broke the byte-for-byte reproducibility this fixture
                # exists to provide.
                "group_id": "sg-example" + hashlib.blake2b(
                    spec["cluster_id"].encode(), digest_size=4
                ).hexdigest(),
                "permissive_rules": list(spec.get("permissive_rules") or []),
            }],
            "errors": [],
        }

        if spec["cluster_type"] == "serverless":
            arn = (f"arn:aws:elasticache:{region}:{ACCOUNT_ID}:"
                   f"serverlesscache:{spec['cluster_id']}")
            clusters.append({
                **common,
                "arn": arn,
                "cache_usage_limits": spec["cache_usage_limits"],
                "endpoints": {
                    "primary": f"{spec['cluster_id']}.serverless.example:6379",
                    "reader": f"{spec['cluster_id']}.serverless.example:6380",
                },
            })
            continue

        arn = (f"arn:aws:elasticache:{region}:{ACCOUNT_ID}:"
               f"replicationgroup:{spec['cluster_id']}")
        node_ids = _node_ids(spec)
        clusters.append({
            **common,
            "arn": arn,
            "node_type": spec["node_type"],
            "num_shards": spec["num_shards"],
            "num_replicas_per_shard": spec["num_replicas_per_shard"],
            "total_nodes": len(node_ids),
            "members": [
                {"cache_cluster_id": nid,
                 "role": "primary" if nid.endswith("001") else "replica"}
                for nid in node_ids
            ],
            "cluster_mode_enabled": spec["num_shards"] > 1,
            "automatic_failover": spec["automatic_failover"],
            # Log delivery (OE-06). Absent from a spec means the empty list Stage
            # 1 produces for a group with no LogDeliveryConfigurations, so both
            # OE-06a and OE-06b fire; a spec that lists an active log silences the
            # matching check. Serverless (below) omits the field entirely, exactly
            # as discover_inventory.py does.
            "log_delivery": [dict(e) for e in spec.get("log_delivery") or []],
            "replication_group_log_delivery_enabled":
                spec.get("replication_group_log_delivery_enabled", False),
            "parameter_group": f"default.{spec['engine']}{spec['engine_version'][0]}",
            "parameters": {
                "activedefrag": "no",
                "maxmemory-policy": spec["maxmemory_policy"],
                "reserved-memory-percent": "25",
                "tcp-keepalive": "300",
                "timeout": "0",
            },
            "subnet_group": {
                "name": f"example-subnets-{region}",
                "subnets": [f"subnet-example{i:08d}" for i in range(1, 4)],
                "availability_zones": azs,
                "az_count": len(azs),
            },
            "availability_zones": azs,
            "endpoints": {
                "primary": f"{spec['cluster_id']}.example.cache:6379",
                "reader": f"{spec['cluster_id']}-ro.example.cache:6379",
            },
        })

    regions = sorted({c["region"] for c in clusters})
    return {
        "metadata": {
            "pipeline_version": PIPELINE_VERSION,
            "account_id": ACCOUNT_ID,
            "caller_identity_arn":
                f"arn:aws:iam::{ACCOUNT_ID}:role/ExampleReadOnlyRole",
            "regions_scanned": regions,
            "scan_timestamp": EPOCH.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "total_clusters": len(clusters),
            "node_based_count": sum(1 for c in clusters
                                    if c["cluster_type"] == "node-based"),
            "serverless_count": sum(1 for c in clusters
                                    if c["cluster_type"] == "serverless"),
            "scan_duration_seconds": 11.4,
            "errors_count": 0,
            "synthetic": True,
            "synthetic_note": (
                "Generated by scripts/make_example_fleet.py. Not a real AWS "
                "account; account id is the AWS documentation placeholder."
            ),
        },
        "clusters": clusters,
    }


def build_metrics(rng: random.Random) -> dict:
    """metrics.json in the exact shape fetch_metrics.py writes."""
    timestamps = _timestamps()
    clusters: dict = {}
    total_datapoints = 0

    for spec in FLEET:
        if spec["cluster_type"] == "serverless":
            metrics = _serverless_metrics(rng, spec)
            total_datapoints += sum(len(s) for m in metrics.values()
                                    for s in m.values())
            clusters[spec["cluster_id"]] = {
                "cluster_type": "serverless",
                "region": spec["region"],
                "timestamps_5min": timestamps,
                "metrics": metrics,
                # Serverless publishes no per-node latency percentiles.
                "latency_detail": {},
                "errors": [],
            }
            continue

        node_ids = _node_ids(spec)
        nodes: dict = {}
        latency: dict = {}
        for nid in node_ids:
            is_primary = nid.endswith("001")
            # Shard 1 of the hot cluster is the hot shard, so imbalance exists
            # to be found rather than being asserted.
            hot_shard = spec["load"] == "hot" and "-0001-" in nid
            node_metrics = _node_metrics(rng, spec, is_primary, hot_shard)
            nodes[nid] = node_metrics
            total_datapoints += sum(len(s) for m in node_metrics.values()
                                    for s in m.values())
            has_traffic = spec["load"] != "idle"
            latency[nid] = {
                "SuccessfulReadRequestLatency": {
                    p: (_series(rng, v, spec["load"], jitter=0.15)
                        if has_traffic else [])
                    for p, v in (("p50", 0.22), ("p95", 0.61), ("p99", 1.04))
                },
                "SuccessfulWriteRequestLatency": {
                    p: (_series(rng, v, spec["load"], jitter=0.15)
                        if (has_traffic and is_primary) else [])
                    for p, v in (("p50", 0.29), ("p95", 0.78), ("p99", 1.31))
                },
            }
        clusters[spec["cluster_id"]] = {
            "cluster_type": "node-based",
            "region": spec["region"],
            "timestamps_5min": timestamps,
            "nodes": nodes,
            "latency_detail": latency,
            "errors": [],
        }

    return {
        "metadata": {
            "pipeline_version": PIPELINE_VERSION,
            "source_inventory": "examples/inventory.json",
            "collection_timestamp": (EPOCH + datetime.timedelta(days=DAYS))
                .strftime("%Y-%m-%dT%H:%M:%SZ"),
            "period_start": EPOCH.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "period_end": (EPOCH + datetime.timedelta(days=DAYS))
                .strftime("%Y-%m-%dT%H:%M:%SZ"),
            "resolution_seconds": RESOLUTION_SECONDS,
            "latency_resolution_seconds": 60,
            "latency_window_hours": 24,
            "clusters_processed": len(FLEET),
            "metrics_collected": len(NODE_METRIC_STATS),
            "total_datapoints": total_datapoints,
            "collection_duration_seconds": 52.8,
            "errors_count": 0,
            "synthetic": True,
        },
        "clusters": clusters,
        "cost": _build_cost(),
    }


def _build_cost() -> dict:
    """Cost Explorer daily totals, with usage types across all three regions.

    Region prefixes matter here: ``EU-`` for eu-west-1 (not EUW1 -- that region
    predates the numbering convention) and ``APS2-`` for ap-southeast-2. A
    fixture using only unprefixed or USE1 types would not have caught the
    six-region bug in ``_derive_facts``.

    us-east-1 is the home region and its node hours appear unprefixed, which is
    how Cost Explorer actually reports them for the account's own region.
    """
    per_day = {
        "NodeUsage:cache.r7g.xlarge": 32.74,   # prod-session-store, 9 nodes
        "NodeUsage:cache.r7g.large": 10.91,    # prod-api-cache, 4 nodes
        "NodeUsage:cache.t4g.medium": 1.64,    # staging-redis-legacy
        "USE1-ServerlessCache:Valkey-ECPU": 14.22,
        "USE1-ServerlessCache:Valkey-CachedData": 3.68,
        "EU-NodeUsage:cache.m7g.large": 8.18,  # prod-eu-catalog, 3 nodes
        "APS2-NodeUsage:cache.t4g.micro": 0.61,  # dev-scratch-idle
        # prod-fanout-relay, 2 nodes. Unprefixed and un-suffixed because it is
        # the same node type as prod-eu-catalog in the account's home region:
        # Cost Explorer would report both under one usage type per region, so a
        # separate key here is the region prefix doing the separating, not the
        # cluster. The DataTransfer line is what makes this cluster's bill look
        # like its bottleneck -- a network-bound cluster whose cost breakdown
        # showed only node hours would contradict its own classification.
        "NodeUsage:cache.m7g.large": 5.45,
        "DataTransfer-Regional-Bytes": 11.87,
    }
    daily = []
    for day in range(DAYS):
        date = (EPOCH + datetime.timedelta(days=day)).strftime("%Y-%m-%d")
        daily.append({
            "date": date,
            "total_usd": round(sum(per_day.values()), 2),
            "by_usage_type": dict(per_day),
        })
    return {"daily": daily}


# ---------------------------------------------------------------------------
# Pricing (Phase 7c) -- SYNTHETIC and deterministic
# ---------------------------------------------------------------------------
#
# price_calculator.py is live and external (it fetches the AWS Price List API),
# so it must never run inside this offline generator -- the same reproducibility
# rule that keeps SEC-06's EOL dates vendored rather than fetched. Instead this
# emits a synthetic pricing.json from a fixed node-type table, in the exact
# schema the real pricing.json carries, so the report's savings layer can be
# exercised offline. Every figure here is invented and obviously round; nothing
# is a real AWS rate.

# Synthetic $/node/month, fixed. Not real rates -- see above.
NODE_MONTHLY = {
    "cache.r7g.xlarge": 372.30,
    "cache.r7g.large": 186.15,
    "cache.m7g.large": 124.10,
    "cache.t4g.medium": 47.30,
    "cache.t4g.micro": 11.68,
}

# Savings ratios (policy, not price -- multipliers, allowed since Phase 2).
_VALKEY_RATIO = 0.20      # Valkey ~20% below Redis on-demand
_DSP_RATIO = 0.20         # Database Savings Plan: 20% off Valkey instances
_RESERVED_1YR_RATIO = 0.32
_RESERVED_3YR_RATIO = 0.48

_PRICING_GUARD = (
    "Valkey rates >40% below the same node's Redis rate are rejected as "
    "anomalous and omitted (a known price_calculator SKU-match bug); such "
    "levers carry no dollar figure. These figures are synthetic and "
    "deterministic -- see make_example_fleet.py."
)


def _node_generation(node_type: str) -> int | None:
    """Instance generation digit from a node type, e.g. cache.r7g.large -> 7."""
    match = re.search(r"cache\.[a-z]+(\d+)", node_type or "")
    return int(match.group(1)) if match else None


def _is_gen7_plus(node_type: str) -> bool:
    """Gen7+ Graviton3/4 (m7g/r7g/m8g/r8g) -- the Database Savings Plan gate."""
    gen = _node_generation(node_type)
    family = (node_type.split(".")[1] if node_type and "." in node_type else "")
    return gen is not None and gen >= 7 and family.endswith("g")


def _is_intel_m5_r5(node_type: str) -> bool:
    """Intel m5/r5 -- the only case with a real Graviton3 on-demand target."""
    family = (node_type.split(".")[1] if node_type and "." in node_type else "")
    return family in ("m5", "r5")


def _build_pricing() -> dict:
    """pricing.json in the schema the live price_calculator output uses.

    Per cluster: current on-demand monthly, plus the gated savings levers as a
    list of options. The renderer joins these against each cluster's
    classification and steadiness -- an IDLE cluster shows only decommission, an
    active+steady one shows commitments as sound, an active+spiky one flags them
    as risky. This function only prices; it does no gating.
    """
    clusters: dict = {}
    for spec in FLEET:
        cid = spec["cluster_id"]
        engine = spec["engine"]

        if spec["cluster_type"] == "serverless":
            clusters[cid] = {
                "node_type": None,
                "nodes": None,
                "engine": engine,
                "current_monthly": None,
                "options": [],
                "note": (
                    "Serverless -- priced on ECPU + data stored, not node "
                    "hours, so no node-level commitment applies. A Database "
                    "Savings Plan covers ElastiCache Serverless for Valkey at "
                    "30%."),
            }
            continue

        node_type = spec["node_type"]
        nodes = spec["num_shards"] * (spec["num_replicas_per_shard"] + 1)
        current = round(NODE_MONTHLY[node_type] * nodes, 2)
        gen7 = _is_gen7_plus(node_type)
        options: list[dict] = []

        # Engine lever: Valkey is ~20% below Redis on-demand, for Redis clusters
        # only. A Valkey cluster is already there, so it gets the Gen7+-gated
        # Database Savings Plan lever instead.
        if engine in ("redis", "redis-oss"):
            options.append({
                "key": "valkey",
                "label": "Migrate to Valkey (on-demand)",
                "monthly": round(current * (1 - _VALKEY_RATIO), 2),
                "saving_monthly": round(current * _VALKEY_RATIO, 2),
                "eligible": True,
                "commitment": False,
            })
        elif gen7:
            options.append({
                "key": "dsp",
                "label": "Database Savings Plan (Valkey, Gen7+, 1yr)",
                "monthly": round(current * (1 - _DSP_RATIO), 2),
                "saving_monthly": round(current * _DSP_RATIO, 2),
                "eligible": True,
                "commitment": True,
                "ratio": _DSP_RATIO,
            })
        else:
            options.append({
                "key": "dsp",
                "label": "Database Savings Plan (Valkey, Gen7+, 1yr)",
                "monthly": None,
                "saving_monthly": None,
                "eligible": False,
                "commitment": True,
                "ratio": _DSP_RATIO,
                "note": (
                    "Requires a Gen7+ node (m7g/r7g/m8g/r8g); this node is an "
                    "earlier generation, so migrate the node type first."),
            })

        # Reserved Nodes -- a commitment, for any node-based cluster.
        options.append({
            "key": "reserved_1yr",
            "label": "Reserved Nodes -- 1yr, no upfront",
            "monthly": round(current * (1 - _RESERVED_1YR_RATIO), 2),
            "saving_monthly": round(current * _RESERVED_1YR_RATIO, 2),
            "eligible": True,
            "commitment": True,
        })
        options.append({
            "key": "reserved_3yr",
            "label": "Reserved Nodes -- 3yr, no upfront",
            "monthly": round(current * (1 - _RESERVED_3YR_RATIO), 2),
            "saving_monthly": round(current * _RESERVED_3YR_RATIO, 2),
            "eligible": True,
            "commitment": True,
        })

        # Graviton -- a generation change, not a discount. Only an Intel m5/r5
        # cluster has a real Graviton3 on-demand target; everything else is
        # already Graviton or has no target, so it is not eligible and carries
        # no dollar. (No m5/r5 cluster exists in this fleet, so this branch is
        # exercised as eligible:false throughout.)
        if _is_intel_m5_r5(node_type):
            options.append({
                "key": "graviton",
                "label": "Graviton3 upgrade",
                "monthly": round(current * 0.98, 2),
                "saving_monthly": round(current * 0.02, 2),
                "eligible": True,
                "commitment": False,
                "note": ("price/performance change; also the Gen7+ gate for the "
                         "Database Savings Plan."),
            })
        else:
            options.append({
                "key": "graviton",
                "label": "Graviton upgrade",
                "monthly": None,
                "saving_monthly": None,
                "eligible": False,
                "commitment": False,
                "note": ("Already on Graviton, or no Graviton on-demand target "
                         "for this node type; Graviton is price/performance, "
                         "not an on-demand discount."),
            })

        clusters[cid] = {
            "node_type": node_type,
            "nodes": nodes,
            "engine": engine,
            "generation": _node_generation(node_type),
            "current_monthly": current,
            "options": options,
        }

    return {
        "metadata": {
            "source": "synthetic (make_example_fleet.py)",
            "region": "us-east-1",
            "retrieved_at": REVIEW_DATE.isoformat(),
            "guard": _PRICING_GUARD,
        },
        "clusters": clusters,
    }


# ---------------------------------------------------------------------------
# Driving the real stages
# ---------------------------------------------------------------------------


def _atomic_write(path: Path, payload: dict) -> None:
    """Write JSON atomically, so a crash cannot leave a half-file behind."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(tmp, str(path))
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def _write_metrics_sharded(out: Path, metrics: dict) -> Path:
    """Write the example metrics in the sharded on-disk format (Phase 9).

    One shard per reviewable unit (replication group / serverless cache) at
    ``out/metrics/<cluster_id>.json``, plus a KB-scale manifest at
    ``out/metrics.json`` carrying ``metadata``, optional ``cost``, the
    ``clusters`` id list and the ``shards`` map and no series -- the same layout
    ``fetch_metrics.py`` now writes. Returns the manifest path.
    """
    clusters = metrics.get("clusters") or {}
    shard_dir = out / "metrics"
    cluster_ids = sorted(clusters.keys())
    shards: dict = {}
    for cid in cluster_ids:
        _atomic_write(shard_dir / f"{cid}.json", clusters[cid])
        shards[cid] = f"metrics/{cid}.json"

    manifest: dict = {"metadata": metrics.get("metadata") or {}}
    if "cost" in metrics:
        manifest["cost"] = metrics["cost"]
    manifest["clusters"] = cluster_ids
    manifest["shards"] = shards

    manifest_path = out / "metrics.json"
    _atomic_write(manifest_path, manifest)
    return manifest_path


def _run_stage(name: str, cmd: list[str]) -> None:
    """Run a real pipeline stage, failing loudly on a non-zero exit."""
    logger.info("Running %s", name)
    # Safe: cmd is a list (no shell=True) of this repo's own pipeline stage
    # scripts with fixed args -- not attacker-controllable, no shell interpretation.
    result = subprocess.run(  # nosemgrep: dangerous-subprocess-use-audit  # nosec B603
        cmd, capture_output=True, text=True, cwd=str(SCRIPT_DIR.parent))
    if result.returncode != 0:
        sys.stderr.write(result.stdout)
        sys.stderr.write(result.stderr)
        raise SystemExit(
            f"{name} failed on the synthetic fleet (exit {result.returncode}). "
            "The fixture is the wrong shape -- fix it here, not in the stage."
        )


def main(argv=None) -> int:
    """Generate the example fleet and run the real Stages 3 and 3.5 over it."""
    parser = argparse.ArgumentParser(
        description=(
            "Generate the synthetic example fleet: a busy, 7-cluster, "
            "3-region fixture for offline demos and regression testing. "
            "Deliberately unlike any single real fleet."
        ),
    )
    parser.add_argument("--output", default="./examples",
                        help="Directory for the generated files (default: ./examples).")
    parser.add_argument("--seed", type=int, default=20260115,
                        help="RNG seed. Fixed by default so output is reproducible.")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    out = Path(args.output).expanduser().resolve()
    rng = random.Random(args.seed)

    inventory_path = out / "inventory.json"
    metrics_path = out / "metrics.json"
    analysis_path = out / "analysis.json"
    config_path = out / "config_findings.json"
    pricing_path = out / "pricing.json"
    report_data_path = out / "report_data.json"

    _atomic_write(inventory_path, build_inventory())
    logger.info("Wrote %s (%d clusters, %d regions)", inventory_path,
                len(FLEET), len({s["region"] for s in FLEET}))

    # Phase 9 sharded format: a KB manifest at metrics.json + one shard per
    # cluster under metrics/. build_metrics still assembles the whole fleet
    # (the fixture is small); the split is a write concern.
    _write_metrics_sharded(out, build_metrics(rng))
    logger.info("Wrote %s (manifest) + %d shards under %s/", metrics_path,
                len(FLEET), (out / "metrics"))

    # Synthetic, deterministic, no network -- see _build_pricing. Independent of
    # the metric RNG, so it can be written any time.
    _atomic_write(pricing_path, _build_pricing())
    logger.info("Wrote %s", pricing_path)

    # The real stages, not reimplementations. If the synthetic metrics are the
    # wrong shape, this is where it surfaces.
    python = sys.executable
    _run_stage("Stage 3 (analyze_metrics.py)", [
        python, str(SCRIPT_DIR / "analyze_metrics.py"),
        "--metrics", str(metrics_path),
        "--inventory", str(inventory_path),
        "--output", str(analysis_path),
    ])
    _run_stage("Stage 3.5 (check_configuration.py)", [
        python, str(SCRIPT_DIR / "check_configuration.py"),
        "--inventory", str(inventory_path),
        "--output", str(config_path),
        "--as-of", REVIEW_DATE.isoformat(),
    ])
    # Phase 9a: the report-facing reduction, so the example report can be
    # rendered without the ~113 MB metrics.json. Same reducer the pipeline runs.
    _run_stage("Report data reduction (generate_html_report.py)", [
        python, str(SCRIPT_DIR / "generate_html_report.py"),
        "--metrics", str(metrics_path),
        "--emit-report-data", str(report_data_path),
    ])

    analysis = json.loads(analysis_path.read_text())
    config = json.loads(config_path.read_text())
    metric_findings = sum(len(c.get("findings") or [])
                          for c in analysis.get("clusters", {}).values())
    logger.info("")
    logger.info("Example fleet ready in %s", out)
    logger.info("  %d clusters across %s", len(FLEET),
                ", ".join(sorted({s["region"] for s in FLEET})))
    logger.info("  %d metric findings, %d configuration findings",
                metric_findings, config["metadata"]["total_findings"])
    logger.info("")
    logger.info("Render the report with no AWS credentials (Phase 9a: reads "
                "report_data.json, not the 113 MB metrics.json):")
    logger.info("  python3 scripts/generate_html_report.py \\")
    logger.info("    --inventory %s --report-data %s \\", inventory_path.name,
                report_data_path.name)
    logger.info("    --analysis %s --output report.html \\", analysis_path.name)
    logger.info("    --generated-at \"2026-01-15 12:00 UTC\"")
    return 0


if __name__ == "__main__":
    sys.exit(main())
