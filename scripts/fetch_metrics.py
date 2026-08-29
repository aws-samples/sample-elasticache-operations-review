#!/usr/bin/env python3
"""
ElastiCache Metrics Collection (CloudWatch)

Fetches 14-day historical CloudWatch metrics for all clusters in the inventory.
Uses batched GetMetricData API calls with time-window chunking for efficiency
at scale. Collects Tier 1, Tier 2, and conditional Tier 3 metrics at 5-minute
resolution, plus latency percentiles (p50/p95/p99) at 1-minute resolution for
the last 24 hours.

This is Stage 2 of the ElastiCache Operations Review pipeline.

Usage:
    python3 fetch_metrics.py --inventory inventory.json --output metrics.json
    python3 fetch_metrics.py --inventory inventory.json --days 14 --profile my-profile
    python3 fetch_metrics.py --inventory inventory.json --concurrency 4 --verbose
    python3 fetch_metrics.py --inventory inventory.json --skip-cost --skip-tier3

Output: JSON file with time-series metric data per cluster/node.

Required IAM permissions:
    - cloudwatch:GetMetricData
    - ce:GetCostAndUsage (unless --skip-cost)
"""

import argparse
import concurrent.futures
import dataclasses
import json
import logging
import os
import random
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone

import boto3
from _pipeline_version import PIPELINE_VERSION
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class CollectionConfig:
    """Configuration for the metrics collection run."""

    inventory_path: str
    output_path: str = "metrics.json"
    days: int = 14
    profile: str | None = None
    concurrency: int = 8
    skip_cost: bool = False
    skip_tier3: bool = False
    verbose: bool = False


@dataclasses.dataclass
class TimeSeries:
    """A single time-series of metric data."""

    timestamps: list[str] = dataclasses.field(default_factory=list)
    values: list[float] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class RegionMetricsResult:
    """Results from collecting metrics for a single region.

    Two shapes flow through this one class:

    * The **collection path** (``RegionCollector.collect``) writes each cluster's
      shard from its own worker and frees the series immediately, so it fills
      only the metadata accumulators -- ``shard_cluster_ids``,
      ``total_datapoints``, ``metric_names`` and ``errors`` -- and leaves
      ``clusters``/``latency_detail`` empty. Peak memory is one shard per worker,
      never the whole fleet.
    * The **direct-writer path** (``ResultWriter.write_sharded``, used by tests
      that hand it an in-memory result) still reads the ``clusters`` /
      ``latency_detail`` / ``cluster_metadata`` series and serialises them.

    The accumulators default empty so either path is valid.
    """

    region: str
    clusters: dict = dataclasses.field(default_factory=dict)
    latency_detail: dict = dataclasses.field(default_factory=dict)
    errors: list = dataclasses.field(default_factory=list)
    cluster_metadata: dict = dataclasses.field(default_factory=dict)
    # Metadata-only accumulators for the collection path (no series retained).
    shard_cluster_ids: list = dataclasses.field(default_factory=list)
    total_datapoints: int = 0
    metric_names: set = dataclasses.field(default_factory=set)


# ---------------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------------


def setup_logging(verbose: bool) -> None:
    """Configure structured logging with timestamps.

    Args:
        verbose: If True, set level to DEBUG; otherwise INFO.
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stderr,
        force=True,
    )


def retry_with_backoff(func, max_retries=5, base_delay=1.0, max_delay=30.0):
    """Retry a function on transient AWS errors with exponential backoff and jitter.

    Retries on ClientError with codes: Throttling, ThrottlingException,
    InternalServiceError. Logs warnings after 3+ consecutive retries.

    Args:
        func: Callable to invoke (no arguments).
        max_retries: Maximum number of retry attempts.
        base_delay: Base delay in seconds for backoff calculation.
        max_delay: Maximum delay cap in seconds.

    Returns:
        The return value of func() on success.

    Raises:
        The last exception if all retries are exhausted, or any non-retryable error.
    """
    retryable_codes = {"Throttling", "ThrottlingException", "InternalServiceError"}

    for attempt in range(max_retries + 1):
        try:
            return func()
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code not in retryable_codes:
                raise
            if attempt == max_retries:
                raise
            delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
            if attempt >= 2:
                logger.warning(
                    "Retry %d/%d for %s (delay=%.1fs)",
                    attempt + 1,
                    max_retries,
                    error_code,
                    delay,
                )
            time.sleep(delay)


# ---------------------------------------------------------------------------
# Metric Definition and Registry
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class MetricDefinition:
    """Definition of a single CloudWatch metric to collect."""

    name: str
    statistics: list[str]
    tier: int
    applies_to: str  # "node-based", "serverless", or "both"
    condition: str | None = None
    period: int = 300


class MetricDefinitionRegistry:
    """Central registry of all ElastiCache CloudWatch metrics by tier."""

    def __init__(self):
        """Initialize all metric definitions from the metrics catalog."""
        self._metrics: list[MetricDefinition] = []
        self._register_tier1()
        self._register_tier2()
        self._register_tier3()

    def _register_tier1(self) -> None:
        """Register all Tier 1 metrics (core operations review)."""
        tier1 = [
            # Node-based only
            MetricDefinition(name="EngineCPUUtilization", statistics=["Maximum", "Average"], tier=1, applies_to="node-based"),
            MetricDefinition(name="CPUUtilization", statistics=["Average"], tier=1, applies_to="node-based"),
            MetricDefinition(name="DatabaseMemoryUsagePercentage", statistics=["Maximum"], tier=1, applies_to="node-based"),
            MetricDefinition(name="DatabaseCapacityUsagePercentage", statistics=["Maximum"], tier=1, applies_to="node-based"),
            MetricDefinition(name="ReplicationLag", statistics=["Maximum"], tier=1, applies_to="node-based"),
            MetricDefinition(name="SaveInProgress", statistics=["Maximum"], tier=1, applies_to="node-based"),
            MetricDefinition(name="TrafficManagementActive", statistics=["Maximum"], tier=1, applies_to="node-based"),
            # Topology: 1 on the shard's primary, 0 on a replica. With the
            # per-node panel charts this is how a reader sees which node is
            # primary -- and a failover shows as the 1 moving between lines.
            MetricDefinition(name="IsMaster", statistics=["Maximum"], tier=1, applies_to="node-based"),
            # Serverless only
            MetricDefinition(name="ElastiCacheProcessingUnits", statistics=["Sum"], tier=1, applies_to="serverless"),
            MetricDefinition(name="ThrottledCmds", statistics=["Sum"], tier=1, applies_to="serverless"),
            # Both
            MetricDefinition(name="CacheHitRate", statistics=["Average"], tier=1, applies_to="both"),
            MetricDefinition(name="CacheHits", statistics=["Sum"], tier=1, applies_to="both"),
            MetricDefinition(name="CacheMisses", statistics=["Sum"], tier=1, applies_to="both"),
            MetricDefinition(name="Evictions", statistics=["Sum"], tier=1, applies_to="both"),
            MetricDefinition(name="CurrConnections", statistics=["Maximum"], tier=1, applies_to="both"),
            MetricDefinition(name="NewConnections", statistics=["Sum"], tier=1, applies_to="both"),
            MetricDefinition(name="NetworkBytesIn", statistics=["Sum"], tier=1, applies_to="both"),
            MetricDefinition(name="NetworkBytesOut", statistics=["Sum"], tier=1, applies_to="both"),
            MetricDefinition(name="BytesUsedForCache", statistics=["Maximum"], tier=1, applies_to="both"),
            MetricDefinition(name="CurrItems", statistics=["Maximum"], tier=1, applies_to="both"),
            MetricDefinition(name="SuccessfulReadRequestLatency", statistics=["Average"], tier=1, applies_to="both"),
            MetricDefinition(name="SuccessfulWriteRequestLatency", statistics=["Average"], tier=1, applies_to="both"),
        ]
        self._metrics.extend(tier1)

    def _register_tier2(self) -> None:
        """Register all Tier 2 metrics (deeper analysis)."""
        tier2 = [
            # Network saturation (node-based)
            MetricDefinition(name="NetworkBandwidthInAllowanceExceeded", statistics=["Maximum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="NetworkBandwidthOutAllowanceExceeded", statistics=["Maximum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="NetworkConntrackAllowanceExceeded", statistics=["Maximum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="NetworkPacketsPerSecondAllowanceExceeded", statistics=["Maximum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="NetworkBaselineUsageInPercentage", statistics=["Average", "Maximum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="NetworkBaselineUsageOutPercentage", statistics=["Average", "Maximum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="NetworkBaselineMaxUsageInPercentage", statistics=["Maximum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="NetworkBaselineMaxUsageOutPercentage", statistics=["Maximum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="NetworkMaxBytesIn", statistics=["Maximum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="NetworkMaxBytesOut", statistics=["Maximum"], tier=2, applies_to="node-based"),
            # Memory/fragmentation (node-based)
            MetricDefinition(name="FreeableMemory", statistics=["Minimum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="SwapUsage", statistics=["Maximum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="MemoryFragmentationRatio", statistics=["Maximum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="DatabaseMemoryUsageCountedForEvictPercentage", statistics=["Maximum"], tier=2, applies_to="node-based"),
            # Key health (node-based)
            MetricDefinition(name="CurrVolatileItems", statistics=["Maximum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="Reclaimed", statistics=["Sum"], tier=2, applies_to="node-based"),
            # Connection (node-based)
            MetricDefinition(name="BlockedConnections", statistics=["Maximum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="RejectedConnections", statistics=["Sum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="ErrorCount", statistics=["Sum"], tier=2, applies_to="node-based"),
            # Command mix (node-based)
            MetricDefinition(name="GetTypeCmds", statistics=["Sum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="SetTypeCmds", statistics=["Sum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="StringBasedCmds", statistics=["Sum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="HashBasedCmds", statistics=["Sum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="SortedSetBasedCmds", statistics=["Sum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="ListBasedCmds", statistics=["Sum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="SetBasedCmds", statistics=["Sum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="StreamBasedCmds", statistics=["Sum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="PubSubBasedCmds", statistics=["Sum"], tier=2, applies_to="node-based"),
            # ECPU breakdown (serverless)
            MetricDefinition(name="GetTypeCmdsECPUs", statistics=["Sum"], tier=2, applies_to="serverless"),
            MetricDefinition(name="SetTypeCmdsECPUs", statistics=["Sum"], tier=2, applies_to="serverless"),
            MetricDefinition(name="StringBasedCmdsECPUs", statistics=["Sum"], tier=2, applies_to="serverless"),
            MetricDefinition(name="HashBasedCmdsECPUs", statistics=["Sum"], tier=2, applies_to="serverless"),
            MetricDefinition(name="SortedSetBasedCmdsECPUs", statistics=["Sum"], tier=2, applies_to="serverless"),
            MetricDefinition(name="ListBasedCmdsECPUs", statistics=["Sum"], tier=2, applies_to="serverless"),
            MetricDefinition(name="SetBasedCmdsECPUs", statistics=["Sum"], tier=2, applies_to="serverless"),
            MetricDefinition(name="StreamBasedCmdsECPUs", statistics=["Sum"], tier=2, applies_to="serverless"),
            MetricDefinition(name="PubSubBasedCmdsECPUs", statistics=["Sum"], tier=2, applies_to="serverless"),

            # --- Topology / replication (node-based) ---
            MetricDefinition(name="ReplicationBytes", statistics=["Maximum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="MasterLinkHealthStatus", statistics=["Minimum"], tier=2, applies_to="node-based"),

            # --- Per-command latency (Average, microseconds) ---
            # We collect the command *counts* above; these are the matching
            # latencies -- where a slow command type actually shows up.
            MetricDefinition(name="GetTypeCmdsLatency", statistics=["Average"], tier=2, applies_to="node-based"),
            MetricDefinition(name="SetTypeCmdsLatency", statistics=["Average"], tier=2, applies_to="node-based"),
            MetricDefinition(name="StringBasedCmdsLatency", statistics=["Average"], tier=2, applies_to="node-based"),
            MetricDefinition(name="HashBasedCmdsLatency", statistics=["Average"], tier=2, applies_to="node-based"),
            MetricDefinition(name="SortedSetBasedCmdsLatency", statistics=["Average"], tier=2, applies_to="node-based"),
            MetricDefinition(name="ListBasedCmdsLatency", statistics=["Average"], tier=2, applies_to="node-based"),
            MetricDefinition(name="SetBasedCmdsLatency", statistics=["Average"], tier=2, applies_to="node-based"),
            MetricDefinition(name="StreamBasedCmdsLatency", statistics=["Average"], tier=2, applies_to="node-based"),
            MetricDefinition(name="PubSubBasedCmdsLatency", statistics=["Average"], tier=2, applies_to="node-based"),

            # --- Command families we did not collect at all (Sum + latency) ---
            MetricDefinition(name="ClusterBasedCmds", statistics=["Sum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="ClusterBasedCmdsLatency", statistics=["Average"], tier=2, applies_to="node-based"),
            MetricDefinition(name="EvalBasedCmds", statistics=["Sum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="EvalBasedCmdsLatency", statistics=["Average"], tier=2, applies_to="node-based"),
            MetricDefinition(name="GeoSpatialBasedCmds", statistics=["Sum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="GeoSpatialBasedCmdsLatency", statistics=["Average"], tier=2, applies_to="node-based"),
            MetricDefinition(name="HyperLogLogBasedCmds", statistics=["Sum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="HyperLogLogBasedCmdsLatency", statistics=["Average"], tier=2, applies_to="node-based"),
            MetricDefinition(name="KeyBasedCmds", statistics=["Sum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="KeyBasedCmdsLatency", statistics=["Average"], tier=2, applies_to="node-based"),
            MetricDefinition(name="NonKeyTypeCmds", statistics=["Sum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="NonKeyTypeCmdsLatency", statistics=["Average"], tier=2, applies_to="node-based"),
            MetricDefinition(name="ProcessedCommands", statistics=["Sum"], tier=2, applies_to="node-based"),

            # --- Memory / fragmentation detail (node-based) ---
            MetricDefinition(name="UsedMemoryDataset", statistics=["Maximum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="AllocatorFragmentationBytes", statistics=["Maximum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="AllocatorFragmentationRatio", statistics=["Maximum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="ActiveDefragHits", statistics=["Sum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="DatabaseCapacityUsageCountedForEvictPercentage", statistics=["Maximum"], tier=2, applies_to="node-based"),

            # --- Host-level (node-based) ---
            MetricDefinition(name="MajorPageFaults", statistics=["Sum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="NetworkPacketsIn", statistics=["Sum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="NetworkPacketsOut", statistics=["Sum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="NetworkMaxPacketsIn", statistics=["Maximum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="NetworkMaxPacketsOut", statistics=["Maximum"], tier=2, applies_to="node-based"),

            # --- Pub/Sub channel gauges, and key tracking / TTL (node-based) ---
            MetricDefinition(name="PubSubChannels", statistics=["Maximum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="PubSubShardChannels", statistics=["Maximum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="KeysTracked", statistics=["Maximum"], tier=2, applies_to="node-based"),
            MetricDefinition(name="DB0AverageTTL", statistics=["Maximum"], tier=2, applies_to="node-based"),
        ]
        self._metrics.extend(tier2)

    def _register_tier3(self) -> None:
        """Register all Tier 3 conditional metrics."""
        tier3 = [
            # Data tiering (node_type contains r6gd)
            MetricDefinition(name="BytesReadFromDisk", statistics=["Sum"], tier=3, applies_to="node-based", condition="data_tiering"),
            MetricDefinition(name="BytesWrittenToDisk", statistics=["Sum"], tier=3, applies_to="node-based", condition="data_tiering"),
            MetricDefinition(name="NumItemsReadFromDisk", statistics=["Sum"], tier=3, applies_to="node-based", condition="data_tiering"),
            MetricDefinition(name="NumItemsWrittenToDisk", statistics=["Sum"], tier=3, applies_to="node-based", condition="data_tiering"),
            # Global datastore
            MetricDefinition(name="GlobalDatastoreReplicationLag", statistics=["Maximum"], tier=3, applies_to="node-based", condition="global_datastore"),
            # Vector search (valkey >= 8.2)
            MetricDefinition(name="SearchBasedCmds", statistics=["Sum"], tier=3, applies_to="node-based", condition="vector_search"),
            MetricDefinition(name="SearchBasedGetCmds", statistics=["Sum"], tier=3, applies_to="node-based", condition="vector_search"),
            MetricDefinition(name="SearchBasedSetCmds", statistics=["Sum"], tier=3, applies_to="node-based", condition="vector_search"),
            MetricDefinition(name="SearchNumberOfIndexes", statistics=["Maximum"], tier=3, applies_to="node-based", condition="vector_search"),
            MetricDefinition(name="SearchTotalIndexedDocuments", statistics=["Maximum"], tier=3, applies_to="node-based", condition="vector_search"),
            MetricDefinition(name="SearchUsedMemoryBytes", statistics=["Maximum"], tier=3, applies_to="node-based", condition="vector_search"),
            # JSON module
            MetricDefinition(name="JsonBasedCmds", statistics=["Sum"], tier=3, applies_to="node-based", condition="json_module"),
            MetricDefinition(name="JsonBasedGetCmds", statistics=["Sum"], tier=3, applies_to="node-based", condition="json_module"),
            MetricDefinition(name="JsonBasedSetCmds", statistics=["Sum"], tier=3, applies_to="node-based", condition="json_module"),
            MetricDefinition(name="JsonBasedCmdsLatency", statistics=["Average"], tier=3, applies_to="node-based", condition="json_module"),
            MetricDefinition(name="JsonBasedGetCmdsLatency", statistics=["Average"], tier=3, applies_to="node-based", condition="json_module"),
            MetricDefinition(name="JsonBasedSetCmdsLatency", statistics=["Average"], tier=3, applies_to="node-based", condition="json_module"),
            # Vector search latency (valkey >= 8.2)
            MetricDefinition(name="SearchBasedCmdsLatency", statistics=["Average"], tier=3, applies_to="node-based", condition="vector_search"),
            MetricDefinition(name="SearchBasedGetCmdsLatency", statistics=["Average"], tier=3, applies_to="node-based", condition="vector_search"),
            MetricDefinition(name="SearchBasedSetCmdsLatency", statistics=["Average"], tier=3, applies_to="node-based", condition="vector_search"),
            # Security audit
            MetricDefinition(name="AuthenticationFailures", statistics=["Sum"], tier=3, applies_to="both", condition="security_audit"),
            MetricDefinition(name="KeyAuthorizationFailures", statistics=["Sum"], tier=3, applies_to="both", condition="security_audit"),
            MetricDefinition(name="CommandAuthorizationFailures", statistics=["Sum"], tier=3, applies_to="both", condition="security_audit"),
            MetricDefinition(name="ChannelAuthorizationFailures", statistics=["Sum"], tier=3, applies_to="both", condition="security_audit"),
            MetricDefinition(name="IamAuthenticationExpirations", statistics=["Sum"], tier=3, applies_to="both", condition="security_audit"),
            MetricDefinition(name="IamAuthenticationThrottling", statistics=["Sum"], tier=3, applies_to="both", condition="security_audit"),
            # Burstable (node_type starts with cache.t)
            MetricDefinition(name="CPUCreditBalance", statistics=["Minimum"], tier=3, applies_to="node-based", condition="burstable"),
            MetricDefinition(name="CPUCreditUsage", statistics=["Sum"], tier=3, applies_to="node-based", condition="burstable"),
            # Valkey 9.x+ engine metrics (return no data on older engines)
            MetricDefinition(name="CurrItemsWithVolatileFields", statistics=["Maximum"], tier=3, applies_to="node-based", condition="valkey_9"),
            MetricDefinition(name="ReclaimedFields", statistics=["Sum"], tier=3, applies_to="node-based", condition="valkey_9"),
            MetricDefinition(name="DatabaseAuthorizationFailures", statistics=["Sum"], tier=3, applies_to="both", condition="valkey_9"),
            # Multi-AZ durability-enabled clusters only
            MetricDefinition(name="DurabilityLag", statistics=["Maximum"], tier=3, applies_to="node-based", condition="durability"),
            MetricDefinition(name="DurabilityBufferExceededErrorCount", statistics=["Sum"], tier=3, applies_to="node-based", condition="durability"),
        ]
        self._metrics.extend(tier3)

    def evaluate_tier3_conditions(self, cluster: dict) -> dict[str, bool]:
        """Evaluate which Tier 3 metric categories apply to a cluster.

        Inspects node_type, engine, engine_version, and other inventory fields
        to determine which conditional metric groups should be collected.

        Args:
            cluster: Cluster dict from inventory containing metadata fields.

        Returns:
            Dict mapping condition keys to boolean eligibility.
        """
        node_type = cluster.get("node_type", "")
        engine = cluster.get("engine", "").lower()
        engine_version = cluster.get("engine_version", "0")

        # Data tiering: node_type contains "r6gd"
        data_tiering = "r6gd" in node_type.lower()

        # Burstable: node_type starts with "cache.t"
        burstable = node_type.lower().startswith("cache.t")

        # Vector search: engine == "valkey" AND version >= 8.2
        vector_search = False
        if engine == "valkey":
            try:
                major_minor = engine_version.split(".")
                version_num = float(f"{major_minor[0]}.{major_minor[1]}" if len(major_minor) >= 2 else major_minor[0])
                vector_search = version_num >= 8.2
            except (ValueError, IndexError):
                vector_search = False

        # Global datastore: check inventory indicator
        global_datastore = bool(cluster.get("global_datastore"))

        # JSON module: redis >= 6.2 or valkey (any version)
        json_module = False
        if engine == "valkey":
            json_module = True
        elif engine == "redis":
            try:
                major_minor = engine_version.split(".")
                version_num = float(f"{major_minor[0]}.{major_minor[1]}" if len(major_minor) >= 2 else major_minor[0])
                json_module = version_num >= 6.2
            except (ValueError, IndexError):
                json_module = False

        # Security audit: always enabled
        security_audit = True

        # Valkey 9.x+: metrics such as DatabaseAuthorizationFailures and the
        # hash-field TTL counters only exist on Valkey 9.0 and later.
        valkey_9 = False
        if engine == "valkey":
            try:
                major_minor = engine_version.split(".")
                version_num = float(f"{major_minor[0]}.{major_minor[1]}"
                                    if len(major_minor) >= 2 else major_minor[0])
                valkey_9 = version_num >= 9.0
            except (ValueError, IndexError):
                valkey_9 = False

        # Durability: Multi-AZ transactional-log durability is an opt-in feature
        # the inventory flags when present; default False so the metrics are not
        # requested on clusters that do not emit them.
        durability = bool(cluster.get("durability_enabled"))

        # Future use: always False
        memory_pressure = False

        return {
            "data_tiering": data_tiering,
            "burstable": burstable,
            "vector_search": vector_search,
            "global_datastore": global_datastore,
            "json_module": json_module,
            "security_audit": security_audit,
            "valkey_9": valkey_9,
            "memory_pressure": memory_pressure,
            "durability": durability,
        }

    def get_metrics_for_cluster(self, cluster: dict, skip_tier3: bool) -> list[MetricDefinition]:
        """Return applicable metrics for a cluster based on type and conditions.

        Args:
            cluster: Cluster dict from inventory.
            skip_tier3: If True, exclude all Tier 3 metrics.

        Returns:
            List of MetricDefinition objects applicable to this cluster.
        """
        cluster_type = cluster.get("cluster_type", "node-based")
        if cluster_type not in ("node-based", "serverless"):
            cluster_type = "node-based"

        result: list[MetricDefinition] = []

        for metric in self._metrics:
            # Filter by tier
            if metric.tier == 3 and skip_tier3:
                continue

            # Filter by applies_to
            if metric.applies_to != "both" and metric.applies_to != cluster_type:
                continue

            # For Tier 3, check conditions
            if metric.tier == 3:
                conditions = self.evaluate_tier3_conditions(cluster)
                if not conditions.get(metric.condition, False):
                    continue

            result.append(metric)

        # Log enabled Tier 3 categories
        if not skip_tier3:
            conditions = self.evaluate_tier3_conditions(cluster)
            enabled = [k for k, v in conditions.items() if v]
            if enabled:
                cluster_id = cluster.get("cluster_id", cluster.get("cache_name", "unknown"))
                logger.info(
                    "Cluster %s: Tier 3 categories enabled: %s",
                    cluster_id,
                    ", ".join(enabled),
                )

        return result


# ---------------------------------------------------------------------------
# CLI Argument Parsing
# ---------------------------------------------------------------------------


def parse_args(args=None) -> argparse.Namespace:
    """Parse command-line arguments for the metrics collection script.

    Args:
        args: Optional argument list (defaults to sys.argv[1:]).

    Returns:
        Parsed argparse.Namespace with all CLI parameters.
    """
    parser = argparse.ArgumentParser(
        description="Fetch CloudWatch metrics for ElastiCache clusters from inventory."
    )
    parser.add_argument(
        "--inventory",
        required=True,
        help="Path to the inventory JSON file from Stage 1.",
    )
    parser.add_argument(
        "--output",
        default="metrics.json",
        help="Output path for the metrics JSON file (default: metrics.json).",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Lookback period in days (default: 14).",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="AWS CLI profile name to use for all API calls.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=8,
        help=(
            "Maximum worker threads collecting clusters in parallel (default: 8). "
            "This bounds both the region pool and, within each region, the "
            "per-cluster pool that fans a replication group / serverless cache "
            "out to its own thread. Raising it speeds up large fleets but risks "
            "CloudWatch ThrottlingException, since every worker shares the "
            "account's GetMetricData rate limit; the conservative default plus "
            "exponential backoff keeps a 100-cluster fleet within limits."
        ),
    )
    parser.add_argument(
        "--skip-cost",
        action="store_true",
        default=False,
        help="Skip Cost Explorer data collection.",
    )
    parser.add_argument(
        "--skip-tier3",
        action="store_true",
        default=False,
        help="Skip all Tier 3 conditional metrics.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Enable detailed logging including per-cluster and per-batch progress.",
    )
    return parser.parse_args(args)


# ---------------------------------------------------------------------------
# Collection Orchestrator
# ---------------------------------------------------------------------------


class CollectionOrchestrator:
    """Coordinates the full metrics collection workflow across regions."""

    def __init__(self, config: CollectionConfig):
        """Initialize the orchestrator with configuration.

        Creates a boto3 session (using the specified profile if provided)
        and initializes the MetricDefinitionRegistry.

        Args:
            config: Parsed CLI arguments and runtime configuration.
        """
        self.config = config

        # Create boto3 session with optional profile
        session_kwargs = {}
        if config.profile:
            session_kwargs["profile_name"] = config.profile
        self.session = boto3.Session(**session_kwargs)

        # Initialize metric registry
        self.registry = MetricDefinitionRegistry()

        logger.info(
            "CollectionOrchestrator initialized (profile=%s, days=%d, concurrency=%d)",
            config.profile or "default",
            config.days,
            config.concurrency,
        )

    def _load_inventory(self) -> dict:
        """Read and validate inventory.json. Exit on failure.

        Reads the inventory JSON file specified in config. Exits with code 2
        and a descriptive error message if the file is missing or contains
        invalid JSON.

        Returns:
            Parsed inventory data as a dict.
        """
        try:
            with open(self.config.inventory_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except FileNotFoundError:
            logger.error("Inventory file not found: %s", self.config.inventory_path)
            sys.exit(2)
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in inventory file %s: %s", self.config.inventory_path, e)
            sys.exit(2)
        return data

    def _group_by_region(self, clusters: list[dict]) -> dict[str, list[dict]]:
        """Group cluster list into {region: [clusters]} dict.

        Args:
            clusters: List of cluster dicts from inventory.

        Returns:
            Dict mapping region strings to lists of clusters in that region.
            Clusters without a region field are grouped under "unknown".
        """
        regions: dict[str, list[dict]] = {}
        for cluster in clusters:
            region = cluster.get("region", "unknown")
            regions.setdefault(region, []).append(cluster)
        return regions

    def run(self) -> int:
        """Execute the full metrics collection pipeline.

        Orchestrates the entire collection workflow:
        1. Loads and validates inventory
        2. Extracts clusters (warns and writes empty output if none)
        3. Groups clusters by region
        4. Launches concurrent region collectors via ThreadPoolExecutor
        5. Collects cost data (optional, if not --skip-cost)
        6. Assembles and writes results via ResultWriter
        7. Returns appropriate exit code

        Returns:
            Exit code: 0 = all succeeded, 1 = some regions had errors,
            2 = all regions failed.
        """
        start_time = time.time()
        inventory = self._load_inventory()
        clusters = inventory.get("clusters", [])

        if not clusters:
            logger.warning("No clusters found in inventory")
            # Write empty valid output
            empty_output = {
                "metadata": {
                    "source_inventory": self.config.inventory_path,
                    "collection_timestamp": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    "clusters_processed": 0,
                    "errors_count": 0,
                    "collection_duration_seconds": time.time() - start_time,
                },
                "clusters": {},
            }
            output_dir = os.path.dirname(self.config.output_path)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
            tmp_fd, tmp_path = tempfile.mkstemp(
                suffix=".json",
                dir=output_dir or ".",
            )
            try:
                with os.fdopen(tmp_fd, "w") as tmp_f:
                    json.dump(empty_output, tmp_f, indent=2)
                os.replace(tmp_path, self.config.output_path)
            except Exception:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                raise
            return 0

        region_groups = self._group_by_region(clusters)
        logger.info(
            "Starting collection for %d clusters across %d regions",
            len(clusters),
            len(region_groups),
        )

        region_results: list[RegionMetricsResult] = []
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.config.concurrency
        ) as executor:
            futures: dict[concurrent.futures.Future, str] = {}
            for region, region_clusters in region_groups.items():
                collector = RegionCollector(
                    region, region_clusters, self.session, self.config, self.registry
                )
                future = executor.submit(collector.collect)
                futures[future] = region

            for future in concurrent.futures.as_completed(futures):
                region = futures[future]
                try:
                    result = future.result()
                    region_results.append(result)
                except Exception as exc:
                    logger.error("Region %s failed: %s", region, exc)
                    region_results.append(
                        RegionMetricsResult(region=region, errors=[str(exc)])
                    )

        # Cost data collection
        cost_data = None
        if not self.config.skip_cost:
            try:
                cost_collector = CostExplorerCollector(self.session)
                end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                start_date = (
                    datetime.now(timezone.utc) - timedelta(days=self.config.days)
                ).strftime("%Y-%m-%d")
                cost_data = cost_collector.collect(start_date, end_date)
            except Exception as exc:
                logger.warning("Cost Explorer collection failed: %s", exc)

        # Assemble the manifest (Phase 9). The shards were already written by
        # the workers as each cluster completed (see RegionCollector.collect), so
        # collection memory never held the whole fleet's series -- the region
        # results carry only metadata. All that is left is the KB-scale manifest.
        duration = time.time() - start_time
        writer = ResultWriter()

        cluster_ids = [
            cid for rr in region_results for cid in rr.shard_cluster_ids
        ]
        total_datapoints = sum(rr.total_datapoints for rr in region_results)
        metric_names: set = set()
        for rr in region_results:
            metric_names.update(rr.metric_names)
        errors_count = sum(len(rr.errors) for rr in region_results)

        manifest = writer.write_manifest(
            self.config,
            self.config.output_path,
            cluster_ids,
            cost_data,
            duration,
            total_datapoints,
            len(metric_names),
            errors_count,
        )

        logger.info(
            "Collection summary: %d clusters, %d datapoints, %.1fs, %d errors",
            manifest["metadata"]["clusters_processed"],
            manifest["metadata"]["total_datapoints"],
            duration,
            manifest["metadata"]["errors_count"],
        )

        # Determine exit code. A region "failed" when it produced no shards yet
        # reported errors (every cluster in it errored before its shard).
        total_errors = sum(len(r.errors) for r in region_results)
        all_failed = all(
            len(r.errors) > 0 and not r.shard_cluster_ids for r in region_results
        )
        if all_failed and region_results:
            return 2
        elif total_errors > 0:
            return 1
        return 0


# ---------------------------------------------------------------------------
# Cluster Query Builder
# ---------------------------------------------------------------------------


class ClusterQueryBuilder:
    """Builds MetricDataQuery objects for a single cluster."""

    def __init__(self, cluster: dict, metrics: list[MetricDefinition]):
        """Initialize the query builder for a cluster.

        Args:
            cluster: Single cluster dict from inventory.
            metrics: Applicable metrics from MetricDefinitionRegistry.
        """
        self.cluster = cluster
        self.metrics = metrics
        self.cluster_type = cluster.get("cluster_type", "node-based")
        self.nodes = self._get_nodes()
        self._query_id_map: dict[str, tuple[str, str, str]] = {}

    def _get_nodes(self) -> list[str]:
        """Extract node IDs from the cluster dict.

        For node-based clusters: looks for 'members' or 'nodes' array
        containing dicts with 'cache_cluster_id'.
        For serverless: uses the cluster's 'name' or 'serverless_cache_name'.

        Returns:
            List of node/cache identifier strings.
        """
        if self.cluster_type == "serverless":
            name = self.cluster.get("serverless_cache_name") or self.cluster.get("name", "") or self.cluster.get("cluster_id", "")
            return [name] if name else []

        # Node-based: look for members or nodes arrays
        members = self.cluster.get("members") or self.cluster.get("nodes") or []
        node_ids = []
        for member in members:
            if isinstance(member, dict):
                node_id = member.get("cache_cluster_id", "")
                if node_id:
                    node_ids.append(node_id)
        return node_ids

    def _encode_query_id(self, metric_name: str, statistic: str, node_id: str) -> str:
        """Encode a unique query ID for a metric/statistic/node combination.

        Format: m_{metric_name_lower}_{statistic_lower}_{sanitized_node_id}
        Sanitized node_id replaces hyphens with underscores and lowercases.
        IDs must match regex [a-z][a-z0-9_]* and be ≤255 chars.

        Args:
            metric_name: CloudWatch metric name.
            statistic: Statistic name (e.g., Maximum, Average, p99).
            node_id: Node or cache identifier.

        Returns:
            Encoded query ID string.
        """
        sanitized_node = node_id.lower().replace("-", "_")
        query_id = f"m_{metric_name.lower()}_{statistic.lower()}_{sanitized_node}"

        # Truncate to 255 chars if needed
        if len(query_id) > 255:
            query_id = query_id[:255]

        # Store mapping for reliable reverse lookup
        self._query_id_map[query_id] = (metric_name, statistic, node_id)

        return query_id

    def _decode_query_id(self, query_id: str) -> tuple[str, str, str]:
        """Decode a query ID back to (metric_name, statistic, node_id).

        Uses the stored mapping dict for reliable reverse lookup since
        metric names don't contain underscores in this dataset but node_ids
        might have embedded underscores after sanitization.

        Args:
            query_id: Previously encoded query ID.

        Returns:
            Tuple of (metric_name, statistic, node_id).

        Raises:
            KeyError: If query_id is not found in the mapping.
        """
        return self._query_id_map[query_id]

    def build_queries(self) -> list[dict]:
        """Build MetricDataQuery objects for all applicable metrics.

        For node-based clusters: one query per (metric, statistic, node_id)
        using CacheClusterId dimension.
        For serverless: one query per (metric, statistic) using the clusterId
        dimension (serverless metrics are not published under
        ServerlessCacheName — that is only the API parameter name).

        Returns:
            List of MetricDataQuery dicts ready for GetMetricData.
        """
        queries = []

        for metric in self.metrics:
            for statistic in metric.statistics:
                for node_id in self.nodes:
                    query_id = self._encode_query_id(metric.name, statistic, node_id)

                    # Determine dimension based on cluster type.
                    # Serverless caches publish CloudWatch metrics under the
                    # "clusterId" dimension, not "ServerlessCacheName" (which is
                    # only the ElastiCache API parameter name). Using the latter
                    # returns zero datapoints for every serverless metric.
                    if self.cluster_type == "serverless":
                        dim_name = "clusterId"
                        dim_value = node_id
                    else:
                        dim_name = "CacheClusterId"
                        dim_value = node_id

                    query = {
                        "Id": query_id,
                        "MetricStat": {
                            "Metric": {
                                "Namespace": "AWS/ElastiCache",
                                "MetricName": metric.name,
                                "Dimensions": [
                                    {"Name": dim_name, "Value": dim_value}
                                ],
                            },
                            "Period": metric.period,
                            "Stat": statistic,
                        },
                        "ReturnData": True,
                    }
                    queries.append(query)

        return queries

    def build_latency_queries(self) -> list[dict]:
        """Build Extended Statistics queries for latency percentiles.

        Constructs queries for p50, p95, p99 at 60-second period for
        SuccessfulReadRequestLatency and SuccessfulWriteRequestLatency.

        Returns:
            List of MetricDataQuery dicts for latency percentile collection.
        """
        latency_metrics = [
            ("SuccessfulReadRequestLatency", "readlatency"),
            ("SuccessfulWriteRequestLatency", "writelatency"),
        ]
        percentiles = ["p50", "p95", "p99"]
        queries = []

        for metric_name, short_name in latency_metrics:
            for percentile in percentiles:
                for node_id in self.nodes:
                    query_id = self._encode_query_id(short_name, percentile, node_id)

                    # Determine dimension based on cluster type.
                    # Serverless caches publish CloudWatch metrics under the
                    # "clusterId" dimension, not "ServerlessCacheName" (which is
                    # only the ElastiCache API parameter name). Using the latter
                    # returns zero datapoints for every serverless metric.
                    if self.cluster_type == "serverless":
                        dim_name = "clusterId"
                        dim_value = node_id
                    else:
                        dim_name = "CacheClusterId"
                        dim_value = node_id

                    query = {
                        "Id": query_id,
                        "MetricStat": {
                            "Metric": {
                                "Namespace": "AWS/ElastiCache",
                                "MetricName": metric_name,
                                "Dimensions": [
                                    {"Name": dim_name, "Value": dim_value}
                                ],
                            },
                            "Period": 60,
                            "Stat": percentile,
                        },
                        "ReturnData": True,
                    }
                    queries.append(query)

        return queries

    @property
    def query_count(self) -> int:
        """Total number of standard queries (for batch planning).

        Returns:
            Count of queries that build_queries() would produce.
        """
        return len(self.build_queries())


# ---------------------------------------------------------------------------
# Time-Series Stitching
# ---------------------------------------------------------------------------


def stitch_time_series(chunks: list[TimeSeries]) -> TimeSeries:
    """Concatenate ordered time-series chunks into a continuous series.

    Handles deduplication of boundary timestamps and validates monotonic
    ordering of the resulting series.

    Args:
        chunks: List of TimeSeries objects representing consecutive time
            windows. May be in any order (will be sorted by first timestamp).

    Returns:
        A single TimeSeries with deduplicated, monotonically ordered
        timestamps and corresponding values. Returns empty TimeSeries
        if chunks is empty.
    """
    if not chunks:
        return TimeSeries()
    if len(chunks) == 1:
        return chunks[0]

    # Sort chunks by first timestamp
    sorted_chunks = sorted(
        chunks, key=lambda c: c.timestamps[0] if c.timestamps else ""
    )

    all_timestamps: list[str] = []
    all_values: list[float] = []
    for chunk in sorted_chunks:
        all_timestamps.extend(chunk.timestamps)
        all_values.extend(chunk.values)

    # Deduplicate boundary timestamps (keep first occurrence)
    if not all_timestamps:
        return TimeSeries()

    seen: set[str] = set()
    deduped_ts: list[str] = []
    deduped_vals: list[float] = []
    for ts, val in zip(all_timestamps, all_values):
        if ts not in seen:
            seen.add(ts)
            deduped_ts.append(ts)
            deduped_vals.append(val)

    # Validate monotonic ordering
    for i in range(1, len(deduped_ts)):
        if deduped_ts[i] < deduped_ts[i - 1]:
            logger.warning("Non-monotonic timestamps detected during stitching")
            break

    return TimeSeries(timestamps=deduped_ts, values=deduped_vals)


# ---------------------------------------------------------------------------
# Batch Executor
# ---------------------------------------------------------------------------


class BatchExecutor:
    """Manages execution of batched GetMetricData calls with time chunking."""

    def __init__(self, cw_client, region: str):
        """Initialize the batch executor.

        Args:
            cw_client: boto3 CloudWatch client for this region.
            region: AWS region code (for logging).
        """
        self.cw_client = cw_client
        self.region = region

    def _chunk_time_range(
        self, start: datetime, end: datetime, chunk_days: int = 2
    ) -> list[tuple[datetime, datetime]]:
        """Split time range into N-day chunks with no gaps.

        Divides [start, end] into consecutive chunks of chunk_days days each.
        The last chunk may be shorter if the remaining time is less than
        chunk_days. Guarantees no gaps: chunk[i].end == chunk[i+1].start.

        Args:
            start: Start of the time range (inclusive).
            end: End of the time range (inclusive).
            chunk_days: Number of days per chunk (default: 2).

        Returns:
            List of (chunk_start, chunk_end) datetime tuples covering
            the entire [start, end] range.
        """
        chunks: list[tuple[datetime, datetime]] = []
        chunk_delta = timedelta(days=chunk_days)
        current = start
        while current < end:
            chunk_end = min(current + chunk_delta, end)
            chunks.append((current, chunk_end))
            current = chunk_end
        return chunks

    def _batch_queries(
        self, queries: list[dict], max_per_batch: int = 500
    ) -> list[list[dict]]:
        """Split query list into batches respecting per-call limits.

        Simple list chunking: splits queries into sublists of at most
        max_per_batch items each. 500 is the GetMetricData ceiling on
        MetricDataQueries per call, so this packs each request to the API's
        limit and issues the fewest calls per cluster.

        Args:
            queries: Full list of MetricDataQuery dicts.
            max_per_batch: Maximum queries per batch (default: 500, the
                GetMetricData API limit).

        Returns:
            List of query sublists, each with at most max_per_batch items.
            Returns empty list if queries is empty.
        """
        if not queries:
            return []
        return [
            queries[i : i + max_per_batch]
            for i in range(0, len(queries), max_per_batch)
        ]

    def _call_get_metric_data(
        self, queries: list[dict], start: datetime, end: datetime
    ) -> list[dict]:
        """Execute a single GetMetricData API call with pagination and retry.

        Handles NextToken pagination by looping until no more pages remain.
        Uses retry_with_backoff for each page call to handle transient errors.

        Args:
            queries: List of MetricDataQuery dicts for this batch.
            start: Start time for the query window.
            end: End time for the query window.

        Returns:
            List of MetricDataResult dicts from all pages of the response.
        """
        results: list[dict] = []
        next_token: list[str | None] = [None]  # Mutable container for closure

        call_start = time.time()

        while True:
            token = next_token[0]

            def _make_call(tok=token):
                kwargs = {
                    "MetricDataQueries": queries,
                    "StartTime": start,
                    "EndTime": end,
                }
                if tok:
                    kwargs["NextToken"] = tok
                return self.cw_client.get_metric_data(**kwargs)

            response = retry_with_backoff(_make_call)
            results.extend(response.get("MetricDataResults", []))
            next_token[0] = response.get("NextToken")
            if not next_token[0]:
                break

        call_duration = time.time() - call_start
        logger.debug(
            "GetMetricData call: %d queries, %s→%s, %.2fs",
            len(queries), start.isoformat(), end.isoformat(), call_duration
        )

        return results

    def execute(
        self,
        queries: list[dict],
        start_time: datetime,
        end_time: datetime,
        period: int,
    ) -> dict[str, TimeSeries]:
        """Execute queries across time windows with batching and stitching.

        Main execution method that:
        1. Chunks the time range into 2-day windows
        2. For each chunk, batches queries into groups of 500 (the API limit)
        3. Calls GetMetricData for each batch
        4. Collects partial TimeSeries results per query ID
        5. Stitches all chunks into continuous time-series per query ID

        Args:
            queries: List of MetricDataQuery dicts to execute.
            start_time: Start of the collection time range.
            end_time: End of the collection time range.
            period: Resolution period in seconds (300 or 60).

        Returns:
            Dict mapping query_id to final stitched TimeSeries.
        """
        if not queries:
            return {}

        chunks = self._chunk_time_range(start_time, end_time)
        logger.debug(
            "Executing %d queries across %d time chunks for region %s",
            len(queries), len(chunks), self.region
        )
        # For each query ID, collect partial TimeSeries from each chunk
        partial_results: dict[str, list[TimeSeries]] = {}

        for chunk_start, chunk_end in chunks:
            batches = self._batch_queries(queries)
            for batch in batches:
                results = self._call_get_metric_data(batch, chunk_start, chunk_end)
                for result in results:
                    query_id = result["Id"]
                    timestamps = [
                        t.isoformat() if hasattr(t, "isoformat") else str(t)
                        for t in result.get("Timestamps", [])
                    ]
                    values = result.get("Values", [])
                    # Sort by timestamp (CloudWatch may return reverse order)
                    if timestamps and values:
                        paired = sorted(zip(timestamps, values))
                        timestamps, values = [list(x) for x in zip(*paired)]
                    ts = TimeSeries(timestamps=timestamps, values=values)
                    partial_results.setdefault(query_id, []).append(ts)

        # Stitch chunks into continuous series
        final: dict[str, TimeSeries] = {}
        for query_id, chunks_list in partial_results.items():
            final[query_id] = stitch_time_series(chunks_list)

        return final


# ---------------------------------------------------------------------------
# Region Collector
# ---------------------------------------------------------------------------


class RegionCollector:
    """Collects all metrics for all clusters in a single region."""

    def __init__(
        self,
        region: str,
        clusters: list[dict],
        session,
        config: CollectionConfig,
        registry: MetricDefinitionRegistry,
    ):
        """Initialize the region collector.

        Clusters in this region are collected in parallel (see ``collect``), so
        this stores the boto3 session and hands each worker thread its own
        CloudWatch client on demand via ``_client`` rather than sharing a single
        client -- boto3 clients are not meant to be shared across threads for
        concurrent calls.

        Args:
            region: AWS region code.
            clusters: List of cluster dicts from inventory (this region only).
            session: boto3 session.
            config: Shared configuration.
            registry: Metric definition registry.
        """
        self.region = region
        self.clusters = clusters
        self.config = config
        self.registry = registry
        self.session = session
        # One CloudWatch client per worker thread, created lazily and reused for
        # that thread's clusters. Never shared across threads.
        self._thread_local = threading.local()
        # ResultWriter holds no mutable instance state (only class constants and
        # pure formatting/atomic-write helpers), so a single instance is safe to
        # share across the per-cluster worker threads that each write their own
        # distinct shard file.
        self.writer = ResultWriter()

    def _client(self):
        """Return a CloudWatch client bound to the calling thread.

        Each worker thread in the per-cluster pool gets its own client (created
        once, reused for every cluster that thread handles), so no mutable
        client is shared between threads.
        """
        client = getattr(self._thread_local, "cw_client", None)
        if client is None:
            client = self.session.client("cloudwatch", region_name=self.region)
            self._thread_local.cw_client = client
        return client

    def _collect_cluster(self, cluster: dict) -> dict:
        """Collect one cluster, write its shard, free the series; return metadata.

        Runs in a worker thread. It never touches the shared
        ``RegionMetricsResult``. As soon as the series are mapped it writes the
        cluster's own shard (a distinct ``metrics/<cluster_id>.json`` path, so
        concurrent workers never contend) via the shared, stateless
        ``ResultWriter.write_one_shard`` and then drops the series -- so peak
        collection memory is one shard per worker, not the whole fleet. It
        returns only small **metadata** for ``collect`` to merge single-threaded
        (the return-and-merge pattern, no lock): the shard-written cluster id,
        its datapoint count, its metric-name set, and any error.

        Steps mirror the original serial body:
        1. Get applicable metrics from the registry
        2. Build queries via ClusterQueryBuilder
        3. Execute standard queries via BatchExecutor (14-day, 300s period)
        4. Collect latency detail via LatencyDetailCollector (24h, 60s period)
        5. Map query results back to a structured dict via _decode_query_id()
        6. Write the shard and free the series

        Partial-progress semantics match the serial code: a cluster whose
        latency step raises still has its already-mapped metrics written to a
        shard *and* reports the error.

        Returns:
            A contribution dict: ``cluster_id``, ``cluster_type``, ``error``
            (None on success), ``datapoint_count``, ``metrics_collected`` (a
            set), ``shard_path`` (None if no shard was written), ``elapsed``.
            It carries **no raw series**.
        """
        cluster_id = cluster.get("cluster_id", cluster.get("cache_name", "unknown"))
        cluster_type = cluster.get("cluster_type", "node-based")

        # Per-thread client + executors -- never shared across worker threads.
        cw_client = self._client()
        batch_executor = BatchExecutor(cw_client, self.region)
        latency_collector = LatencyDetailCollector(cw_client, self.region)

        # Held only for the lifetime of this worker's cluster, then dropped.
        cluster_metrics: dict | None = None
        latency_data: dict = {}
        error: str | None = None

        cluster_start = time.time()
        try:
            # 1. Get applicable metrics from registry
            metrics = self.registry.get_metrics_for_cluster(
                cluster, self.config.skip_tier3
            )

            # 2. Build queries via ClusterQueryBuilder
            query_builder = ClusterQueryBuilder(cluster, metrics)
            queries = query_builder.build_queries()

            # 3. Execute standard queries via BatchExecutor (14-day, 300s period)
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=self.config.days)

            ts_data = batch_executor.execute(queries, start_time, end_time, 300)

            # 5. Map query results back to structured dict
            if cluster_type == "serverless":
                # Serverless: {metric_name: {statistic: TimeSeries}}
                cluster_metrics = {}
                for query_id, ts in ts_data.items():
                    try:
                        metric_name, statistic, _node_id = (
                            query_builder._decode_query_id(query_id)
                        )
                    except KeyError:
                        logger.debug(
                            "Unknown query ID %s for cluster %s",
                            query_id,
                            cluster_id,
                        )
                        continue
                    cluster_metrics.setdefault(metric_name, {})[statistic] = ts
            else:
                # Node-based: {node_id: {metric_name: {statistic: TimeSeries}}}
                cluster_metrics = {}
                for query_id, ts in ts_data.items():
                    try:
                        metric_name, statistic, node_id = (
                            query_builder._decode_query_id(query_id)
                        )
                    except KeyError:
                        logger.debug(
                            "Unknown query ID %s for cluster %s",
                            query_id,
                            cluster_id,
                        )
                        continue
                    cluster_metrics.setdefault(node_id, {}).setdefault(
                        metric_name, {}
                    )[statistic] = ts

            # 4. Collect latency detail via LatencyDetailCollector (24h, 60s)
            ld = latency_collector.collect(cluster, metrics)
            if ld:
                latency_data = ld

        except Exception as exc:
            logger.error(
                "Cluster %s in %s failed: %s", cluster_id, self.region, exc
            )
            error = str(exc)

        # 6. Write this cluster's shard and drop the series. A shard is written
        # whenever the mapping produced a result (mirrors "cid in result.clusters"
        # in the old serial path), even if the later latency step raised.
        shard_path: str | None = None
        datapoint_count = 0
        metrics_collected: set = set()
        if cluster_metrics is not None:
            # Own error only: the byte-identical writer path filters
            # ``rr.errors`` by id, which for a real run resolves to this same
            # single string, so the shard bytes match.
            own_errors = [f"{cluster_id}: {error}"] if error is not None else []
            shard_path, datapoint_count, metrics_collected = (
                self.writer.write_one_shard(
                    self.config.output_path,
                    cluster_id,
                    cluster_type,
                    cluster_metrics,
                    latency_data,
                    self.region,
                    own_errors,
                )
            )
        # Free the series now that they are on disk.
        cluster_metrics = None
        latency_data = {}

        return {
            "cluster_id": cluster_id,
            "cluster_type": cluster_type,
            "error": error,
            "datapoint_count": datapoint_count,
            "metrics_collected": metrics_collected,
            "shard_path": shard_path,
            "elapsed": time.time() - cluster_start,
        }

    def collect(self) -> RegionMetricsResult:
        """Collect metrics for all clusters in this region, in parallel.

        Each cluster (a replication group / serverless cache) is submitted to a
        bounded ``ThreadPoolExecutor`` (``--concurrency`` workers) so a large
        single-region fleet is no longer one serial thread. Each worker builds
        its queries, runs ``BatchExecutor.execute``, maps the results, collects
        latency detail using its **own** CloudWatch client, **writes its own
        shard, and frees the series** -- so peak memory is bounded by
        ``concurrency`` shards, never the whole fleet.

        The returned ``RegionMetricsResult`` therefore holds no series: only the
        metadata accumulators (``shard_cluster_ids``, ``total_datapoints``,
        ``metric_names``, ``errors``) the manifest is later built from. That
        merge is single-threaded here on the calling thread as each future
        completes -- the return-and-merge pattern, no lock needed.

        Byte-identical output for any worker count follows because everything on
        disk is order-independent: shards are keyed by ``cluster_id`` and written
        with the same serialisation the direct writer uses, and the manifest's
        cluster list is sorted downstream.

        Returns:
            A metadata-only RegionMetricsResult (shards already on disk).
        """
        total_clusters = len(self.clusters)
        logger.info(
            "Collecting metrics for region %s (%d clusters, up to %d in parallel)",
            self.region,
            total_clusters,
            max(1, self.config.concurrency),
        )
        result = RegionMetricsResult(region=self.region)

        # Create the shard directory once, before the pool, so workers never
        # race to create it (they only write distinct files into it).
        self.writer.ensure_shard_dir(self.config.output_path)

        completed = 0
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max(1, self.config.concurrency)
        ) as executor:
            futures = {
                executor.submit(self._collect_cluster, cluster): cluster
                for cluster in self.clusters
            }
            for future in concurrent.futures.as_completed(futures):
                # _collect_cluster catches its own errors and writes its own
                # shard, so future.result() returns metadata rather than raising.
                contribution = future.result()
                completed += 1
                cluster_id = contribution["cluster_id"]

                # Merge metadata only (no series retained): mirror the serial
                # code's partial-progress semantics exactly.
                result.cluster_metadata[cluster_id] = {
                    "cluster_type": contribution["cluster_type"],
                }
                if contribution["shard_path"] is not None:
                    result.shard_cluster_ids.append(cluster_id)
                    result.total_datapoints += contribution["datapoint_count"]
                    result.metric_names.update(contribution["metrics_collected"])
                if contribution["error"] is not None:
                    result.errors.append(
                        f"{cluster_id}: {contribution['error']}"
                    )

                logger.info(
                    "Cluster %s in %s completed in %.1fs (%d/%d done)",
                    cluster_id,
                    self.region,
                    contribution["elapsed"],
                    completed,
                    total_clusters,
                )

        # Order the errors so any downstream count/text is independent of the
        # order workers happened to finish in.
        result.errors.sort()
        return result


# ---------------------------------------------------------------------------
# Latency Detail Collector
# ---------------------------------------------------------------------------


class LatencyDetailCollector:
    """Collects high-resolution latency percentiles (p50/p95/p99) at 60s period."""

    def __init__(self, cw_client, region: str):
        """Initialize the latency detail collector.

        Args:
            cw_client: boto3 CloudWatch client for this region.
            region: AWS region code (for logging).
        """
        self.cw_client = cw_client
        self.region = region

    def collect(self, cluster: dict, metrics: list[MetricDefinition]) -> dict:
        """Collect p50, p95, p99 latency at 60-second resolution for last 24 hours.

        Determines nodes using the same logic as ClusterQueryBuilder._get_nodes,
        builds Extended Statistics queries for SuccessfulReadRequestLatency and
        SuccessfulWriteRequestLatency, and executes via GetMetricData with
        pagination. Handles batching if node count exceeds single-call budget.

        Budget per call: 100,800 datapoints
        Per query: 1,440 datapoints (24h at 60s)
        Max queries per call: 100,800 / 1,440 = 70
        Per cluster with N nodes: 2 metrics x 3 percentiles x N = 6N queries
        If 6N > 70 (N > 11), batch into multiple calls.

        Args:
            cluster: Cluster dict from inventory.
            metrics: List of applicable MetricDefinition objects (used to check
                if latency metrics are relevant).

        Returns:
            For node-based: {node_id: {MetricName: {percentile: TimeSeries}}}
            For serverless: {MetricName: {percentile: TimeSeries}}
            Returns empty dict if no latency data is collected.
        """
        cluster_type = cluster.get("cluster_type", "node-based")

        # Determine nodes (same logic as ClusterQueryBuilder._get_nodes)
        if cluster_type == "serverless":
            name = cluster.get("serverless_cache_name") or cluster.get("name", "")
            nodes = [name] if name else []
        else:
            members = cluster.get("members") or cluster.get("nodes") or []
            nodes = []
            for member in members:
                if isinstance(member, dict):
                    node_id = member.get("cache_cluster_id", "")
                    if node_id:
                        nodes.append(node_id)

        if not nodes:
            return {}

        # Build Extended Statistics queries for latency percentiles
        latency_metrics = [
            "SuccessfulReadRequestLatency",
            "SuccessfulWriteRequestLatency",
        ]
        percentiles = ["p50", "p95", "p99"]

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=24)

        # Build all queries
        all_queries: list[dict] = []
        # Track mapping: query_id -> (metric_name, percentile, node_id)
        query_map: dict[str, tuple[str, str, str]] = {}

        for metric_name in latency_metrics:
            for percentile in percentiles:
                for node_id in nodes:
                    sanitized_node = node_id.lower().replace("-", "_")
                    short_name = (
                        "readlatency"
                        if "Read" in metric_name
                        else "writelatency"
                    )
                    query_id = f"l_{short_name}_{percentile}_{sanitized_node}"
                    if len(query_id) > 255:
                        query_id = query_id[:255]

                    # Serverless publishes under "clusterId" — see build_queries.
                    if cluster_type == "serverless":
                        dim_name = "clusterId"
                    else:
                        dim_name = "CacheClusterId"

                    query = {
                        "Id": query_id,
                        "MetricStat": {
                            "Metric": {
                                "Namespace": "AWS/ElastiCache",
                                "MetricName": metric_name,
                                "Dimensions": [
                                    {"Name": dim_name, "Value": node_id}
                                ],
                            },
                            "Period": 60,
                            "Stat": percentile,
                        },
                        "ReturnData": True,
                    }
                    all_queries.append(query)
                    query_map[query_id] = (metric_name, percentile, node_id)

        if not all_queries:
            return {}

        # Batch queries if needed: max 70 queries per call (100,800 / 1,440)
        max_queries_per_call = 70
        batches = [
            all_queries[i : i + max_queries_per_call]
            for i in range(0, len(all_queries), max_queries_per_call)
        ]

        # Execute each batch with pagination
        all_results: list[dict] = []
        for batch in batches:
            next_token: str | None = None
            while True:

                def _make_call(tok=next_token, q=batch):
                    kwargs = {
                        "MetricDataQueries": q,
                        "StartTime": start_time,
                        "EndTime": end_time,
                    }
                    if tok:
                        kwargs["NextToken"] = tok
                    return self.cw_client.get_metric_data(**kwargs)

                response = retry_with_backoff(_make_call)
                all_results.extend(response.get("MetricDataResults", []))
                next_token = response.get("NextToken")
                if not next_token:
                    break

        # Map results back to structured dict
        if cluster_type == "serverless":
            # {MetricName: {percentile: TimeSeries}}
            output: dict = {}
            for result in all_results:
                query_id = result["Id"]
                if query_id not in query_map:
                    continue
                metric_name, percentile, _node_id = query_map[query_id]
                timestamps = [
                    t.isoformat() if hasattr(t, "isoformat") else str(t)
                    for t in result.get("Timestamps", [])
                ]
                values = result.get("Values", [])
                # Sort by timestamp
                if timestamps and values:
                    paired = sorted(zip(timestamps, values))
                    timestamps, values = [list(x) for x in zip(*paired)]
                ts = TimeSeries(timestamps=timestamps, values=values)
                output.setdefault(metric_name, {})[percentile] = ts
            return output
        else:
            # {node_id: {MetricName: {percentile: TimeSeries}}}
            output = {}
            for result in all_results:
                query_id = result["Id"]
                if query_id not in query_map:
                    continue
                metric_name, percentile, node_id = query_map[query_id]
                timestamps = [
                    t.isoformat() if hasattr(t, "isoformat") else str(t)
                    for t in result.get("Timestamps", [])
                ]
                values = result.get("Values", [])
                # Sort by timestamp
                if timestamps and values:
                    paired = sorted(zip(timestamps, values))
                    timestamps, values = [list(x) for x in zip(*paired)]
                ts = TimeSeries(timestamps=timestamps, values=values)
                output.setdefault(node_id, {}).setdefault(metric_name, {})[
                    percentile
                ] = ts
            return output


# ---------------------------------------------------------------------------
# Cost Explorer Collector
# ---------------------------------------------------------------------------


class CostExplorerCollector:
    """Retrieves daily cost data from AWS Cost Explorer."""

    def __init__(self, session):
        """Initialize the cost explorer collector.

        Creates a Cost Explorer client using the us-east-1 global endpoint.

        Args:
            session: boto3 session (Cost Explorer uses us-east-1 global endpoint).
        """
        self.ce_client = session.client("ce", region_name="us-east-1")

    def collect(self, start_date: str, end_date: str) -> dict | None:
        """Collect daily ElastiCache cost data from Cost Explorer.

        Calls ce:GetCostAndUsage with DAILY granularity, filtered to the
        Amazon ElastiCache service, grouped by USAGE_TYPE. Uses
        retry_with_backoff for transient error handling.

        Args:
            start_date: Start date in YYYY-MM-DD format.
            end_date: End date in YYYY-MM-DD format.

        Returns:
            Dict with {"daily": [{date, total_usd, by_usage_type}]} on success,
            or None if the API call fails (access denied, throttling after
            retries, or any other error). The pipeline continues without
            cost data in all failure cases.
        """
        try:

            def _call():
                return self.ce_client.get_cost_and_usage(
                    TimePeriod={"Start": start_date, "End": end_date},
                    Granularity="DAILY",
                    Filter={
                        "Dimensions": {
                            "Key": "SERVICE",
                            "Values": ["Amazon ElastiCache"],
                        }
                    },
                    GroupBy=[{"Type": "DIMENSION", "Key": "USAGE_TYPE"}],
                    Metrics=["UnblendedCost"],
                )

            response = retry_with_backoff(_call)

            daily = []
            for result in response["ResultsByTime"]:
                date = result["TimePeriod"]["Start"]
                by_usage = {}
                total = 0.0
                for group in result.get("Groups", []):
                    usage_type = group["Keys"][0]
                    amount = float(group["Metrics"]["UnblendedCost"]["Amount"])
                    by_usage[usage_type] = round(amount, 2)
                    total += amount
                daily.append(
                    {"date": date, "total_usd": round(total, 2), "by_usage_type": by_usage}
                )
            return {"daily": daily}

        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code == "AccessDeniedException":
                logger.warning("Cost Explorer access denied — skipping cost data")
            else:
                logger.warning(
                    "Cost Explorer API error (%s) — skipping cost data", error_code
                )
            return None
        except Exception as exc:
            logger.warning("Cost Explorer collection failed: %s — skipping", exc)
            return None


# ---------------------------------------------------------------------------
# Result Writer
# ---------------------------------------------------------------------------


class ResultWriter:
    """Assembles all results and writes atomic JSON output.

    Implements shared timestamps optimization: stores timestamps_5min and
    timestamps_1min once per cluster, with only values arrays per metric-statistic.
    Uses compact number formatting (2 decimal places for percentages, 0 for counts).
    """

    # Metrics whose values are clearly counts (integers) when using Sum statistic
    _COUNT_METRICS = {
        "CacheHits", "CacheMisses", "Evictions", "NewConnections",
        "Reclaimed", "GetTypeCmds", "SetTypeCmds", "StringBasedCmds",
        "HashBasedCmds", "SortedSetBasedCmds", "ListBasedCmds", "SetBasedCmds",
        "StreamBasedCmds", "PubSubBasedCmds", "ThrottledCmds",
        "BytesReadFromDisk", "BytesWrittenToDisk", "NumItemsReadFromDisk",
        "NumItemsWrittenToDisk", "SearchBasedCmds", "SearchBasedGetCmds",
        "SearchBasedSetCmds", "JsonBasedCmds", "JsonBasedGetCmds",
        "JsonBasedSetCmds", "AuthenticationFailures", "KeyAuthorizationFailures",
        "CommandAuthorizationFailures", "IamAuthenticationExpirations",
        "IamAuthenticationThrottling", "RejectedConnections", "ErrorCount",
        "BlockedConnections", "NetworkBytesIn", "NetworkBytesOut",
        "NetworkMaxBytesIn", "NetworkMaxBytesOut", "GetTypeCmdsECPUs",
        "SetTypeCmdsECPUs", "StringBasedCmdsECPUs", "HashBasedCmdsECPUs",
        "SortedSetBasedCmdsECPUs", "ListBasedCmdsECPUs", "SetBasedCmdsECPUs",
        "StreamBasedCmdsECPUs", "PubSubBasedCmdsECPUs",
        "ElastiCacheProcessingUnits", "CurrItems", "CurrConnections",
        "CurrVolatileItems", "CPUCreditUsage",
    }

    # Keywords in metric names that indicate percentage/rate/ratio values
    _PERCENTAGE_KEYWORDS = ("Percentage", "Rate", "Ratio", "Latency")

    def _format_values(self, values: list[float], statistic: str, metric_name: str) -> list:
        """Format numeric values with appropriate decimal precision.

        Rules:
        - Metrics with "Percentage", "Rate", "Ratio", or "Latency" in name → 2 decimals
        - Statistics of "Sum" with count metrics → 0 decimals (round to int)
        - Default: 2 decimals

        Args:
            values: Raw float values from the time-series.
            statistic: The statistic type (e.g., "Sum", "Maximum", "Average").
            metric_name: The CloudWatch metric name.

        Returns:
            List of formatted numeric values.
        """
        # Check if metric name contains percentage/rate/ratio/latency keywords
        if any(kw in metric_name for kw in self._PERCENTAGE_KEYWORDS):
            return [round(v, 2) for v in values]

        # Check if this is a count metric with Sum statistic
        if statistic == "Sum" and metric_name in self._COUNT_METRICS:
            return [int(round(v)) for v in values]

        # Default: 2 decimal places
        return [round(v, 2) for v in values]

    def _extract_timestamps_5min(self, cluster_data: dict, cluster_type: str) -> list[str]:
        """Extract shared 5-min timestamps from the first available TimeSeries in a cluster.

        For node-based clusters, iterates nodes → metrics → stats to find the first
        non-empty TimeSeries. For serverless, iterates metrics → stats directly.

        Args:
            cluster_data: The cluster's metric data dict from RegionMetricsResult.
            cluster_type: Either "node-based" or "serverless".

        Returns:
            List of ISO 8601 timestamp strings, or empty list if none found.
        """
        if cluster_type == "node-based":
            # Structure: {node_id: {metric_name: {statistic: TimeSeries}}}
            for node_id, node_metrics in cluster_data.items():
                if not isinstance(node_metrics, dict):
                    continue
                for metric_name, stats in node_metrics.items():
                    if not isinstance(stats, dict):
                        continue
                    for stat, ts in stats.items():
                        if isinstance(ts, TimeSeries) and ts.timestamps:
                            return ts.timestamps
        else:
            # Serverless structure: {metric_name: {statistic: TimeSeries}}
            for metric_name, stats in cluster_data.items():
                if not isinstance(stats, dict):
                    continue
                for stat, ts in stats.items():
                    if isinstance(ts, TimeSeries) and ts.timestamps:
                        return ts.timestamps
        return []

    def _extract_timestamps_1min(self, latency_data: dict, cluster_type: str) -> list[str]:
        """Extract shared 1-min timestamps from the first available latency TimeSeries.

        For node-based clusters, iterates nodes → metrics → percentiles.
        For serverless, iterates metrics → percentiles directly.

        Args:
            latency_data: The cluster's latency_detail dict from RegionMetricsResult.
            cluster_type: Either "node-based" or "serverless".

        Returns:
            List of ISO 8601 timestamp strings, or empty list if none found.
        """
        if cluster_type == "node-based":
            # Structure: {node_id: {metric_name: {percentile: TimeSeries}}}
            for node_id, node_metrics in latency_data.items():
                if not isinstance(node_metrics, dict):
                    continue
                for metric_name, percentiles in node_metrics.items():
                    if not isinstance(percentiles, dict):
                        continue
                    for pct, ts in percentiles.items():
                        if isinstance(ts, TimeSeries) and ts.timestamps:
                            return ts.timestamps
        else:
            # Serverless structure: {metric_name: {percentile: TimeSeries}}
            for metric_name, percentiles in latency_data.items():
                if not isinstance(percentiles, dict):
                    continue
                for pct, ts in percentiles.items():
                    if isinstance(ts, TimeSeries) and ts.timestamps:
                        return ts.timestamps
        return []

    def _build_node_based_cluster(
        self, cluster_id: str, cluster_data: dict, latency_data: dict, region: str
    ) -> tuple[dict, int, set]:
        """Build output structure for a node-based cluster.

        Output format:
        {
            "cluster_type": "node-based",
            "region": "us-east-1",
            "timestamps_5min": [...],
            "nodes": {node_id: {Metric: {Stat: [values]}}},
            "latency_detail": {
                "timestamps_1min": [...],
                node_id: {Metric: {percentile: [values]}}
            },
            "errors": []
        }

        Args:
            cluster_id: The cluster identifier.
            cluster_data: {node_id: {metric_name: {statistic: TimeSeries}}}
            latency_data: {node_id: {metric_name: {percentile: TimeSeries}}}
            region: AWS region.

        Returns:
            Tuple of (cluster_output_dict, datapoint_count, metric_names_set).
        """
        total_datapoints = 0
        metric_names: set = set()

        timestamps_5min = self._extract_timestamps_5min(cluster_data, "node-based")

        nodes_output: dict = {}
        for node_id, node_metrics in cluster_data.items():
            if not isinstance(node_metrics, dict):
                continue
            node_output: dict = {}
            for metric_name, stats in node_metrics.items():
                if not isinstance(stats, dict):
                    continue
                metric_names.add(metric_name)
                metric_output: dict = {}
                for stat, ts in stats.items():
                    if isinstance(ts, TimeSeries):
                        formatted_values = self._format_values(ts.values, stat, metric_name)
                        metric_output[stat] = formatted_values
                        total_datapoints += len(ts.values)
                if metric_output:
                    node_output[metric_name] = metric_output
            if node_output:
                nodes_output[node_id] = node_output

        # Build latency detail
        latency_output: dict = {}
        timestamps_1min = self._extract_timestamps_1min(latency_data, "node-based")
        if timestamps_1min:
            latency_output["timestamps_1min"] = timestamps_1min

        for node_id, node_latency in latency_data.items():
            if not isinstance(node_latency, dict):
                continue
            node_lat_output: dict = {}
            for metric_name, percentiles in node_latency.items():
                if not isinstance(percentiles, dict):
                    continue
                metric_names.add(metric_name)
                pct_output: dict = {}
                for pct, ts in percentiles.items():
                    if isinstance(ts, TimeSeries):
                        formatted_values = self._format_values(ts.values, pct, metric_name)
                        pct_output[pct] = formatted_values
                        total_datapoints += len(ts.values)
                if pct_output:
                    node_lat_output[metric_name] = pct_output
            if node_lat_output:
                latency_output[node_id] = node_lat_output

        cluster_output = {
            "cluster_type": "node-based",
            "region": region,
            "timestamps_5min": timestamps_5min,
            "nodes": nodes_output,
            "latency_detail": latency_output,
            "errors": [],
        }

        return cluster_output, total_datapoints, metric_names

    def _build_serverless_cluster(
        self, cluster_id: str, cluster_data: dict, latency_data: dict, region: str
    ) -> tuple[dict, int, set]:
        """Build output structure for a serverless cluster.

        Output format:
        {
            "cluster_type": "serverless",
            "region": "us-east-1",
            "timestamps_5min": [...],
            "metrics": {Metric: {Stat: [values]}},
            "latency_detail": {
                "timestamps_1min": [...],
                Metric: {percentile: [values]}
            },
            "errors": []
        }

        Args:
            cluster_id: The cluster identifier.
            cluster_data: {metric_name: {statistic: TimeSeries}}
            latency_data: {metric_name: {percentile: TimeSeries}}
            region: AWS region.

        Returns:
            Tuple of (cluster_output_dict, datapoint_count, metric_names_set).
        """
        total_datapoints = 0
        metric_names: set = set()

        timestamps_5min = self._extract_timestamps_5min(cluster_data, "serverless")

        metrics_output: dict = {}
        for metric_name, stats in cluster_data.items():
            if not isinstance(stats, dict):
                continue
            metric_names.add(metric_name)
            stat_output: dict = {}
            for stat, ts in stats.items():
                if isinstance(ts, TimeSeries):
                    formatted_values = self._format_values(ts.values, stat, metric_name)
                    stat_output[stat] = formatted_values
                    total_datapoints += len(ts.values)
            if stat_output:
                metrics_output[metric_name] = stat_output

        # Build latency detail
        latency_output: dict = {}
        timestamps_1min = self._extract_timestamps_1min(latency_data, "serverless")
        if timestamps_1min:
            latency_output["timestamps_1min"] = timestamps_1min

        for metric_name, percentiles in latency_data.items():
            if not isinstance(percentiles, dict):
                continue
            metric_names.add(metric_name)
            pct_output: dict = {}
            for pct, ts in percentiles.items():
                if isinstance(ts, TimeSeries):
                    formatted_values = self._format_values(ts.values, pct, metric_name)
                    pct_output[pct] = formatted_values
                    total_datapoints += len(ts.values)
            if pct_output:
                latency_output[metric_name] = pct_output

        cluster_output = {
            "cluster_type": "serverless",
            "region": region,
            "timestamps_5min": timestamps_5min,
            "metrics": metrics_output,
            "latency_detail": latency_output,
            "errors": [],
        }

        return cluster_output, total_datapoints, metric_names

    # Shards live in a "metrics/" directory beside the manifest; paths recorded
    # in the manifest are relative to it, so a reader resolves them against the
    # file it just read.
    _SHARD_SUBDIR = "metrics"

    def _shard_paths(self, output_path: str, cluster_id: str) -> tuple[str, str]:
        """Return (relative, absolute) shard paths for a cluster.

        The relative path is what the manifest records; the absolute path is
        where the shard is written.
        """
        output_dir = os.path.dirname(output_path)
        shard_rel = f"{self._SHARD_SUBDIR}/{cluster_id}.json"
        shard_abs = (
            os.path.join(output_dir, shard_rel) if output_dir else shard_rel
        )
        return shard_rel, shard_abs

    def ensure_shard_dir(self, output_path: str) -> None:
        """Create the shard directory once, up front (before any worker runs).

        Workers only ever write distinct files into this directory, so creating
        it here keeps them off the racy ``makedirs`` path.
        """
        output_dir = os.path.dirname(output_path)
        shard_dir = (
            os.path.join(output_dir, self._SHARD_SUBDIR)
            if output_dir
            else self._SHARD_SUBDIR
        )
        os.makedirs(shard_dir, exist_ok=True)

    def write_one_shard(
        self,
        output_path: str,
        cluster_id: str,
        cluster_type: str,
        cluster_data: dict,
        latency_data: dict,
        region: str,
        errors: list,
    ) -> tuple[str, int, set]:
        """Serialise one cluster to its own shard file and return manifest facts.

        This is the single serialisation seam: both the parallel collection
        workers (each writing its own shard as it finishes) and the direct
        ``write_sharded`` path call it, so a shard's bytes are identical no
        matter which path produced it. The shard is exactly one reviewable unit
        (replication group / serverless cache) -- its series, ``latency_detail``
        and ``errors`` -- keyed on the generic ``cluster_id``.

        Args:
            output_path: The manifest path; the shard is written beside it under
                ``metrics/<cluster_id>.json``.
            cluster_id: The cluster identifier.
            cluster_type: ``"serverless"`` or ``"node-based"``.
            cluster_data: The cluster's raw metric series.
            latency_data: The cluster's raw latency-detail series (may be empty).
            region: AWS region code.
            errors: Per-cluster error strings to embed in the shard.

        Returns:
            Tuple of (relative shard path, datapoint count, metric-name set).
        """
        if cluster_type == "serverless":
            cluster_output, dp_count, metrics = self._build_serverless_cluster(
                cluster_id, cluster_data, latency_data, region
            )
        else:
            cluster_output, dp_count, metrics = self._build_node_based_cluster(
                cluster_id, cluster_data, latency_data, region
            )
        cluster_output["errors"] = errors

        shard_rel, shard_abs = self._shard_paths(output_path, cluster_id)
        self.write_atomic(cluster_output, shard_abs)
        return shard_rel, dp_count, metrics

    def write_manifest(
        self,
        config: CollectionConfig,
        output_path: str,
        cluster_ids: list[str],
        cost_data: dict | None,
        duration: float,
        total_datapoints: int,
        metrics_collected: int,
        errors_count: int,
    ) -> dict:
        """Write the KB-scale manifest after all shards are on disk.

        The manifest carries ``metadata``, optional ``cost``, the sorted
        ``clusters`` id list and a ``shards`` map -- and no series. It is written
        last, so a reader that finds it can trust every shard it names exists.

        Args:
            config: Collection configuration.
            output_path: Destination for the manifest.
            cluster_ids: Ids of clusters that produced a shard (sorted here).
            cost_data: Cost Explorer data dict or None if skipped/failed.
            duration: Total collection duration in seconds.
            total_datapoints: Fleet-wide datapoint count.
            metrics_collected: Number of distinct metric names collected.
            errors_count: Total error count across the fleet.

        Returns:
            The manifest dict (metadata, optional cost, clusters, shards).
        """
        now = datetime.now(timezone.utc)
        # Sort so the manifest is deterministic regardless of the order clusters
        # or regions happened to complete in.
        cluster_ids = sorted(cluster_ids)
        shards = {cid: f"{self._SHARD_SUBDIR}/{cid}.json" for cid in cluster_ids}

        metadata = {
            "pipeline_version": PIPELINE_VERSION,
            "source_inventory": config.inventory_path,
            "collection_timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "period_start": (now - timedelta(days=config.days)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "period_end": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "resolution_seconds": 300,
            "latency_resolution_seconds": 60,
            "latency_window_hours": 24,
            "clusters_processed": len(cluster_ids),
            "metrics_collected": metrics_collected,
            "total_datapoints": total_datapoints,
            "collection_duration_seconds": round(duration, 1),
            "errors_count": errors_count,
        }

        manifest: dict = {"metadata": metadata}
        if cost_data is not None:
            manifest["cost"] = cost_data
        manifest["clusters"] = cluster_ids
        manifest["shards"] = shards

        self.write_atomic(manifest, output_path)
        return manifest

    def write_sharded(
        self,
        config: CollectionConfig,
        region_results: list[RegionMetricsResult],
        cost_data: dict | None,
        duration: float,
        output_path: str,
    ) -> dict:
        """Write shards + manifest from in-memory region results (direct path).

        This is the non-streaming path: it serialises the ``clusters`` series a
        caller has already assembled in memory (used by tests that hand it a
        fully-populated ``RegionMetricsResult``). The parallel collection path
        instead has each worker call ``write_one_shard`` directly and never
        materialises the whole fleet; both paths share ``write_one_shard`` /
        ``write_manifest``, so their bytes are identical.

        Args:
            config: Collection configuration.
            region_results: List of RegionMetricsResult from all region collectors.
            cost_data: Cost Explorer data dict or None if skipped/failed.
            duration: Total collection duration in seconds.
            output_path: Destination for the manifest (shards go in a sibling
                ``metrics/`` directory).

        Returns:
            The manifest dict (metadata, optional cost, clusters, shards).
        """
        self.ensure_shard_dir(output_path)

        total_datapoints = 0
        all_metric_names: set = set()
        errors_count = sum(len(rr.errors) for rr in region_results)
        cluster_ids: list[str] = []

        for rr in region_results:
            for cluster_id, cluster_data in rr.clusters.items():
                cluster_type = "node-based"
                if cluster_id in rr.cluster_metadata:
                    cluster_type = rr.cluster_metadata[cluster_id].get(
                        "cluster_type", "node-based"
                    )
                latency_data = rr.latency_detail.get(cluster_id, {})
                # Per-cluster errors: region errors that mention this cluster.
                own_errors = [e for e in rr.errors if cluster_id in e]
                _shard_rel, dp_count, metrics = self.write_one_shard(
                    output_path,
                    cluster_id,
                    cluster_type,
                    cluster_data,
                    latency_data,
                    rr.region,
                    own_errors,
                )
                cluster_ids.append(cluster_id)
                total_datapoints += dp_count
                all_metric_names.update(metrics)

        return self.write_manifest(
            config,
            output_path,
            cluster_ids,
            cost_data,
            duration,
            total_datapoints,
            len(all_metric_names),
            errors_count,
        )

    def write_atomic(self, data: dict, output_path: str) -> None:
        """Write JSON to temp file, then atomic rename via os.replace().

        Ensures the output directory exists, writes to a temporary file in the
        same directory, then atomically replaces the target path. If any error
        occurs during writing, the temporary file is cleaned up.

        Args:
            data: Complete output dict to serialize as JSON.
            output_path: Final destination path for the JSON file.
        """
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".json", dir=output_dir or ".")
        try:
            with os.fdopen(tmp_fd, 'w') as tmp_f:
                json.dump(data, tmp_f, indent=2)
            os.replace(tmp_path, output_path)
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------


def main() -> None:
    """Main entry point: parse CLI, run collection, log summary, exit."""
    args = parse_args()
    setup_logging(args.verbose)

    config = CollectionConfig(
        inventory_path=args.inventory,
        output_path=args.output,
        days=args.days,
        profile=args.profile,
        concurrency=args.concurrency,
        skip_cost=args.skip_cost,
        skip_tier3=args.skip_tier3,
        verbose=args.verbose,
    )

    orchestrator = CollectionOrchestrator(config)
    exit_code = orchestrator.run()

    # Log completion summary
    logger.info("Collection complete — exit code: %d", exit_code)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
