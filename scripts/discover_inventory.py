#!/usr/bin/env python3
"""
ElastiCache Inventory Discovery

Discovers all ElastiCache resources (node-based replication groups and serverless
caches) across specified AWS regions. Collects configuration, topology, security
posture, and tagging information, then outputs structured JSON for downstream
analysis stages.

This is Stage 1 of the ElastiCache Operations Review pipeline.

Usage:
    python3 discover_inventory.py --regions us-east-1 eu-west-1
    python3 discover_inventory.py --regions all --profile my-profile
    python3 discover_inventory.py --regions us-east-1 --replication-groups rg-1 rg-2
    python3 discover_inventory.py --regions us-east-1 --serverless-caches sc-1
    python3 discover_inventory.py --regions us-east-1 us-west-2 --concurrency 2
    python3 discover_inventory.py --regions us-east-1 --output inventory.json --verbose

Output: JSON inventory file with full cluster metadata.

Required IAM permissions:
    - elasticache:Describe*
    - elasticache:ListTagsForResource
    - ec2:DescribeSecurityGroups
    - ec2:DescribeRegions (when --regions all)
    - sts:GetCallerIdentity (when --account-id not provided)
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
import time
from datetime import datetime, timezone
from typing import Any, Callable, Generator, Optional

import boto3
from _pipeline_version import PIPELINE_VERSION
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class DiscoveryConfig:
    """Configuration for the inventory discovery run."""

    regions: list
    replication_group_ids: Optional[list] = None
    serverless_cache_names: Optional[list] = None
    profile: Optional[str] = None
    account_id: Optional[str] = None
    output_path: str = "inventory.json"
    concurrency: int = 4


@dataclasses.dataclass
class AccountIdentity:
    """Resolved AWS account identity."""

    account_id: str
    caller_arn: str


@dataclasses.dataclass
class RegionResult:
    """Results from scanning a single region."""

    region: str
    clusters: list
    errors: list
    rg_count: int
    serverless_count: int


@dataclasses.dataclass
class InventoryResult:
    """Final result of the discovery run."""

    metadata: dict
    clusters: list
    exit_code: int


# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure structured logging with timestamps and region context.

    Args:
        verbose: If True, set log level to DEBUG; otherwise INFO.

    Returns:
        The configured root logger.
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    log_format = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    date_format = "%Y-%m-%dT%H:%M:%S"

    logging.basicConfig(
        level=log_level,
        format=log_format,
        datefmt=date_format,
    )

    # Reduce noise from boto3/botocore unless verbose
    if not verbose:
        logging.getLogger("boto3").setLevel(logging.WARNING)
        logging.getLogger("botocore").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)

    return logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Retry with Exponential Backoff
# ---------------------------------------------------------------------------

# Error codes that should trigger a retry
_RETRYABLE_ERROR_CODES = frozenset([
    "Throttling",
    "ThrottlingException",
    "RequestLimitExceeded",
    "TooManyRequestsException",
])

# Error codes that should NOT be retried
_NON_RETRYABLE_ERROR_CODES = frozenset([
    "AccessDeniedException",
])


def _is_not_found_fault(error_code: str) -> bool:
    """Check if an error code is a *NotFoundFault variant."""
    return error_code.endswith("NotFoundFault")


def retry_with_backoff(
    func: Callable,
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
) -> Any:
    """Retry a callable with exponential backoff and jitter on throttling errors.

    Implements the retry strategy:
        delay = min(base_delay * 2^attempt + random.uniform(0, base_delay), max_delay)

    Retries on: Throttling, ThrottlingException, RequestLimitExceeded,
                TooManyRequestsException.
    Does NOT retry on: AccessDeniedException, *NotFoundFault errors.

    Args:
        func: A callable (no arguments) to execute and potentially retry.
        max_retries: Maximum number of retry attempts (default: 5).
        base_delay: Base delay in seconds for backoff calculation (default: 1.0).
        max_delay: Maximum delay cap in seconds (default: 30.0).

    Returns:
        The return value of the callable on success.

    Raises:
        ClientError: If a non-retryable error occurs, or retries are exhausted.
    """
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            return func()
        except ClientError as e:
            error_code = e.response["Error"].get("Code", "")
            last_exception = e

            # Non-retryable errors: raise immediately
            if error_code in _NON_RETRYABLE_ERROR_CODES:
                raise
            if _is_not_found_fault(error_code):
                raise

            # Retryable errors: back off and retry
            if error_code in _RETRYABLE_ERROR_CODES:
                if attempt < max_retries:
                    delay = min(
                        base_delay * (2 ** attempt) + random.uniform(0, base_delay),
                        max_delay,
                    )
                    logger.warning(
                        "Throttled (%s), attempt %d/%d. Retrying in %.2fs...",
                        error_code,
                        attempt + 1,
                        max_retries,
                        delay,
                    )
                    time.sleep(delay)
                else:
                    # Exhausted retries
                    logger.error(
                        "Exhausted %d retries due to %s", max_retries, error_code
                    )
                    raise
            else:
                # Unknown ClientError — do not retry
                raise

    # Should not reach here, but raise the last exception if we do
    raise last_exception  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Generic Paginator
# ---------------------------------------------------------------------------


def paginate_api(
    client: Any,
    method_name: str,
    result_key: str,
    **kwargs: Any,
) -> Generator[dict, None, None]:
    """Generic paginator that handles both Marker and NextToken pagination patterns.

    Automatically detects which pagination token the API uses by inspecting
    the response for 'Marker' or 'NextToken' fields and follows pages until
    all items are collected.

    Args:
        client: A boto3 service client instance.
        method_name: The name of the client method to call (e.g., 'describe_replication_groups').
        result_key: The key in the response dict that contains the list of items.
        **kwargs: Additional keyword arguments passed to the API call.

    Yields:
        Individual items from the paginated result set.
    """
    method = getattr(client, method_name)

    while True:
        response = retry_with_backoff(lambda: method(**kwargs))

        items = response.get(result_key, [])
        for item in items:
            yield item

        # Check for NextToken-based pagination
        next_token = response.get("NextToken")
        if next_token:
            kwargs["NextToken"] = next_token
            continue

        # Check for Marker-based pagination
        marker = response.get("Marker")
        if marker:
            kwargs["Marker"] = marker
            continue

        # No more pages
        break


# ---------------------------------------------------------------------------
# CLI Argument Parsing
# ---------------------------------------------------------------------------


def _build_argument_parser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser.

    Returns:
        Configured argparse.ArgumentParser instance.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Discover ElastiCache resources across AWS regions. "
            "Outputs structured JSON inventory for downstream analysis."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python3 discover_inventory.py --regions us-east-1 eu-west-1\n"
            "  python3 discover_inventory.py --regions all --profile my-profile\n"
            "  python3 discover_inventory.py --regions us-east-1 --replication-groups rg-1 rg-2\n"
            "  python3 discover_inventory.py --regions us-east-1 --serverless-caches sc-1\n"
            "  python3 discover_inventory.py --regions us-east-1 --output inventory.json --verbose\n"
        ),
    )

    parser.add_argument(
        "--regions",
        nargs="+",
        required=True,
        help=(
            "One or more AWS region identifiers to scan (e.g., us-east-1 eu-west-1), "
            "or 'all' to auto-discover all enabled regions."
        ),
    )
    parser.add_argument(
        "--replication-groups",
        nargs="*",
        default=None,
        help="Optional list of Replication Group IDs to filter discovery.",
    )
    parser.add_argument(
        "--serverless-caches",
        nargs="*",
        default=None,
        help="Optional list of Serverless Cache names to filter discovery.",
    )
    parser.add_argument(
        "--account-id",
        default=None,
        help="AWS account ID for labeling. If not provided, auto-detected via STS.",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="AWS CLI profile name to use for API calls.",
    )
    parser.add_argument(
        "--output",
        default="inventory.json",
        help="Output file path for the inventory JSON (default: inventory.json).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Maximum number of regions to scan in parallel (default: 4).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Enable verbose (DEBUG-level) logging.",
    )

    return parser


def parse_cli_args(argv: Optional[list] = None) -> DiscoveryConfig:
    """Parse CLI arguments and return a DiscoveryConfig.

    Args:
        argv: Optional list of arguments (defaults to sys.argv[1:]).

    Returns:
        DiscoveryConfig populated from CLI arguments.
    """
    parser = _build_argument_parser()
    args = parser.parse_args(argv)

    # Validate regions is non-empty (argparse required=True + nargs='+' handles this,
    # but be explicit for safety)
    if not args.regions:
        parser.error("--regions is required and must contain at least one region or 'all'.")

    return DiscoveryConfig(
        regions=args.regions,
        replication_group_ids=args.replication_groups,
        serverless_cache_names=args.serverless_caches,
        profile=args.profile,
        account_id=args.account_id,
        output_path=args.output,
        concurrency=args.concurrency,
    )


# ---------------------------------------------------------------------------
# Session and Identity Resolution
# ---------------------------------------------------------------------------


def _create_session(config: DiscoveryConfig) -> boto3.Session:
    """Create a boto3 Session, optionally using the specified profile.

    Args:
        config: Discovery configuration with optional profile name.

    Returns:
        A boto3.Session instance.
    """
    if config.profile:
        logger.debug("Creating boto3 session with profile: %s", config.profile)
        return boto3.Session(profile_name=config.profile)
    return boto3.Session()


def _resolve_account_identity(
    config: DiscoveryConfig, session: boto3.Session
) -> AccountIdentity:
    """Resolve the AWS account identity.

    When --account-id is provided, uses it directly (still calls STS for the caller ARN).
    When --account-id is NOT provided, calls sts:GetCallerIdentity to auto-detect both.

    Args:
        config: Discovery configuration with optional account_id.
        session: boto3 session to use for STS call.

    Returns:
        AccountIdentity with account_id and caller_arn.

    Raises:
        ClientError: If the STS call fails.
    """
    sts_client = session.client("sts")

    if config.account_id:
        # Account ID provided — still call STS for the caller ARN (audit trail)
        try:
            response = retry_with_backoff(lambda: sts_client.get_caller_identity())
            caller_arn = response["Arn"]
            logger.info(
                "Using provided account ID: %s (caller: %s)",
                config.account_id,
                caller_arn,
            )
            return AccountIdentity(
                account_id=config.account_id, caller_arn=caller_arn
            )
        except ClientError as e:
            # If STS fails but account-id was provided, use a placeholder ARN
            logger.warning(
                "STS GetCallerIdentity failed (%s), using provided account ID",
                e.response["Error"].get("Code", "Unknown"),
            )
            return AccountIdentity(
                account_id=config.account_id, caller_arn="unknown"
            )
    else:
        # Auto-detect account ID from STS
        logger.debug("Calling STS GetCallerIdentity to resolve account ID...")
        response = retry_with_backoff(lambda: sts_client.get_caller_identity())
        account_id = response["Account"]
        caller_arn = response["Arn"]
        logger.info("Resolved account ID: %s (caller: %s)", account_id, caller_arn)
        return AccountIdentity(account_id=account_id, caller_arn=caller_arn)


def _resolve_regions(
    config: DiscoveryConfig, session: boto3.Session
) -> list:
    """Resolve the target regions for scanning.

    When regions contains 'all', calls ec2:DescribeRegions to discover all enabled
    regions (filtering by opt-in-status: opt-in-not-required or opted-in).
    Otherwise, returns the provided region list as-is.

    Args:
        config: Discovery configuration with regions list.
        session: boto3 session for EC2 call.

    Returns:
        List of AWS region codes to scan.
    """
    if "all" in config.regions:
        logger.info("Resolving all enabled regions via ec2:DescribeRegions...")
        ec2_client = session.client("ec2")
        response = retry_with_backoff(
            lambda: ec2_client.describe_regions(
                Filters=[
                    {
                        "Name": "opt-in-status",
                        "Values": ["opt-in-not-required", "opted-in"],
                    }
                ]
            )
        )
        regions = [r["RegionName"] for r in response.get("Regions", [])]
        regions.sort()
        logger.info("Discovered %d enabled regions: %s", len(regions), regions)
        return regions
    else:
        logger.info("Using provided regions: %s", config.regions)
        return list(config.regions)


# ---------------------------------------------------------------------------
# Region Scanner
# ---------------------------------------------------------------------------


class RegionScanner:
    """Scans a single AWS region for ElastiCache resources.

    Discovers replication groups and serverless caches, handling both
    full-scan (paginated) and filtered (per-ID) lookup modes.
    """

    def __init__(self, region: str, session: boto3.Session, config: DiscoveryConfig):
        """Initialize the region scanner with per-region clients.

        Args:
            region: AWS region code (e.g., 'us-east-1').
            session: boto3 session to create regional clients from.
            config: Shared discovery configuration (filters, etc.).
        """
        self.region = region
        self.session = session
        self.config = config
        self.ec_client = session.client("elasticache", region_name=region)
        self.ec2_client = session.client("ec2", region_name=region)

    def _discover_replication_groups(self) -> list:
        """Discover replication groups in this region.

        When config.replication_group_ids is set, calls DescribeReplicationGroups
        with each ID individually. Otherwise, paginates through all RGs.

        Returns:
            List of replication group dicts from the API response.
        """
        if self.config.replication_group_ids:
            # Filtered mode: call API once per specified ID
            results = []
            for rg_id in self.config.replication_group_ids:
                try:
                    response = retry_with_backoff(
                        lambda rid=rg_id: self.ec_client.describe_replication_groups(
                            ReplicationGroupId=rid
                        )
                    )
                    rgs = response.get("ReplicationGroups", [])
                    results.extend(rgs)
                except ClientError as e:
                    error_code = e.response["Error"].get("Code", "")
                    if error_code == "ReplicationGroupNotFoundFault":
                        logger.warning(
                            "Replication group '%s' not found in %s, skipping",
                            rg_id,
                            self.region,
                        )
                    else:
                        raise
            return results
        else:
            # Full scan mode: paginate through all replication groups
            return list(
                paginate_api(
                    self.ec_client,
                    "describe_replication_groups",
                    "ReplicationGroups",
                )
            )

    def _discover_serverless_caches(self) -> list:
        """Discover serverless caches in this region.

        When config.serverless_cache_names is set, calls DescribeServerlessCaches
        with each name individually. Otherwise, paginates through all caches.

        Returns:
            List of serverless cache dicts from the API response.
        """
        if self.config.serverless_cache_names:
            # Filtered mode: call API once per specified name
            results = []
            for cache_name in self.config.serverless_cache_names:
                try:
                    response = retry_with_backoff(
                        lambda name=cache_name: self.ec_client.describe_serverless_caches(
                            ServerlessCacheName=name
                        )
                    )
                    caches = response.get("ServerlessCaches", [])
                    results.extend(caches)
                except ClientError as e:
                    error_code = e.response["Error"].get("Code", "")
                    if error_code == "ServerlessCacheNotFoundFault":
                        logger.warning(
                            "Serverless cache '%s' not found in %s, skipping",
                            cache_name,
                            self.region,
                        )
                    else:
                        raise
            return results
        else:
            # Full scan mode: paginate through all serverless caches
            return list(
                paginate_api(
                    self.ec_client,
                    "describe_serverless_caches",
                    "ServerlessCaches",
                )
            )

    def scan(self) -> RegionResult:
        """Discover all ElastiCache resources in this region.

        Calls both discovery methods (replication groups and serverless caches),
        then returns a RegionResult with the raw responses.

        Returns:
            RegionResult with discovered clusters and any errors.
        """
        errors = []

        # Discover replication groups
        try:
            replication_groups = self._discover_replication_groups()
        except Exception as exc:
            logger.error(
                "Failed to discover replication groups in %s: %s",
                self.region,
                str(exc),
            )
            replication_groups = []
            errors.append(f"DescribeReplicationGroups failed: {str(exc)}")

        # Discover serverless caches
        try:
            serverless_caches = self._discover_serverless_caches()
        except Exception as exc:
            logger.error(
                "Failed to discover serverless caches in %s: %s",
                self.region,
                str(exc),
            )
            serverless_caches = []
            errors.append(f"DescribeServerlessCaches failed: {str(exc)}")

        logger.info(
            "Region %s: discovered %d replication groups, %d serverless caches",
            self.region,
            len(replication_groups),
            len(serverless_caches),
        )

        # Enrich discovered clusters via ClusterEnricher
        enricher = ClusterEnricher(self.ec_client, self.ec2_client, self.region)
        all_clusters = []

        for rg in replication_groups:
            try:
                enriched = enricher.enrich_replication_group(rg)
                all_clusters.append(enriched)
            except Exception as exc:
                logger.error(
                    "Failed to enrich replication group %s: %s",
                    rg.get("ReplicationGroupId", "unknown"),
                    str(exc),
                )
                errors.append(
                    f"Enrichment failed for RG {rg.get('ReplicationGroupId', 'unknown')}: {str(exc)}"
                )

        for cache in serverless_caches:
            try:
                enriched = enricher.enrich_serverless_cache(cache)
                all_clusters.append(enriched)
            except Exception as exc:
                logger.error(
                    "Failed to enrich serverless cache %s: %s",
                    cache.get("ServerlessCacheName", "unknown"),
                    str(exc),
                )
                errors.append(
                    f"Enrichment failed for serverless cache {cache.get('ServerlessCacheName', 'unknown')}: {str(exc)}"
                )

        return RegionResult(
            region=self.region,
            clusters=all_clusters,
            errors=errors,
            rg_count=len(replication_groups),
            serverless_count=len(serverless_caches),
        )


# ---------------------------------------------------------------------------
# Cluster Enricher
# ---------------------------------------------------------------------------

# Target parameters to extract from parameter groups
_TARGET_PARAMETERS = frozenset([
    "maxmemory-policy",
    "cluster-enabled",
    "timeout",
    "tcp-keepalive",
    "activedefrag",
    "reserved-memory-percent",
])


class ClusterEnricher:
    """Enriches raw cluster records with detailed metadata.

    Uses per-region caches to avoid redundant API calls for parameter groups,
    security groups, and subnet groups that are shared across multiple clusters.
    """

    def __init__(self, ec_client, ec2_client, region: str):
        """Initialize the enricher with AWS clients and empty caches.

        Args:
            ec_client: boto3 ElastiCache client for this region.
            ec2_client: boto3 EC2 client for this region.
            region: AWS region code.
        """
        self.ec_client = ec_client
        self.ec2_client = ec2_client
        self.region = region
        self._param_group_cache: dict = {}
        self._security_group_cache: dict = {}
        self._subnet_group_cache: dict = {}

    # -------------------------------------------------------------------
    # Cache Cluster Details (Task 6.2)
    # -------------------------------------------------------------------

    def _get_cluster_details(self, cluster_id: str, errors: Optional[list] = None) -> dict:
        """Fetch detailed info for a single cache cluster member.

        Calls DescribeCacheClusters with ShowCacheNodeInfo=True and extracts
        key fields: CacheNodeType, Engine, EngineVersion, parameter group name,
        AZ, number of nodes, security groups, and subnet group name.

        Args:
            cluster_id: The CacheClusterId to describe.
            errors: Optional mutable list to record errors into.

        Returns:
            Dict with extracted cluster details, or empty dict on failure.
        """
        if errors is None:
            errors = []

        try:
            response = retry_with_backoff(
                lambda: self.ec_client.describe_cache_clusters(
                    CacheClusterId=cluster_id,
                    ShowCacheNodeInfo=True,
                )
            )

            clusters = response.get("CacheClusters", [])
            if not clusters:
                return {}

            cluster = clusters[0]

            # Extract parameter group name from nested structure
            param_group_info = cluster.get("CacheParameterGroup", {})
            param_group_name = param_group_info.get("CacheParameterGroupName")

            # Extract security group IDs from the SecurityGroups list
            sg_list = cluster.get("SecurityGroups", [])
            sg_ids = [
                sg.get("SecurityGroupId", "")
                for sg in sg_list
                if sg.get("SecurityGroupId")
            ]

            return {
                "CacheNodeType": cluster.get("CacheNodeType"),
                "Engine": cluster.get("Engine"),
                "EngineVersion": cluster.get("EngineVersion"),
                "CacheParameterGroupName": param_group_name,
                "PreferredAvailabilityZone": cluster.get("PreferredAvailabilityZone"),
                "NumCacheNodes": cluster.get("NumCacheNodes"),
                "SecurityGroupIds": sg_ids,
                "CacheSubnetGroupName": cluster.get("CacheSubnetGroupName"),
                # Node-level corroboration for log delivery (OE-06). When a
                # replication group configures log delivery, its member nodes
                # report an EMPTY LogDeliveryConfigurations array and signal the
                # inherited config through this boolean instead. The group-level
                # array (read in enrich_replication_group) is authoritative; this
                # is only cross-checked against it, never read as the config.
                "ReplicationGroupLogDeliveryEnabled":
                    cluster.get("ReplicationGroupLogDeliveryEnabled", False),
            }
        except ClientError as e:
            error_code = e.response["Error"].get("Code", "")
            if error_code == "CacheClusterNotFound" or _is_not_found_fault(error_code):
                errors.append(
                    f"DescribeCacheClusters failed for {cluster_id}: {error_code}"
                )
                logger.warning(
                    "Cache cluster '%s' not found in %s, continuing with partial data",
                    cluster_id,
                    self.region,
                )
                return {}
            errors.append(
                f"DescribeCacheClusters failed for {cluster_id}: {error_code}"
            )
            logger.error(
                "DescribeCacheClusters failed for %s: %s", cluster_id, e
            )
            return {}
        except Exception as e:
            errors.append(
                f"DescribeCacheClusters failed for {cluster_id}: {str(e)}"
            )
            logger.error(
                "Unexpected error fetching cluster details for %s: %s",
                cluster_id,
                e,
            )
            return {}

    # -------------------------------------------------------------------
    # Topology Computation (Task 6.3)
    # -------------------------------------------------------------------

    @staticmethod
    def compute_topology(rg: dict) -> dict:
        """Compute topology metrics from a replication group's NodeGroups.

        Calculates:
        - num_shards: number of node groups (shards)
        - num_replicas_per_shard: members minus primary (from first node group)
        - total_nodes: sum of all NodeGroupMembers across all NodeGroups

        Args:
            rg: Replication group dict from DescribeReplicationGroups response.

        Returns:
            Dict with num_shards, num_replicas_per_shard, and total_nodes.
        """
        node_groups = rg.get("NodeGroups", [])
        num_shards = len(node_groups)

        total_nodes = 0
        num_replicas_per_shard = 0
        member_nodes = []

        for i, ng in enumerate(node_groups):
            members = ng.get("NodeGroupMembers", [])
            total_nodes += len(members)
            if i == 0:
                # Subtract the primary to get replica count
                num_replicas_per_shard = max(0, len(members) - 1)
            for member in members:
                cache_cluster_id = member.get("CacheClusterId", "")
                if cache_cluster_id:
                    member_nodes.append({
                        "cache_cluster_id": cache_cluster_id,
                        "role": member.get("CurrentRole", "unknown"),
                    })

        return {
            "num_shards": num_shards,
            "num_replicas_per_shard": num_replicas_per_shard,
            "total_nodes": total_nodes,
            "members": member_nodes,
        }

    # -------------------------------------------------------------------
    # Endpoint Extraction (Task 6.4)
    # -------------------------------------------------------------------

    @staticmethod
    def extract_endpoints(rg: dict) -> dict:
        """Extract primary and reader endpoints from a replication group.

        For cluster-mode-enabled groups, uses ConfigurationEndpoint.
        Otherwise, uses PrimaryEndpoint and ReaderEndpoint from the first
        NodeGroup.

        Args:
            rg: Replication group dict from DescribeReplicationGroups response.

        Returns:
            Dict with 'primary' and 'reader' endpoint strings (host:port), or None.
        """
        endpoints = {"primary": None, "reader": None}

        # Check for cluster-mode configuration endpoint first
        config_endpoint = rg.get("ConfigurationEndpoint")
        if config_endpoint:
            addr = config_endpoint.get("Address", "")
            port = config_endpoint.get("Port", "")
            endpoint_str = f"{addr}:{port}" if addr else None
            endpoints["primary"] = endpoint_str
            endpoints["reader"] = endpoint_str
            return endpoints

        # Fall back to NodeGroups endpoints
        node_groups = rg.get("NodeGroups", [])
        if node_groups:
            first_ng = node_groups[0]

            primary_ep = first_ng.get("PrimaryEndpoint")
            if primary_ep:
                addr = primary_ep.get("Address", "")
                port = primary_ep.get("Port", "")
                endpoints["primary"] = f"{addr}:{port}" if addr else None

            reader_ep = first_ng.get("ReaderEndpoint")
            if reader_ep:
                addr = reader_ep.get("Address", "")
                port = reader_ep.get("Port", "")
                endpoints["reader"] = f"{addr}:{port}" if addr else None

        return endpoints

    # -------------------------------------------------------------------
    # Parameter Group (Task 7.1)
    # -------------------------------------------------------------------

    def _get_parameter_group(self, pg_name: str, errors: Optional[list] = None) -> dict:
        """Fetch and cache parameter group settings.

        Checks the cache first. On cache miss, paginates through
        DescribeCacheParameters and extracts the target parameters.

        Target parameters: maxmemory-policy, cluster-enabled, timeout,
        tcp-keepalive, activedefrag, reserved-memory-percent.

        Args:
            pg_name: The parameter group name to look up.
            errors: Optional mutable list to record errors into.

        Returns:
            Dict mapping parameter name to its value for target parameters.
        """
        if errors is None:
            errors = []

        if pg_name in self._param_group_cache:
            return self._param_group_cache[pg_name]

        params = {}

        try:
            for param in paginate_api(
                self.ec_client,
                "describe_cache_parameters",
                "Parameters",
                CacheParameterGroupName=pg_name,
            ):
                param_name = param.get("ParameterName", "")
                if param_name in _TARGET_PARAMETERS:
                    params[param_name] = param.get("ParameterValue")
        except ClientError as e:
            error_code = e.response["Error"].get("Code", "")
            errors.append(
                f"DescribeCacheParameters failed for {pg_name}: {error_code}"
            )
            logger.warning(
                "DescribeCacheParameters failed for %s: %s", pg_name, error_code
            )
            # Store empty dict in cache so we don't retry
            self._param_group_cache[pg_name] = params
            return params
        except Exception as e:
            errors.append(
                f"DescribeCacheParameters failed for {pg_name}: {str(e)}"
            )
            logger.warning(
                "DescribeCacheParameters failed for %s: %s", pg_name, str(e)
            )
            self._param_group_cache[pg_name] = params
            return params

        self._param_group_cache[pg_name] = params
        return params

    # -------------------------------------------------------------------
    # Security Groups (Task 7.2)
    # -------------------------------------------------------------------

    @staticmethod
    def _is_permissive_cidr(cidr: str) -> bool:
        """Check whether a CIDR is overly permissive.

        A CIDR is considered permissive if:
        - It equals '0.0.0.0/0' (all IPv4)
        - It equals '::/0' (all IPv6)
        - Its prefix length is ≤ 8 (very broad network)

        Args:
            cidr: CIDR string (e.g., '10.0.0.0/16').

        Returns:
            True if the CIDR is permissive, False otherwise.
        """
        if cidr == "0.0.0.0/0" or cidr == "::/0":
            return True

        # Check prefix length
        if "/" in cidr:
            try:
                prefix_len = int(cidr.split("/")[-1])
                if prefix_len <= 8:
                    return True
            except (ValueError, IndexError):
                pass

        return False

    def _get_security_groups(self, sg_ids: list, errors: Optional[list] = None) -> list:
        """Inspect security groups for permissive inbound rules with caching.

        For each SG ID not already cached, calls ec2:DescribeSecurityGroups
        and inspects IpPermissions for permissive CIDRs.

        Args:
            sg_ids: List of security group IDs to inspect.
            errors: Optional mutable list to record errors into.

        Returns:
            List of dicts with 'group_id' and 'permissive_rules' keys.
        """
        if errors is None:
            errors = []

        # Identify which SG IDs need fetching
        uncached_ids = [sid for sid in sg_ids if sid not in self._security_group_cache]

        if uncached_ids:
            try:
                response = retry_with_backoff(
                    lambda: self.ec2_client.describe_security_groups(
                        GroupIds=uncached_ids
                    )
                )

                for sg in response.get("SecurityGroups", []):
                    group_id = sg.get("GroupId", "")
                    permissive_rules = []

                    for rule in sg.get("IpPermissions", []):
                        from_port = rule.get("FromPort", 0)
                        to_port = rule.get("ToPort", 0)

                        # Check IPv4 ranges
                        for ip_range in rule.get("IpRanges", []):
                            cidr = ip_range.get("CidrIp", "")
                            if self._is_permissive_cidr(cidr):
                                permissive_rules.append(
                                    f"{cidr} on ports {from_port}-{to_port}"
                                )

                        # Check IPv6 ranges
                        for ipv6_range in rule.get("Ipv6Ranges", []):
                            cidr = ipv6_range.get("CidrIpv6", "")
                            if self._is_permissive_cidr(cidr):
                                permissive_rules.append(
                                    f"{cidr} on ports {from_port}-{to_port}"
                                )

                    self._security_group_cache[group_id] = {
                        "group_id": group_id,
                        "permissive_rules": permissive_rules,
                    }

            except ClientError as e:
                error_code = e.response["Error"].get("Code", "")
                errors.append(
                    f"DescribeSecurityGroups failed for {uncached_ids}: {error_code}"
                )
                logger.warning(
                    "DescribeSecurityGroups failed for %s: %s",
                    uncached_ids,
                    error_code,
                )
                # Cache empty results for the failed IDs
                for sid in uncached_ids:
                    if sid not in self._security_group_cache:
                        self._security_group_cache[sid] = {
                            "group_id": sid,
                            "permissive_rules": [],
                        }
            except Exception as e:
                errors.append(
                    f"DescribeSecurityGroups failed for {uncached_ids}: {str(e)}"
                )
                logger.warning(
                    "DescribeSecurityGroups failed for %s: %s",
                    uncached_ids,
                    str(e),
                )
                for sid in uncached_ids:
                    if sid not in self._security_group_cache:
                        self._security_group_cache[sid] = {
                            "group_id": sid,
                            "permissive_rules": [],
                        }

        # Return results for all requested IDs
        return [
            self._security_group_cache.get(sid, {"group_id": sid, "permissive_rules": []})
            for sid in sg_ids
        ]

    # -------------------------------------------------------------------
    # Subnet Group (Task 7.3)
    # -------------------------------------------------------------------

    def _get_subnet_group(self, subnet_group_name: str, errors: Optional[list] = None) -> dict:
        """Fetch and cache subnet group details.

        Checks the cache first. On miss, calls DescribeCacheSubnetGroups,
        extracts subnets and AZs, computes az_count.

        Args:
            subnet_group_name: The subnet group name to look up.
            errors: Optional mutable list to record errors into.

        Returns:
            Dict with 'name', 'subnets', 'availability_zones', and 'az_count'.
        """
        if errors is None:
            errors = []

        if subnet_group_name in self._subnet_group_cache:
            return self._subnet_group_cache[subnet_group_name]

        result = {
            "name": subnet_group_name,
            "subnets": [],
            "availability_zones": [],
            "az_count": 0,
        }

        try:
            response = retry_with_backoff(
                lambda: self.ec_client.describe_cache_subnet_groups(
                    CacheSubnetGroupName=subnet_group_name
                )
            )

            subnet_groups = response.get("CacheSubnetGroups", [])
            if subnet_groups:
                sg = subnet_groups[0]
                subnets = sg.get("Subnets", [])

                subnet_ids = []
                azs = []
                for subnet in subnets:
                    subnet_id = subnet.get("SubnetIdentifier", "")
                    if subnet_id:
                        subnet_ids.append(subnet_id)
                    az_info = subnet.get("SubnetAvailabilityZone", {})
                    az_name = az_info.get("Name", "")
                    if az_name:
                        azs.append(az_name)

                distinct_azs = list(set(azs))
                result = {
                    "name": subnet_group_name,
                    "subnets": subnet_ids,
                    "availability_zones": distinct_azs,
                    "az_count": len(distinct_azs),
                }

        except ClientError as e:
            error_code = e.response["Error"].get("Code", "")
            errors.append(
                f"DescribeCacheSubnetGroups failed for {subnet_group_name}: {error_code}"
            )
            logger.warning(
                "DescribeCacheSubnetGroups failed for %s: %s",
                subnet_group_name,
                error_code,
            )
        except Exception as e:
            errors.append(
                f"DescribeCacheSubnetGroups failed for {subnet_group_name}: {str(e)}"
            )
            logger.warning(
                "DescribeCacheSubnetGroups failed for %s: %s",
                subnet_group_name,
                str(e),
            )

        self._subnet_group_cache[subnet_group_name] = result
        return result

    # -------------------------------------------------------------------
    # Tags (Task 7.4)
    # -------------------------------------------------------------------

    def _get_tags(self, arn: str, errors: Optional[list] = None) -> dict:
        """Fetch tags for a resource and convert to a key-value dict.

        Calls ListTagsForResource and converts the AWS tag format
        [{"Key": k, "Value": v}] to a simple {k: v} dictionary.

        On ANY failure, logs a warning and returns an empty dict rather than
        raising (Req 7.3).

        Args:
            arn: The ARN of the resource to fetch tags for.
            errors: Optional mutable list to record errors into.

        Returns:
            Dict mapping tag keys to tag values, or empty dict on failure.
        """
        if errors is None:
            errors = []

        try:
            response = retry_with_backoff(
                lambda: self.ec_client.list_tags_for_resource(ResourceName=arn)
            )
            tag_list = response.get("TagList", [])
            return {
                tag["Key"]: tag["Value"]
                for tag in tag_list
                if "Key" in tag and "Value" in tag
            }
        except Exception as e:
            logger.warning(
                "ListTagsForResource failed for %s: %s", arn, str(e)
            )
            errors.append(f"ListTagsForResource failed for {arn}: {str(e)}")
            return {}

    # -------------------------------------------------------------------
    # Auth Mode Determination (Task 8.3)
    # -------------------------------------------------------------------

    @staticmethod
    def _determine_auth_mode(resource: dict) -> str:
        """Determine the authentication mode for a cluster.

        Logic:
        - If UserGroupIds is a non-empty list → "RBAC"
        - Else if AuthTokenEnabled is True → "AUTH-token"
        - Otherwise → "none"

        For serverless caches, the field is UserGroupId (singular string).

        Args:
            resource: Raw API response dict for a replication group or serverless cache.

        Returns:
            One of "RBAC", "AUTH-token", or "none".
        """
        # Check UserGroupIds (replication groups use list)
        user_group_ids = resource.get("UserGroupIds", [])
        if user_group_ids:
            return "RBAC"

        # Check UserGroupId (serverless caches use singular string)
        user_group_id = resource.get("UserGroupId")
        if user_group_id:
            return "RBAC"

        # Check AuthTokenEnabled
        if resource.get("AuthTokenEnabled", False):
            return "AUTH-token"

        return "none"

    @staticmethod
    def _normalize_log_delivery(configs) -> list:
        """Normalize a replication group's LogDeliveryConfigurations (OE-06).

        Each AWS entry carries LogType (slow-log | engine-log), DestinationType
        (cloudwatch-logs | kinesis-firehose), DestinationDetails, LogFormat
        (text | json), and Status (active | enabling | modifying | disabling |
        error). Keys are snake_cased to match the rest of the normalized record;
        values pass through unchanged so a downstream check can read LogType and
        Status without re-deriving them.

        A missing or empty array yields an empty list -- log delivery is simply
        not configured, which OE-06a/OE-06b report. That is distinct from a
        serverless cache, which omits the field entirely because the platform
        does not support log delivery at all.
        """
        result = []
        for cfg in configs or []:
            result.append({
                "log_type": cfg.get("LogType"),
                "destination_type": cfg.get("DestinationType"),
                "destination_details": cfg.get("DestinationDetails"),
                "log_format": cfg.get("LogFormat"),
                "status": cfg.get("Status"),
            })
        return result

    # -------------------------------------------------------------------
    # Enrich Replication Group (Task 8.1)
    # -------------------------------------------------------------------

    def enrich_replication_group(self, rg: dict) -> dict:
        """Enrich a replication group into a normalized cluster record.

        Steps:
        1. Call _get_cluster_details for one member cluster to get node type/engine/version
        2. Call _get_parameter_group
        3. Call _get_security_groups
        4. Call _get_subnet_group
        5. Call _get_tags
        6. Build and return normalized dict matching the output schema

        Args:
            rg: Raw replication group dict from DescribeReplicationGroups.

        Returns:
            Normalized cluster record conforming to the node-based output schema.
        """
        errors = []

        # Step 1: Get cluster details from one member cluster
        member_clusters = rg.get("MemberClusters", [])
        cluster_details = {}
        if member_clusters:
            cluster_details = self._get_cluster_details(member_clusters[0], errors)

        # Extract values from cluster details (with defaults)
        node_type = cluster_details.get("CacheNodeType")
        engine = cluster_details.get("Engine") or rg.get("Engine")
        engine_version = cluster_details.get("EngineVersion") or rg.get("EngineVersion")
        param_group_name = cluster_details.get("CacheParameterGroupName")
        sg_ids = cluster_details.get("SecurityGroupIds", [])
        subnet_group_name = cluster_details.get("CacheSubnetGroupName")

        # Step 2: Get parameter group
        parameters = {}
        if param_group_name:
            parameters = self._get_parameter_group(param_group_name, errors)

        # Step 3: Get security groups
        security_groups = []
        if sg_ids:
            security_groups = self._get_security_groups(sg_ids, errors)

        # Step 4: Get subnet group
        subnet_group = {
            "name": None,
            "subnets": [],
            "availability_zones": [],
            "az_count": 0,
        }
        if subnet_group_name:
            subnet_group = self._get_subnet_group(subnet_group_name, errors)

        # Step 5: Get tags
        arn = rg.get("ARN", "")
        tags = self._get_tags(arn, errors) if arn else {}

        # Compute topology
        topology = self.compute_topology(rg)

        # Extract endpoints
        endpoints = self.extract_endpoints(rg)

        # Determine auth mode
        auth_mode = self._determine_auth_mode(rg)

        # Build normalized record (Task 8.4 — all base fields always present)
        return {
            # Base fields (always present)
            "cluster_id": rg.get("ReplicationGroupId"),
            "cluster_type": "node-based",
            "region": self.region,
            "engine": engine,
            "engine_version": engine_version,
            "status": rg.get("Status"),
            "arn": arn,
            "tls_enabled": rg.get("TransitEncryptionEnabled", False),
            "auth_mode": auth_mode,
            "encryption_at_rest": rg.get("AtRestEncryptionEnabled", False),
            "multi_az": rg.get("MultiAZ") == "enabled",
            "snapshot_retention_days": rg.get("SnapshotRetentionLimit", 0),
            "tags": tags,
            "security_groups": security_groups,
            "errors": errors,
            # Node-based specific fields
            "node_type": node_type,
            "num_shards": topology["num_shards"],
            "num_replicas_per_shard": topology["num_replicas_per_shard"],
            "total_nodes": topology["total_nodes"],
            "members": topology["members"],
            "cluster_mode_enabled": rg.get("ClusterEnabled", False),
            "automatic_failover": rg.get("AutomaticFailover") == "enabled",
            # Log delivery (OE-06). The replication group's own configuration is
            # authoritative; the per-node array is empty when the config is
            # inherited, so it is NOT read here. The node-level boolean is kept
            # only as corroboration.
            "log_delivery": self._normalize_log_delivery(
                rg.get("LogDeliveryConfigurations")),
            "replication_group_log_delivery_enabled":
                cluster_details.get("ReplicationGroupLogDeliveryEnabled", False),
            "parameter_group": param_group_name,
            "parameters": parameters,
            "subnet_group": subnet_group,
            "availability_zones": subnet_group.get("availability_zones", []),
            "endpoints": endpoints,
        }

    # -------------------------------------------------------------------
    # Enrich Serverless Cache (Task 8.2)
    # -------------------------------------------------------------------

    def enrich_serverless_cache(self, cache: dict) -> dict:
        """Enrich a serverless cache into a normalized cluster record.

        Steps:
        1. Call _get_security_groups
        2. Call _get_tags
        3. Build and return normalized dict matching the output schema

        Args:
            cache: Raw serverless cache dict from DescribeServerlessCaches.

        Returns:
            Normalized cluster record conforming to the serverless output schema.
        """
        errors = []

        # Step 1: Get security groups
        sg_ids = cache.get("SecurityGroupIds", [])
        security_groups = []
        if sg_ids:
            security_groups = self._get_security_groups(sg_ids, errors)

        # Step 2: Get tags
        arn = cache.get("ARN", "")
        tags = self._get_tags(arn, errors) if arn else {}

        # Determine auth mode
        auth_mode = self._determine_auth_mode(cache)

        # Format cache usage limits
        raw_limits = cache.get("CacheUsageLimits", {})
        data_storage_raw = raw_limits.get("DataStorage", {})
        ecpu_raw = raw_limits.get("ECPUPerSecond", {})
        cache_usage_limits = {
            "data_storage": {
                "maximum": data_storage_raw.get("Maximum"),
                "unit": data_storage_raw.get("Unit", "GB"),
            },
            "ecpu_per_second": {
                "maximum": ecpu_raw.get("Maximum"),
            },
        }

        # Format endpoints
        primary_ep = cache.get("Endpoint", {})
        reader_ep = cache.get("ReaderEndpoint", {})
        endpoints = {
            "primary": None,
            "reader": None,
        }
        if primary_ep:
            addr = primary_ep.get("Address", "")
            port = primary_ep.get("Port", "")
            if addr:
                endpoints["primary"] = f"{addr}:{port}"
        if reader_ep:
            addr = reader_ep.get("Address", "")
            port = reader_ep.get("Port", "")
            if addr:
                endpoints["reader"] = f"{addr}:{port}"

        # Build normalized record (Task 8.4 — all base fields always present)
        # Serverless caches are always TLS-enabled, encrypted at rest, and multi-AZ
        return {
            # Base fields (always present)
            "cluster_id": cache.get("ServerlessCacheName"),
            "cluster_type": "serverless",
            "region": self.region,
            "engine": cache.get("Engine"),
            "engine_version": cache.get("MajorEngineVersion"),
            "status": cache.get("Status"),
            "arn": arn,
            "tls_enabled": True,
            "auth_mode": auth_mode,
            "encryption_at_rest": True,
            "multi_az": True,
            "snapshot_retention_days": cache.get("SnapshotRetentionLimit", 0),
            "tags": tags,
            "security_groups": security_groups,
            "errors": errors,
            # Serverless specific fields
            "cache_usage_limits": cache_usage_limits,
            "endpoints": endpoints,
        }


# ---------------------------------------------------------------------------
# Result Assembler (stub — full implementation in Task 9)
# ---------------------------------------------------------------------------


class ResultAssembler:
    """Aggregates per-region results into the final output structure.

    Flattens all clusters from all RegionResults, computes metadata counts
    (including errors from both region-level and per-cluster error arrays),
    and writes the final inventory JSON atomically.
    """

    def assemble(
        self,
        account: AccountIdentity,
        regions_scanned: list,
        region_results: list,
        duration: float,
    ) -> dict:
        """Combine all region results into the final inventory JSON structure.

        Flattens all clusters from all RegionResults into one list, computes
        metadata counts (total_clusters, node_based_count, serverless_count,
        errors_count), and builds the final inventory dict.

        errors_count includes ALL errors: region-level errors from each
        RegionResult.errors plus per-cluster errors from each cluster's
        'errors' array.

        Args:
            account: Resolved account identity.
            regions_scanned: List of regions that were scanned.
            region_results: List of RegionResult objects from each region.
            duration: Total scan duration in seconds.

        Returns:
            Complete inventory dict with 'metadata' and 'clusters' keys.
        """
        all_clusters = []
        errors_count = 0
        node_based_count = 0
        serverless_count = 0

        for result in region_results:
            all_clusters.extend(result.clusters)
            # Count region-level errors
            errors_count += len(result.errors)
            node_based_count += result.rg_count
            serverless_count += result.serverless_count

        # Count per-cluster errors from each cluster's errors array
        for cluster in all_clusters:
            cluster_errors = cluster.get("errors", [])
            errors_count += len(cluster_errors)

        metadata = {
            "pipeline_version": PIPELINE_VERSION,
            "account_id": account.account_id,
            "caller_identity_arn": account.caller_arn,
            "regions_scanned": regions_scanned,
            "scan_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "total_clusters": len(all_clusters),
            "node_based_count": node_based_count,
            "serverless_count": serverless_count,
            "scan_duration_seconds": round(duration, 2),
            "errors_count": errors_count,
        }

        return {
            "metadata": metadata,
            "clusters": all_clusters,
        }

    def write_atomic(self, inventory: dict, output_path: str) -> None:
        """Write inventory JSON atomically to the output path.

        Writes JSON to a temporary file in the same directory as the output,
        then atomically renames to the final path using os.replace(). This
        ensures no corrupted output if the process is interrupted.

        Uses json.dumps with indent=2 and default=str for human-readable
        output with datetime serialization.

        Args:
            inventory: The complete inventory dict to serialize.
            output_path: Final output file path.
        """
        # Ensure output directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        # Write to temp file in the same directory, then atomically rename
        dir_name = os.path.dirname(os.path.abspath(output_path))
        os.makedirs(dir_name, exist_ok=True)

        tmp_file = tempfile.NamedTemporaryFile(
            mode="w", dir=dir_name, suffix=".tmp", delete=False
        )
        # .name is only consumed by os.replace() below, which runs AFTER
        # tmp_file.close() -- the file is fully written and flushed by then.
        tmp_path = tmp_file.name  # nosemgrep: tempfile-without-flush
        try:
            tmp_file.write(json.dumps(inventory, indent=2, default=str))
            tmp_file.close()
            os.replace(tmp_path, output_path)
        except Exception:
            # Clean up temp file on failure
            tmp_file.close()
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise


# ---------------------------------------------------------------------------
# Discovery Orchestrator
# ---------------------------------------------------------------------------


class DiscoveryOrchestrator:
    """Coordinates the full multi-region discovery workflow.

    Resolves account identity and target regions, then launches concurrent
    region scanners using a ThreadPoolExecutor. Collects results, handles
    region-level failures gracefully, and assembles the final inventory.
    """

    def __init__(self, config: DiscoveryConfig):
        """Initialize the orchestrator.

        Args:
            config: Parsed CLI arguments and runtime configuration.
        """
        self.config = config
        self.session = _create_session(config)

    def run(self) -> InventoryResult:
        """Execute the full discovery pipeline.

        Steps:
            1. Resolve account identity (STS)
            2. Resolve target regions (EC2 DescribeRegions if 'all')
            3. Launch concurrent region scanners
            4. Collect RegionResult objects (handling failures)
            5. Pass results to ResultAssembler
            6. Determine exit code

        Returns:
            InventoryResult with metadata, clusters, and exit_code.
        """
        start_time = time.time()

        # Step 1: Resolve account identity
        account = _resolve_account_identity(self.config, self.session)

        # Step 2: Resolve regions
        regions = _resolve_regions(self.config, self.session)
        if not regions:
            logger.error("No regions to scan. Exiting.")
            duration = time.time() - start_time
            return InventoryResult(
                metadata={
                    "account_id": account.account_id,
                    "caller_identity_arn": account.caller_arn,
                    "regions_scanned": [],
                    "scan_timestamp": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    "total_clusters": 0,
                    "node_based_count": 0,
                    "serverless_count": 0,
                    "scan_duration_seconds": round(duration, 2),
                    "errors_count": 0,
                },
                clusters=[],
                exit_code=2,
            )

        # Step 3 & 4: Launch concurrent region scanners and collect results
        region_results = self._scan_regions_concurrently(regions)

        # Step 5: Assemble results
        duration = time.time() - start_time
        assembler = ResultAssembler()
        inventory = assembler.assemble(account, regions, region_results, duration)

        # Step 6: Determine exit code
        total_clusters = inventory["metadata"]["total_clusters"]
        total_errors = inventory["metadata"]["errors_count"]
        exit_code = self._determine_exit_code(total_clusters, total_errors)

        logger.info(
            "Discovery complete: %d clusters found, %d errors, duration=%.2fs, exit_code=%d",
            total_clusters,
            total_errors,
            duration,
            exit_code,
        )

        return InventoryResult(
            metadata=inventory["metadata"],
            clusters=inventory["clusters"],
            exit_code=exit_code,
        )

    def _scan_regions_concurrently(self, regions: list) -> list:
        """Launch concurrent region scanners using ThreadPoolExecutor.

        Args:
            regions: List of AWS region codes to scan.

        Returns:
            List of RegionResult objects (one per region, including failed ones).
        """
        region_results = []

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.config.concurrency
        ) as executor:
            future_to_region = {
                executor.submit(self._scan_single_region, region): region
                for region in regions
            }

            for future in concurrent.futures.as_completed(future_to_region):
                region = future_to_region[future]
                try:
                    result = future.result()
                    total_found = result.rg_count + result.serverless_count
                    logger.info(
                        "Region %s: found %d clusters", region, total_found
                    )
                    region_results.append(result)
                except Exception as exc:
                    # Region-level failure: log error and continue (Req 9.3)
                    logger.error(
                        "Region %s failed with error: %s", region, str(exc)
                    )
                    region_results.append(
                        RegionResult(
                            region=region,
                            clusters=[],
                            errors=[f"Region scan failed: {str(exc)}"],
                            rg_count=0,
                            serverless_count=0,
                        )
                    )

        return region_results

    def _scan_single_region(self, region: str) -> RegionResult:
        """Scan a single region for ElastiCache resources.

        Args:
            region: AWS region code to scan.

        Returns:
            RegionResult from the RegionScanner.
        """
        logger.info("Scanning region %s...", region)
        scanner = RegionScanner(region, self.session, self.config)
        return scanner.scan()

    @staticmethod
    def _determine_exit_code(total_clusters: int, total_errors: int) -> int:
        """Determine the process exit code based on results.

        Exit codes:
            0 — No errors occurred
            1 — Some errors but clusters were still found
            2 — No clusters discovered (total failure)

        Args:
            total_clusters: Total number of clusters discovered.
            total_errors: Total number of errors recorded.

        Returns:
            Integer exit code (0, 1, or 2).
        """
        if total_clusters == 0:
            return 2
        if total_errors > 0:
            return 1
        return 0


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------


def main() -> None:
    """Main entry point for the inventory discovery script.

    Parses CLI arguments, sets up logging, runs the discovery orchestrator,
    writes the output JSON atomically, and exits with the appropriate code.
    """
    # Parse CLI arguments
    parser = _build_argument_parser()
    args = parser.parse_args()

    # Setup logging first so all subsequent messages are captured
    setup_logging(verbose=args.verbose)

    # Build DiscoveryConfig from parsed args
    config = DiscoveryConfig(
        regions=args.regions,
        replication_group_ids=args.replication_groups,
        serverless_cache_names=args.serverless_caches,
        profile=args.profile,
        account_id=args.account_id,
        output_path=args.output,
        concurrency=args.concurrency,
    )

    # Run the discovery orchestrator
    orchestrator = DiscoveryOrchestrator(config)
    try:
        result = orchestrator.run()
    except ClientError as e:
        logger.error("Fatal error during discovery: %s", e)
        sys.exit(2)

    # Write output atomically
    assembler = ResultAssembler()
    inventory = {"metadata": result.metadata, "clusters": result.clusters}
    assembler.write_atomic(inventory, config.output_path)
    logger.info("Inventory written to %s", config.output_path)

    sys.exit(result.exit_code)


if __name__ == "__main__":
    main()
