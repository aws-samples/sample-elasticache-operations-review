"""Unit tests for RegionScanner (Tasks 4 and 5).

Tests replication group discovery, serverless cache discovery,
filtered lookups, not-found fault handling, and the scan() orchestration method.
"""

import os
import sys
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

# Add scripts to path so we can import the module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from discover_inventory import (
    DiscoveryConfig,
    RegionResult,
    RegionScanner,
)


def _make_config(**overrides):
    """Create a DiscoveryConfig with sensible defaults."""
    defaults = {
        "regions": ["us-east-1"],
        "replication_group_ids": None,
        "serverless_cache_names": None,
        "profile": None,
        "account_id": None,
        "output_path": "inventory.json",
        "concurrency": 4,
    }
    defaults.update(overrides)
    return DiscoveryConfig(**defaults)


def _make_client_error(code, message="Error"):
    """Create a ClientError with the given error code."""
    return ClientError(
        {"Error": {"Code": code, "Message": message}},
        "TestOperation",
    )


class TestRegionScannerInit:
    """Tests for RegionScanner.__init__ (Task 4.1)."""

    def test_creates_elasticache_client(self):
        session = MagicMock()
        config = _make_config()

        scanner = RegionScanner("us-east-1", session, config)

        session.client.assert_any_call("elasticache", region_name="us-east-1")
        assert scanner.ec_client is not None

    def test_creates_ec2_client(self):
        session = MagicMock()
        config = _make_config()

        scanner = RegionScanner("eu-west-1", session, config)

        session.client.assert_any_call("ec2", region_name="eu-west-1")
        assert scanner.ec2_client is not None

    def test_stores_region_and_config(self):
        session = MagicMock()
        config = _make_config()

        scanner = RegionScanner("ap-southeast-1", session, config)

        assert scanner.region == "ap-southeast-1"
        assert scanner.config is config


class TestDiscoverReplicationGroups:
    """Tests for RegionScanner._discover_replication_groups() (Tasks 4.2–4.5)."""

    def test_full_scan_uses_paginate_api(self):
        """Task 4.2: Full scan paginates through all RGs."""
        session = MagicMock()
        config = _make_config()
        scanner = RegionScanner("us-east-1", session, config)

        # Mock the elasticache client to return RGs without pagination
        rg1 = {"ReplicationGroupId": "rg-1", "Status": "available"}
        rg2 = {"ReplicationGroupId": "rg-2", "Status": "available"}
        scanner.ec_client.describe_replication_groups = MagicMock(
            return_value={"ReplicationGroups": [rg1, rg2]}
        )

        result = scanner._discover_replication_groups()

        assert len(result) == 2
        assert result[0]["ReplicationGroupId"] == "rg-1"
        assert result[1]["ReplicationGroupId"] == "rg-2"

    def test_filtered_lookup_calls_per_id(self):
        """Task 4.3: Filtered mode calls API once per specified ID."""
        session = MagicMock()
        config = _make_config(replication_group_ids=["rg-a", "rg-b"])
        scanner = RegionScanner("us-east-1", session, config)

        rg_a = {"ReplicationGroupId": "rg-a", "Status": "available"}
        rg_b = {"ReplicationGroupId": "rg-b", "Status": "available"}

        scanner.ec_client.describe_replication_groups = MagicMock(
            side_effect=[
                {"ReplicationGroups": [rg_a]},
                {"ReplicationGroups": [rg_b]},
            ]
        )

        result = scanner._discover_replication_groups()

        assert len(result) == 2
        assert scanner.ec_client.describe_replication_groups.call_count == 2
        # Verify each call used the specific ID
        calls = scanner.ec_client.describe_replication_groups.call_args_list
        assert calls[0][1]["ReplicationGroupId"] == "rg-a"
        assert calls[1][1]["ReplicationGroupId"] == "rg-b"

    def test_not_found_fault_logs_warning_and_continues(self, caplog):
        """Task 4.4: ReplicationGroupNotFoundFault logs warning and skips."""
        session = MagicMock()
        config = _make_config(replication_group_ids=["rg-missing", "rg-exists"])
        scanner = RegionScanner("us-east-1", session, config)

        rg_exists = {"ReplicationGroupId": "rg-exists", "Status": "available"}
        scanner.ec_client.describe_replication_groups = MagicMock(
            side_effect=[
                _make_client_error("ReplicationGroupNotFoundFault"),
                {"ReplicationGroups": [rg_exists]},
            ]
        )

        import logging
        with caplog.at_level(logging.WARNING):
            result = scanner._discover_replication_groups()

        assert len(result) == 1
        assert result[0]["ReplicationGroupId"] == "rg-exists"
        assert "Replication group 'rg-missing' not found in us-east-1, skipping" in caplog.text

    def test_other_client_errors_are_raised(self):
        """Non-NotFound errors should propagate."""
        session = MagicMock()
        config = _make_config(replication_group_ids=["rg-denied"])
        scanner = RegionScanner("us-east-1", session, config)

        scanner.ec_client.describe_replication_groups = MagicMock(
            side_effect=_make_client_error("AccessDeniedException")
        )

        with pytest.raises(ClientError) as exc_info:
            scanner._discover_replication_groups()
        assert exc_info.value.response["Error"]["Code"] == "AccessDeniedException"

    def test_extracts_all_required_fields(self):
        """Task 4.5: All required fields are present in the returned dict."""
        session = MagicMock()
        config = _make_config()
        scanner = RegionScanner("us-east-1", session, config)

        full_rg = {
            "ReplicationGroupId": "rg-full",
            "Status": "available",
            "Description": "Test RG",
            "ClusterEnabled": True,
            "MultiAZ": "enabled",
            "AutomaticFailover": "enabled",
            "SnapshotRetentionLimit": 7,
            "TransitEncryptionEnabled": True,
            "AtRestEncryptionEnabled": True,
            "AuthTokenEnabled": False,
            "UserGroupIds": ["ug-1"],
            "KmsKeyId": "arn:aws:kms:us-east-1:123:key/abc",
            "ARN": "arn:aws:elasticache:us-east-1:123:replicationgroup:rg-full",
            "NodeGroups": [{"NodeGroupId": "0001", "NodeGroupMembers": []}],
            "MemberClusters": ["rg-full-001", "rg-full-002"],
        }

        scanner.ec_client.describe_replication_groups = MagicMock(
            return_value={"ReplicationGroups": [full_rg]}
        )

        result = scanner._discover_replication_groups()

        assert len(result) == 1
        rg = result[0]
        # Verify all required fields are present
        required_fields = [
            "ReplicationGroupId", "Status", "Description", "ClusterEnabled",
            "MultiAZ", "AutomaticFailover", "SnapshotRetentionLimit",
            "TransitEncryptionEnabled", "AtRestEncryptionEnabled",
            "AuthTokenEnabled", "UserGroupIds", "KmsKeyId", "ARN",
            "NodeGroups", "MemberClusters",
        ]
        for field in required_fields:
            assert field in rg, f"Missing field: {field}"


class TestDiscoverServerlessCaches:
    """Tests for RegionScanner._discover_serverless_caches() (Tasks 5.1–5.4)."""

    def test_full_scan_uses_paginate_api(self):
        """Task 5.1: Full scan paginates through all serverless caches."""
        session = MagicMock()
        config = _make_config()
        scanner = RegionScanner("us-east-1", session, config)

        sc1 = {"ServerlessCacheName": "sc-1", "Status": "available"}
        sc2 = {"ServerlessCacheName": "sc-2", "Status": "available"}
        scanner.ec_client.describe_serverless_caches = MagicMock(
            return_value={"ServerlessCaches": [sc1, sc2]}
        )

        result = scanner._discover_serverless_caches()

        assert len(result) == 2
        assert result[0]["ServerlessCacheName"] == "sc-1"
        assert result[1]["ServerlessCacheName"] == "sc-2"

    def test_filtered_lookup_calls_per_name(self):
        """Task 5.2: Filtered mode calls API once per specified name."""
        session = MagicMock()
        config = _make_config(serverless_cache_names=["sc-a", "sc-b"])
        scanner = RegionScanner("us-east-1", session, config)

        sc_a = {"ServerlessCacheName": "sc-a", "Status": "available"}
        sc_b = {"ServerlessCacheName": "sc-b", "Status": "available"}

        scanner.ec_client.describe_serverless_caches = MagicMock(
            side_effect=[
                {"ServerlessCaches": [sc_a]},
                {"ServerlessCaches": [sc_b]},
            ]
        )

        result = scanner._discover_serverless_caches()

        assert len(result) == 2
        assert scanner.ec_client.describe_serverless_caches.call_count == 2
        calls = scanner.ec_client.describe_serverless_caches.call_args_list
        assert calls[0][1]["ServerlessCacheName"] == "sc-a"
        assert calls[1][1]["ServerlessCacheName"] == "sc-b"

    def test_not_found_fault_logs_warning_and_continues(self, caplog):
        """Task 5.3: ServerlessCacheNotFoundFault logs warning and skips."""
        session = MagicMock()
        config = _make_config(serverless_cache_names=["sc-missing", "sc-exists"])
        scanner = RegionScanner("us-east-1", session, config)

        sc_exists = {"ServerlessCacheName": "sc-exists", "Status": "available"}
        scanner.ec_client.describe_serverless_caches = MagicMock(
            side_effect=[
                _make_client_error("ServerlessCacheNotFoundFault"),
                {"ServerlessCaches": [sc_exists]},
            ]
        )

        import logging
        with caplog.at_level(logging.WARNING):
            result = scanner._discover_serverless_caches()

        assert len(result) == 1
        assert result[0]["ServerlessCacheName"] == "sc-exists"
        assert "Serverless cache 'sc-missing' not found in us-east-1, skipping" in caplog.text

    def test_other_client_errors_are_raised(self):
        """Non-NotFound errors should propagate."""
        session = MagicMock()
        config = _make_config(serverless_cache_names=["sc-denied"])
        scanner = RegionScanner("us-east-1", session, config)

        scanner.ec_client.describe_serverless_caches = MagicMock(
            side_effect=_make_client_error("AccessDeniedException")
        )

        with pytest.raises(ClientError) as exc_info:
            scanner._discover_serverless_caches()
        assert exc_info.value.response["Error"]["Code"] == "AccessDeniedException"

    def test_extracts_all_required_fields(self):
        """Task 5.4: All required fields are present in the returned dict."""
        session = MagicMock()
        config = _make_config()
        scanner = RegionScanner("us-east-1", session, config)

        full_sc = {
            "ServerlessCacheName": "sc-full",
            "Status": "available",
            "Engine": "valkey",
            "MajorEngineVersion": "8",
            "SecurityGroupIds": ["sg-123"],
            "SubnetIds": ["subnet-a", "subnet-b"],
            "ARN": "arn:aws:elasticache:us-east-1:123:serverlesscache:sc-full",
            "CacheUsageLimits": {
                "DataStorage": {"Maximum": 5, "Unit": "GB"},
                "ECPUPerSecond": {"Maximum": 15000},
            },
            "UserGroupId": "ug-1",
            "KmsKeyId": "arn:aws:kms:us-east-1:123:key/xyz",
            "SnapshotRetentionLimit": 1,
            "DailySnapshotTime": "05:00",
            "Endpoint": {"Address": "sc-full.cache.amazonaws.com", "Port": 6379},
            "ReaderEndpoint": {"Address": "sc-full-ro.cache.amazonaws.com", "Port": 6380},
        }

        scanner.ec_client.describe_serverless_caches = MagicMock(
            return_value={"ServerlessCaches": [full_sc]}
        )

        result = scanner._discover_serverless_caches()

        assert len(result) == 1
        sc = result[0]
        required_fields = [
            "ServerlessCacheName", "Status", "Engine", "MajorEngineVersion",
            "SecurityGroupIds", "SubnetIds", "ARN", "CacheUsageLimits",
            "UserGroupId", "KmsKeyId", "SnapshotRetentionLimit",
            "DailySnapshotTime", "Endpoint", "ReaderEndpoint",
        ]
        for field in required_fields:
            assert field in sc, f"Missing field: {field}"


class TestScanMethod:
    """Tests for RegionScanner.scan() (Task 5.5)."""

    def test_scan_returns_region_result_with_both_types(self):
        """scan() combines results from both discovery methods."""
        session = MagicMock()
        config = _make_config()
        scanner = RegionScanner("us-east-1", session, config)

        rg = {"ReplicationGroupId": "rg-1", "Status": "available"}
        sc = {"ServerlessCacheName": "sc-1", "Status": "available"}

        scanner.ec_client.describe_replication_groups = MagicMock(
            return_value={"ReplicationGroups": [rg]}
        )
        scanner.ec_client.describe_serverless_caches = MagicMock(
            return_value={"ServerlessCaches": [sc]}
        )

        result = scanner.scan()

        assert isinstance(result, RegionResult)
        assert result.region == "us-east-1"
        assert result.rg_count == 1
        assert result.serverless_count == 1
        assert len(result.clusters) == 2
        assert result.errors == []

    def test_scan_handles_rg_discovery_failure(self):
        """scan() records errors when RG discovery fails."""
        session = MagicMock()
        config = _make_config()
        scanner = RegionScanner("us-east-1", session, config)

        scanner.ec_client.describe_replication_groups = MagicMock(
            side_effect=_make_client_error("AccessDeniedException")
        )
        scanner.ec_client.describe_serverless_caches = MagicMock(
            return_value={"ServerlessCaches": [{"ServerlessCacheName": "sc-1"}]}
        )

        result = scanner.scan()

        assert result.rg_count == 0
        assert result.serverless_count == 1
        assert len(result.errors) == 1
        assert "DescribeReplicationGroups failed" in result.errors[0]

    def test_scan_handles_serverless_discovery_failure(self):
        """scan() records errors when serverless discovery fails."""
        session = MagicMock()
        config = _make_config()
        scanner = RegionScanner("us-east-1", session, config)

        scanner.ec_client.describe_replication_groups = MagicMock(
            return_value={"ReplicationGroups": [{"ReplicationGroupId": "rg-1"}]}
        )
        scanner.ec_client.describe_serverless_caches = MagicMock(
            side_effect=_make_client_error("AccessDeniedException")
        )

        result = scanner.scan()

        assert result.rg_count == 1
        assert result.serverless_count == 0
        assert len(result.errors) == 1
        assert "DescribeServerlessCaches failed" in result.errors[0]

    def test_scan_handles_both_failures_gracefully(self):
        """scan() records errors for both and returns empty results."""
        session = MagicMock()
        config = _make_config()
        scanner = RegionScanner("us-east-1", session, config)

        scanner.ec_client.describe_replication_groups = MagicMock(
            side_effect=Exception("Network error")
        )
        scanner.ec_client.describe_serverless_caches = MagicMock(
            side_effect=Exception("Timeout")
        )

        result = scanner.scan()

        assert result.rg_count == 0
        assert result.serverless_count == 0
        assert len(result.clusters) == 0
        assert len(result.errors) == 2

    def test_scan_pagination_multiple_pages(self):
        """scan() handles multi-page responses via paginate_api."""
        session = MagicMock()
        config = _make_config()
        scanner = RegionScanner("us-east-1", session, config)

        # Simulate pagination: first call has Marker, second is final
        rg1 = {"ReplicationGroupId": "rg-1"}
        rg2 = {"ReplicationGroupId": "rg-2"}
        scanner.ec_client.describe_replication_groups = MagicMock(
            side_effect=[
                {"ReplicationGroups": [rg1], "Marker": "page2"},
                {"ReplicationGroups": [rg2]},
            ]
        )
        scanner.ec_client.describe_serverless_caches = MagicMock(
            return_value={"ServerlessCaches": []}
        )

        result = scanner.scan()

        assert result.rg_count == 2
        assert len(result.clusters) == 2
