"""Unit tests for ClusterEnricher (Tasks 6 and 7).

Tests cache cluster details retrieval, topology computation, endpoint extraction,
parameter group collection, security group inspection, subnet group resolution,
and tag collection.
"""

import os
import sys
from unittest.mock import MagicMock

from botocore.exceptions import ClientError

# Add scripts to path so we can import the module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from discover_inventory import ClusterEnricher


def _make_client_error(code, message="Error"):
    """Create a ClientError with the given error code."""
    return ClientError(
        {"Error": {"Code": code, "Message": message}},
        "TestOperation",
    )


def _make_enricher():
    """Create a ClusterEnricher with mock clients."""
    ec_client = MagicMock()
    ec2_client = MagicMock()
    return ClusterEnricher(ec_client, ec2_client, "us-east-1")


# ---------------------------------------------------------------------------
# Task 6.1: __init__
# ---------------------------------------------------------------------------


class TestClusterEnricherInit:
    """Tests for ClusterEnricher.__init__ (Task 6.1)."""

    def test_initializes_clients_and_region(self):
        ec_client = MagicMock()
        ec2_client = MagicMock()
        enricher = ClusterEnricher(ec_client, ec2_client, "eu-west-1")
        assert enricher.ec_client is ec_client
        assert enricher.ec2_client is ec2_client
        assert enricher.region == "eu-west-1"

    def test_initializes_empty_caches(self):
        enricher = _make_enricher()
        assert enricher._param_group_cache == {}
        assert enricher._security_group_cache == {}
        assert enricher._subnet_group_cache == {}


# ---------------------------------------------------------------------------
# Task 6.2: _get_cluster_details
# ---------------------------------------------------------------------------


class TestGetClusterDetails:
    """Tests for ClusterEnricher._get_cluster_details (Task 6.2)."""

    def test_extracts_all_fields(self):
        enricher = _make_enricher()
        enricher.ec_client.describe_cache_clusters = MagicMock(return_value={
            "CacheClusters": [{
                "CacheClusterId": "rg-001",
                "CacheNodeType": "cache.r7g.xlarge",
                "Engine": "redis",
                "EngineVersion": "7.1.0",
                "CacheParameterGroup": {
                    "CacheParameterGroupName": "custom-redis7",
                },
                "PreferredAvailabilityZone": "us-east-1a",
                "NumCacheNodes": 1,
                "SecurityGroups": [
                    {"SecurityGroupId": "sg-123", "Status": "active"},
                    {"SecurityGroupId": "sg-456", "Status": "active"},
                ],
                "CacheSubnetGroupName": "my-subnet-group",
            }]
        })
        result = enricher._get_cluster_details("rg-001")
        assert result["CacheNodeType"] == "cache.r7g.xlarge"
        assert result["Engine"] == "redis"
        assert result["EngineVersion"] == "7.1.0"
        assert result["CacheParameterGroupName"] == "custom-redis7"
        assert result["PreferredAvailabilityZone"] == "us-east-1a"
        assert result["NumCacheNodes"] == 1
        assert result["SecurityGroupIds"] == ["sg-123", "sg-456"]
        assert result["CacheSubnetGroupName"] == "my-subnet-group"

    def test_handles_empty_response(self):
        enricher = _make_enricher()
        enricher.ec_client.describe_cache_clusters = MagicMock(return_value={
            "CacheClusters": []
        })
        result = enricher._get_cluster_details("nonexistent")
        assert result == {}

    def test_calls_with_show_cache_node_info(self):
        enricher = _make_enricher()
        enricher.ec_client.describe_cache_clusters = MagicMock(return_value={
            "CacheClusters": [{"CacheClusterId": "rg-001"}]
        })
        enricher._get_cluster_details("rg-001")
        enricher.ec_client.describe_cache_clusters.assert_called_once_with(
            CacheClusterId="rg-001",
            ShowCacheNodeInfo=True,
        )

    def test_handles_not_found_fault_records_error(self):
        """Task 6.5: CacheClusterNotFoundFault records error and returns {}."""
        enricher = _make_enricher()
        enricher.ec_client.describe_cache_clusters = MagicMock(
            side_effect=_make_client_error("CacheClusterNotFoundFault")
        )
        errors = []
        result = enricher._get_cluster_details("missing-cluster", errors)
        assert result == {}
        assert len(errors) == 1
        assert "CacheClusterNotFoundFault" in errors[0]

    def test_handles_generic_exception(self):
        enricher = _make_enricher()
        enricher.ec_client.describe_cache_clusters = MagicMock(
            side_effect=Exception("Network error")
        )
        errors = []
        result = enricher._get_cluster_details("broken-cluster", errors)
        assert result == {}
        assert len(errors) == 1


# ---------------------------------------------------------------------------
# Task 6.3: Topology Computation
# ---------------------------------------------------------------------------


class TestComputeTopology:
    """Tests for ClusterEnricher.compute_topology (Task 6.3)."""

    def test_single_shard_with_replicas(self):
        rg = {"NodeGroups": [{"NodeGroupId": "0001", "NodeGroupMembers": [
            {"CacheClusterId": "rg-001", "CurrentRole": "primary"},
            {"CacheClusterId": "rg-002", "CurrentRole": "replica"},
            {"CacheClusterId": "rg-003", "CurrentRole": "replica"},
        ]}]}
        result = ClusterEnricher.compute_topology(rg)
        assert result["num_shards"] == 1
        assert result["num_replicas_per_shard"] == 2
        assert result["total_nodes"] == 3

    def test_multiple_shards(self):
        rg = {"NodeGroups": [
            {"NodeGroupId": "0001", "NodeGroupMembers": [
                {"CacheClusterId": "rg-001"}, {"CacheClusterId": "rg-002"},
            ]},
            {"NodeGroupId": "0002", "NodeGroupMembers": [
                {"CacheClusterId": "rg-003"}, {"CacheClusterId": "rg-004"},
            ]},
            {"NodeGroupId": "0003", "NodeGroupMembers": [
                {"CacheClusterId": "rg-005"}, {"CacheClusterId": "rg-006"},
            ]},
        ]}
        result = ClusterEnricher.compute_topology(rg)
        assert result["num_shards"] == 3
        assert result["num_replicas_per_shard"] == 1
        assert result["total_nodes"] == 6

    def test_empty_node_groups(self):
        result = ClusterEnricher.compute_topology({"NodeGroups": []})
        assert result["num_shards"] == 0
        assert result["num_replicas_per_shard"] == 0
        assert result["total_nodes"] == 0

    def test_single_node_no_replicas(self):
        rg = {"NodeGroups": [{"NodeGroupId": "0001", "NodeGroupMembers": [
            {"CacheClusterId": "rg-001", "CurrentRole": "primary"},
        ]}]}
        result = ClusterEnricher.compute_topology(rg)
        assert result["num_shards"] == 1
        assert result["num_replicas_per_shard"] == 0
        assert result["total_nodes"] == 1

    def test_missing_node_groups_key(self):
        result = ClusterEnricher.compute_topology({})
        assert result["num_shards"] == 0
        assert result["num_replicas_per_shard"] == 0
        assert result["total_nodes"] == 0


# ---------------------------------------------------------------------------
# Task 6.4: Endpoint Extraction
# ---------------------------------------------------------------------------


class TestExtractEndpoints:
    """Tests for ClusterEnricher.extract_endpoints (Task 6.4)."""

    def test_non_cluster_mode_endpoints(self):
        rg = {"NodeGroups": [{"NodeGroupId": "0001",
            "PrimaryEndpoint": {"Address": "my-rg.use1.cache.amazonaws.com", "Port": 6379},
            "ReaderEndpoint": {"Address": "my-rg-ro.use1.cache.amazonaws.com", "Port": 6379},
        }]}
        result = ClusterEnricher.extract_endpoints(rg)
        assert result["primary"] == "my-rg.use1.cache.amazonaws.com:6379"
        assert result["reader"] == "my-rg-ro.use1.cache.amazonaws.com:6379"

    def test_cluster_mode_uses_configuration_endpoint(self):
        rg = {
            "ConfigurationEndpoint": {"Address": "my-rg.clustercfg.use1.cache.amazonaws.com", "Port": 6379},
            "NodeGroups": [{"NodeGroupId": "0001",
                "PrimaryEndpoint": {"Address": "ignored.cache.amazonaws.com", "Port": 6379},
            }],
        }
        result = ClusterEnricher.extract_endpoints(rg)
        assert result["primary"] == "my-rg.clustercfg.use1.cache.amazonaws.com:6379"
        assert result["reader"] == "my-rg.clustercfg.use1.cache.amazonaws.com:6379"

    def test_empty_node_groups_returns_none(self):
        result = ClusterEnricher.extract_endpoints({"NodeGroups": []})
        assert result["primary"] is None
        assert result["reader"] is None

    def test_missing_reader_endpoint_returns_none(self):
        rg = {"NodeGroups": [{"NodeGroupId": "0001",
            "PrimaryEndpoint": {"Address": "my-rg.cache.amazonaws.com", "Port": 6379},
        }]}
        result = ClusterEnricher.extract_endpoints(rg)
        assert result["primary"] == "my-rg.cache.amazonaws.com:6379"
        assert result["reader"] is None


# ---------------------------------------------------------------------------
# Task 7.1: Parameter Groups
# ---------------------------------------------------------------------------


class TestGetParameterGroup:
    """Tests for ClusterEnricher._get_parameter_group (Task 7.1)."""

    def test_fetches_and_caches_target_parameters(self):
        enricher = _make_enricher()
        enricher.ec_client.describe_cache_parameters = MagicMock(return_value={
            "Parameters": [
                {"ParameterName": "maxmemory-policy", "ParameterValue": "volatile-lru"},
                {"ParameterName": "cluster-enabled", "ParameterValue": "yes"},
                {"ParameterName": "timeout", "ParameterValue": "300"},
                {"ParameterName": "tcp-keepalive", "ParameterValue": "300"},
                {"ParameterName": "activedefrag", "ParameterValue": "yes"},
                {"ParameterName": "reserved-memory-percent", "ParameterValue": "25"},
                {"ParameterName": "some-other-param", "ParameterValue": "ignored"},
            ]
        })
        result = enricher._get_parameter_group("custom-redis7")
        assert result["maxmemory-policy"] == "volatile-lru"
        assert result["cluster-enabled"] == "yes"
        assert result["timeout"] == "300"
        assert result["tcp-keepalive"] == "300"
        assert result["activedefrag"] == "yes"
        assert result["reserved-memory-percent"] == "25"
        assert "some-other-param" not in result

    def test_returns_cached_result_on_subsequent_call(self):
        enricher = _make_enricher()
        enricher.ec_client.describe_cache_parameters = MagicMock(return_value={
            "Parameters": [
                {"ParameterName": "maxmemory-policy", "ParameterValue": "allkeys-lru"},
            ]
        })
        result1 = enricher._get_parameter_group("pg-shared")
        result2 = enricher._get_parameter_group("pg-shared")
        assert result1 == result2
        assert enricher.ec_client.describe_cache_parameters.call_count == 1

    def test_handles_api_failure_gracefully(self):
        enricher = _make_enricher()
        enricher.ec_client.describe_cache_parameters = MagicMock(
            side_effect=_make_client_error("AccessDeniedException")
        )
        errors = []
        result = enricher._get_parameter_group("pg-denied", errors)
        assert result == {}
        assert "pg-denied" in enricher._param_group_cache
        assert len(errors) == 1

    def test_handles_pagination(self):
        enricher = _make_enricher()
        enricher.ec_client.describe_cache_parameters = MagicMock(side_effect=[
            {
                "Parameters": [
                    {"ParameterName": "maxmemory-policy", "ParameterValue": "noeviction"},
                    {"ParameterName": "timeout", "ParameterValue": "0"},
                ],
                "Marker": "page2",
            },
            {
                "Parameters": [
                    {"ParameterName": "cluster-enabled", "ParameterValue": "no"},
                ],
            },
        ])
        result = enricher._get_parameter_group("pg-paged")
        assert result["maxmemory-policy"] == "noeviction"
        assert result["timeout"] == "0"
        assert result["cluster-enabled"] == "no"


# ---------------------------------------------------------------------------
# Task 7.2: Security Groups
# ---------------------------------------------------------------------------


class TestGetSecurityGroups:
    """Tests for ClusterEnricher._get_security_groups (Task 7.2)."""

    def test_detects_permissive_ipv4_rule(self):
        enricher = _make_enricher()
        enricher.ec2_client.describe_security_groups = MagicMock(return_value={
            "SecurityGroups": [{"GroupId": "sg-123", "IpPermissions": [{
                "FromPort": 6379, "ToPort": 6379,
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}], "Ipv6Ranges": [],
            }]}]
        })
        result = enricher._get_security_groups(["sg-123"])
        assert len(result) == 1
        assert result[0]["group_id"] == "sg-123"
        assert "0.0.0.0/0 on ports 6379-6379" in result[0]["permissive_rules"]

    def test_detects_permissive_ipv6_rule(self):
        enricher = _make_enricher()
        enricher.ec2_client.describe_security_groups = MagicMock(return_value={
            "SecurityGroups": [{"GroupId": "sg-456", "IpPermissions": [{
                "FromPort": 6379, "ToPort": 6379,
                "IpRanges": [], "Ipv6Ranges": [{"CidrIpv6": "::/0"}],
            }]}]
        })
        result = enricher._get_security_groups(["sg-456"])
        assert "::/0 on ports 6379-6379" in result[0]["permissive_rules"]

    def test_detects_broad_prefix_cidr(self):
        enricher = _make_enricher()
        enricher.ec2_client.describe_security_groups = MagicMock(return_value={
            "SecurityGroups": [{"GroupId": "sg-789", "IpPermissions": [{
                "FromPort": 6379, "ToPort": 6379,
                "IpRanges": [{"CidrIp": "10.0.0.0/8"}], "Ipv6Ranges": [],
            }]}]
        })
        result = enricher._get_security_groups(["sg-789"])
        assert "10.0.0.0/8 on ports 6379-6379" in result[0]["permissive_rules"]

    def test_does_not_flag_specific_cidr(self):
        enricher = _make_enricher()
        enricher.ec2_client.describe_security_groups = MagicMock(return_value={
            "SecurityGroups": [{"GroupId": "sg-safe", "IpPermissions": [{
                "FromPort": 6379, "ToPort": 6379,
                "IpRanges": [{"CidrIp": "10.0.0.0/16"}], "Ipv6Ranges": [],
            }]}]
        })
        result = enricher._get_security_groups(["sg-safe"])
        assert result[0]["permissive_rules"] == []

    def test_caches_security_group_results(self):
        enricher = _make_enricher()
        enricher.ec2_client.describe_security_groups = MagicMock(return_value={
            "SecurityGroups": [{"GroupId": "sg-cached", "IpPermissions": []}]
        })
        enricher._get_security_groups(["sg-cached"])
        enricher._get_security_groups(["sg-cached"])
        assert enricher.ec2_client.describe_security_groups.call_count == 1

    def test_handles_api_failure_gracefully(self):
        enricher = _make_enricher()
        enricher.ec2_client.describe_security_groups = MagicMock(
            side_effect=_make_client_error("AccessDeniedException")
        )
        errors = []
        result = enricher._get_security_groups(["sg-denied"], errors)
        assert len(result) == 1
        assert result[0]["group_id"] == "sg-denied"
        assert result[0]["permissive_rules"] == []
        assert len(errors) == 1

    def test_multiple_sgs_some_cached(self):
        enricher = _make_enricher()
        enricher._security_group_cache["sg-old"] = {
            "group_id": "sg-old",
            "permissive_rules": ["0.0.0.0/0 on ports 6379-6379"],
        }
        enricher.ec2_client.describe_security_groups = MagicMock(return_value={
            "SecurityGroups": [{"GroupId": "sg-new", "IpPermissions": []}]
        })
        result = enricher._get_security_groups(["sg-old", "sg-new"])
        assert len(result) == 2
        assert result[0]["permissive_rules"] == ["0.0.0.0/0 on ports 6379-6379"]
        assert result[1]["permissive_rules"] == []
        enricher.ec2_client.describe_security_groups.assert_called_once_with(
            GroupIds=["sg-new"]
        )


# ---------------------------------------------------------------------------
# Task 7.3: Subnet Groups
# ---------------------------------------------------------------------------


class TestGetSubnetGroup:
    """Tests for ClusterEnricher._get_subnet_group (Task 7.3)."""

    def test_extracts_subnets_and_azs(self):
        enricher = _make_enricher()
        enricher.ec_client.describe_cache_subnet_groups = MagicMock(return_value={
            "CacheSubnetGroups": [{
                "CacheSubnetGroupName": "my-subnet-group",
                "Subnets": [
                    {"SubnetIdentifier": "subnet-a", "SubnetAvailabilityZone": {"Name": "us-east-1a"}},
                    {"SubnetIdentifier": "subnet-b", "SubnetAvailabilityZone": {"Name": "us-east-1b"}},
                    {"SubnetIdentifier": "subnet-c", "SubnetAvailabilityZone": {"Name": "us-east-1c"}},
                ],
            }]
        })
        result = enricher._get_subnet_group("my-subnet-group")
        assert result["name"] == "my-subnet-group"
        assert result["subnets"] == ["subnet-a", "subnet-b", "subnet-c"]
        assert result["az_count"] == 3
        assert set(result["availability_zones"]) == {"us-east-1a", "us-east-1b", "us-east-1c"}

    def test_computes_distinct_az_count(self):
        """Multiple subnets in same AZ should not inflate az_count."""
        enricher = _make_enricher()
        enricher.ec_client.describe_cache_subnet_groups = MagicMock(return_value={
            "CacheSubnetGroups": [{
                "CacheSubnetGroupName": "dup-az-group",
                "Subnets": [
                    {"SubnetIdentifier": "subnet-1", "SubnetAvailabilityZone": {"Name": "us-east-1a"}},
                    {"SubnetIdentifier": "subnet-2", "SubnetAvailabilityZone": {"Name": "us-east-1a"}},
                    {"SubnetIdentifier": "subnet-3", "SubnetAvailabilityZone": {"Name": "us-east-1b"}},
                ],
            }]
        })
        result = enricher._get_subnet_group("dup-az-group")
        assert result["az_count"] == 2

    def test_caches_subnet_group_results(self):
        enricher = _make_enricher()
        enricher.ec_client.describe_cache_subnet_groups = MagicMock(return_value={
            "CacheSubnetGroups": [{
                "CacheSubnetGroupName": "sg-cached",
                "Subnets": [
                    {"SubnetIdentifier": "subnet-x", "SubnetAvailabilityZone": {"Name": "us-east-1a"}},
                ],
            }]
        })
        enricher._get_subnet_group("sg-cached")
        enricher._get_subnet_group("sg-cached")
        assert enricher.ec_client.describe_cache_subnet_groups.call_count == 1

    def test_handles_api_failure_gracefully(self):
        enricher = _make_enricher()
        enricher.ec_client.describe_cache_subnet_groups = MagicMock(
            side_effect=_make_client_error("CacheSubnetGroupNotFoundFault")
        )
        errors = []
        result = enricher._get_subnet_group("missing-group", errors)
        assert result["name"] == "missing-group"
        assert result["subnets"] == []
        assert result["az_count"] == 0
        assert len(errors) == 1


# ---------------------------------------------------------------------------
# Task 7.4: Tags
# ---------------------------------------------------------------------------


class TestGetTags:
    """Tests for ClusterEnricher._get_tags (Task 7.4)."""

    def test_converts_tag_list_to_dict(self):
        enricher = _make_enricher()
        enricher.ec_client.list_tags_for_resource = MagicMock(return_value={
            "TagList": [
                {"Key": "Environment", "Value": "production"},
                {"Key": "Owner", "Value": "team-cache"},
                {"Key": "Application", "Value": "my-app"},
            ]
        })
        arn = "arn:aws:elasticache:us-east-1:123:replicationgroup:rg-1"
        result = enricher._get_tags(arn)
        assert result == {
            "Environment": "production",
            "Owner": "team-cache",
            "Application": "my-app",
        }

    def test_returns_empty_dict_on_client_error(self):
        enricher = _make_enricher()
        enricher.ec_client.list_tags_for_resource = MagicMock(
            side_effect=_make_client_error("AccessDeniedException")
        )
        errors = []
        arn = "arn:aws:elasticache:us-east-1:123:replicationgroup:rg-1"
        result = enricher._get_tags(arn, errors)
        assert result == {}
        assert len(errors) == 1
        assert "ListTagsForResource failed" in errors[0]

    def test_returns_empty_dict_on_generic_exception(self):
        enricher = _make_enricher()
        enricher.ec_client.list_tags_for_resource = MagicMock(
            side_effect=Exception("Unexpected error")
        )
        errors = []
        arn = "arn:aws:elasticache:us-east-1:123:replicationgroup:rg-1"
        result = enricher._get_tags(arn, errors)
        assert result == {}
        assert len(errors) == 1

    def test_handles_empty_tag_list(self):
        enricher = _make_enricher()
        enricher.ec_client.list_tags_for_resource = MagicMock(return_value={
            "TagList": []
        })
        result = enricher._get_tags("arn:aws:elasticache:us-east-1:123:replicationgroup:rg-1")
        assert result == {}


# ---------------------------------------------------------------------------
# Task 7.2 helper: _is_permissive_cidr
# ---------------------------------------------------------------------------


class TestIsPermissiveCidr:
    """Tests for ClusterEnricher._is_permissive_cidr."""

    def test_ipv4_all_traffic(self):
        assert ClusterEnricher._is_permissive_cidr("0.0.0.0/0") is True

    def test_ipv6_all_traffic(self):
        assert ClusterEnricher._is_permissive_cidr("::/0") is True

    def test_prefix_8_is_permissive(self):
        assert ClusterEnricher._is_permissive_cidr("10.0.0.0/8") is True

    def test_prefix_0_is_permissive(self):
        assert ClusterEnricher._is_permissive_cidr("192.0.0.0/0") is True

    def test_prefix_9_is_not_permissive(self):
        assert ClusterEnricher._is_permissive_cidr("10.0.0.0/9") is False

    def test_prefix_16_is_not_permissive(self):
        assert ClusterEnricher._is_permissive_cidr("10.0.0.0/16") is False

    def test_prefix_24_is_not_permissive(self):
        assert ClusterEnricher._is_permissive_cidr("192.168.1.0/24") is False

    def test_prefix_32_is_not_permissive(self):
        assert ClusterEnricher._is_permissive_cidr("10.0.0.1/32") is False


# ---------------------------------------------------------------------------
# Task 7.5: Error handling - all methods record errors gracefully
# ---------------------------------------------------------------------------


class TestErrorHandling:
    """Tests for Task 7.5: All enrichment methods record errors."""

    def test_get_parameter_group_generic_exception(self):
        enricher = _make_enricher()
        enricher.ec_client.describe_cache_parameters = MagicMock(
            side_effect=Exception("Connection reset")
        )
        errors = []
        result = enricher._get_parameter_group("pg-broken", errors)
        assert result == {}
        assert len(errors) == 1
        assert "Connection reset" in errors[0]

    def test_get_subnet_group_generic_exception(self):
        enricher = _make_enricher()
        enricher.ec_client.describe_cache_subnet_groups = MagicMock(
            side_effect=Exception("Boom")
        )
        errors = []
        result = enricher._get_subnet_group("sg-broken", errors)
        assert result["name"] == "sg-broken"
        assert result["subnets"] == []
        assert result["az_count"] == 0
        assert len(errors) == 1


# ---------------------------------------------------------------------------
# Task 8.1: enrich_replication_group
# ---------------------------------------------------------------------------


class TestEnrichReplicationGroup:
    """Tests for ClusterEnricher.enrich_replication_group (Task 8.1)."""

    def _make_rg(self, **overrides):
        """Create a minimal replication group dict."""
        rg = {
            "ReplicationGroupId": "my-rg",
            "Status": "available",
            "ARN": "arn:aws:elasticache:us-east-1:123456:replicationgroup:my-rg",
            "TransitEncryptionEnabled": True,
            "AtRestEncryptionEnabled": True,
            "MultiAZ": "enabled",
            "AutomaticFailover": "enabled",
            "ClusterEnabled": True,
            "SnapshotRetentionLimit": 7,
            "AuthTokenEnabled": False,
            "UserGroupIds": ["ug-rbac"],
            "MemberClusters": ["my-rg-001"],
            "NodeGroups": [
                {
                    "NodeGroupId": "0001",
                    "NodeGroupMembers": [
                        {"CacheClusterId": "my-rg-001", "CurrentRole": "primary"},
                        {"CacheClusterId": "my-rg-002", "CurrentRole": "replica"},
                    ],
                    "PrimaryEndpoint": {"Address": "my-rg.use1.cache.amazonaws.com", "Port": 6379},
                    "ReaderEndpoint": {"Address": "my-rg-ro.use1.cache.amazonaws.com", "Port": 6379},
                }
            ],
        }
        rg.update(overrides)
        return rg

    def _setup_enricher_mocks(self, enricher):
        """Configure mock clients with successful responses."""
        enricher.ec_client.describe_cache_clusters = MagicMock(return_value={
            "CacheClusters": [{
                "CacheClusterId": "my-rg-001",
                "CacheNodeType": "cache.r7g.xlarge",
                "Engine": "redis",
                "EngineVersion": "7.1.0",
                "CacheParameterGroup": {"CacheParameterGroupName": "custom-redis7"},
                "PreferredAvailabilityZone": "us-east-1a",
                "NumCacheNodes": 1,
                "SecurityGroups": [{"SecurityGroupId": "sg-123", "Status": "active"}],
                "CacheSubnetGroupName": "my-subnet-group",
            }]
        })
        enricher.ec_client.describe_cache_parameters = MagicMock(return_value={
            "Parameters": [
                {"ParameterName": "maxmemory-policy", "ParameterValue": "volatile-lru"},
            ]
        })
        enricher.ec2_client.describe_security_groups = MagicMock(return_value={
            "SecurityGroups": [{"GroupId": "sg-123", "IpPermissions": []}]
        })
        enricher.ec_client.describe_cache_subnet_groups = MagicMock(return_value={
            "CacheSubnetGroups": [{
                "CacheSubnetGroupName": "my-subnet-group",
                "Subnets": [
                    {"SubnetIdentifier": "subnet-a", "SubnetAvailabilityZone": {"Name": "us-east-1a"}},
                    {"SubnetIdentifier": "subnet-b", "SubnetAvailabilityZone": {"Name": "us-east-1b"}},
                ],
            }]
        })
        enricher.ec_client.list_tags_for_resource = MagicMock(return_value={
            "TagList": [{"Key": "Environment", "Value": "production"}]
        })

    def test_returns_complete_normalized_dict(self):
        enricher = _make_enricher()
        self._setup_enricher_mocks(enricher)
        rg = self._make_rg()

        result = enricher.enrich_replication_group(rg)

        # Base fields
        assert result["cluster_id"] == "my-rg"
        assert result["cluster_type"] == "node-based"
        assert result["region"] == "us-east-1"
        assert result["engine"] == "redis"
        assert result["engine_version"] == "7.1.0"
        assert result["status"] == "available"
        assert result["arn"] == "arn:aws:elasticache:us-east-1:123456:replicationgroup:my-rg"
        assert result["tls_enabled"] is True
        assert result["auth_mode"] == "RBAC"
        assert result["encryption_at_rest"] is True
        assert result["multi_az"] is True
        assert result["snapshot_retention_days"] == 7
        assert result["tags"] == {"Environment": "production"}
        assert result["errors"] == []

        # Node-based specific fields
        assert result["node_type"] == "cache.r7g.xlarge"
        assert result["num_shards"] == 1
        assert result["num_replicas_per_shard"] == 1
        assert result["total_nodes"] == 2
        assert result["cluster_mode_enabled"] is True
        assert result["automatic_failover"] is True
        assert result["parameter_group"] == "custom-redis7"
        assert result["parameters"] == {"maxmemory-policy": "volatile-lru"}
        assert result["subnet_group"]["name"] == "my-subnet-group"
        assert result["endpoints"]["primary"] == "my-rg.use1.cache.amazonaws.com:6379"
        assert result["endpoints"]["reader"] == "my-rg-ro.use1.cache.amazonaws.com:6379"

    def test_calls_get_cluster_details_for_first_member(self):
        enricher = _make_enricher()
        self._setup_enricher_mocks(enricher)
        rg = self._make_rg(MemberClusters=["first-member", "second-member"])

        enricher.enrich_replication_group(rg)

        enricher.ec_client.describe_cache_clusters.assert_called_once_with(
            CacheClusterId="first-member",
            ShowCacheNodeInfo=True,
        )

    def test_handles_empty_member_clusters(self):
        enricher = _make_enricher()
        self._setup_enricher_mocks(enricher)
        rg = self._make_rg(MemberClusters=[])

        result = enricher.enrich_replication_group(rg)

        assert result["node_type"] is None
        assert result["parameter_group"] is None
        assert result["parameters"] == {}

    def test_collects_enrichment_errors(self):
        """Task 8.5: errors from enrichment calls go into the errors array."""
        enricher = _make_enricher()
        enricher.ec_client.describe_cache_clusters = MagicMock(
            side_effect=_make_client_error("CacheClusterNotFoundFault")
        )
        enricher.ec_client.list_tags_for_resource = MagicMock(
            side_effect=Exception("Tag fetch failed")
        )
        rg = self._make_rg()

        result = enricher.enrich_replication_group(rg)

        assert len(result["errors"]) >= 1
        assert any("CacheClusterNotFoundFault" in e for e in result["errors"])

    def test_tls_disabled(self):
        enricher = _make_enricher()
        self._setup_enricher_mocks(enricher)
        rg = self._make_rg(TransitEncryptionEnabled=False)

        result = enricher.enrich_replication_group(rg)
        assert result["tls_enabled"] is False

    def test_multi_az_disabled(self):
        enricher = _make_enricher()
        self._setup_enricher_mocks(enricher)
        rg = self._make_rg(MultiAZ="disabled")

        result = enricher.enrich_replication_group(rg)
        assert result["multi_az"] is False

    def test_automatic_failover_disabled(self):
        enricher = _make_enricher()
        self._setup_enricher_mocks(enricher)
        rg = self._make_rg(AutomaticFailover="disabled")

        result = enricher.enrich_replication_group(rg)
        assert result["automatic_failover"] is False

    def test_collects_log_delivery_from_the_replication_group(self):
        # OE-06: the group-level LogDeliveryConfigurations is authoritative and
        # is normalized to snake_case, exactly as the rest of the record is.
        enricher = _make_enricher()
        self._setup_enricher_mocks(enricher)
        rg = self._make_rg(LogDeliveryConfigurations=[{
            "LogType": "engine-log",
            "DestinationType": "cloudwatch-logs",
            "DestinationDetails": {
                "CloudWatchLogsDetails": {"LogGroup": "my-ec-log"}},
            "LogFormat": "json",
            "Status": "active",
        }])

        result = enricher.enrich_replication_group(rg)

        assert result["log_delivery"] == [{
            "log_type": "engine-log",
            "destination_type": "cloudwatch-logs",
            "destination_details": {
                "CloudWatchLogsDetails": {"LogGroup": "my-ec-log"}},
            "log_format": "json",
            "status": "active",
        }]

    def test_log_delivery_is_empty_list_when_group_has_none(self):
        # No LogDeliveryConfigurations means log delivery is simply not
        # configured -- an empty list, which OE-06a/OE-06b report.
        enricher = _make_enricher()
        self._setup_enricher_mocks(enricher)
        result = enricher.enrich_replication_group(self._make_rg())
        assert result["log_delivery"] == []

    def test_node_level_boolean_is_corroboration_not_the_config(self):
        # When the config is inherited, the group array is authoritative and the
        # per-node boolean is carried only as corroboration.
        enricher = _make_enricher()
        self._setup_enricher_mocks(enricher)
        enricher.ec_client.describe_cache_clusters = MagicMock(return_value={
            "CacheClusters": [{
                "CacheClusterId": "my-rg-001",
                "CacheNodeType": "cache.r7g.xlarge",
                "Engine": "redis",
                "EngineVersion": "7.1.0",
                "CacheParameterGroup": {"CacheParameterGroupName": "p"},
                "NumCacheNodes": 1,
                "SecurityGroups": [],
                "CacheSubnetGroupName": "my-subnet-group",
                # Node reports an EMPTY array but signals inheritance here.
                "ReplicationGroupLogDeliveryEnabled": True,
            }]
        })
        rg = self._make_rg(LogDeliveryConfigurations=[{
            "LogType": "slow-log", "DestinationType": "cloudwatch-logs",
            "DestinationDetails": {}, "LogFormat": "json", "Status": "active",
        }])

        result = enricher.enrich_replication_group(rg)

        assert result["replication_group_log_delivery_enabled"] is True
        assert [e["log_type"] for e in result["log_delivery"]] == ["slow-log"]


# ---------------------------------------------------------------------------
# Task 8.2: enrich_serverless_cache
# ---------------------------------------------------------------------------


class TestEnrichServerlessCache:
    """Tests for ClusterEnricher.enrich_serverless_cache (Task 8.2)."""

    def _make_serverless_cache(self, **overrides):
        """Create a minimal serverless cache dict."""
        cache = {
            "ServerlessCacheName": "my-serverless",
            "Status": "available",
            "Engine": "valkey",
            "MajorEngineVersion": "8",
            "ARN": "arn:aws:elasticache:us-east-1:123456:serverlesscache:my-serverless",
            "SecurityGroupIds": ["sg-456"],
            "UserGroupId": "ug-serverless",
            "SnapshotRetentionLimit": 1,
            "CacheUsageLimits": {
                "DataStorage": {"Maximum": 5, "Unit": "GB"},
                "ECPUPerSecond": {"Maximum": 15000},
            },
            "Endpoint": {"Address": "my-sc.serverless.use1.cache.amazonaws.com", "Port": 6379},
            "ReaderEndpoint": {"Address": "my-sc.serverless.use1.cache.amazonaws.com", "Port": 6380},
        }
        cache.update(overrides)
        return cache

    def _setup_enricher_mocks(self, enricher):
        """Configure mock clients with successful responses."""
        enricher.ec2_client.describe_security_groups = MagicMock(return_value={
            "SecurityGroups": [{"GroupId": "sg-456", "IpPermissions": []}]
        })
        enricher.ec_client.list_tags_for_resource = MagicMock(return_value={
            "TagList": [{"Key": "Team", "Value": "platform"}]
        })

    def test_returns_complete_normalized_dict(self):
        enricher = _make_enricher()
        self._setup_enricher_mocks(enricher)
        cache = self._make_serverless_cache()

        result = enricher.enrich_serverless_cache(cache)

        # Base fields
        assert result["cluster_id"] == "my-serverless"
        assert result["cluster_type"] == "serverless"
        assert result["region"] == "us-east-1"
        assert result["engine"] == "valkey"
        assert result["engine_version"] == "8"
        assert result["status"] == "available"
        assert result["arn"] == "arn:aws:elasticache:us-east-1:123456:serverlesscache:my-serverless"
        assert result["tls_enabled"] is True
        assert result["auth_mode"] == "RBAC"
        assert result["encryption_at_rest"] is True
        assert result["multi_az"] is True
        assert result["snapshot_retention_days"] == 1
        assert result["tags"] == {"Team": "platform"}
        assert result["security_groups"] == [{"group_id": "sg-456", "permissive_rules": []}]
        assert result["errors"] == []

        # Serverless specific fields
        assert result["cache_usage_limits"] == {
            "data_storage": {"maximum": 5, "unit": "GB"},
            "ecpu_per_second": {"maximum": 15000},
        }
        assert result["endpoints"]["primary"] == "my-sc.serverless.use1.cache.amazonaws.com:6379"
        assert result["endpoints"]["reader"] == "my-sc.serverless.use1.cache.amazonaws.com:6380"

    def test_serverless_always_tls_encrypted_multiaz(self):
        """Serverless caches always have TLS, encryption at rest, and multi-AZ."""
        enricher = _make_enricher()
        self._setup_enricher_mocks(enricher)
        cache = self._make_serverless_cache()

        result = enricher.enrich_serverless_cache(cache)

        assert result["tls_enabled"] is True
        assert result["encryption_at_rest"] is True
        assert result["multi_az"] is True

    def test_handles_missing_endpoints(self):
        enricher = _make_enricher()
        self._setup_enricher_mocks(enricher)
        cache = self._make_serverless_cache()
        del cache["Endpoint"]
        del cache["ReaderEndpoint"]

        result = enricher.enrich_serverless_cache(cache)

        assert result["endpoints"]["primary"] is None
        assert result["endpoints"]["reader"] is None

    def test_handles_empty_security_group_ids(self):
        enricher = _make_enricher()
        self._setup_enricher_mocks(enricher)
        cache = self._make_serverless_cache(SecurityGroupIds=[])

        result = enricher.enrich_serverless_cache(cache)

        assert result["security_groups"] == []

    def test_collects_enrichment_errors(self):
        """Task 8.5: errors from enrichment calls go into the errors array."""
        enricher = _make_enricher()
        enricher.ec2_client.describe_security_groups = MagicMock(
            side_effect=_make_client_error("AccessDeniedException")
        )
        enricher.ec_client.list_tags_for_resource = MagicMock(
            side_effect=Exception("Tag error")
        )
        cache = self._make_serverless_cache()

        result = enricher.enrich_serverless_cache(cache)

        assert len(result["errors"]) >= 1

    def test_handles_missing_cache_usage_limits(self):
        enricher = _make_enricher()
        self._setup_enricher_mocks(enricher)
        cache = self._make_serverless_cache()
        del cache["CacheUsageLimits"]

        result = enricher.enrich_serverless_cache(cache)

        assert result["cache_usage_limits"] == {
            "data_storage": {"maximum": None, "unit": "GB"},
            "ecpu_per_second": {"maximum": None},
        }

    def test_serverless_omits_log_delivery_fields(self):
        # OE-06: serverless does not support log delivery, so the record omits
        # the fields entirely rather than carrying an empty/false value that
        # would read as "configured off". applies_to=("node-based",) skips it.
        enricher = _make_enricher()
        self._setup_enricher_mocks(enricher)
        result = enricher.enrich_serverless_cache(self._make_serverless_cache())
        assert "log_delivery" not in result
        assert "replication_group_log_delivery_enabled" not in result


# ---------------------------------------------------------------------------
# Task 8.3: auth_mode determination
# ---------------------------------------------------------------------------


class TestDetermineAuthMode:
    """Tests for ClusterEnricher._determine_auth_mode (Task 8.3)."""

    def test_rbac_from_user_group_ids(self):
        """UserGroupIds non-empty → RBAC."""
        resource = {"UserGroupIds": ["ug-1"], "AuthTokenEnabled": False}
        assert ClusterEnricher._determine_auth_mode(resource) == "RBAC"

    def test_rbac_from_user_group_id_singular(self):
        """UserGroupId (singular, serverless) → RBAC."""
        resource = {"UserGroupId": "ug-serverless", "AuthTokenEnabled": False}
        assert ClusterEnricher._determine_auth_mode(resource) == "RBAC"

    def test_auth_token_when_no_user_groups(self):
        """AuthTokenEnabled with empty UserGroupIds → AUTH-token."""
        resource = {"UserGroupIds": [], "AuthTokenEnabled": True}
        assert ClusterEnricher._determine_auth_mode(resource) == "AUTH-token"

    def test_none_when_nothing_set(self):
        """Neither UserGroupIds nor AuthTokenEnabled → none."""
        resource = {"UserGroupIds": [], "AuthTokenEnabled": False}
        assert ClusterEnricher._determine_auth_mode(resource) == "none"

    def test_none_when_fields_missing(self):
        """Completely missing fields → none."""
        resource = {}
        assert ClusterEnricher._determine_auth_mode(resource) == "none"

    def test_rbac_takes_priority_over_auth_token(self):
        """If both UserGroupIds and AuthTokenEnabled are set, RBAC wins."""
        resource = {"UserGroupIds": ["ug-1"], "AuthTokenEnabled": True}
        assert ClusterEnricher._determine_auth_mode(resource) == "RBAC"


# ---------------------------------------------------------------------------
# Task 8.4: All base fields always present
# ---------------------------------------------------------------------------


class TestBaseFieldsAlwaysPresent:
    """Tests for Task 8.4: all base fields present even with missing data."""

    _REQUIRED_BASE_FIELDS = {
        "cluster_id", "cluster_type", "region", "engine", "engine_version",
        "status", "arn", "tls_enabled", "auth_mode", "encryption_at_rest",
        "multi_az", "snapshot_retention_days", "tags", "security_groups", "errors",
    }

    _NODE_BASED_EXTRA_FIELDS = {
        "node_type", "num_shards", "num_replicas_per_shard", "total_nodes",
        "cluster_mode_enabled", "automatic_failover", "parameter_group",
        "parameters", "subnet_group", "availability_zones", "endpoints",
    }

    _SERVERLESS_EXTRA_FIELDS = {
        "cache_usage_limits", "endpoints",
    }

    def test_node_based_has_all_base_fields(self):
        enricher = _make_enricher()
        # Minimal RG with minimal mocks
        enricher.ec_client.describe_cache_clusters = MagicMock(return_value={"CacheClusters": []})
        enricher.ec_client.list_tags_for_resource = MagicMock(return_value={"TagList": []})
        rg = {
            "ReplicationGroupId": "rg-minimal",
            "Status": "available",
            "ARN": "arn:aws:elasticache:us-east-1:123:replicationgroup:rg-minimal",
            "MemberClusters": [],
            "NodeGroups": [],
        }

        result = enricher.enrich_replication_group(rg)

        for field in self._REQUIRED_BASE_FIELDS:
            assert field in result, f"Missing base field: {field}"
        for field in self._NODE_BASED_EXTRA_FIELDS:
            assert field in result, f"Missing node-based field: {field}"

    def test_serverless_has_all_base_fields(self):
        enricher = _make_enricher()
        enricher.ec_client.list_tags_for_resource = MagicMock(return_value={"TagList": []})
        cache = {
            "ServerlessCacheName": "sc-minimal",
            "Status": "available",
            "Engine": "redis",
            "MajorEngineVersion": "7",
            "ARN": "arn:aws:elasticache:us-east-1:123:serverlesscache:sc-minimal",
            "SecurityGroupIds": [],
        }

        result = enricher.enrich_serverless_cache(cache)

        for field in self._REQUIRED_BASE_FIELDS:
            assert field in result, f"Missing base field: {field}"
        for field in self._SERVERLESS_EXTRA_FIELDS:
            assert field in result, f"Missing serverless field: {field}"

    def test_node_based_defaults_for_missing_data(self):
        """When no member clusters are found, fields use sensible defaults."""
        enricher = _make_enricher()
        enricher.ec_client.describe_cache_clusters = MagicMock(return_value={"CacheClusters": []})
        enricher.ec_client.list_tags_for_resource = MagicMock(return_value={"TagList": []})
        rg = {
            "ReplicationGroupId": "rg-empty",
            "Status": "creating",
            "ARN": "arn:aws:elasticache:us-east-1:123:replicationgroup:rg-empty",
            "MemberClusters": ["rg-empty-001"],
            "NodeGroups": [],
        }

        result = enricher.enrich_replication_group(rg)

        assert result["node_type"] is None
        assert result["parameter_group"] is None
        assert result["parameters"] == {}
        assert result["security_groups"] == []
        assert result["subnet_group"]["subnets"] == []
        assert result["tls_enabled"] is False
        assert result["encryption_at_rest"] is False
        assert result["multi_az"] is False
        assert result["snapshot_retention_days"] == 0
