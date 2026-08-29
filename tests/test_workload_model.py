"""Unit tests for WorkloadModel (Task 5).

Tests workload classification from command-mix distribution, percentage
computation, read/write ratio, and edge cases (all-zero, missing data).
"""

import os
import sys

# Add scripts to path so we can import the module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from analyze_metrics import WorkloadModel


def _make_cluster_data(family_values: dict, get_cmds: float = 0.0, set_cmds: float = 0.0) -> dict:
    """Helper to build cluster_data with command family metrics.

    Args:
        family_values: Dict mapping metric name to total sum value.
            Values are placed in a single node's Sum time-series.
        get_cmds: Total GetTypeCmds value.
        set_cmds: Total SetTypeCmds value.

    Returns:
        A cluster_data dict matching the Stage 2 metrics.json structure.
    """
    metrics = {}
    for metric_name, total in family_values.items():
        metrics[metric_name] = {
            "Sum": {
                "timestamps": ["2024-01-01T00:00:00Z"],
                "values": [total],
            }
        }
    # Add GetTypeCmds and SetTypeCmds
    metrics["GetTypeCmds"] = {
        "Sum": {
            "timestamps": ["2024-01-01T00:00:00Z"],
            "values": [get_cmds],
        }
    }
    metrics["SetTypeCmds"] = {
        "Sum": {
            "timestamps": ["2024-01-01T00:00:00Z"],
            "values": [set_cmds],
        }
    }

    return {
        "nodes": {
            "node-001": {
                "metrics": metrics,
            }
        }
    }


class TestWorkloadModelCacheAside:
    """Test cache-aside classification: string >60% AND R/W >4."""

    def test_cache_aside_basic(self):
        model = WorkloadModel()
        cluster_data = _make_cluster_data(
            {
                "StringBasedCmds": 7000.0,
                "HashBasedCmds": 1000.0,
                "SortedSetBasedCmds": 500.0,
                "ListBasedCmds": 500.0,
                "SetBasedCmds": 500.0,
                "StreamBasedCmds": 300.0,
                "PubSubBasedCmds": 200.0,
            },
            get_cmds=8000.0,
            set_cmds=1000.0,
        )
        result = model.compute(cluster_data, None)
        assert result["workload_class"] == "cache-aside"
        assert result["read_write_ratio"] == 8.0
        assert result["dominant_family"] == "StringBasedCmds"


class TestWorkloadModelSessionStore:
    """Test session-store classification: hash >35% AND R/W 1-4."""

    def test_session_store_basic(self):
        model = WorkloadModel()
        cluster_data = _make_cluster_data(
            {
                "StringBasedCmds": 2000.0,
                "HashBasedCmds": 5000.0,
                "SortedSetBasedCmds": 500.0,
                "ListBasedCmds": 500.0,
                "SetBasedCmds": 500.0,
                "StreamBasedCmds": 300.0,
                "PubSubBasedCmds": 200.0,
            },
            get_cmds=3000.0,
            set_cmds=1500.0,
        )
        result = model.compute(cluster_data, None)
        assert result["workload_class"] == "session-store"
        assert result["dominant_family"] == "HashBasedCmds"


class TestWorkloadModelLeaderboard:
    """Test leaderboard classification: sorted_set >25%."""

    def test_leaderboard_basic(self):
        model = WorkloadModel()
        cluster_data = _make_cluster_data(
            {
                "StringBasedCmds": 2000.0,
                "HashBasedCmds": 1000.0,
                "SortedSetBasedCmds": 4000.0,
                "ListBasedCmds": 500.0,
                "SetBasedCmds": 500.0,
                "StreamBasedCmds": 500.0,
                "PubSubBasedCmds": 500.0,
            },
            get_cmds=5000.0,
            set_cmds=4000.0,
        )
        result = model.compute(cluster_data, None)
        assert result["workload_class"] == "leaderboard"
        assert result["dominant_family"] == "SortedSetBasedCmds"


class TestWorkloadModelRateLimiter:
    """Test rate-limiter classification: string >60% AND R/W <2."""

    def test_rate_limiter_basic(self):
        model = WorkloadModel()
        cluster_data = _make_cluster_data(
            {
                "StringBasedCmds": 7000.0,
                "HashBasedCmds": 500.0,
                "SortedSetBasedCmds": 500.0,
                "ListBasedCmds": 500.0,
                "SetBasedCmds": 500.0,
                "StreamBasedCmds": 500.0,
                "PubSubBasedCmds": 500.0,
            },
            get_cmds=1000.0,
            set_cmds=5000.0,
        )
        result = model.compute(cluster_data, None)
        assert result["workload_class"] == "rate-limiter"
        assert result["read_write_ratio"] < 2


class TestWorkloadModelEventStream:
    """Test event-stream classification: stream >15%."""

    def test_event_stream_basic(self):
        model = WorkloadModel()
        cluster_data = _make_cluster_data(
            {
                "StringBasedCmds": 2000.0,
                "HashBasedCmds": 1000.0,
                "SortedSetBasedCmds": 1000.0,
                "ListBasedCmds": 1000.0,
                "SetBasedCmds": 1000.0,
                "StreamBasedCmds": 2500.0,
                "PubSubBasedCmds": 500.0,
            },
            get_cmds=3000.0,
            set_cmds=3000.0,
        )
        result = model.compute(cluster_data, None)
        assert result["workload_class"] == "event-stream"


class TestWorkloadModelRealTimeMessaging:
    """Test real-time-messaging classification: pubsub >10%."""

    def test_messaging_basic(self):
        model = WorkloadModel()
        cluster_data = _make_cluster_data(
            {
                "StringBasedCmds": 2000.0,
                "HashBasedCmds": 2000.0,
                "SortedSetBasedCmds": 1000.0,
                "ListBasedCmds": 1000.0,
                "SetBasedCmds": 1000.0,
                "StreamBasedCmds": 500.0,
                "PubSubBasedCmds": 2500.0,
            },
            get_cmds=3000.0,
            set_cmds=3000.0,
        )
        result = model.compute(cluster_data, None)
        assert result["workload_class"] == "real-time-messaging"


class TestWorkloadModelQueue:
    """Test queue classification: list >20%."""

    def test_queue_basic(self):
        model = WorkloadModel()
        cluster_data = _make_cluster_data(
            {
                "StringBasedCmds": 2000.0,
                "HashBasedCmds": 2000.0,
                "SortedSetBasedCmds": 1000.0,
                "ListBasedCmds": 3000.0,
                "SetBasedCmds": 1000.0,
                "StreamBasedCmds": 500.0,
                "PubSubBasedCmds": 500.0,
            },
            get_cmds=3000.0,
            set_cmds=3000.0,
        )
        result = model.compute(cluster_data, None)
        assert result["workload_class"] == "queue"


class TestWorkloadModelGeneralPurpose:
    """Test general-purpose fallback classification."""

    def test_general_purpose_no_dominant(self):
        model = WorkloadModel()
        cluster_data = _make_cluster_data(
            {
                "StringBasedCmds": 2000.0,
                "HashBasedCmds": 2000.0,
                "SortedSetBasedCmds": 2000.0,
                "ListBasedCmds": 1500.0,
                "SetBasedCmds": 1500.0,
                "StreamBasedCmds": 500.0,
                "PubSubBasedCmds": 500.0,
            },
            get_cmds=3000.0,
            set_cmds=3000.0,
        )
        result = model.compute(cluster_data, None)
        assert result["workload_class"] == "general-purpose"


class TestWorkloadModelUnknown:
    """Test unknown classification for all-zero or unavailable data."""

    def test_all_zero_commands(self):
        model = WorkloadModel()
        cluster_data = _make_cluster_data(
            {
                "StringBasedCmds": 0.0,
                "HashBasedCmds": 0.0,
                "SortedSetBasedCmds": 0.0,
                "ListBasedCmds": 0.0,
                "SetBasedCmds": 0.0,
                "StreamBasedCmds": 0.0,
                "PubSubBasedCmds": 0.0,
            },
            get_cmds=0.0,
            set_cmds=0.0,
        )
        result = model.compute(cluster_data, None)
        assert result["workload_class"] == "unknown"
        assert result["command_profile"] == {}
        assert result["dominant_family"] == "none"

    def test_empty_nodes(self):
        model = WorkloadModel()
        cluster_data = {"nodes": {}}
        result = model.compute(cluster_data, None)
        assert result["workload_class"] == "unknown"
        assert result["command_profile"] == {}

    def test_missing_metrics(self):
        model = WorkloadModel()
        cluster_data = {
            "nodes": {
                "node-001": {
                    "metrics": {}
                }
            }
        }
        result = model.compute(cluster_data, None)
        assert result["workload_class"] == "unknown"
        assert result["command_profile"] == {}


class TestWorkloadModelOutput:
    """Test output structure and data types."""

    def test_output_keys(self):
        model = WorkloadModel()
        cluster_data = _make_cluster_data(
            {
                "StringBasedCmds": 7000.0,
                "HashBasedCmds": 1000.0,
                "SortedSetBasedCmds": 500.0,
                "ListBasedCmds": 500.0,
                "SetBasedCmds": 500.0,
                "StreamBasedCmds": 300.0,
                "PubSubBasedCmds": 200.0,
            },
            get_cmds=8000.0,
            set_cmds=1000.0,
        )
        result = model.compute(cluster_data, None)
        assert "workload_class" in result
        assert "command_profile" in result
        assert "read_write_ratio" in result
        assert "dominant_family" in result

    def test_percentage_sum_approx_100(self):
        model = WorkloadModel()
        cluster_data = _make_cluster_data(
            {
                "StringBasedCmds": 3000.0,
                "HashBasedCmds": 2000.0,
                "SortedSetBasedCmds": 1500.0,
                "ListBasedCmds": 1000.0,
                "SetBasedCmds": 1000.0,
                "StreamBasedCmds": 800.0,
                "PubSubBasedCmds": 700.0,
            },
            get_cmds=5000.0,
            set_cmds=5000.0,
        )
        result = model.compute(cluster_data, None)
        total_pct = sum(result["command_profile"].values())
        assert abs(total_pct - 100.0) < 0.1

    def test_read_write_ratio_zero_set_cmds(self):
        """When SetTypeCmds is 0, denominator is clamped to 1."""
        model = WorkloadModel()
        cluster_data = _make_cluster_data(
            {
                "StringBasedCmds": 5000.0,
                "HashBasedCmds": 2000.0,
                "SortedSetBasedCmds": 1000.0,
                "ListBasedCmds": 1000.0,
                "SetBasedCmds": 500.0,
                "StreamBasedCmds": 300.0,
                "PubSubBasedCmds": 200.0,
            },
            get_cmds=10000.0,
            set_cmds=0.0,
        )
        result = model.compute(cluster_data, None)
        # Should not raise, denominator clamped to 1
        assert result["read_write_ratio"] == 10000.0


class TestWorkloadModelMultiNode:
    """Test that metrics are summed across multiple nodes."""

    def test_two_nodes_summed(self):
        model = WorkloadModel()
        # Two nodes each contributing 3500 StringBasedCmds = 7000 total
        cluster_data = {
            "nodes": {
                "node-001": {
                    "metrics": {
                        "StringBasedCmds": {"Sum": {"timestamps": ["2024-01-01T00:00:00Z"], "values": [3500.0]}},
                        "HashBasedCmds": {"Sum": {"timestamps": ["2024-01-01T00:00:00Z"], "values": [500.0]}},
                        "SortedSetBasedCmds": {"Sum": {"timestamps": ["2024-01-01T00:00:00Z"], "values": [250.0]}},
                        "ListBasedCmds": {"Sum": {"timestamps": ["2024-01-01T00:00:00Z"], "values": [250.0]}},
                        "SetBasedCmds": {"Sum": {"timestamps": ["2024-01-01T00:00:00Z"], "values": [250.0]}},
                        "StreamBasedCmds": {"Sum": {"timestamps": ["2024-01-01T00:00:00Z"], "values": [150.0]}},
                        "PubSubBasedCmds": {"Sum": {"timestamps": ["2024-01-01T00:00:00Z"], "values": [100.0]}},
                        "GetTypeCmds": {"Sum": {"timestamps": ["2024-01-01T00:00:00Z"], "values": [4000.0]}},
                        "SetTypeCmds": {"Sum": {"timestamps": ["2024-01-01T00:00:00Z"], "values": [500.0]}},
                    }
                },
                "node-002": {
                    "metrics": {
                        "StringBasedCmds": {"Sum": {"timestamps": ["2024-01-01T00:00:00Z"], "values": [3500.0]}},
                        "HashBasedCmds": {"Sum": {"timestamps": ["2024-01-01T00:00:00Z"], "values": [500.0]}},
                        "SortedSetBasedCmds": {"Sum": {"timestamps": ["2024-01-01T00:00:00Z"], "values": [250.0]}},
                        "ListBasedCmds": {"Sum": {"timestamps": ["2024-01-01T00:00:00Z"], "values": [250.0]}},
                        "SetBasedCmds": {"Sum": {"timestamps": ["2024-01-01T00:00:00Z"], "values": [250.0]}},
                        "StreamBasedCmds": {"Sum": {"timestamps": ["2024-01-01T00:00:00Z"], "values": [150.0]}},
                        "PubSubBasedCmds": {"Sum": {"timestamps": ["2024-01-01T00:00:00Z"], "values": [100.0]}},
                        "GetTypeCmds": {"Sum": {"timestamps": ["2024-01-01T00:00:00Z"], "values": [4000.0]}},
                        "SetTypeCmds": {"Sum": {"timestamps": ["2024-01-01T00:00:00Z"], "values": [500.0]}},
                    }
                },
            }
        }
        result = model.compute(cluster_data, None)
        # 7000 / 10000 total = 70% string → >60%, R/W = 8000/1000 = 8 → >4
        assert result["workload_class"] == "cache-aside"
        assert result["read_write_ratio"] == 8.0


class TestWorkloadModelClassificationOrder:
    """Test that classification rules are applied in order (first match wins)."""

    def test_cache_aside_before_rate_limiter(self):
        """String >60% with R/W >4 should be cache-aside, not rate-limiter."""
        model = WorkloadModel()
        cluster_data = _make_cluster_data(
            {
                "StringBasedCmds": 8000.0,
                "HashBasedCmds": 500.0,
                "SortedSetBasedCmds": 500.0,
                "ListBasedCmds": 500.0,
                "SetBasedCmds": 200.0,
                "StreamBasedCmds": 200.0,
                "PubSubBasedCmds": 100.0,
            },
            get_cmds=9000.0,
            set_cmds=1000.0,
        )
        result = model.compute(cluster_data, None)
        assert result["workload_class"] == "cache-aside"
