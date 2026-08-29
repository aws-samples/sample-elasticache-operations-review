"""Tests for the Phase-7 per-cluster characterization signals:

- ``SteadinessModel`` — steady / variable / spiky from CPU+memory variability,
  network reported but not a label driver, ``not_measured`` when idle.
- ``EfficiencyModel`` read/write mix — read_pct/write_pct + a
  read-heavy/write-heavy/balanced class, with a serverless ECPU fallback and a
  ``not_measured`` result when no commands were served.

Both feed the commitment recommendation (steady → Reserved/Savings-Plan is sound;
idle → decommission, not a commitment), so the honest "not_measured" case matters
as much as the classified one.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from analyze_metrics import EfficiencyModel, SteadinessModel  # noqa: E402


def _cluster(metrics: dict) -> dict:
    """A one-node cluster whose metrics are {name: {stat: [values]}}."""
    return {"nodes": {"node-1": {"metrics": {
        name: {stat: {"values": vals} for stat, vals in stats.items()}
        for name, stats in metrics.items()
    }}}}


def _cpu(vals, stat="Average"):
    return {"EngineCPUUtilization": {stat: vals}}


class TestSteadiness:
    def setup_method(self):
        self.m = SteadinessModel()

    def test_low_variation_cpu_is_steady(self):
        r = self.m.compute(_cluster(_cpu([40, 40.2, 39.8, 40.1, 39.9])), None)
        assert r["label"] == "steady"
        assert r["measured"] is True
        assert r["driver"] == "cpu"

    def test_high_variation_cpu_is_spiky(self):
        r = self.m.compute(_cluster(_cpu([5, 80, 5, 80, 5, 80])), None)
        assert r["label"] == "spiky"

    def test_mid_variation_cpu_is_variable(self):
        r = self.m.compute(_cluster(_cpu([20, 60, 30, 55, 25])), None)
        assert r["label"] == "variable"

    def test_idle_cluster_is_not_measured(self):
        # CPU below the activity floor and no commands: steadiness is moot.
        r = self.m.compute(_cluster(_cpu([0.3, 0.35, 0.3, 0.32])), None)
        assert r["label"] == "not_measured"
        assert r["measured"] is False
        assert r["driver"] is None

    def test_command_traffic_makes_a_low_cpu_cluster_active(self):
        # Below the CPU floor, but serving commands -> characterize it.
        data = _cluster({
            "EngineCPUUtilization": {"Average": [2, 2.1, 1.9, 2.05]},
            "GetTypeCmds": {"Sum": [100, 100, 100, 100]},
        })
        r = self.m.compute(data, None)
        assert r["measured"] is True

    def test_network_burstiness_does_not_drive_the_label(self):
        # Steady CPU + steady memory but a wildly bursty network: the capacity
        # axes say steady, and network must not override that (it is a
        # NETWORK-BOUND concern, not a commitment one).
        data = _cluster({
            "EngineCPUUtilization": {"Average": [40, 40.2, 39.8, 40.1]},
            "DatabaseMemoryUsagePercentage": {"Maximum": [50, 50.1, 49.9, 50]},
            "NetworkBytesIn": {"Sum": [1, 5000, 1, 5000]},
        })
        r = self.m.compute(data, None)
        assert r["label"] == "steady"
        assert r["driver"] in ("cpu", "memory")
        # Network variability is still reported, just not decisive.
        assert r["network_cov"] is not None and r["network_cov"] > 0.75

    def test_all_three_axis_covs_are_reported(self):
        data = _cluster({
            "EngineCPUUtilization": {"Average": [40, 45, 38, 42]},
            "DatabaseMemoryUsagePercentage": {"Maximum": [50, 51, 49, 50]},
            "NetworkBytesIn": {"Sum": [100, 120, 90, 110]},
        })
        r = self.m.compute(data, None)
        for key in ("cpu_cov", "memory_cov", "network_cov"):
            assert isinstance(r[key], float)


class TestReadWriteMix:
    def setup_method(self):
        self.m = EfficiencyModel()

    def _rw(self, get, set_, extra=None):
        metrics = {"GetTypeCmds": {"Sum": get}, "SetTypeCmds": {"Sum": set_}}
        if extra:
            metrics.update(extra)
        return self.m._compute_read_write_ratio(_cluster(metrics))

    def test_read_heavy(self):
        r = self._rw([800, 800], [100, 100])  # 1600 reads / 200 writes
        assert r["class"] == "read-heavy"
        assert r["read_pct"] == 88.9
        assert r["write_pct"] == 11.1
        assert r["value"] == 8.0

    def test_write_heavy(self):
        r = self._rw([100, 100], [800, 800])
        assert r["class"] == "write-heavy"
        assert r["write_pct"] > 66

    def test_balanced(self):
        r = self._rw([500, 500], [500, 500])
        assert r["class"] == "balanced"
        assert r["read_pct"] == 50.0

    def test_no_commands_is_not_measured_not_fifty_fifty(self):
        r = self._rw([0, 0], [0, 0])
        assert r["class"] == "not_measured"
        assert r["read_pct"] is None and r["write_pct"] is None
        assert r["value"] is None

    def test_pure_reads_have_no_ratio_but_a_full_percentage(self):
        # All reads, zero writes: the ratio is undefined (None, not infinity),
        # but the split still describes the mix as 100% reads.
        r = self._rw([500, 500], [0, 0])
        assert r["class"] == "read-heavy"
        assert r["read_pct"] == 100.0
        assert r["value"] is None

    def test_serverless_ecpu_fallback(self):
        # No raw command counts, but serverless ECPU command metrics present.
        r = self.m._compute_read_write_ratio(_cluster({
            "GetTypeCmdsECPUs": {"Sum": [900, 900]},
            "SetTypeCmdsECPUs": {"Sum": [100, 100]},
        }))
        assert r["basis"] == "ecpus"
        assert r["class"] == "read-heavy"
        assert r["read_pct"] == 90.0
