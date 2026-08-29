"""Hard guards for the sharded metrics format and its shared loader (Phase 9).

Stage 2 now writes one shard per reviewable unit (replication group / serverless
cache) plus a KB-scale manifest, and ``scripts/_metrics_store.py`` is the single
seam every reader goes through. These tests pin the two properties the split
hinges on:

1. The loader reads BOTH shapes -- a legacy single-file inline ``clusters`` map
   and a manifest+shards directory -- identically.
2. Sharding changed *nothing* downstream: Stage 3's analysis and the report-data
   reduction are identical whether read from a legacy single file or from a
   manifest+shards layout. If this ever fails, fix the format, do not loosen the
   guard.
3. Stage 3 streams: it calls the loader once per cluster (one shard in memory at
   a time), never materialising the whole fleet's series up front.
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import analyze_metrics  # noqa: E402
import fetch_metrics  # noqa: E402
from _metrics_store import (  # noqa: E402
    iter_cluster_ids,
    load_cluster_metrics,
)
from generate_html_report import build_report_data  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures: one fleet, expressed in both on-disk shapes.
# ---------------------------------------------------------------------------
def _cluster(seed: float) -> dict:
    """A minimal, self-contained node-based per-cluster dict (one shard's worth)."""
    stamps = [f"2026-08-01T{i:02d}:00:00Z" for i in range(14)]
    return {
        "cluster_type": "node-based",
        "region": "us-east-1",
        "timestamps_5min": stamps,
        "nodes": {
            "node-001": {
                "EngineCPUUtilization": {
                    "Maximum": [seed + i for i in range(14)]
                },
                "NetworkBytesIn": {"Sum": [1048576.0] * 14},
                "BytesUsedForCache": {"Maximum": [2097152.0] * 14},
            }
        },
        "latency_detail": {},
        "errors": [],
    }


def _full_metrics() -> dict:
    """A legacy single-file metrics.json: inline ``clusters`` map + cost/metadata."""
    return {
        "metadata": {
            "period_start": "2026-08-01T00:00:00Z",
            "period_end": "2026-08-14T00:00:00Z",
            "resolution_seconds": 300,
            "total_datapoints": 84,
        },
        "clusters": {
            # Deliberately not in sorted order, to prove the loader/stream sorts.
            "beta-cache": _cluster(5.0),
            "alpha-cache": _cluster(1.0),
        },
        "cost": {"daily": [{"date": "2026-08-01", "total_usd": 3.0,
                            "by_usage_type": {"NodeUsage:cache.m5.large": 3.0}}]},
    }


def _write_legacy(tmp_path) -> str:
    """Write the legacy single-file layout; return the metrics.json path."""
    path = tmp_path / "legacy" / "metrics.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_full_metrics()), encoding="utf-8")
    return str(path)


def _write_sharded(tmp_path) -> str:
    """Write the manifest+shards layout for the SAME fleet; return manifest path."""
    full = _full_metrics()
    root = tmp_path / "sharded"
    (root / "metrics").mkdir(parents=True, exist_ok=True)
    ids = sorted(full["clusters"].keys())
    shards = {}
    for cid in ids:
        (root / "metrics" / f"{cid}.json").write_text(
            json.dumps(full["clusters"][cid]), encoding="utf-8"
        )
        shards[cid] = f"metrics/{cid}.json"
    manifest = {
        "metadata": full["metadata"],
        "cost": full["cost"],
        "clusters": ids,
        "shards": shards,
    }
    path = root / "metrics.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return str(path)


def _write_inventory(tmp_path) -> str:
    path = tmp_path / "inventory.json"
    path.write_text(json.dumps({"clusters": []}), encoding="utf-8")
    return str(path)


# ---------------------------------------------------------------------------
# 1. The loader reads both shapes identically.
# ---------------------------------------------------------------------------
class TestLoaderHandlesBothShapes:
    def test_iter_ids_on_a_legacy_inline_map(self):
        full = _full_metrics()
        assert set(iter_cluster_ids(full, None)) == {"alpha-cache", "beta-cache"}

    def test_iter_ids_on_a_manifest(self, tmp_path):
        manifest_path = _write_sharded(tmp_path)
        manifest = json.loads(open(manifest_path).read())
        # Order follows the manifest's clusters list, which is sorted on write.
        assert iter_cluster_ids(manifest, manifest_path) == [
            "alpha-cache", "beta-cache"]

    def test_load_from_legacy_returns_the_inline_entry(self):
        full = _full_metrics()
        got = load_cluster_metrics(full, None, "alpha-cache")
        assert got is full["clusters"]["alpha-cache"]

    def test_load_from_manifest_reads_the_shard_file(self, tmp_path):
        manifest_path = _write_sharded(tmp_path)
        manifest = json.loads(open(manifest_path).read())
        got = load_cluster_metrics(manifest, manifest_path, "alpha-cache")
        # The shard content equals the legacy inline entry byte for byte.
        assert got == _full_metrics()["clusters"]["alpha-cache"]

    def test_both_shapes_yield_equal_per_cluster_dicts(self, tmp_path):
        full = _full_metrics()
        manifest_path = _write_sharded(tmp_path)
        manifest = json.loads(open(manifest_path).read())
        for cid in iter_cluster_ids(full, None):
            assert (load_cluster_metrics(full, None, cid)
                    == load_cluster_metrics(manifest, manifest_path, cid))

    def test_unknown_cluster_raises(self, tmp_path):
        manifest_path = _write_sharded(tmp_path)
        manifest = json.loads(open(manifest_path).read())
        with pytest.raises(KeyError):
            load_cluster_metrics(manifest, manifest_path, "does-not-exist")


# ---------------------------------------------------------------------------
# 2. Sharding changed nothing downstream.
# ---------------------------------------------------------------------------
class TestShardingChangesNothing:
    """Byte-identical Stage 3 + report-data across the two on-disk shapes."""

    def _run_stage3(self, tmp_path, metrics_path):
        inv = _write_inventory(tmp_path)
        out = str(tmp_path / f"analysis-{os.path.basename(os.path.dirname(metrics_path))}.json")
        rc = analyze_metrics.main(
            ["--metrics", metrics_path, "--inventory", inv, "--output", out])
        assert rc == 0, "Stage 3 failed"
        return json.loads(open(out).read())

    def test_stage3_analysis_is_identical_across_formats(self, tmp_path):
        legacy = self._run_stage3(tmp_path, _write_legacy(tmp_path))
        sharded = self._run_stage3(tmp_path, _write_sharded(tmp_path))
        # The substantive analysis -- every per-cluster result -- must match byte
        # for byte. Only the metadata differs (timestamp, duration, and the
        # source path, which is a manifest in one run and a single file in the
        # other), and those are run-specific by design.
        assert (json.dumps(legacy["clusters"], sort_keys=True)
                == json.dumps(sharded["clusters"], sort_keys=True))
        assert legacy["metadata"]["total_findings"] == \
            sharded["metadata"]["total_findings"]

    def test_report_data_is_byte_identical_across_formats(self, tmp_path):
        # report_data carries no timestamp and copies metadata/cost verbatim, so
        # it must be byte-for-byte identical whichever shape it was reduced from.
        legacy_path = _write_legacy(tmp_path)
        sharded_path = _write_sharded(tmp_path)
        legacy_metrics = json.loads(open(legacy_path).read())
        sharded_manifest = json.loads(open(sharded_path).read())
        rd_legacy = build_report_data(legacy_metrics, legacy_path)
        rd_sharded = build_report_data(sharded_manifest, sharded_path)
        assert (json.dumps(rd_legacy, indent=2)
                == json.dumps(rd_sharded, indent=2))

    def test_the_two_fixtures_are_genuinely_different_layouts(self, tmp_path):
        # Guard the guard: if both fixtures were secretly the same shape, the
        # equality above would prove nothing. One is a manifest (has shards, no
        # inline series); the other is a single file (inline series, no shards).
        legacy = json.loads(open(_write_legacy(tmp_path)).read())
        sharded = json.loads(open(_write_sharded(tmp_path)).read())
        assert "shards" in sharded and "shards" not in legacy
        assert isinstance(sharded["clusters"], list)
        assert isinstance(legacy["clusters"], dict)


# ---------------------------------------------------------------------------
# 3. Stage 3 streams: one loader call per cluster, not an upfront full read.
# ---------------------------------------------------------------------------
class TestStage3StreamsLazily:
    def test_loader_is_called_once_per_cluster(self, tmp_path, monkeypatch):
        manifest_path = _write_sharded(tmp_path)
        inv = _write_inventory(tmp_path)

        calls = []
        real = analyze_metrics.load_cluster_metrics

        def spy(metrics_obj, path, cid):
            calls.append(cid)
            return real(metrics_obj, path, cid)

        monkeypatch.setattr(analyze_metrics, "load_cluster_metrics", spy)

        out = str(tmp_path / "analysis.json")
        rc = analyze_metrics.main(
            ["--metrics", manifest_path, "--inventory", inv, "--output", out])
        assert rc == 0
        # Exactly one shard load per cluster -- the series are pulled one at a
        # time, never materialised as a whole-fleet map up front.
        assert sorted(calls) == ["alpha-cache", "beta-cache"]
        assert len(calls) == 2


# ---------------------------------------------------------------------------
# 4. Stage 2 writes the sharded format the loader reads back.
# ---------------------------------------------------------------------------
class TestStage2WritesSharded:
    """Close the write->read loop: what write_sharded emits, the loader reads."""

    def _region_result(self):
        ts = ["2026-08-01T00:00:00Z", "2026-08-01T00:05:00Z"]

        def series(vals):
            return fetch_metrics.TimeSeries(timestamps=ts, values=vals)

        return fetch_metrics.RegionMetricsResult(
            region="us-east-1",
            clusters={
                # Out of order on purpose -- the manifest must come out sorted.
                "beta": {"beta-001": {"EngineCPUUtilization": {
                    "Maximum": series([5.0, 6.0])}}},
                "alpha": {"alpha-001": {"EngineCPUUtilization": {
                    "Maximum": series([1.0, 2.0])}}},
            },
            latency_detail={},
            errors=[],
            cluster_metadata={
                "alpha": {"cluster_type": "node-based"},
                "beta": {"cluster_type": "node-based"},
            },
        )

    def test_manifest_and_shards_round_trip_through_the_loader(self, tmp_path):
        out = str(tmp_path / "out" / "metrics.json")
        cfg = fetch_metrics.CollectionConfig(
            inventory_path="inv.json", output_path=out)
        manifest = fetch_metrics.ResultWriter().write_sharded(
            cfg, [self._region_result()],
            {"daily": [{"date": "2026-08-01", "total_usd": 1.0}]}, 3.0, out)

        # The manifest carries no series and lists clusters sorted.
        assert manifest["clusters"] == ["alpha", "beta"]
        assert set(manifest["shards"]) == {"alpha", "beta"}
        assert "cost" in manifest
        on_disk = json.loads(open(out).read())
        assert "nodes" not in json.dumps(on_disk["shards"])  # map, not series
        # Manifest is KB-scale (no series inside it).
        assert os.path.getsize(out) < 4096

        # Each shard is a real file the loader reads back with its series intact.
        for cid in iter_cluster_ids(on_disk, out):
            shard = load_cluster_metrics(on_disk, out, cid)
            assert shard["cluster_type"] == "node-based"
            assert shard["nodes"][f"{cid}-001"]["EngineCPUUtilization"]["Maximum"]
        assert os.path.isfile(str(tmp_path / "out" / "metrics" / "alpha.json"))
