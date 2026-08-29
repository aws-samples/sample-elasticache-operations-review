"""Hard guard for Stage 2's parallel per-cluster collection (Phase 9, step 2 / 9c).

Stage 2 now fans each reviewable unit (replication group / serverless cache) out
to its own worker thread inside a bounded pool, instead of a single serial loop.
Parallelism is only allowed to change *how fast* collection runs, never *what* it
writes. These tests pin exactly that:

1. Collecting the same inventory with ``--concurrency 1`` and with a larger worker
   count produces **byte-identical** shards and a byte-identical manifest, modulo
   the volatile run metadata (timestamps / duration). If this ever fails, fix the
   concurrency code -- do not loosen the guard.
2. Each worker thread uses its **own** CloudWatch client (no mutable client is
   shared across threads), and the concurrent run really does run on more than one
   thread and finishes faster than the serial one.
3. The collection path never materialises the whole fleet's series at once: each
   worker writes its own shard and frees the series, so the returned region result
   holds only metadata (no ``clusters`` series) and a worker's contribution
   carries no raw series. Peak memory is one shard per worker, not fleet-sized.
4. Shard bytes are unchanged by the worker->shard->free refactor: a shard written
   by a worker is byte-identical to one the direct ``write_sharded`` writer emits
   for the same data (same serialisation seam).

No real AWS is touched: ``boto3.Session`` is replaced with a fake whose CloudWatch
client returns deterministic, window-independent series keyed by query id, so the
only thing that varies between two runs is the worker count.
"""
import hashlib
import json
import os
import sys
import threading
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import fetch_metrics  # noqa: E402

# ---------------------------------------------------------------------------
# A deterministic fake CloudWatch client + session.
# ---------------------------------------------------------------------------
# Fixed timestamps, independent of the (wall-clock-derived) query window, so two
# runs started a few milliseconds apart still return identical series. Real
# CloudWatch is likewise window-position-independent for a given metric.
_BASE = datetime(2026, 8, 1, 0, 0, 0, tzinfo=timezone.utc)
_FIXED_TIMESTAMPS = [
    _BASE.replace(hour=0),
    _BASE.replace(hour=1),
    _BASE.replace(hour=2),
]


def _values_for(query_id: str) -> list[float]:
    """Deterministic values derived only from the query id (stable across runs)."""
    # Not security: md5 only turns a query id into a stable pseudo-value for the
    # fake CloudWatch client. usedforsecurity=False documents that (and clears B324).
    seed = int(hashlib.md5(query_id.encode(), usedforsecurity=False).hexdigest(), 16)
    return [
        round((seed % 1000) / 10.0, 2),
        round((seed % 777) / 7.0, 2),
        round((seed % 555) / 5.0, 2),
    ]


class FakeCloudWatch:
    """Records the threads that use it; returns deterministic GetMetricData data."""

    def __init__(self, sleep: float = 0.0):
        self._sleep = sleep
        self.thread_idents: set[int] = set()
        self.calls = 0

    def get_metric_data(self, **kwargs):
        self.thread_idents.add(threading.get_ident())
        self.calls += 1
        if self._sleep:
            time.sleep(self._sleep)
        results = []
        for query in kwargs["MetricDataQueries"]:
            qid = query["Id"]
            results.append(
                {
                    "Id": qid,
                    "Timestamps": list(_FIXED_TIMESTAMPS),
                    "Values": _values_for(qid),
                }
            )
        # Single page -- no NextToken.
        return {"MetricDataResults": results}


class FakeSession:
    """Stands in for boto3.Session: hands out a fresh FakeCloudWatch per call."""

    def __init__(self, sleep: float = 0.0):
        self._sleep = sleep
        self.cw_clients: list[FakeCloudWatch] = []
        self._lock = threading.Lock()

    def client(self, service_name, region_name=None):
        assert service_name == "cloudwatch"
        client = FakeCloudWatch(sleep=self._sleep)
        with self._lock:
            self.cw_clients.append(client)
        return client


def _write_inventory(path: str, n_node: int = 4, n_serverless: int = 2) -> None:
    """A small single-region synthetic inventory (no real resources)."""
    clusters = []
    for i in range(n_node):
        cid = f"rg-node-{i:02d}"
        clusters.append(
            {
                "cluster_id": cid,
                "cluster_type": "node-based",
                "region": "us-east-1",
                "engine": "redis",
                "members": [
                    {"cache_cluster_id": f"{cid}-001"},
                    {"cache_cluster_id": f"{cid}-002"},
                ],
            }
        )
    for i in range(n_serverless):
        cid = f"sls-{i:02d}"
        clusters.append(
            {
                "cluster_id": cid,
                "cluster_type": "serverless",
                "region": "us-east-1",
                "engine": "redis",
                "serverless_cache_name": cid,
            }
        )
    with open(path, "w", encoding="utf-8") as handle:
        json.dump({"clusters": clusters}, handle)


def _run_collection(tmpdir, inventory_path, concurrency, session):
    """Drive the real orchestrator against the fake session; return the out dir."""
    out_dir = os.path.join(tmpdir, f"c{concurrency}_{id(session)}")
    os.makedirs(out_dir, exist_ok=True)
    config = fetch_metrics.CollectionConfig(
        inventory_path=inventory_path,
        output_path=os.path.join(out_dir, "metrics.json"),
        days=2,  # 1 time-chunk -> fast; series content is window-independent
        concurrency=concurrency,
        skip_cost=True,  # avoid Cost Explorer entirely
    )
    orch = fetch_metrics.CollectionOrchestrator.__new__(
        fetch_metrics.CollectionOrchestrator
    )
    orch.config = config
    orch.session = session
    orch.registry = fetch_metrics.MetricDefinitionRegistry()
    exit_code = orch.run()
    assert exit_code == 0
    return out_dir


_VOLATILE = {
    "collection_timestamp",
    "period_start",
    "period_end",
    "collection_duration_seconds",
}


def _stable_manifest(out_dir):
    with open(os.path.join(out_dir, "metrics.json"), encoding="utf-8") as handle:
        manifest = json.load(handle)
    manifest["metadata"] = {
        k: v for k, v in manifest["metadata"].items() if k not in _VOLATILE
    }
    return manifest


def _shard_bytes(out_dir):
    shard_dir = os.path.join(out_dir, "metrics")
    return {
        name: open(os.path.join(shard_dir, name), "rb").read()
        for name in sorted(os.listdir(shard_dir))
    }


class TestConcurrencyChangesNothing:
    """--concurrency 1 vs N over one inventory: identical shards + manifest."""

    def test_serial_and_concurrent_are_byte_identical(self, tmp_path):
        inv = os.path.join(str(tmp_path), "inventory.json")
        _write_inventory(inv)

        serial = _run_collection(str(tmp_path), inv, 1, FakeSession())
        concurrent = _run_collection(str(tmp_path), inv, 8, FakeSession())

        # Manifest identical apart from volatile run metadata.
        assert _stable_manifest(serial) == _stable_manifest(concurrent)

        # Every shard byte-for-byte identical.
        serial_shards = _shard_bytes(serial)
        concurrent_shards = _shard_bytes(concurrent)
        assert serial_shards.keys() == concurrent_shards.keys()
        for name in serial_shards:
            assert serial_shards[name] == concurrent_shards[name], name

    def test_manifest_cluster_list_is_sorted(self, tmp_path):
        inv = os.path.join(str(tmp_path), "inventory.json")
        _write_inventory(inv)
        out = _run_collection(str(tmp_path), inv, 8, FakeSession())
        with open(os.path.join(out, "metrics.json"), encoding="utf-8") as handle:
            manifest = json.load(handle)
        assert manifest["clusters"] == sorted(manifest["clusters"])


class TestPerWorkerClient:
    """No CloudWatch client is shared across worker threads."""

    def test_serial_uses_one_client_on_one_thread(self, tmp_path):
        inv = os.path.join(str(tmp_path), "inventory.json")
        _write_inventory(inv)
        session = FakeSession(sleep=0.01)
        _run_collection(str(tmp_path), inv, 1, session)
        # One worker thread -> one lazily-created client, used by one thread.
        assert len(session.cw_clients) == 1
        assert len(session.cw_clients[0].thread_idents) == 1

    def test_concurrent_uses_a_distinct_client_per_thread(self, tmp_path):
        inv = os.path.join(str(tmp_path), "inventory.json")
        _write_inventory(inv, n_node=8, n_serverless=4)
        session = FakeSession(sleep=0.03)
        _run_collection(str(tmp_path), inv, 8, session)
        # More than one worker thread actually ran...
        used = [c for c in session.cw_clients if c.thread_idents]
        assert len(used) >= 2, "expected the pool to use multiple threads"
        # ...and no single client was ever touched by two threads.
        for client in used:
            assert len(client.thread_idents) == 1

    def test_concurrent_is_faster_than_serial(self, tmp_path):
        """With simulated per-call latency, the pool beats the serial loop.

        This exercises wall-clock improvement with the fake client. It proves
        the loop parallelises; real-fleet speedup is still bounded by CloudWatch
        rate limits, which only a real large fleet exercises.
        """
        inv = os.path.join(str(tmp_path), "inventory.json")
        _write_inventory(inv, n_node=8, n_serverless=0)

        t0 = time.time()
        _run_collection(str(tmp_path), inv, 1, FakeSession(sleep=0.02))
        serial_wall = time.time() - t0

        t0 = time.time()
        _run_collection(str(tmp_path), inv, 8, FakeSession(sleep=0.02))
        concurrent_wall = time.time() - t0

        assert concurrent_wall < serial_wall


class TestMemoryInvariant:
    """Collection never holds the whole fleet's series in memory at once."""

    def _region_collector(self, tmp_path, session, concurrency=4, **inv_kwargs):
        inv = os.path.join(str(tmp_path), "inventory.json")
        _write_inventory(inv, **inv_kwargs)
        with open(inv, encoding="utf-8") as handle:
            clusters = json.load(handle)["clusters"]
        os.makedirs(os.path.join(str(tmp_path), "out"), exist_ok=True)
        config = fetch_metrics.CollectionConfig(
            inventory_path=inv,
            output_path=os.path.join(str(tmp_path), "out", "metrics.json"),
            days=2,
            concurrency=concurrency,
            skip_cost=True,
        )
        registry = fetch_metrics.MetricDefinitionRegistry()
        return fetch_metrics.RegionCollector(
            "us-east-1", clusters, session, config, registry
        )

    def test_region_result_holds_no_series_after_collect(self, tmp_path):
        collector = self._region_collector(tmp_path, FakeSession())
        result = collector.collect()

        # Shards were written; only metadata comes back -- no fleet-wide series.
        assert result.clusters == {}
        assert result.latency_detail == {}
        assert result.shard_cluster_ids, "expected shards to have been written"
        assert result.total_datapoints > 0
        assert result.metric_names

        # Every shard the run claims is a real file on disk.
        for cid in result.shard_cluster_ids:
            assert os.path.isfile(
                os.path.join(str(tmp_path), "out", "metrics", f"{cid}.json")
            )

    def test_worker_contribution_carries_no_raw_series(self, tmp_path):
        collector = self._region_collector(tmp_path, FakeSession())
        contribution = collector._collect_cluster(collector.clusters[0])

        assert set(contribution) == {
            "cluster_id",
            "cluster_type",
            "error",
            "datapoint_count",
            "metrics_collected",
            "shard_path",
            "elapsed",
        }
        # Explicitly: none of the removed series keys leak through.
        assert "cluster_metrics" not in contribution
        assert "latency_data" not in contribution
        # metrics_collected is a small set of names, not series.
        assert isinstance(contribution["metrics_collected"], set)
        assert all(
            isinstance(name, str) for name in contribution["metrics_collected"]
        )


class TestShardBytesUnchangedByRefactor:
    """A worker-written shard equals the direct writer's shard for same data."""

    def test_write_one_shard_matches_write_sharded(self, tmp_path):
        ts = ["2026-08-01T00:00:00Z", "2026-08-01T00:05:00Z"]

        def series(vals):
            return fetch_metrics.TimeSeries(timestamps=ts, values=vals)

        cid = "rg-x"
        cluster_data = {
            f"{cid}-001": {
                "EngineCPUUtilization": {"Maximum": series([5.0, 6.0])},
                "CacheHits": {"Sum": series([100.0, 200.0])},
            }
        }
        latency_data = {}
        writer = fetch_metrics.ResultWriter()

        # Path A: the worker seam writes the shard directly.
        out_a = os.path.join(str(tmp_path), "a", "metrics.json")
        writer.ensure_shard_dir(out_a)
        writer.write_one_shard(
            out_a, cid, "node-based", cluster_data, latency_data, "us-east-1", []
        )

        # Path B: the direct in-memory writer path (the pre-refactor route).
        out_b = os.path.join(str(tmp_path), "b", "metrics.json")
        rr = fetch_metrics.RegionMetricsResult(
            region="us-east-1",
            clusters={cid: cluster_data},
            latency_detail={cid: latency_data},
            cluster_metadata={cid: {"cluster_type": "node-based"}},
        )
        cfg = fetch_metrics.CollectionConfig(
            inventory_path="inv.json", output_path=out_b
        )
        writer.write_sharded(cfg, [rr], None, 1.0, out_b)

        a_bytes = open(
            os.path.join(str(tmp_path), "a", "metrics", f"{cid}.json"), "rb"
        ).read()
        b_bytes = open(
            os.path.join(str(tmp_path), "b", "metrics", f"{cid}.json"), "rb"
        ).read()
        assert a_bytes == b_bytes
