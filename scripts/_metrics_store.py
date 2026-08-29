"""Shared seam for reading Stage 2's metrics, whichever on-disk shape they take.

As of Phase 9 (step 1) Stage 2 shards the metrics: one file per reviewable unit
-- a **replication group** (node-based) or a **serverless cache** -- at
``output/metrics/<cluster_id>.json``, and ``output/metrics.json`` becomes a small
**manifest**::

    {"metadata": {...}, "cost": {...},
     "clusters": ["<id>", ...],
     "shards": {"<id>": "metrics/<id>.json"}}

The manifest carries no series, so it stays KB-scale however large the fleet is.
A shard is self-contained: it is exactly the per-cluster dict that used to live
under ``metrics["clusters"][cid]`` (its ``latency_detail`` already inline).

Older runs -- and many tests -- instead hold a single ``metrics.json`` whose
``"clusters"`` key is an inline map of ``cluster_id -> per-cluster dict``. Both
shapes flow through the two functions below so no caller has to branch on which
one it was handed; that keeps the many ``metrics["clusters"]`` consumers working
through one central point instead of a scattered rewrite.

Note the generic ``cluster_id`` key is kept for both unit kinds on purpose -- the
code has always keyed both on it, and renaming it is out of scope here.

Imported as a sibling module (like ``_pipeline_version``), so it resolves both as
a subprocess -- ``scripts/`` is on ``sys.path`` -- and in tests.
"""

import json
import os


def _is_manifest(metrics_obj: dict) -> bool:
    """True when ``metrics_obj`` is the sharded manifest, not a legacy inline map.

    The distinguishing feature is a ``shards`` mapping: a manifest carries it and
    no per-cluster series; a legacy single file carries an inline ``clusters``
    map and no ``shards``.
    """
    return isinstance(metrics_obj, dict) and isinstance(
        metrics_obj.get("shards"), dict
    )


def iter_cluster_ids(metrics_obj: dict, metrics_path: str | None) -> list[str]:
    """Return the cluster ids present in ``metrics_obj``, without reading series.

    For a manifest this is the ``clusters`` list (falling back to the ``shards``
    keys); for a legacy inline file it is the keys of the ``clusters`` map. In
    both cases no shard file is opened -- only the small top-level object is read.

    Args:
        metrics_obj: The parsed manifest, or a legacy single-file metrics dict.
        metrics_path: Path ``metrics_obj`` was read from. Unused for the id list
            but part of the seam's signature so callers pass it uniformly.

    Returns:
        A list of ``cluster_id`` strings (unsorted; callers sort as needed).
    """
    if _is_manifest(metrics_obj):
        clusters = metrics_obj.get("clusters")
        if isinstance(clusters, list):
            return list(clusters)
        return list((metrics_obj.get("shards") or {}).keys())
    return list((metrics_obj.get("clusters") or {}).keys())


def load_cluster_metrics(
    metrics_obj: dict, metrics_path: str | None, cid: str
) -> dict:
    """Return one cluster's raw per-cluster dict (series + ``latency_detail``).

    For a manifest this opens exactly that cluster's shard -- so a streaming
    caller holds one shard in memory at a time, never the whole fleet. For a
    legacy inline file it returns the entry from memory. Either way the dict is
    byte-for-byte the same content, so analysis over the two shapes is identical.

    Args:
        metrics_obj: The parsed manifest, or a legacy single-file metrics dict.
        metrics_path: Path ``metrics_obj`` was read from. Required for a manifest
            (shard paths are resolved relative to its directory); may be ``None``
            for a legacy in-memory dict.
        cid: The ``cluster_id`` to load.

    Returns:
        The per-cluster metrics dict.

    Raises:
        KeyError: if ``cid`` is not present in the manifest/inline map.
    """
    if _is_manifest(metrics_obj):
        shards = metrics_obj.get("shards") or {}
        rel = shards.get(cid)
        if rel is None:
            raise KeyError(
                f"cluster '{cid}' is not listed in the metrics manifest"
            )
        base = (
            os.path.dirname(os.path.abspath(metrics_path))
            if metrics_path
            else "."
        )
        shard_path = os.path.join(base, rel)
        with open(shard_path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    clusters = metrics_obj.get("clusters") or {}
    return clusters[cid]
