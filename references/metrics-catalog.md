# Metrics Catalog

Defines which CloudWatch metrics to collect for the operations review, their dimensions,
statistics, and significance for analysis. Based on the official AWS documentation:
- [Metrics for Valkey and Redis OSS](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/CacheMetrics.Redis.html)
- [Host-Level Metrics](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/CacheMetrics.HostLevel.html)

## Collection Strategy

Use `GetMetricData` API with batched `MetricDataQuery` objects (up to 500 per call).

For large accounts, parallelize by region using concurrent API calls.

### Tier System

| Tier | When to Collect | Typical Count |
|------|----------------|---------------|
| **Tier 1** | Always — core operations review | 19 metrics |
| **Tier 2** | Always — deeper analysis and cost optimization | 30 metrics |
| **Tier 3** | Conditionally — based on engine version, node type, features | ~14 metrics |

### Resolution Strategy

**Primary resolution: 5 minutes (300s) for all Tier 1+2 metrics over 14 days.**

Never use 1-hour resolution for the primary analysis. 1-hour dilutes:
- Spike detection (anomaly model) — a 98% CPU spike lasting 8 minutes becomes a single blip
- Shard balance (CV calculation) — intermittent hot-shard pattern smoothed away
- Capacity forecasting (low R² on noisy metrics) — 336 points insufficient for cyclic patterns
- Threshold assessment — hides sustained violations shorter than 1 hour

**Exception: Latency metrics at 1-minute (60s) for percentile support.**

| Metric Category | Resolution | Window | Datapoints per Metric | Rationale |
|---|---|---|---|---|
| All Tier 1+2 (except latency) | **300s (5 min)** | 14 days | ~4,032 | Full fidelity for spike detection, trend analysis, shard balance |
| Latency (Avg, Max) | **300s (5 min)** | 14 days | ~4,032 | Trend and sustained-latency view |
| Latency percentiles (p50/p95/p99) | **60s (1 min)** | Last 24 hours | ~1,440 | Tail latency detection. Too expensive for 14 days at 1-min. |

**Why this works with CloudWatch retention:**
- ElastiCache publishes metrics at 60-second intervals
- 60-second data is retained for 15 days → our 14-day window fits
- 300-second data is retained for 63 days → comfortable margin
- 1-hour is only retained for 15 months and is the fallback for >63-day lookbacks

### API Limits and Batching

**`GetMetricData` hard limits:**
- Max 500 metric queries per call
- Max 100,800 datapoints per call (across all queries combined)

**Math at 5-min resolution (14 days = 4,032 datapoints per metric):**
- 1 cluster with 9 nodes × 49 metrics = 441 queries × 4,032 = ~1.78M datapoints
- Exceeds 100,800 limit → must chunk by time

**Batching approach: Split by time windows + metric batches**

```
Datapoints per query per 2-day chunk: 576 (2 days ÷ 5 min)
Max queries per call: 100,800 ÷ 576 = 175 queries per call
441 total queries ÷ 175 = 3 metric batches per 2-day chunk
14 days ÷ 2 days = 7 time windows
Total calls per cluster: 7 windows × 3 batches = 21 API calls
```

For 50 clusters across 3 regions: 50 × 21 = 1,050 API calls total.
Parallelized per region with rate limiting → completes in ~3-5 minutes.

**Cost**: CloudWatch charges ~$0.01 per 1,000 metrics queried.
1,050 calls × ~175 queries/call = ~184K metric queries ≈ **$1.84** for a full 50-cluster review.

### Why Not 1-Hour?

| Scenario | 5-min Resolution | 1-hour Resolution |
|---|---|---|
| CPU spike to 98% for 8 minutes | Visible as 2 consecutive high datapoints | Visible but looks like a single blip (1 of 336 total points) |
| Replication lag burst to 5s for 3 minutes | CRITICAL: clearly detected | Average shows 0.08s — **completely hidden** |
| Shard imbalance (20 min/hour hot period) | CV > 0.5 for 4 consecutive points — confirmed | CV smoothed to ~0.2 — **might not trigger** |
| Memory trend (smooth monotonic growth) | 4,032 points — excellent regression fit | 336 points — adequate but less reliable |
| Connection storm (5-min burst during deploy) | Visible as distinct spike | Merged into hourly average — **invisible** |

**Bottom line**: 1-hour is only acceptable for executive summary dashboards or 90-day trend charts. It is never acceptable for an operations review where scaling and remediation decisions are made.

---

## Tier 1: Always Collect (Core Operations Review)

Non-negotiable for any meaningful assessment. These metrics drive capacity planning,
performance analysis, reliability assessment, and cost evaluation.

### Node-Based Metrics

Dimension: `CacheClusterId` (per-node metrics)

| Metric | Unit | Statistic | Period | Significance |
|--------|------|-----------|--------|--------------|
| EngineCPUUtilization | Percent | Maximum, Average | 300s | Engine thread saturation — primary scaling signal. Single-threaded; can hit 100% on one core while host CPU is low. Use for nodes with 4+ vCPUs. |
| CPUUtilization | Percent | Average | 300s | Overall host CPU including OS and management processes. Use this (not EngineCPU) for nodes with ≤2 vCPUs (t4g.micro, t4g.small). |
| DatabaseMemoryUsagePercentage | Percent | Maximum | 300s | Memory pressure — calculated as `used_memory/maxmemory`. Evictions begin based on `maxmemory-policy` when high. |
| DatabaseCapacityUsagePercentage | Percent | Maximum | 300s | On data-tiering (r6gd): includes SSD storage. On all others: equivalent to `used_memory/maxmemory`. Available on all node-based clusters. |
| CacheHitRate | Percent | Average | 300s | Cache effectiveness — `cache_hits / (cache_hits + cache_misses)`. Below 0.8 means significant keys are evicted, expired, or don't exist. |
| CacheHits | Count | Sum | 300s | Absolute hit volume — needed for ratio calculations and throughput analysis. |
| CacheMisses | Count | Sum | 300s | Absolute miss volume — combined with CacheHits for hit rate validation. |
| Evictions | Count | Sum | 300s | Keys evicted due to maxmemory limit. Derived from `evicted_keys`. **A per-period total, not a rate** — Stage 3 derives `EvictionsPerMinute` for threshold comparison. Sustained evictions = dataset outgrew cache. |
| CurrConnections | Count | Maximum | 300s | Client connections excluding read-replica connections. ElastiCache uses 4-6 connections for monitoring. |
| NewConnections | Count | Sum | 300s | Total connections accepted. **A per-period total, not a rate** — Stage 3 derives `NewConnectionsPerMinute` for threshold comparison. High values with stable CurrConnections = connection churn (missing pooling). |
| ReplicationLag | Seconds | Maximum | 300s | Replica staleness. For Valkey 7.2+ / Redis OSS 5.0.6+, measured in milliseconds precision. Critical for read consistency and failover data-loss risk. |
| SuccessfulReadRequestLatency | Microseconds | Average | 300s | Client-perceived read latency. 5000μs = 5ms. Only includes successfully executed commands. Percentiles (p50/p95/p99) come from the separate 24-hour 60s window, not this sweep — see **Latency Metrics: Extended Statistics** below. |
| SuccessfulWriteRequestLatency | Microseconds | Average | 300s | Client-perceived write latency. 5000μs = 5ms. Only includes successfully executed commands. Percentiles come from the 24-hour 60s window — see **Latency Metrics: Extended Statistics** below. |
| NetworkBytesIn | Bytes | Sum | 300s | Inbound bandwidth — bytes read from network by the host. |
| NetworkBytesOut | Bytes | Sum | 300s | Outbound bandwidth — bytes sent out on all network interfaces. |
| BytesUsedForCache | Bytes | Maximum | 300s | Total bytes allocated by the engine for all purposes (dataset, buffers, etc.). |
| CurrItems | Count | Maximum | 300s | Number of items in cache. Derived from keyspace statistic, summing all keys. |
| SaveInProgress | Boolean (0/1) | Maximum | 300s | 1 during background save (snapshots/syncs). Correlates with degraded performance and replication lag spikes. |
| TrafficManagementActive | Boolean (0/1) | Maximum | 300s | 1 = ElastiCache is actively throttling traffic to protect the engine. Definitive signal that the node is underscaled. Evaluate scaling up or out immediately. |
| IsMaster | Boolean (0/1) | Maximum | 300s | 1 on the shard's primary node, 0 on a replica. Identifies which node is primary; with per-node charts, a failover appears as the 1 moving between nodes. |

### Serverless Metrics

Dimension: `clusterId` — **not** `ServerlessCacheName`, which is only the
ElastiCache API parameter name. Querying CloudWatch with the latter returns
zero datapoints for every serverless metric (see `fetch_metrics.py:769`).

| Metric | Unit | Statistic | Period | Significance |
|--------|------|-----------|--------|--------------|
| ElastiCacheProcessingUnits | Count | Sum | 300s | Cost driver — ECPUs consumed. **A per-period total, not a rate or a percentage**: divide by the collection period to compare against the `ECPUPerSecond` usage limit. Charged per million ECPUs. |
| ThrottledCmds | Count | Sum | 300s | Commands rejected due to hitting ECPU limit. **A per-period total, not a rate** — Stage 3 derives `ThrottledCmdsPerMinute` for threshold comparison. Any sustained throttling = immediate action needed. |
| CacheHitRate | Percent | Average | 300s | Cache effectiveness — same calculation as node-based. |
| CacheHits | Count | Sum | 300s | Absolute hit volume. |
| CacheMisses | Count | Sum | 300s | Absolute miss volume. |
| SuccessfulReadRequestLatency | Microseconds | Average | 300s | Read performance. Serverless typically 1-3ms at p50. |
| SuccessfulWriteRequestLatency | Microseconds | Average | 300s | Write performance. |
| CurrConnections | Count | Maximum | 300s | Connection health. |
| NewConnections | Count | Sum | 300s | Connection churn. **A per-period total** — Stage 3 derives `NewConnectionsPerMinute` for threshold comparison. |
| BytesUsedForCache | Bytes | Maximum | 300s | Storage consumption vs configured DataStorage.Maximum. |
| Evictions | Count | Sum | 300s | Eviction activity. Serverless uses `volatile-lru` (not configurable). |
| NetworkBytesIn | Bytes | Sum | 300s | Inbound bandwidth. |
| NetworkBytesOut | Bytes | Sum | 300s | Outbound bandwidth. |
| CurrItems | Count | Maximum | 300s | Item count trend. |

---

## Tier 2: Important (Deeper Analysis)

Always collect alongside Tier 1. Drives cost optimization, pattern detection, network
saturation analysis, and workload classification.

### Network Saturation and Burst (Node-Based)

Dimension: `CacheClusterId`

| Metric | Unit | Statistic | Period | Significance |
|--------|------|-----------|--------|--------------|
| NetworkBandwidthInAllowanceExceeded | Count | Maximum | 300s | Packets queued/dropped because inbound bandwidth exceeded instance maximum. First signal before general latency degradation. |
| NetworkBandwidthOutAllowanceExceeded | Count | Maximum | 300s | Packets queued/dropped because outbound bandwidth exceeded instance maximum. |
| NetworkConntrackAllowanceExceeded | Count | Maximum | 300s | Packets dropped because connection tracking exceeded maximum. New connections cannot be established. |
| NetworkPacketsPerSecondAllowanceExceeded | Count | Maximum | 300s | Packets queued/dropped because bidirectional PPS exceeded maximum. |
| NetworkBaselineUsageInPercentage | Percent | Average, Maximum | 300s | Inbound bandwidth as % of baseline. >100% = consuming burst credits. Leading indicator of future throttling. |
| NetworkBaselineUsageOutPercentage | Percent | Average, Maximum | 300s | Outbound bandwidth as % of baseline. >100% = consuming burst credits. |
| NetworkBaselineMaxUsageInPercentage | Percent | Maximum | 300s | Peak per-second inbound as % of baseline. Catches microbursts hidden by averaged metrics. |
| NetworkBaselineMaxUsageOutPercentage | Percent | Maximum | 300s | Peak per-second outbound as % of baseline. Catches microbursts. |
| NetworkMaxBytesIn | Bytes | Maximum | 300s | Maximum per-second burst of received bytes within each minute. |
| NetworkMaxBytesOut | Bytes | Maximum | 300s | Maximum per-second burst of transmitted bytes within each minute. |
| NetworkPacketsIn | Count | Sum | 300s | Packets received on all interfaces — packet-rate volume, complements the byte metrics. |
| NetworkPacketsOut | Count | Sum | 300s | Packets sent on all interfaces. |
| NetworkMaxPacketsIn | Count | Maximum | 300s | Maximum per-second burst of received packets within each minute. |
| NetworkMaxPacketsOut | Count | Maximum | 300s | Maximum per-second burst of transmitted packets within each minute. |

### Memory and Fragmentation (Node-Based)

| Metric | Unit | Statistic | Period | Significance |
|--------|------|-----------|--------|--------------|
| FreeableMemory | Bytes | Minimum | 300s | OS-level free memory (RAM + buffers + cache). Low values = host memory pressure. |
| SwapUsage | Bytes | Maximum | 300s | Should be 0. Any swap indicates severe memory pressure degrading performance. |
| MemoryFragmentationRatio | Number | Maximum | 300s | Ratio of RSS to used_memory. Should be >1.0. High values = wasted memory addressable by `activedefrag`. Collected as Maximum, so a datapoint is the worst ratio in the period — read a single high value as a transient (a fork or rewrite will spike it), and only a sustained series as real fragmentation. |
| DatabaseMemoryUsageCountedForEvictPercentage | Percent | Maximum | 300s | True eviction pressure — excludes overhead and COB memory. More accurate than DatabaseMemoryUsagePercentage for eviction risk. |
| DatabaseCapacityUsageCountedForEvictPercentage | Percent | Maximum | 300s | Capacity-based eviction pressure (data-tiered clusters include SSD), excluding overhead and COB. |
| UsedMemoryDataset | Bytes | Maximum | 300s | Memory used by actual user data (keys/values), excluding overhead. Read with BytesUsedForCache to see data vs overhead. |
| ActiveDefragHits | Count | Sum | 300s | Value reallocations by the active-defragmentation process per minute. |

`AllocatorFragmentationBytes`, `AllocatorFragmentationRatio` and
`MajorPageFaults` are now collected on every node-based cluster; see the *Memory
Pressure Deep Dive* rows below for their definitions.

### Key and TTL Health (Node-Based)

| Metric | Unit | Statistic | Period | Significance |
|--------|------|-----------|--------|--------------|
| CurrVolatileItems | Count | Maximum | 300s | Keys with TTL set. Compare with CurrItems: if CurrVolatileItems << CurrItems, many keys lack TTL (unbounded growth risk). |
| Reclaimed | Count | Sum | 300s | Expired keys per period. Derived from `expired_keys`. Healthy TTL activity indicator. |
| KeysTracked | Count | Maximum | 300s | Keys tracked for client-side caching, as a share of tracking-table-max-keys. |
| DB0AverageTTL | Milliseconds | Maximum | 300s | Average TTL of keys in DB0. Relevant on primary nodes only; ignore on replicas. |

### Connection Health (Node-Based)

| Metric | Unit | Statistic | Period | Significance |
|--------|------|-----------|--------|--------------|
| BlockedConnections | Count | Maximum | 300s | Clients blocked waiting on BLPOP, BRPOP, etc. High values may indicate slow consumers or queue backup. |
| RejectedConnections | Count | Sum | 300s | Connections rejected because maxclients limit reached. Any non-zero = connection exhaustion. |
| ErrorCount | Count | Sum | 300s | Total failed commands. Non-zero warrants investigation into command errors. |

### Topology / Replication (Node-Based)

| Metric | Unit | Statistic | Period | Significance |
|--------|------|-----------|--------|--------------|
| ReplicationBytes | Bytes | Maximum | 300s | Bytes the primary is sending to all replicas — the write load on the replication group. |
| MasterLinkHealthStatus | Boolean (0/1) | Minimum | 300s | 1 = replica data in sync with the primary; 0 = out of sync (chiefly relevant during migration from an external Redis). |

### Command Mix (Node-Based) — Workload Classification

| Metric | Unit | Statistic | Period | Significance |
|--------|------|-----------|--------|--------------|
| GetTypeCmds | Count | Sum | 300s | All read-only commands (get, hget, scard, lrange, etc.). Read vs write ratio. |
| SetTypeCmds | Count | Sum | 300s | All mutative commands (set, hset, sadd, lpop, etc.). Read vs write ratio. |
| StringBasedCmds | Count | Sum | 300s | GET/SET workload volume. Dominant in cache-aside patterns. |
| HashBasedCmds | Count | Sum | 300s | Hash operations. Dominant in session stores, user profiles. |
| SortedSetBasedCmds | Count | Sum | 300s | Sorted set ops. Dominant in leaderboards, ranking. |
| ListBasedCmds | Count | Sum | 300s | List operations. Job queues, activity feeds. |
| SetBasedCmds | Count | Sum | 300s | Set operations. Membership checks, deduplication. |
| StreamBasedCmds | Count | Sum | 300s | Stream operations. Event sourcing, durable queues. |
| PubSubBasedCmds | Count | Sum | 300s | Pub/sub operations. Real-time messaging. |
| ClusterBasedCmds | Count | Sum | 300s | Cluster commands (cluster slot/info, etc.). |
| EvalBasedCmds | Count | Sum | 300s | Lua eval/evalsha volume. |
| GeoSpatialBasedCmds | Count | Sum | 300s | Geospatial commands (geoadd, geodist, etc.). |
| HyperLogLogBasedCmds | Count | Sum | 300s | HyperLogLog commands (pfadd, pfcount, pfmerge). |
| KeyBasedCmds | Count | Sum | 300s | Key-scoped commands across types (del, expire, rename, etc.). |
| NonKeyTypeCmds | Count | Sum | 300s | Commands that act on no key (acl, dbsize, info). |
| ProcessedCommands | Count | Sum | 300s | Total commands processed by the engine — overall throughput. |
| PubSubChannels | Count | Maximum | 300s | Active pub/sub channels with at least one subscriber. |
| PubSubShardChannels | Count | Maximum | 300s | Active sharded pub/sub channels (Valkey 7.2+ / Redis OSS 7.0+). Growth suggests moving to sharded pub/sub to scale out. |

### Command Latency (Node-Based) — microseconds, Average

CPU time per command family, `delta(usec)/delta(calls)` from `commandstats`. We
collect the counts above; these are the matching latencies, where a slow command
type shows up.

| Metric | Unit | Statistic | Period | Significance |
|--------|------|-----------|--------|--------------|
| GetTypeCmdsLatency | Microseconds | Average | 300s | Read command latency. |
| SetTypeCmdsLatency | Microseconds | Average | 300s | Write command latency. |
| StringBasedCmdsLatency | Microseconds | Average | 300s | String command latency. |
| HashBasedCmdsLatency | Microseconds | Average | 300s | Hash command latency. |
| SortedSetBasedCmdsLatency | Microseconds | Average | 300s | Sorted-set command latency. |
| ListBasedCmdsLatency | Microseconds | Average | 300s | List command latency. |
| SetBasedCmdsLatency | Microseconds | Average | 300s | Set command latency. |
| StreamBasedCmdsLatency | Microseconds | Average | 300s | Stream command latency. |
| PubSubBasedCmdsLatency | Microseconds | Average | 300s | Pub/sub command latency. |
| ClusterBasedCmdsLatency | Microseconds | Average | 300s | Cluster command latency. |
| EvalBasedCmdsLatency | Microseconds | Average | 300s | Eval command latency. |
| GeoSpatialBasedCmdsLatency | Microseconds | Average | 300s | Geospatial command latency. |
| HyperLogLogBasedCmdsLatency | Microseconds | Average | 300s | HyperLogLog command latency. |
| KeyBasedCmdsLatency | Microseconds | Average | 300s | Key-scoped command latency. |
| NonKeyTypeCmdsLatency | Microseconds | Average | 300s | Non-key command latency. |

### Serverless ECPU Breakdown (Cost Attribution)

Dimension: `clusterId` — **not** `ServerlessCacheName`, which is only the
ElastiCache API parameter name. Querying CloudWatch with the latter returns
zero datapoints for every serverless metric (see `fetch_metrics.py:769`).

| Metric | Unit | Statistic | Period | Significance |
|--------|------|-----------|--------|--------------|
| GetTypeCmdsECPUs | Count | Sum | 300s | ECPUs consumed by read commands — cost attribution. |
| SetTypeCmdsECPUs | Count | Sum | 300s | ECPUs consumed by write commands — cost attribution. |
| StringBasedCmdsECPUs | Count | Sum | 300s | String command ECPU cost. |
| HashBasedCmdsECPUs | Count | Sum | 300s | Hash command ECPU cost. |
| SortedSetBasedCmdsECPUs | Count | Sum | 300s | Sorted set ECPU cost. |
| ListBasedCmdsECPUs | Count | Sum | 300s | List command ECPU cost. |
| SetBasedCmdsECPUs | Count | Sum | 300s | Set command ECPU cost. |
| StreamBasedCmdsECPUs | Count | Sum | 300s | Stream command ECPU cost. |
| PubSubBasedCmdsECPUs | Count | Sum | 300s | Pub/sub ECPU cost. |

---

## Tier 3: Specialized (Conditional Collection)

Collect only when inventory data indicates the feature/capability is relevant.

### Data Tiering (r6gd instances only)

Condition: `node_type` contains `r6gd`

| Metric | Unit | Statistic | Period | Significance |
|--------|------|-----------|--------|--------------|
| BytesReadFromDisk | Bytes | Sum | 300s | SSD read I/O volume. High values may indicate hot data not fitting in memory tier. |
| BytesWrittenToDisk | Bytes | Sum | 300s | SSD write I/O volume. |
| NumItemsReadFromDisk | Count | Sum | 300s | Items fetched from SSD per minute. |
| NumItemsWrittenToDisk | Count | Sum | 300s | Items written to SSD per minute. |

### Global Datastore (cross-region replication)

Condition: Cluster is part of a Global Datastore (from `describe-global-replication-groups`)

| Metric | Unit | Statistic | Period | Significance |
|--------|------|-----------|--------|--------------|
| GlobalDatastoreReplicationLag | Seconds | Maximum | 300s | Lag between secondary and primary region. For cluster-mode-enabled, reports max delay among shards. |

### Vector Search (Valkey 8.2+ node-based)

Condition: `engine == valkey` AND `engine_version >= 8.2`

| Metric | Unit | Statistic | Period | Significance |
|--------|------|-----------|--------|--------------|
| SearchBasedCmds | Count | Sum | 300s | All search commands (read + write). |
| SearchBasedGetCmds | Count | Sum | 300s | Search read-only commands. |
| SearchBasedSetCmds | Count | Sum | 300s | Search write commands. |
| SearchBasedCmdsLatency | Microseconds | Average | 300s | Latency of all search commands. |
| SearchBasedGetCmdsLatency | Microseconds | Average | 300s | Latency of search read commands. |
| SearchBasedSetCmdsLatency | Microseconds | Average | 300s | Latency of search write commands. |
| SearchNumberOfIndexes | Count | Maximum | 300s | Number of created indexes. |
| SearchTotalIndexedDocuments | Count | Maximum | 300s | Total keys in all indexes. |
| SearchUsedMemoryBytes | Bytes | Maximum | 300s | Memory consumed by search data structures. |

### JSON Module

Condition: `JsonBasedCmds > 0` in first sample OR engine supports JSON

| Metric | Unit | Statistic | Period | Significance |
|--------|------|-----------|--------|--------------|
| JsonBasedCmds | Count | Sum | 300s | All JSON commands. |
| JsonBasedGetCmds | Count | Sum | 300s | JSON read commands. |
| JsonBasedSetCmds | Count | Sum | 300s | JSON write commands. |
| JsonBasedCmdsLatency | Microseconds | Average | 300s | Latency of all JSON commands. |
| JsonBasedGetCmdsLatency | Microseconds | Average | 300s | Latency of JSON read commands. |
| JsonBasedSetCmdsLatency | Microseconds | Average | 300s | Latency of JSON write commands. |

### Valkey 9.x Engine Metrics (node-based)

Condition: `engine == valkey` AND `engine_version >= 9.0`

| Metric | Unit | Statistic | Period | Significance |
|--------|------|-----------|--------|--------------|
| CurrItemsWithVolatileFields | Count | Maximum | 300s | Keys containing hash fields with per-field expiration set. |
| ReclaimedFields | Count | Sum | 300s | Expired hash fields reclaimed by the active-expiration process. |

### Security Audit

Condition: Always collect (but flag only when non-zero)

| Metric | Unit | Statistic | Period | Significance |
|--------|------|-----------|--------|--------------|
| AuthenticationFailures | Count | Sum | 300s | Failed AUTH attempts. Any non-zero = potential unauthorized access. |
| KeyAuthorizationFailures | Count | Sum | 300s | Failed key access attempts (ACL violations). |
| CommandAuthorizationFailures | Count | Sum | 300s | Failed command authorization (ACL violations). |
| IamAuthenticationExpirations | Count | Sum | 300s | Expired IAM-authenticated connections. |
| IamAuthenticationThrottling | Count | Sum | 300s | Throttled IAM auth requests. |
| DatabaseAuthorizationFailures | Count | Sum | 300s | Failed database access attempts (Valkey 9.1+). |
| ChannelAuthorizationFailures | Count | Sum | 300s | Failed channel access attempts. |

### Burstable Instances (T-series)

Condition: `node_type` starts with `cache.t`

| Metric | Unit | Statistic | Period | Significance |
|--------|------|-----------|--------|--------------|
| CPUCreditBalance | Credits | Minimum | 300s | Available burst credits. Low balance = approaching throttling. |
| CPUCreditUsage | Credits | Sum | 300s | Credits consumed. Sustained high usage depletes balance. |

### Memory Pressure Deep Dive

Condition: `DatabaseMemoryUsagePercentage > 70%` OR `SwapUsage > 0`

| Metric | Unit | Statistic | Period | Significance |
|--------|------|-----------|--------|--------------|
| MajorPageFaults | Count | Sum | 300s | Engine process swapping. High values = active swapping degrading performance. |
| AllocatorFragmentationBytes | Bytes | Maximum | 300s | Memory wasted by allocator fragmentation. Quantifies what `activedefrag` can reclaim. |
| AllocatorFragmentationRatio | Number | Maximum | 300s | Ratio of allocator fragmentation. Higher = more severe. |

### Durability-Enabled Clusters

Condition: Cluster has durability/Multi-AZ transactional log enabled

| Metric | Unit | Statistic | Period | Significance |
|--------|------|-----------|--------|--------------|
| DurabilityLag | Milliseconds | Maximum | 300s | Age of oldest write acknowledged but not persisted. |
| DurabilityBufferExceededErrorCount | Count | Sum | 300s | Writes rejected due to exceeding 10-second durability window. |

---

## Latency Metrics: Extended Statistics (Percentiles)

Both `SuccessfulReadRequestLatency` and `SuccessfulWriteRequestLatency` support
CloudWatch Extended Statistics (percentiles p0 through p100). For the operations review,
collect these percentiles:

| Percentile | Purpose |
|------------|---------|
| p50 | Median — typical user experience |
| p95 | Sustained behavior — capacity planning threshold |
| p99 | Tail latency — worst-case for most users |
| p100 (Max) | Absolute worst case — spike detection |

Use `GetMetricData` with `ExtendedStatistics` parameter to request percentiles.
Note: Percentile statistics require a 60-second period (not 300s).

---

## Dimension Reference

| Deployment | Dimension Name | Dimension Value | Scope |
|------------|---------------|-----------------|-------|
| Node-based | `CacheClusterId` | e.g., `my-rg-001` | Per-node (primary or replica) |
| Node-based | `CacheNodeId` | `0001` | Combined with CacheClusterId for node-level |
| Serverless | `clusterId` | e.g., `my-cache` | Per-cache (aggregate) |

**Important**: Do NOT use `ReplicationGroupId` as a dimension for per-node metrics — it
aggregates across nodes and hides shard imbalance. Always use `CacheClusterId` for
node-based clusters to get per-node visibility.

---

## Period Selection Guide

| Metric Category | Resolution | Window | Datapoints | Use Case |
|---|---|---|---|---|
| **All Tier 1+2 (except latency)** | 300s (5 min) | 14 days | ~4,032 | Primary analysis — spike detection, trends, shard balance |
| **Latency (Avg, Max)** | 300s (5 min) | 14 days | ~4,032 | Sustained latency trend |
| **Latency percentiles (p50/p95/p99)** | 60s (1 min) | Last 24h | ~1,440 | Tail latency distribution (recent window) |
| **Trend-only fallback** (100+ cluster accounts with budget pressure) | 300s (5 min) | 14 days | ~4,032 | Same — never downgrade to 1-hour |

**Never use 1-hour for operational review analysis.** See Resolution Strategy section above for detailed justification.

---

## Collection Batching Strategy

### GetMetricData Limits

- Maximum 500 `MetricDataQuery` objects per API call
- Maximum 100,800 datapoints per call (across all queries)
- Can request multiple statistics for the same metric in one query using metric math

### Batch Planning

At 5-min resolution over 14 days, each metric query returns ~4,032 datapoints.
This exceeds the 100,800 limit for typical clusters, so we chunk by time:

```
Budget per call: 100,800 datapoints
Queries per cluster: ~441 (9 nodes × 49 metrics)
Max datapoints per query per chunk: 100,800 ÷ 175 queries = 576 → ~2 days at 5-min
```

**Strategy: 7 time windows × 3 metric batches = 21 calls per cluster**

Parallelized per region with rate limiting. 50 clusters ≈ 1,050 total API calls → 3-5 minutes.

### Cost

CloudWatch `GetMetricData` charges ~$0.01 per 1,000 metrics queried.
Full review of 50 clusters: ~184K metric queries ≈ $1.84.

---

## Metrics-to-Analysis Mapping

How each metric feeds into the mathematical models (Stage 3):

| Model | Primary Metrics Used |
|-------|---------------------|
| Percentile Analysis | SuccessfulRead/WriteRequestLatency (p50/p95/p99), EngineCPUUtilization, DatabaseMemoryUsagePercentage |
| Trend Detection | DatabaseMemoryUsagePercentage, BytesUsedForCache, CurrItems, EngineCPUUtilization, CurrConnections |
| Capacity Forecast | DatabaseMemoryUsagePercentage, EngineCPUUtilization, CurrConnections, NetworkBaselineUsage% |
| Utilization Scoring | EngineCPUUtilization (p95) + DatabaseMemoryUsagePercentage (Max) weighted composite |
| Anomaly Detection | All Tier 1 metrics — spike frequency via rolling σ bands |
| Efficiency Ratios | CacheHits+CacheMisses (hit rate), BytesUsedForCache (memory efficiency), GetTypeCmds+SetTypeCmds (R/W ratio) |
| Shard Balance | EngineCPUUtilization per-node, DatabaseMemoryUsagePercentage per-node (coefficient of variation) |
| Traffic Pattern | GetTypeCmds+SetTypeCmds by hour-of-day (peak-to-trough ratio for serverless fit) |

---

## Serverless vs Node-Based Metric Availability

| Metric Category | Node-Based | Serverless | Notes |
|----------------|------------|------------|-------|
| EngineCPUUtilization | ✓ | ✗ | Serverless abstracts engine CPU |
| DatabaseMemoryUsagePercentage | ✓ | ✗ | Serverless auto-scales storage |
| ReplicationLag | ✓ | ✗ | Serverless handles internally |
| ECPU metrics | ✗ | ✓ | Serverless-only billing metric |
| ThrottledCmds | ✗ | ✓ | Serverless-only rate limiting |
| Network saturation (*Exceeded) | ✓ | ✗ | Serverless abstracts network |
| NetworkBaseline*Percentage | ✓ | ✗ | Serverless abstracts network |
| TrafficManagementActive | ✓ | ✗ | Node-based only |
| CacheHitRate | ✓ | ✓ | Both |
| Latency (Read/Write) | ✓ | ✓ | Both |
| CurrConnections | ✓ | ✓ | Both |
| Evictions | ✓ | ✓ | Both |
| BytesUsedForCache | ✓ | ✓ | Both |
| Command mix (*BasedCmds) | ✓ | ✓ | Both |
| *ECPUs (per-command-type) | ✗ | ✓ | Serverless-only cost attribution |
