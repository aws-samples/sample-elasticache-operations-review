# Well-Architected Framework Mapping

Maps ElastiCache operational findings to AWS Well-Architected Framework pillars.
Each finding includes the check, data source, and recommended action.

## Reading the Status column

The distinction matters because it tells you whether a finding you see in the
output can be traced back to a row here.

| Status | Meaning |
|--------|---------|
| ✅ Stage 3.5 check | A `Check` in `check_configuration.py`'s registry. Emits this exact `check_id` in `config_findings.json`, so a row here and a finding in the output are the same object. Verify with `check_ids` in that file's metadata. |
| 📊 Stage 3 threshold | Implemented as a metric threshold in `analyze_metrics.py`, which emits findings keyed by `metric_name` and `model_source` — **not** by `check_id`. The condition is evaluated; the ID in this table is documentation only and appears nowhere in `analysis.json`. |
| ➡️ Covered elsewhere | The condition is evaluated, under a different check ID. Named here so the row is not re-implemented as a duplicate. |
| 🔲 Planned | Not implemented. No code evaluates this. |

This table was previously wrong in a way worth naming: eleven rows claimed
"✅ Implemented" for conditions no code evaluated, and several more conflated the
two kinds of implementation above. A mapping document that overstates coverage is
worse than an incomplete one — it invites a reader to assume a cluster passed a
check that never ran. Every ✅ below is verifiable against `check_ids`; every 📊
against the `ThresholdRegistry` in `analyze_metrics.py`.

## Pillar 1: Operational Excellence

Focus: Monitoring, observability, event management, operational procedures.

| Check ID | Check | Data Source | Finding Condition | Recommendation | Status |
|----------|-------|-------------|-------------------|----------------|--------|
| OE-01 | CloudWatch alarms configured | describe-events, list-metrics | No alarms exist for this cluster | Deploy alarm pack using generate_dashboards.py | 🔲 Planned |
| OE-02 | Recent operational events | describe-events (14 days) | Failover, maintenance, or error events occurred | Review event history and ensure runbooks are in place | 🔲 Planned |
| OE-03 | Engine version currency | describe-cache-clusters | Engine version is 2+ minor versions behind latest | Plan upgrade to benefit from performance and security fixes | 🔲 Planned — needs a "latest version" source; `SEC-06` covers the dated case |
| OE-04 | Resource tagging | list-tags-for-resource | Missing Environment, Owner, or Application tags | Add tags for operational ownership and cost attribution | ✅ Stage 3.5 check |
| OE-05 | Parameter group customization | describe-cache-parameters | Using default parameter group | Create custom parameter group for tuning flexibility | 🔲 Planned |
| OE-06a | Slow-log delivery configured | describe-replication-groups `LogDeliveryConfigurations` | No active slow-log delivery (node-based; gated on Redis OSS 6.0+ or Valkey) | Enable slow-log delivery via modify-* for command-level incident diagnosis | ✅ Stage 3.5 check |
| OE-06b | Engine-log delivery configured | describe-replication-groups `LogDeliveryConfigurations` | No active engine-log delivery (node-based; gated on Redis OSS 6.2+ or Valkey) | Enable engine-log delivery via modify-* for failed-sync/backup/critical events | ✅ Stage 3.5 check |

## Pillar 2: Security

Focus: Encryption, authentication, network controls, least privilege.

| Check ID | Check | Data Source | Finding Condition | Recommendation | Status |
|----------|-------|-------------|-------------------|----------------|--------|
| SEC-01 | In-transit encryption (TLS) | describe-replication-groups | TransitEncryptionEnabled = false | Enable TLS using preferred→required migration | ✅ Stage 3.5 check |
| SEC-02 | At-rest encryption | describe-replication-groups | AtRestEncryptionEnabled = false | Requires new cluster (cannot enable on existing) | ✅ Stage 3.5 check |
| SEC-03 | Authentication configured | describe-replication-groups | No RBAC user group and no AUTH token | Configure RBAC with dedicated users per application | ✅ Stage 3.5 check |
| SEC-04 | Auth model is RBAC (not legacy AUTH) | describe-replication-groups | AuthTokenEnabled = true but no UserGroupIds | Migrate from AUTH token to RBAC for granular access | ✅ Stage 3.5 check |
| SEC-05 | Security group least-privilege | ec2:describe-security-groups | 0.0.0.0/0 or overly broad CIDR in inbound rules | Restrict to application security groups only | ✅ Stage 3.5 check |
| SEC-06 | Engine under standard support | inventory `engine` + `engine_version` | Redis OSS 4/5/6 past, or approaching, end of standard support (dates: `references/engine-support-lifecycle.md`) | Upgrade to Valkey or a Redis OSS version under standard support | ✅ Stage 3.5 check |
| SEC-07 | Default user disabled | User group configuration | Default user has access | Disable default user access string | 🔲 Planned |

## Pillar 3: Reliability

Focus: High availability, fault tolerance, backup/recovery, replication health.

| Check ID | Check | Data Source | Finding Condition | Recommendation | Status |
|----------|-------|-------------|-------------------|----------------|--------|
| REL-01 | Multi-AZ enabled | describe-replication-groups | MultiAZ != enabled | Enable Multi-AZ for automatic cross-AZ failover | ✅ Stage 3.5 check |
| REL-02 | Automatic failover | describe-replication-groups | AutomaticFailover != enabled | Enable automatic failover | ✅ Stage 3.5 check |
| REL-03 | Replica count >= 2 | describe-replication-groups | Fewer than 2 replicas per shard | Add replicas for HA (WA recommends minimum 2) | ✅ Stage 3.5 check |
| REL-04 | Backups configured | describe-replication-groups | SnapshotRetentionLimit = 0 | Enable daily backups with 7+ day retention | ✅ Stage 3.5 check |
| REL-04b | Backup retention meets policy | describe-replication-groups | 0 < SnapshotRetentionLimit < 7 days | Raise retention to cover a full week of recovery points | ✅ Stage 3.5 check — split from REL-04 so "no backups at all" (HIGH) and "backups too short" (LOW) are not reported as the same finding |
| REL-05 | Replication lag healthy | CloudWatch ReplicationLag | Max ReplicationLag > 1s sustained | Investigate write volume, scale up, or add shards | 🔲 Planned |
| REL-06 | No recent failover events | describe-events | Failover event in last 14 days | Review cause, verify recovery was clean | 🔲 Planned |
| REL-07 | Subnet spans multiple AZs | describe-cache-subnet-groups | Subnets in < 3 AZs | Add subnets in additional AZs for fault tolerance | 🔲 Planned |
| REL-08 | Connection headroom | CloudWatch CurrConnections | > 80% of maxclients | Scale up or implement connection pooling | 🔲 Planned |

## Pillar 4: Performance Efficiency

Focus: Right-sizing, latency, throughput, resource utilization, hot spots.

| Check ID | Check | Data Source | Finding Condition | Recommendation | Status |
|----------|-------|-------------|-------------------|----------------|--------|
| PERF-01 | Engine CPU headroom | CloudWatch EngineCPUUtilization | p95 > 70% over 14 days | Scale up node type or add shards | 📊 Stage 3 threshold |
| PERF-02 | Read latency | CloudWatch SuccessfulReadRequestLatency (Average) | p95 of the Average series > 5000μs | Investigate slow commands, consider scaling | 📊 Stage 3 threshold |
| PERF-03 | Write latency | CloudWatch SuccessfulWriteRequestLatency (Average) | p95 of the Average series > 5000μs | Investigate slow commands, consider scaling | 🔲 Planned — collected by Stage 2, but no threshold is registered, so it is never classified |
| PERF-04 | Cache hit rate | CloudWatch CacheHitRate (Average) | p95 < 80% (inverted metric) | Review TTL strategy, key design, eviction policy | 📊 Stage 3 threshold |
| PERF-05 | Per-shard imbalance (hot shard) | CloudWatch EngineCPUUtilization / DatabaseMemoryUsagePercentage / CacheHits per primary | Coefficient of variation across primaries > 0.15, not "max > 1.5× median" | Investigate hot key/slot, consider resharding | 📊 Stage 3 threshold (`ShardBalanceModel`; cluster-mode with 2+ shards only) |
| PERF-06 | Connection churn | CloudWatch NewConnections (Sum, 300s) | p95 of the derived `NewConnectionsPerMinute` series > 1000/min | Implement connection pooling | 📊 Stage 3 threshold |
| PERF-07 | Network saturation | CloudWatch NetworkBandwidth*Exceeded | Any > 0 | Scale to larger instance type | 🔲 Planned |
| PERF-08 | Throttling (serverless) | CloudWatch ThrottledCmds (Sum, 300s) | p95 of the derived `ThrottledCmdsPerMinute` series > 0 | Increase ECPU limit or optimize commands | 📊 Stage 3 threshold (also breach-analysed: CRITICAL boundary is 0) |
| PERF-09 | Over-provisioned CPU | CloudWatch EngineCPUUtilization | p95 < 10% for 14 days | Consider smaller node type or serverless | 🔲 Planned |
| PERF-10 | Over-provisioned memory | CloudWatch DatabaseMemoryUsage% | Max < 30% for 14 days | Consider smaller node type or serverless | 🔲 Planned |
| PERF-11 | Slow commands detected | CloudWatch Logs (slow log) | Commands with duration > 10ms appearing > 10×/day | Replace KEYS with SCAN, use HMGET instead of HGETALL, decompose large data structures | 🔲 Planned |
| PERF-12 | Expensive command patterns | CloudWatch Logs (slow log) | HGETALL, SMEMBERS, LRANGE 0 -1, SORT, or KEYS in slow log | Refactor to use cursor-based iteration or fetch specific fields only | 🔲 Planned |

## Pillar 5: Cost Optimization

Focus: Right-sizing, pricing model, engine choice, waste elimination.

| Check ID | Check | Data Source | Finding Condition | Recommendation | Status |
|----------|-------|-------------|-------------------|----------------|--------|
| COST-01 | Engine is Valkey | describe-cache-clusters | Engine = redis | Migrate to Valkey for cost reduction | ✅ Stage 3.5 check |
| COST-02 | Right-size (CPU under-utilized) | CloudWatch EngineCPUUtilization | p95 < 10% sustained | Scale down node type | 🔲 Planned |
| COST-03 | Right-size (memory under-utilized) | CloudWatch DatabaseMemoryUsage% | Max < 30% sustained | Scale down node type | 🔲 Planned |
| COST-04 | Serverless fit (variable traffic) | CloudWatch command metrics | Peak-to-trough ratio > 3x | Evaluate serverless for variable workloads | 🔲 Planned |
| COST-05 | Node-based fit (steady traffic) | CloudWatch ECPU metrics | Consistent utilization pattern | Compare node-based with RI vs serverless cost | 🔲 Planned |
| COST-06 | Cost limits set (serverless) | describe-serverless-caches | No DataStorage.Maximum or ECPUPerSecond.Maximum | Set usage limits to prevent unbounded costs | ✅ Stage 3.5 check |
| COST-07 | Extended Support charges pending | inventory `engine` + `engine_version` | Redis OSS version approaching or past end-of-support | Upgrade to avoid Extended Support charges | ➡️ Covered by `SEC-06`, deliberately not a second check — one configuration fact must not be penalized twice by the scoring formula. SEC-06's finding states the premium multiplier; the agent prices it |
| COST-08 | Idle cluster | CloudWatch TotalCmdsCount or CacheHits | Near-zero commands for 14 days | Decommission or consolidate | 🔲 Planned |
| COST-09 | Intel to Graviton migration | describe-cache-clusters | node_type contains r5, m5, r4, or m4 (Intel instances) | Migrate to r7g/m7g equivalent for cost reduction + better performance | 🔲 Planned |
| COST-10 | Graviton generation upgrade | describe-cache-clusters | node_type contains r6g or m6g (Graviton2) | Upgrade to r7g/m7g for cost improvement | 🔲 Planned |

## Pillar 6: Sustainability

Focus: Resource efficiency, minimizing waste, right-sizing for actual demand.

| Check ID | Check | Data Source | Finding Condition | Recommendation | Status |
|----------|-------|-------------|-------------------|----------------|--------|
| SUS-01 | Over-provisioned resources | PERF-09 + PERF-10 combined | Both CPU and memory under-utilized | Significant waste — scale down or consolidate | 🔲 Planned |
| SUS-02 | Idle clusters | COST-08 | Zero or near-zero traffic | Decommission unused resources | 🔲 Planned |
| SUS-03 | Efficient engine | COST-01 | Using Redis OSS instead of Valkey | Valkey uses fewer resources for same workload | 🔲 Planned |

## Scoring per Pillar

Each pillar score (0-100) is computed from the checks above:

```
pillar_score = 100 - sum(severity_penalty for each failing check)
```

Penalty per severity:
- CRITICAL: 25 points
- HIGH: 15 points
- MEDIUM: 8 points
- LOW: 3 points

Floor: 0, Ceiling: 100.

Penalties use the severity **on the finding**, not the severity on the row above.
For almost every check these are the same. `SEC-06` is the exception: its urgency
depends on the review date, so it emits LOW, MEDIUM, or HIGH depending on how far
the engine's end-of-support date is from `metadata.review_date` in
`config_findings.json`. This is why the same unchanged cluster can score lower
next quarter than it does today — and why the review date is recorded.

## Fleet-Wide Aggregation

Beyond per-cluster scores, produce fleet-level summaries:

- **Fleet health score**: Weighted average of all cluster scores
- **Common findings**: Most frequent findings across the fleet (prioritize systemic issues)
- **Risk distribution**: Count of clusters per severity level per pillar
- **Top recommendations**: Ranked by (severity × number of affected clusters)
