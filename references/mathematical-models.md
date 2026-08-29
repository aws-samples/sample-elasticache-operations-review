# Mathematical Models

Defines the statistical and analytical models applied to raw CloudWatch metrics
to produce actionable findings for the ElastiCache Operations Review.

All models use `numpy` only — no ML, no complex time-series libraries, no training data.
Each model produces concrete findings with severity levels and recommendations.

---

## Model 1: Percentile Summary

**Purpose:** Characterize the distribution of each metric to distinguish normal
operation from sustained degradation vs transient spikes.

**Method:**
- Compute p50 (median), p95, p99, max from the 14-day dataset
- The input is the statistic already selected at collection time:
  - For CPU: percentiles of 5-min Maximum values → "p95 of the worst-case per window"
  - For counts: percentiles of 5-min Sum values → "p95 of the volume per window"
  - For latency: percentiles of 5-min Average + separate 1-min p50/p95/p99 from latency detail
- Compare p95 (sustained behavior) against thresholds for capacity planning
- Use max/p95 spread (spike ratio) to characterize spike severity

**Key insight:** The statistic chosen in Stage 2 (Maximum for CPU, Sum for counters, Average for rates) already aggregates within each 5-min window. Computing p95 over those pre-aggregated values gives the right answer: "for 95% of the time, the 5-minute worst-case was below this level."

**Formula:**
```python
percentiles = numpy.percentile(values, [50, 95, 99])
spike_ratio = max(values) / percentiles[1]  # max / p95
```

**Application:**
- Latency: p95(Avg) > 5ms = sustained issue; p99(p99 from latency_detail) > 10ms = tail problem
- CPU: p95(Max) is THE capacity planning metric
- Hit rate: p50(Avg) is the sustained effectiveness

**Outputs per metric:**
- p50, p95, p99, max values
- spike_ratio (max/p95) — >3 notable, >5 severe

---

## Model 2: Trend Detection

**Purpose:** Identify metrics that are growing or declining over the 14-day window,
signaling capacity needs or degradation.

**Method:**
- Aggregate raw 5-min data into **daily averages** (14 data points) to remove intra-day cyclicality
- Apply simple linear regression (least squares) on the daily averages
- Slope = rate of change per day
- R² = goodness of fit (high R² = reliable trend, low = noisy/no trend)
- Only flag trends where R² > 0.4 AND slope is operationally meaningful

**Pre-filtering:** Only apply to metrics with meaningful trends:

| Metric | Why Trend Matters |
|--------|------------------|
| DatabaseMemoryUsagePercentage | Slow leak or organic growth |
| BytesUsedForCache | Data accumulation |
| CurrItems | Key count growth (TTL gap indicator) |
| CurrConnections (daily avg) | Application scaling or connection leak |
| Evictions (daily total) | Growing memory pressure |
| CacheHitRate (daily avg) | Degrading effectiveness |
| ReplicationLag (daily max) | Write load outpacing replication |

**Not applicable to:** EngineCPUUtilization (too cyclical), latency (event-driven),
NetworkBytes (traffic-dependent), command counts (traffic-dependent).

**Formula (on daily averages):**
```python
daily_avgs = [numpy.mean(day_chunk) for day_chunk in chunk_by_day(values)]
# len(daily_avgs) == 14 for a 14-day window
coeffs = numpy.polyfit(range(len(daily_avgs)), daily_avgs, 1)
slope_per_day = coeffs[0]
slope_per_week = slope_per_day * 7

# R² calculation
predicted = numpy.polyval(coeffs, range(len(daily_avgs)))
ss_res = numpy.sum((daily_avgs - predicted) ** 2)
ss_tot = numpy.sum((daily_avgs - numpy.mean(daily_avgs)) ** 2)
r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
```

**Interpretation:**
- R² > 0.7: Strong linear trend (memory leak, sustained growth)
- R² 0.4-0.7: Moderate trend (directional but noisy)
- R² < 0.4: No meaningful linear trend → don't report

**Severity mapping:**
| Metric | Trend | Severity |
|--------|-------|----------|
| DatabaseMemoryUsagePercentage | Rising > 2% per week | MEDIUM |
| EngineCPUUtilization (if applied) | Rising > 5% per week | MEDIUM |
| CacheHitRate | Declining > 5% per week | HIGH |
| Evictions | Increasing (positive slope, R² > 0.4) | HIGH |
| CurrConnections | Sustained growth without traffic growth | MEDIUM |
| ReplicationLag | Increasing trend | HIGH |

**Outputs per metric:**
- slope_per_week (% or absolute units per week)
- r_squared (trend confidence)
- direction: "rising" | "declining" | "stable"

---

## Model 3: Utilization Matrix (CPU × Memory × Network)

**Purpose:** Classify each cluster on a 3-axis spectrum from over-provisioned to saturated,
with specific recommendations per zone.

**Method:**
- Compute utilization level for each axis using the most recent 7 days (reflects current state, not 14-day history which might include a past scaling event):
  - **CPU axis**: p95 of EngineCPUUtilization (Maximum) over last 7 days
  - **Memory axis**: Maximum of DatabaseMemoryUsagePercentage over last 7 days
  - **Network axis**: p95 of NetworkBaselineUsageInPercentage (or OutPercentage, whichever direction has the higher p95) over last 7 days. **p95, not maximum:** Network=High forces NETWORK-BOUND regardless of the other two axes, so a single five-minute burst above 80% would relabel an idle cluster as network-bound. Burst allowances are designed to absorb spikes. If not available (serverless), use ThrottledCmds presence.

- Map each axis to a level:

| Level | CPU Range | Memory Range | Network Range |
|-------|-----------|-------------|---------------|
| Low | < 20% | < 30% | < 40% |
| Medium | 20-70% | 30-70% | 40-80% |
| High | > 70% | > 70% | > 80% |

- Classify cluster by the combination:

| Classification | Condition | Recommendation |
|---|---|---|
| **IDLE** | CPU=Low AND Memory=Low AND Network=Low | Decommission or consolidate |
| **OVER-PROVISIONED** | CPU=Low AND Memory=Low, Network=any-low | Scale down node type |
| **CPU-BOUND** | CPU=High, Memory=Low/Med | Scale up node type or add shards |
| **MEMORY-BOUND** | Memory=High, CPU=Low/Med | Scale up to larger memory node |
| **NETWORK-BOUND** | Network=High, CPU/Memory=any | Scale to larger instance with more bandwidth |
| **BALANCED** | CPU=Med AND Memory=Med AND Network≤Med | Well-sized (OPTIMAL) |
| **HEAVY** | Two axes High | Monitor closely, plan scaling |
| **SATURATED** | CPU=High AND Memory=High | Immediate scaling needed |

**For serverless:** Replace CPU/Memory/Network with:
- ECPU utilization (% of configured maximum)
- Storage utilization (% of configured DataStorage.Maximum)
- Throttling presence (ThrottledCmds > 0 sustained)

**Outputs per cluster:**
- cpu_level, memory_level, network_level
- classification (string)
- recommendation (string)
- utilization_scores: {cpu_p95, memory_max, network_p95} for node-based;
  {cpu_p95, memory_max, network_throttle_score} for serverless. Each key names the
  statistic it holds — the serverless network axis is a throttling proxy (90/50/0 by
  the share of windows with any ThrottledCmds), not a bandwidth percentage, so it
  does not share the node-based key. Any axis whose ceiling is unconfigured is `null`,
  never `0.0`, and its level is `"Unknown"`.

---

## Model 4: Breach Analysis (Simplified Anomaly Detection)

**Purpose:** Identify notable incidents and sustained threshold violations over the
14-day period. Answers: "Were there problems, and how long did they last?"

**Method (simple, no rolling σ):**

1. **Spike ratio**: `max / p95`. Measures how extreme spikes are relative to normal.
   - < 2: Stable (spikes within normal range)
   - 2-3: Notable spikes
   - 3-5: Significant spikes
   - > 5: Severe spikes (investigate)

2. **Breach duration**: Count 5-min windows exceeding the CRITICAL threshold for each metric.
   ```python
   breach_windows = sum(1 for v in values if v > critical_threshold)
   breach_minutes = breach_windows * 5
   breach_hours = breach_minutes / 60
   ```

3. **Incident clustering**: Find consecutive breaches to identify distinct incidents.
   ```python
   # Group consecutive breach windows into incidents
   incidents = []
   current_incident_start = None
   for i, v in enumerate(values):
       if v > threshold:
           if current_incident_start is None:
               current_incident_start = i
       else:
           if current_incident_start is not None:
               incidents.append((current_incident_start, i - 1, max(values[current_incident_start:i])))
               current_incident_start = None
   ```
   Report top 3 incidents by duration × severity.

4. **Sustained breach check**: Is the metric CURRENTLY (last 24h) above threshold?
   ```python
   recent_values = values[-288:]  # last 24h at 5-min = 288 points
   currently_breaching = numpy.percentile(recent_values, 95) > threshold
   ```

**Severity classification:**
| Condition | Severity |
|---|---|
| Currently breaching AND breach > 4 hours total | CRITICAL |
| Currently breaching OR breach > 2 hours total | HIGH |
| Breach > 30 minutes total, but recovered | MEDIUM |
| Spike ratio > 5 but no sustained breach | LOW |
| No breach, spike ratio < 3 | HEALTHY |

**Outputs per metric:**
- spike_ratio
- breach_minutes (total over 14 days)
- breach_percent (% of total time in breach)
- incidents (top 3: start_time, duration_minutes, peak_value)
- currently_breaching (bool)

---

## Model 5: Efficiency Ratios

**Purpose:** Compute derived metrics that indicate operational efficiency and
cache effectiveness.

**Method:** Calculate each ratio from the 14-day metric totals or recent peaks.

| Ratio | Formula | Source | Good | Bad | Severity if Bad |
|---|---|---|---|---|---|
| **Hit Rate** | Sum(CacheHits) / (Sum(CacheHits) + Sum(CacheMisses)) | 14-day sums | > 80% | < 60% | HIGH |
| **TTL Coverage** | Max(CurrVolatileItems) / Max(CurrItems) | Recent max | > 90% | < 50% | MEDIUM |
| **Eviction Pressure** | Sum(Evictions) / Max(CurrItems) | 14-day evictions / current items | < 0.1% | > 1% | HIGH |
| **Memory Utilization** | Max(DatabaseMemoryUsagePercentage) last 7 days | Recent peak | 40-70% | < 20% or > 85% | LOW or HIGH |
| **Connection Utilization** | Max(CurrConnections) / maxclients | Peak vs capacity | < 80% | > 80% | HIGH |
| **Network Burst Risk** | Max(NetworkBaselineMaxUsageInPercentage or Out) | 14-day peak | < 80% | > 100% | HIGH |
| **Write Amplification** | Sum(ReplicationBytes) / Sum(NetworkBytesIn) | 14-day sums | < 3x | > 5x | MEDIUM |
| **Cost per Million Ops** | monthly_cost / (total_commands / 1M) | Cost + commands | < fleet median | > 2× fleet median | MEDIUM |
| **Read/Write Ratio** | Sum(GetTypeCmds) / Sum(SetTypeCmds) | 14-day sums | N/A | N/A | Informational |

**Special handling:**
- **Cost per Million Ops**: Only meaningful when cost data is available. Compare across the fleet to identify outlier clusters.
- **TTL Coverage**: If < 50%, flag as "unbounded key growth risk" (especially for node-based where serverless uses volatile-lru).
- **Connection Utilization**: maxclients defaults to 65,000 for most node types. Use lower values for t2/t3/t4g small instances.

**Outputs per cluster:**
- Each ratio as a float
- Assessment per ratio (HEALTHY / MEDIUM / HIGH / CRITICAL)
- Recommendation text if ratio is bad

---

## Model 6: Shard Balance Analysis (Cluster-Mode Only)

**Purpose:** Detect uneven load distribution across shards that indicates hot keys,
hash-tag concentration, or slot imbalance.

**Applicability:** Node-based clusters with `cluster_mode_enabled == true` AND `num_shards >= 2`.

**Method:**
- For each primary node in the cluster, extract the per-node metric values
- Compute the Coefficient of Variation (CV): `CV = std_dev / mean`
- Compute CV at two time scales:
  - **CV of p95 values** (one p95 per node) → sustained/structural imbalance
  - **CV of Max values** (one max per node) → intermittent/spike imbalance

**Metrics to balance-check:**
- EngineCPUUtilization (primary indicator of hot shard)
- DatabaseMemoryUsagePercentage (memory slot imbalance)
- CacheHits (uneven traffic distribution)

**CV interpretation:**

| CV Range | Classification | Meaning |
|----------|---------------|---------|
| < 0.15 | Well-balanced | Shards share load evenly |
| 0.15-0.30 | Minor imbalance | Slight skew, monitor |
| 0.30-0.50 | Significant imbalance | Hot shard likely, investigate |
| > 0.50 | Severe imbalance | Single shard dominates |

**Combined interpretation:**
| CV(p95) | CV(Max) | Interpretation |
|---------|---------|---------------|
| Low | Low | Balanced |
| Low | High | Intermittent hot key (occasional traffic burst to one shard) |
| High | High | Persistent structural imbalance (hash-tag concentration) |
| High | Low | Unlikely — structural implies sustained peaks |

**Formula:**
```python
# per_node_p95 = [p95 for node in primary_nodes]
mean = numpy.mean(per_node_p95)
std = numpy.std(per_node_p95)
cv_sustained = std / mean if mean > 0 else 0

# per_node_max = [max for node in primary_nodes]
mean_max = numpy.mean(per_node_max)
std_max = numpy.std(per_node_max)
cv_peak = std_max / mean_max if mean_max > 0 else 0
```

**Outputs per cluster:**
- cv_sustained (CPU), cv_peak (CPU)
- cv_sustained (Memory), cv_peak (Memory)
- classification: "balanced" | "minor_imbalance" | "significant_imbalance" | "severe_imbalance"
- hottest_node_id (the node with highest p95 CPU)
- imbalance_type: "intermittent" | "structural" | "none"

---

## Model 7: Traffic Pattern Analysis

**Purpose:** Identify cyclical patterns that inform scaling strategy and the
serverless vs node-based cost decision.

**Method:**
- Aggregate command throughput (GetTypeCmds + SetTypeCmds) by hour-of-day (24 buckets)
- Aggregate by day-of-week (7 buckets)
- Compute key ratios

**Calculations:**
```python
# Hourly pattern (aggregate all same-hour values across 14 days)
hourly_avgs = []
for hour in range(24):
    hour_values = [v for ts, v in zip(timestamps, values) if parse(ts).hour == hour]
    hourly_avgs.append(numpy.mean(hour_values))

peak_to_trough = max(hourly_avgs) / max(min(hourly_avgs), 1)

# Idle hours: hours where traffic < 20% of peak
idle_hours = sum(1 for h in hourly_avgs if h < 0.2 * max(hourly_avgs))

# Weekend factor
weekday_values = [v for ts, v in zip(timestamps, values) if parse(ts).weekday() < 5]
weekend_values = [v for ts, v in zip(timestamps, values) if parse(ts).weekday() >= 5]
weekend_factor = numpy.mean(weekend_values) / numpy.mean(weekday_values) if weekday_values else 1.0

# Peak window (4-hour block with highest average)
rolling_4h = [numpy.mean(hourly_avgs[i:i+4]) for i in range(21)]
peak_start_hour = numpy.argmax(rolling_4h)
```

**Classification:**
| Pattern | Condition | Implication |
|---|---|---|
| **Steady** | peak_to_trough < 2× AND idle_hours < 4 | Node-based with RI is cheapest |
| **Business-hours** | peak_to_trough 2-5× AND idle_hours 8-16 | Evaluate serverless vs node-based |
| **Highly variable** | peak_to_trough > 5× OR idle_hours > 16 | Serverless strong fit |
| **Weekend-heavy** | weekend_factor > 1.5 | Unusual — gaming, social, consumer workload |
| **Weekday-only** | weekend_factor < 0.3 | Enterprise/B2B pattern |

**Outputs per cluster:**
- peak_to_trough_ratio
- idle_hours (count of hours below 20% of peak)
- weekend_factor
- peak_hours (e.g., "09:00-13:00 UTC")
- pattern_classification
- serverless_fit_score (0-100, higher = better fit for serverless)

---

## Model 8: Correlation Analysis

**Purpose:** Detect when two metrics move together, indicating cause-effect relationships
that help identify root causes.

**Method:**
- Compute Pearson correlation coefficient between specific metric pairs
- Only report correlations with |r| > 0.7 (strong relationship)
- Use aligned timestamps (same 5-min grid)

**Strength gates reporting; the pair decides severity.** `r` measures how
reliably two metrics move together, not how much it matters that they do — and
the two diverge most on healthy clusters. `NetworkBytesIn ↔ EngineCPU` is a cache
doing its job, and the better it behaves the closer r gets to 1.0. Severity is
therefore declared per pair (`MetricPair.max_severity`) and does not vary with r.
`INFO` pairs stay in the `correlations` output as context and produce **no
finding**.

**Direction is checked, not discarded.** Each interpretation is a causal claim,
so a correlation whose sign opposes the described mechanism is evidence against
it. `SaveInProgress ↔ ReplicationLag` at r = −0.95 means lag *falls* during
snapshots — backups scheduled in the quiet window — and is not reported as
"snapshots interfering with replication". Pairs declare `expected_sign`; an
unexpected sign is dropped unless the pair supplies an `inverse_interpretation`,
which reports at `INFO`.

**Key metric pairs to correlate:**

| Pair | Expected sign | Severity | If Correlated (\|r\| > 0.7) | Root Cause Indication |
|---|---|---|---|---|
| EngineCPU ↔ SuccessfulReadLatency | + | MEDIUM | CPU saturation causing latency | Scale up |
| EngineCPU ↔ NewConnections | + | LOW | Connection churn driving CPU | Fix connection pooling |
| SaveInProgress ↔ ReplicationLag | + | MEDIUM | Snapshots interfering with replication | Schedule snapshots off-peak |
| Evictions ↔ CacheMisses | + | MEDIUM | Evictions causing cascading misses | Increase memory |
| NetworkBytesIn ↔ EngineCPU | + | **INFO** (no finding) | Traffic-proportional CPU (normal) | Workload-driven, not pathological |
| CurrConnections ↔ EngineCPU | + | LOW | Connection overhead | Reduce connections or scale up |
| TrafficManagementActive ↔ SuccessfulReadLatency | + | HIGH | Throttling causing latency | Scale up immediately |

**Formula:**
```python
# Pearson correlation
r = numpy.corrcoef(metric_a_values, metric_b_values)[0, 1]
```

**Handling:**
- Skip if either metric has all-zero or constant values (correlation undefined)
- Skip if either metric has > 20% missing values
- Only report strong correlations (|r| > 0.7) at all
- Drop a correlation whose sign contradicts the pair's `expected_sign`
- Severity comes from the pair, never from |r|

**Outputs per cluster:**
- List of strong correlations: [{metric_a, metric_b, r_value, interpretation, severity}]
- Only non-obvious correlations reported (skip trivial ones like CacheHits ↔ GetTypeCmds)

---

## Model 9: Workload Classification

**Purpose:** Automatically classify what type of workload each cluster is running
based on command mix, so recommendations can be workload-appropriate.

**Method:**
- Compute command distribution from 14-day Sum totals
- Classify based on dominant command family and read/write ratio

```python
total_cmds = sum([StringBasedCmds, HashBasedCmds, SortedSetBasedCmds,
                  ListBasedCmds, SetBasedCmds, StreamBasedCmds, PubSubBasedCmds])

profile = {
    "string_pct": StringBasedCmds / total_cmds,
    "hash_pct": HashBasedCmds / total_cmds,
    "sorted_set_pct": SortedSetBasedCmds / total_cmds,
    "list_pct": ListBasedCmds / total_cmds,
    "set_pct": SetBasedCmds / total_cmds,
    "stream_pct": StreamBasedCmds / total_cmds,
    "pubsub_pct": PubSubBasedCmds / total_cmds,
}
read_write_ratio = GetTypeCmds / max(SetTypeCmds, 1)
```

**Classification rules (evaluated in order, first match wins):**

| Classification | Condition |
|---|---|
| **Cache-aside / Query cache** | string_pct > 60% AND read_write_ratio > 4 |
| **Session store** | hash_pct > 35% AND read_write_ratio 1-4 |
| **Leaderboard / Ranking** | sorted_set_pct > 25% |
| **Rate limiter / Counter** | string_pct > 60% AND read_write_ratio < 2 |
| **Event stream** | stream_pct > 15% |
| **Real-time messaging** | pubsub_pct > 10% |
| **Queue / Job processor** | list_pct > 20% |
| **General purpose** | No single family dominates (all < 35%) |

**Why it matters for the review:**
- A 60% hit rate on a **rate limiter** is normal (write-heavy, not designed for high hit rate)
- A 60% hit rate on a **cache-aside** pattern is a problem (should be >80%)
- High evictions on a **session store** with volatile-lru is expected behavior
- High evictions on a **cache-aside** with allkeys-lru means undersized

The classification adjusts threshold interpretation in the findings.

**Outputs per cluster:**
- workload_class (string)
- command_profile (dict of percentages)
- read_write_ratio (float)
- dominant_family (string)

---

## Implementation Notes

### Dependencies
- `numpy` for all statistical computation (percentile, polyfit, corrcoef, mean, std)
- No scipy, no scikit-learn, no pandas

### Missing Data Handling
- CloudWatch can have gaps in time-series (new cache, no traffic periods)
- **Small gaps (< 3 consecutive missing)**: Interpolate with linear fill
- **Large gaps (≥ 3 consecutive missing)**: Leave as NaN, exclude from calculations
- **> 30% missing**: Mark metric as "insufficient data", don't compute models
- Use `numpy.nanmean()`, `numpy.nanpercentile()` variants where available

### Input/Output Contract
- **Input**: `metrics.json` from Stage 2 (time-series per cluster per node per metric)
- **Output**: `analysis.json` with per-cluster model results, scores, and findings

### Output Schema (analysis.json)
```json
{
  "metadata": {
    "source_metrics": "metrics.json",
    "analysis_timestamp": "2026-07-17T...",
    "models_applied": ["percentile", "trend", "utilization", "breach", "efficiency", "balance", "traffic_pattern", "correlation", "workload"],
    "clusters_analyzed": 15
  },
  "clusters": {
    "my-cluster": {
      "workload_class": "cache-aside",
      "utilization": {"cpu_level": "Medium", "memory_level": "Medium", "network_level": "Low", "classification": "BALANCED"},
      "percentiles": {"EngineCPUUtilization_Maximum": {"p50": 35, "p95": 62, "p99": 78, "max": 91, "spike_ratio": 1.47}},
      "trends": {"DatabaseMemoryUsagePercentage": {"slope_per_week": 1.8, "r_squared": 0.72, "direction": "rising"}},
      "breaches": {"EngineCPUUtilization": {"breach_minutes": 45, "spike_ratio": 1.5, "currently_breaching": false}},
      "efficiency": {"hit_rate": 0.92, "ttl_coverage": 0.95, "eviction_pressure": 0.002},
      "shard_balance": {"cpu_cv_sustained": 0.12, "cpu_cv_peak": 0.18, "classification": "balanced"},
      "traffic_pattern": {"peak_to_trough": 3.2, "idle_hours": 8, "classification": "business-hours"},
      "correlations": [{"metric_a": "EngineCPU", "metric_b": "ReadLatency", "r": 0.82}],
      "findings": [...]
    }
  }
}
```

### Execution Order

Models can be computed independently per cluster (parallelizable), but some have soft dependencies:
1. **Percentile Summary** — must run first (other models reference p95/p99/max values)
2. **Workload Classification** — should run early (contextualizes threshold interpretation)
3. **All others** — can run in any order after 1 and 2
4. **Findings generation** — runs last, aggregates all model outputs into severity-classified findings
