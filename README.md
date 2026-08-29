# Amazon ElastiCache Operations Review — AI Agent Skill for AWS Well-Architected Reviews

Review the health of an **Amazon ElastiCache** fleet — **Valkey and Redis OSS**, on both **serverless and node-based** clusters — against **AWS Well-Architected** best practices, and get a prioritized, shareable report. Deterministic Python scripts collect **Amazon CloudWatch** metrics and **AWS Cost Explorer** data and run mechanical configuration checks; an **AI agent** (for example, Claude Code) interprets the results — prioritizing findings, explaining root causes, estimating cost savings, and generating remediation guidance.

It ships as an installable **agent skill** (`SKILL.md`), runs strictly **read-only**, and works **offline** against a bundled example fleet, so you can try it without an AWS account.

## What it does

- **Security & reliability posture** — TLS in transit, encryption at rest, authentication (RBAC / AUTH token), Multi-AZ, automatic failover, replicas per shard, backups, security-group exposure, and engine end-of-support (Redis OSS Extended Support).
- **Performance analysis** — CPU / memory / network utilization, cache hit rate, evictions, replication lag, throttling, shard imbalance (hot-shard detection), and trend detection over 14 days of CloudWatch metrics.
- **Cost optimization** — idle and over-provisioned detection, right-sizing, Graviton and Valkey migration savings, and serverless-fit analysis, grounded in Cost Explorer actuals.
- **Well-Architected scoring** — per-pillar and fleet scores that are reproducible and hand-checkable, stable across runs.
- **A self-contained HTML report** — per-cluster charts plus an AI-written assessment in which every number is verified against the collected data.

## Prerequisites

- **[AWS Agent Toolkit](https://github.com/aws/agent-toolkit-for-aws) — its `amazon-elasticache` skill (required).** The AI assistant driving this review loads that skill on demand for cost pricing, remediation, and per-cluster live diagnostics — this review delegates all of those to it rather than reimplementing them.
- **Python 3.9+** with `boto3` and `numpy` (`pip install -r requirements.txt`).
- **AWS credentials** with the read-only permissions listed under [Required IAM Permissions](#required-iam-permissions).

## Architecture

Three layers cooperate — this skill's deterministic scripts, the AI assistant that drives
them, and the **AWS Agent Toolkit** the assistant reaches for when a task needs live pricing,
remediation, or hands-on cluster access:

```
This skill (data + math):     discover → fetch metrics → compute statistics → check config
                                                    │
                    inventory.json + metrics.json + analysis.json + config_findings.json
                                                    │
AI assistant (reasoning):     interpret → prioritize → present → recommend
                                                    │  loads / delegates to
                                                    ▼
AWS Agent Toolkit             cost pricing (price_calculator.py, serverless_estimator.py) ·
(amazon-elasticache skill):   remediation playbooks (TLS / Valkey / Graviton migrations) ·
                              per-cluster live diagnostics (slow-log, hot keys, big keys)
```

**This skill's scripts do:** Bulk API calls, pagination, millions of datapoints, numpy math,
and every deterministic pass/fail check (TLS enabled, backups configured, replica count,
required tags).

**The AI assistant does:** Interpretation, prioritization, presentation, conversational
follow-up — and orchestration: it decides *when* to call the toolkit and feeds it the
findings and cluster details this skill produced.

**The AWS Agent Toolkit (`amazon-elasticache`) does:** Everything that needs live data or
write actions rather than a reproducible measurement — fetching current prices, generating
remediation commands, and connecting to a cluster over an SSM tunnel for slow-log / hot-key /
big-key inspection. It is a **required companion** (see Prerequisites); the assistant loads
its sub-skills on demand.

**Where the line sits, and why it matters:** any number a customer might act on is computed
by a script and read by the agent, never estimated in prose. Severity comes from the check
registry in `check_configuration.py`; utilization classes (IDLE / BALANCED / SATURATED) come
from `analyze_metrics.py`. The agent decides what to say *first* and what a finding *means*
for this fleet — it does not decide what the numbers are. Two runs a week apart should differ
only where the fleet differs.

One check is deliberately not a pure function of the configuration: `SEC-06` grades engine
end-of-support by proximity, so an unchanged Redis OSS 6 cluster is LOW today, MEDIUM within
90 days of its date, and HIGH after it. The review date is an input (`--as-of`, defaulting to
today in UTC) and is recorded in `config_findings.json`'s `metadata.review_date`, so the
grading is still reproducible — you just have to pin the date, which is what the example fleet
does. The dates come from `references/engine-support-lifecycle.md`, a vendored copy of AWS's
published schedule with a staleness test; the pipeline makes no network calls.

## How It Works

```
User: "Review my ElastiCache fleet in us-east-1"
  ↓
Assistant runs run_review.py → discovers clusters → collects 14 days of metrics
                             → computes statistics → evaluates 16 configuration checks
  ↓
Assistant reads the four JSON outputs → applies WA knowledge → prioritizes findings
      └─ for dollar figures, loads the amazon-elasticache toolkit's price_calculator.py
         / serverless_estimator.py (live rates never enter the reproducible pipeline)
  ↓
Assistant: "Your fleet scores 85/100 — Good, but staging-redis-legacy scores 63.
            3 CRITICAL findings, all on that cluster: TLS disabled, no auth, and a
            security group open to 0.0.0.0/0. Separately, prod-api-cache is
            SATURATED — scale it before the next peak. Want me to generate fixes?"
  ↓
User: "Fix the TLS issue"
  ↓
Assistant: follows the toolkit's amazon-elasticache migration playbook and presents
           the AWS CLI commands, with the preferred→required TLS migration explained
```

That example is the output for the offline example fleet, so you can reproduce it — see **Try
it without an AWS account** below. The findings and the SATURATED class are read from
`config_findings.json` and `analysis.json`; the 85 and the 63 are derived from those findings
by the penalty formula in `SKILL.md`'s Scoring Methodology, so both can be checked by hand.
Note that the agent reports the fleet score *and* its worst cluster — an average alone would
say the fleet is fine while one cluster has three CRITICAL findings.

## Usage

### With AI Agent (Primary)

This skill is designed to be consumed by an AI agent (Kiro, Claude Code, Amazon Q, Strands).
The agent reads `SKILL.md` to understand the workflow, executes the scripts for data
collection, then applies its own reasoning for interpretation and presentation.

#### Installing the skill (so your agent discovers it)

**Claude Code** discovers skills under `~/.claude/skills/<name>/SKILL.md` (personal —
available in every project) or `<project>/.claude/skills/<name>/SKILL.md` (project-scoped).
Point either at this repo; a symlink is supported and keeps you on the latest checkout:

```bash
# Personal (available everywhere) — run from the repo root:
ln -s "$PWD" ~/.claude/skills/elasticache-operations-review

# — or project-scoped, so only one project loads it:
mkdir -p /path/to/your-project/.claude/skills
ln -s "$PWD" /path/to/your-project/.claude/skills/elasticache-operations-review
```

The link directory name — `elasticache-operations-review` — is the skill's invocation
name and matches the `name:` in `SKILL.md`'s frontmatter. Claude auto-loads the skill
when a request matches its description; you can also invoke it explicitly with
`/elasticache-operations-review`. Restart Claude Code, or wait for live change
detection, after installing.

**Kiro** discovers skills under `~/.kiro/skills/<name>/SKILL.md` (global — every workspace)
or `.kiro/skills/<name>/SKILL.md` (workspace-scoped; workspace wins on a name clash). It's
the same folder-with-`SKILL.md` layout, so a symlink works here too:

```bash
# Global (available in every workspace) — run from the repo root:
ln -s "$PWD" ~/.kiro/skills/elasticache-operations-review

# — or workspace-scoped, from your project root:
mkdir -p .kiro/skills
ln -s "$PWD" .kiro/skills/elasticache-operations-review
```

**Other agents (Amazon Q, Strands, …).** This is a standard `SKILL.md`-format skill — a
directory whose `SKILL.md` carries `name:`/`description:` frontmatter. Place it wherever
your agent discovers skills, or point the agent at `SKILL.md` directly; consult that tool's
skill / agent-instructions documentation for the exact location.

### Standalone CLI (Data Collection Only)

```bash
pip install -r requirements.txt        # boto3, numpy

# Run the collection pipeline (Stages 1-3.5)
python3 scripts/run_review.py --regions us-east-1 --profile my-profile

# Writes inventory.json, metrics.json, analysis.json, config_findings.json to ./output/
# The agent reads these to produce the assessment.
```

Useful flags: `--days N` (metric lookback, default 14), `--regions all` (every enabled
region, resolved via `ec2:DescribeRegions`), `--replication-groups` / `--serverless-caches`
to narrow the scope, `--skip-cost` if you lack Cost Explorer access, `--output DIR`,
`--verbose`. Run `--help` for the full list.

Policy the review grades against is overridable, so it fits your standard rather than
ours: `--required-tags KEY [KEY ...]` (OE-04 tag policy; default `Environment Owner
Application`, pass with no values to require none), `--min-replicas-per-shard N`
(REL-03, default 2), and `--min-snapshot-retention-days DAYS` (REL-04b, default 7). The
effective policy is recorded in `config_findings.json`'s `metadata.policy`, so a report
always states which standard produced a finding.

If you use `--skip-cost`, or the Cost Explorer call is denied, spend is reported as **not
collected** — not as `$0`. An uncollected number and a measured zero are different facts.

### Try it without an AWS account

```bash
python3 scripts/make_example_fleet.py --output examples/    # ~30s, no AWS calls
```

Generates a seven-cluster fleet across three regions — including a saturated production
cluster, a network-bound one, an idle one, a non-compliant staging cluster, and a
serverless cache — then runs the **real** Stage 3 and Stage 3.5 over it. Byte-for-byte
reproducible from a fixed seed. `examples/` is git-ignored because `metrics.json` is
~113 MB.

## What Scripts Produce (Data)

| File | Content | Source |
|------|---------|--------|
| `inventory.json` | Cluster configs, topology, security groups, parameter groups, tags | ElastiCache + EC2 + STS APIs |
| `metrics.json` | 14-day time-series (5-min resolution) + 1-min latency window + daily cost | CloudWatch + Cost Explorer |
| `analysis.json` | Computed statistics from ten models: percentiles, trends, utilization, breaches, efficiency, shard balance, traffic pattern, steadiness, correlation, workload | numpy math |
| `config_findings.json` | Pass/fail results for 16 configuration checks, with severity and remediation | inventory + check registry |

## What the Agent Produces (Judgment)

- Prioritization — which of the findings matters most for *this* fleet, and why
- Interpretation — what a statistic implies in context ("p99 latency is fine, but the shard
  imbalance explains the CPU spread")
- Cost opportunities with dollar estimates (Valkey migration, Graviton, right-sizing)
- Remediation commands (AWS CLI, Terraform, CDK) with impact explained before you run them
- Conversational follow-up — explain findings, answer "why", drill into a cluster

Findings, severities, and utilization classes come from the scripts. The agent groups,
ranks, and explains them.

That judgment reaches the HTML report through `generate_html_report.py --notes`, which
is the only path by which it survives past the chat window. It is checked rather than
trusted: every figure in the agent's prose must appear under a path that note cites, or
the render fails and names the figure. A finding the agent rules a false positive is
struck through with its reasoning attached, never deleted — deleting it would hide a
pipeline bug behind the agent's opinion.

## Required IAM Permissions

Read-only. The scripts never modify AWS resources.

- `elasticache:DescribeReplicationGroups`, `DescribeCacheClusters`,
  `DescribeServerlessCaches`, `DescribeCacheSubnetGroups`, `DescribeCacheParameters`,
  `ListTagsForResource`
- `cloudwatch:GetMetricData`
- `ec2:DescribeSecurityGroups`
- `sts:GetCallerIdentity` (records the caller in the output for audit)
- `ec2:DescribeRegions` (only for `--regions all`)
- `ce:GetCostAndUsage` (optional — without it, use `--skip-cost`)

**Make read-only enforceable, not just documented.** "Read-only" above is a property of
the code, not of your credential — run the tool under an admin profile and nothing stops a
write. Attach a policy whose grants are exactly the actions above **plus an explicit
`Deny` on everything else** (see the `DenyAnyMutation` statement in `SKILL.md` →
*Required IAM Permissions*). An explicit `Deny` overrides any `Allow` from another attached
policy, so the review cannot mutate the fleet even under an over-privileged profile, and a
compromised dependency cannot reach a write API. Prefer a per-operator role over a shared
one — `sts:GetCallerIdentity` is recorded in the output, and CloudTrail then attributes the
read calls to a person.

## Security & Data Handling

This is a local, read-only tool, but the data it produces and the network it runs on both
matter:

- **Treat the outputs as Confidential.** `inventory.json`, `analysis.json`,
  `config_findings.json`, and `report.html` map your fleet's weak points — account id,
  endpoints, security-group rules, and which clusters have TLS or auth disabled. `output/`
  is git-ignored, but there is no at-rest encryption: store these on encrypted volumes,
  restrict who you share the report with, and delete them when you are done. Redact the
  account id and endpoints before sharing a report outside your team.
- **The report treats untrusted metadata safely.** Cluster names and tags are rendered as
  text (never as HTML), and the reviewing agent is instructed to treat AWS-sourced strings
  as data, not instructions — so a tag crafted to inject markup or steer the agent is
  contained. Do not defeat this by hand-editing the report or pasting raw tag values into
  a shell.
- **Never disable TLS verification.** The scripts rely on boto3's default certificate
  validation. Do not set `verify=False` or point at an untrusted CA bundle, and run
  collection from a trusted network — a man-in-the-middle could otherwise return forged
  inventory or metrics that silently corrupt the assessment.
- **Broad scans cost money.** `--regions all` plus Cost Explorer (`ce:GetCostAndUsage` is
  billed per request) can add up on a large fleet. Scope `--regions`, and use `--skip-cost`
  where Cost Explorer access or cost is a concern.
- **Pin your dependencies.** Install with pinned, hash-verified requirements
  (`pip install --require-hashes -r requirements.txt`) and clone from the canonical
  aws-samples URL, so a tampered dependency or fork cannot run in place of the reviewed
  code. The read-only IAM policy above caps the damage if one slips through.

## Project Structure

```
elasticache-op-review/
├── SKILL.md                        # Agent instructions (the skill definition)
├── requirements.txt                # Runtime deps (boto3, numpy)
├── requirements-dev.txt            # Dev-only deps (pytest, ruff)
├── pyproject.toml                  # Ruff lint config
├── scripts/
│   ├── run_review.py               # Orchestrator (Stages 1-3.5)
│   ├── discover_inventory.py       # Stage 1: Cluster discovery
│   ├── fetch_metrics.py            # Stage 2: CloudWatch + Cost Explorer collection
│   ├── analyze_metrics.py          # Stage 3: Statistical computation (numpy)
│   ├── check_configuration.py      # Stage 3.5: Deterministic config checks
│   ├── generate_html_report.py     # Optional standalone HTML report (4 outputs + agent notes)
│   └── make_example_fleet.py       # Offline example fleet generator (no AWS)
├── references/                     # Agent knowledge base
│   ├── thresholds.md               # Severity levels per metric
│   ├── well-architected-mapping.md # WA checks per pillar
│   ├── metrics-catalog.md          # Metric definitions and collection strategy
│   ├── mathematical-models.md      # Analysis model explanations
│   ├── engine-support-lifecycle.md # Vendored engine EOL dates (SEC-06)
│   ├── report-generation.md        # notes.json + pricing.json report mechanics
│   └── iam-policy.json             # The read-only policy to attach
├── tests/                          # 1135 tests
├── examples/                       # Generated example fleet (git-ignored)
└── output/                         # Generated data (git-ignored)
```

## FAQ

### Does it modify my ElastiCache clusters?

No — it is strictly read-only. The scripts only call `Describe*` / `List*` /
`GetMetricData` / `GetCostAndUsage`, and the recommended IAM policy adds an explicit
`Deny` on every mutating action, so it cannot change your fleet even under an
over-privileged profile (see [Required IAM Permissions](#required-iam-permissions)).

### Which engines and deployment types are supported?

Valkey and Redis OSS — on both serverless and node-based (replication group)
clusters. Memcached is not covered: discovery enumerates replication groups and
serverless caches, and the configuration checks assume Redis/Valkey semantics
(RBAC/AUTH, Multi-AZ, replicas, backups).

### Can I try it without an AWS account?

Yes. `python3 scripts/make_example_fleet.py --output examples/` generates a synthetic
seven-cluster fleet and runs the real analysis over it, with no AWS calls.

### Which AI agents can run it?

Any agent that loads a `SKILL.md`-format skill. Installation is documented for Claude
Code and Kiro (see [Installing the skill](#installing-the-skill-so-your-agent-discovers-it));
Amazon Q, Strands, and other agents can consume the same skill.

### Do I need the AWS Agent Toolkit?

Yes — the [AWS Agent Toolkit](https://github.com/aws/agent-toolkit-for-aws)'s
`amazon-elasticache` skill is required. The assistant loads it for cost pricing,
remediation, and per-cluster live diagnostics (see [Prerequisites](#prerequisites)).

### Is the analysis reproducible?

Yes. The scripts are deterministic, so two runs on the same data produce byte-identical
numbers — only the AI's wording varies, never its figures. This makes week-over-week
diffs meaningful.

### How much does it cost to run?

Nothing beyond ordinary AWS API usage. CloudWatch `GetMetricData` and Cost Explorer
`GetCostAndUsage` are billed per request; scope `--regions` and use `--skip-cost` to
minimize. It makes no third-party network calls — engine end-of-support dates are
vendored, not fetched.

### What does it produce?

Four JSON artifacts (`inventory.json`, `metrics.json`, `analysis.json`,
`config_findings.json`) and an optional self-contained HTML report.

## Why This Architecture?

| Task | Scripts | Agent |
|------|---------|-------|
| Paginate 100 clusters | ✅ Mechanical | ❌ Can't hold state |
| Fetch 2M datapoints | ✅ Bulk I/O | ❌ Won't fit in context |
| Compute p95 of 4,032 points | ✅ numpy | ❌ Hallucinates math |
| Decide if TLS is enabled | ✅ Reads the field | ❌ No reason to re-derive it |
| Assign a finding's severity | ✅ Fixed registry, so it's stable across runs | ❌ Would drift run to run |
| Judge "is 78% CPU bad *here*?" | ❌ Rigid rules | ✅ Considers context |
| Estimate cost savings | ❌ Needs pricing DB | ✅ Knows AWS pricing |
| Explain WHY something is flagged | ❌ Template text | ✅ Real reasoning |
| Cross-cluster pattern recognition | ❌ Per-cluster only | ✅ Sees full picture |
| Generate remediation commands | ❌ Hardcoded templates | ✅ AWS knowledge |
| Prioritize with business context | ❌ severity × count | ✅ Understands risk |
