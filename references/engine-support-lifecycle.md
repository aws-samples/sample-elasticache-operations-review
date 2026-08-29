# Engine Support Lifecycle (Extended Support and EOL)

**Source:** https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/extended-support-versions.html
**Premium rates:** https://aws.amazon.com/elasticache/pricing/ (section "Extended support")
**Last verified:** 2026-08-11

This file is the **single source of truth for the dates** used by check `SEC-06`
(`_sec06_extended_support` in `scripts/check_configuration.py`). The table below and
the `ENGINE_SUPPORT_SCHEDULE` constant in that script must agree;
`tests/test_engine_lifecycle.py` fails if they drift.

## Why this is vendored rather than fetched

Every number in `analysis.json` and `config_findings.json` is script-computed and
byte-identical across runs, so any difference between two reviews is attributable to
the fleet changing. A network fetch inside the pipeline would break that, make tests
flaky, and stop the offline example fleet from running without credentials.

The EOL schedule is not a measurement, though — it is a published table that changes
roughly once a year, in the same category as `references/thresholds.md`. So it is
vendored with a `Last verified` date, and the check computes urgency from it by pure
date arithmetic: deterministic, offline, testable.

**What is deliberately NOT vendored: the premium rates in dollars.** Those are
region-dependent and change without notice. The check reports *dates and multipliers*
and never a dollar figure. Cost estimation belongs to the agent at presentation time,
where variation is already allowed — see `SKILL.md`'s Cost Estimation Guide.

## Extended Support and EOL schedule

Redis OSS only. Standard support ends on the date shown; **the day after**, the cache
is automatically enrolled in Extended Support and starts accruing a premium.

| Major version | End of standard support | Y1 premium starts | Y2 premium starts | Y3 premium starts | End of Extended Support (version EOL) |
|---|---|---|---|---|---|
| Redis OSS 4 | 2026-01-31 | 2026-02-01 | 2027-02-01 | 2028-02-01 | 2029-01-31 |
| Redis OSS 5 | 2026-01-31 | 2026-02-01 | 2027-02-01 | 2028-02-01 | 2029-01-31 |
| Redis OSS 6 | 2027-01-31 | 2027-02-01 | 2028-02-01 | 2029-02-01 | 2030-01-31 |

**Premium multipliers on the On-Demand node rate:** 80% for Y1 and Y2, 160% for Y3.
So a node at $0.1560/hr costs $0.2808/hr in Y1–Y2 and $0.4056/hr in Y3.

Extended Support is offered only for the latest patch release of each major version.
Clusters not already on the latest patch are upgraded to it automatically when
Extended Support begins.

## Versions with no announced end-of-support date

**Valkey (all versions), Memcached (all versions), and Redis OSS 7.x have no
announced end of standard support.** AWS has said older versions of every engine will
eventually be deprecated with advance notice, but no date exists today.

`SEC-06` must therefore report *nothing* for these, and must not extrapolate a date.
An invented deadline is worse than a missing one: a customer who acts on it
reschedules real work around a date AWS never published. The check's default is
silence, and only the three rows above can produce a finding.

## How the check grades urgency

Severity escalates with proximity, because the same configuration means something
different at different times. A Redis OSS 6 cluster is a LOW-priority upgrade in 2026
and an active surcharge in 2027 — one fixed severity cannot express both.

| Condition relative to the review date | Severity | Reasoning |
|---|---|---|
| Past end of standard support (premium accruing now) | HIGH | Paying a real surcharge every hour, and on a version receiving only critical fixes |
| Within 90 days of end of standard support | MEDIUM | An engine upgrade needs change-management lead time; 90 days is the point where starting late becomes the risk |
| More than 90 days out | LOW | Real and dated, but plannable |

The finding always names the date and the days remaining, so a reader can check the
grading rather than trust it.

## Remediation

Upgrading to Valkey ends Extended Support charges and additionally costs 20% less
per node-hour (33% less on serverless) than Redis OSS. For Multi-AZ replication
groups on Redis OSS 5.0.6 or later, the in-place engine upgrade is zero-downtime.
Single-node clusters must be converted to a replication group first. Upgrading from
earlier Redis OSS versions may see brief unavailability during DNS propagation.

Deleting a cluster running a past-EOS version also stops the charge — relevant when
the finding lands on something idle, which is a common pairing (a legacy staging
cluster nobody has touched).

## Keeping this current

`tests/test_engine_lifecycle.py` fails when `Last verified` is more than 365 days
old, so this file cannot silently rot. Re-verify against the two URLs at the top,
update the table, the `ENGINE_SUPPORT_SCHEDULE` constant, and the date — the test
checks the file and the code agree.
