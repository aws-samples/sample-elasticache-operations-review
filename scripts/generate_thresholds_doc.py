#!/usr/bin/env python3
"""
Generates the threshold tables in references/thresholds.md from the code.

Why this document is generated while the repo's other reference docs are only
asserted against
-------------------------------------------------------------------------------
references/well-architected-mapping.md, references/metrics-catalog.md, and
references/engine-support-lifecycle.md are hand-written, with a test that fails
when they disagree with the code. That pattern is right for them: each carries
material the code does not contain — a pillar rationale, a source URL, a
"last verified" date — so generating them would mean deleting the very content
that makes them worth reading.

references/thresholds.md is different in kind, because it is not documentation.
SKILL.md Step 3 instructs the agent to read it *before* interpreting
analysis.json:

    Apply your knowledge from: references/thresholds.md

So every row is an instruction. A row describing a band the registry cannot
produce is not a stale comment — it tells the agent to expect a severity that
will never arrive, and the agent reads that absence as a pass. The committed
version carried three such rows (an Evictions LOW band, an ECPU LOW band, and a
MEDIUM band for sporadic throttling), plus a trend row for a metric
TRENDABLE_METRICS does not list, and headings naming raw metrics whose thresholds
are registered under derived names. An assert-against-doc test would have caught
the last of those and none of the rest, because a test can only check the claims
someone thought to encode.

Generation makes the whole table underivable-by-hand: the numbers, the units, the
statistics, and the interpretations all come from ThresholdLevel, so a band
exists in the doc if and only if it exists in the code. `ThresholdLevel.means`
carries the interpretation text for the same reason — it has to travel with the
number it describes.

What is generated and what is not
---------------------------------
Only the region between the BEGIN/END markers. The prose above it — the severity
ladder, the note on units — is hand-written and stays hand-written: it explains
why the pipeline works this way, which is not a fact any dataclass holds.

Usage:
  python3 scripts/generate_thresholds_doc.py            # rewrite the doc
  python3 scripts/generate_thresholds_doc.py --check    # exit 1 if it would change
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from analyze_metrics import (  # noqa: E402
    FindingsGenerator,
    ThresholdRegistry,
    TrendModel,
)
from check_configuration import CHECKS  # noqa: E402

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DOC_PATH = os.path.join(REPO_ROOT, "references", "thresholds.md")

BEGIN_MARKER = "<!-- BEGIN GENERATED -->"
END_MARKER = "<!-- END GENERATED -->"

# How a unit is written in a heading. The registry's identifier is deliberately
# terse; the doc says it the way an operator reads it.
UNIT_LABELS = {
    "percent": "%",
    "per_minute": "per minute",
    "seconds": "seconds",
    "microseconds": "microseconds",
}


# Which section each threshold belongs under. Serverless metrics are graded
# against a configured ceiling rather than node capacity, so their bands mean
# something different and are read separately.
SERVERLESS_METRICS = (
    "ThrottledCmdsPerMinute",
    "ECPUUtilizationPercent",
    "BytesUsedForCachePercent",
)

# What each metric's numbers are measured *against*, when that is not simply the
# value itself. Stated in the heading because "> 90%" is meaningless without it:
# 90% of a node's memory is a capacity fact, 90% of a configured serverless
# maximum is a billing-limit fact.
RELATIVE_TO = {
    "ECPUUtilizationPercent": "relative to the configured ECPUPerSecond.Maximum",
    "BytesUsedForCachePercent": "relative to the configured DataStorage.Maximum",
}

# What one unit of a trend's slope is, per trendable metric. A slope is a change
# per week in the metric's own unit, and leaving it bare ("> 2 per week") makes
# two percentage points indistinguishable from two seconds.
#
# These are the RAW series' units, not the graded ones: TrendModel reads
# TRENDABLE_METRICS directly, so its Evictions slope is in per-period totals —
# the derived per-minute series is not what it fits. Absent from this map means
# the slope is in whatever the metric counts, and the row says so.
SLOPE_UNITS = {
    "DatabaseMemoryUsagePercentage": "percentage points per week",
    "CacheHitRate": "percentage points per week",
    "ReplicationLag": "seconds per week",
    "BytesUsedForCache": "bytes per week",
    "CurrItems": "items per week",
    "CurrConnections": "connections per week",
    "Evictions": "evictions per collection period, per week",
}

# The series name a reader will find in analysis.json, when it differs from the
# CloudWatch metric name. Naming both is the whole point of D11: the threshold is
# registered under the derived name, and a reader who greps for the raw one finds
# an ungraded series.
DERIVED_FROM = {
    "EvictionsPerMinute": "Evictions",
    "NewConnectionsPerMinute": "NewConnections",
    "ThrottledCmdsPerMinute": "ThrottledCmds",
    "ECPUUtilizationPercent": "ElastiCacheProcessingUnits",
    "BytesUsedForCachePercent": "BytesUsedForCache",
}


def _fmt_slope(value: float, slope_unit: str) -> str:
    """A slope with its unit, singular when the number is exactly one.

    "above 1 seconds per week" is the kind of wrongness that makes a reader
    wonder what else was assembled without being read. The plural noun is the
    last word before the first "per", so that is the one to singularize.
    """
    if value != 1:
        return f"{value:g} {slope_unit}"

    head, sep, tail = slope_unit.partition(" per ")
    words = head.split()
    if words and words[-1].endswith("s"):
        words[-1] = words[-1][:-1]
    return f"{value:g} {' '.join(words)}{sep}{tail}"


def _fmt_number(value: float, unit: str) -> str:
    """Render a boundary without inventing precision it does not have."""
    text = f"{value:g}"
    if unit == "percent":
        return f"{text}%"
    if unit == "seconds":
        return f"{text}s"
    if unit == "per_minute":
        return f"{text}/min"
    if unit == "microseconds":
        # Microseconds are unreadable at these magnitudes; give both.
        return f"{text} ({value / 1000:g}ms)"
    return text


def _bands(name: str, level) -> list[tuple[str, str]]:
    """The (range, severity) rows for one metric, in classify_value's order.

    Read off `classify_value` rather than restated: the ranges below are the
    conditions that function tests, in the order it tests them, so a boundary
    that is `>` in the code cannot become `>=` in the doc. The HEALTHY row is
    last because it is the fallthrough — everything not matched above.
    """
    unit = level.unit
    inverted = name in ThresholdRegistry.INVERTED_METRICS
    rows: list[tuple[str, str]] = []

    ordered = (
        ("CRITICAL", level.critical),
        ("HIGH", level.high),
        ("MEDIUM", level.medium),
    )

    if inverted:
        # Lower is worse: each band is "below this, and not below the stricter
        # one above it".
        previous = None
        for severity, boundary in ordered:
            if boundary is None:
                continue
            if previous is None:
                rows.append((f"< {_fmt_number(boundary, unit)}", severity))
            else:
                rows.append((
                    f"{_fmt_number(previous, unit)} – "
                    f"{_fmt_number(boundary, unit)}",
                    severity,
                ))
            previous = boundary
        healthy = (f">= {_fmt_number(previous, unit)}" if previous is not None
                   else "any value")
        rows.append((healthy, "HEALTHY"))
        return rows

    # Higher is worse.
    previous = None
    for severity, boundary in ordered:
        if boundary is None:
            continue
        if previous is None:
            rows.append((f"> {_fmt_number(boundary, unit)}", severity))
        else:
            rows.append((
                f"{_fmt_number(boundary, unit)} – "
                f"{_fmt_number(previous, unit)}",
                severity,
            ))
        previous = boundary

    lowest_bad = previous
    if lowest_bad == 0 and level.low is None:
        # A boundary of zero has nothing below it: "<= 0/min" is literally what
        # classify_value tests, but a rate cannot be negative, so the honest
        # rendering of the HEALTHY band is the single value zero.
        rows.append((f"{_fmt_number(0, unit)}", "HEALTHY"))
        return rows

    if level.low is not None:
        if lowest_bad is not None:
            rows.append((
                f"{_fmt_number(level.low, unit)} – "
                f"{_fmt_number(lowest_bad, unit)}",
                "HEALTHY",
            ))
        else:
            rows.append((f">= {_fmt_number(level.low, unit)}", "HEALTHY"))
        rows.append((f"< {_fmt_number(level.low, unit)}", "LOW"))
    else:
        healthy = (f"<= {_fmt_number(lowest_bad, unit)}"
                   if lowest_bad is not None else "any value")
        rows.append((healthy, "HEALTHY"))

    return rows


def _heading(name: str, level) -> str:
    """The `###` line for one metric: what is graded, how, and against what."""
    parts = [level.statistic]
    if level.unit != "percent":
        parts.append(UNIT_LABELS[level.unit])
    relative = RELATIVE_TO.get(name)
    if relative:
        parts.append(relative)
    source = DERIVED_FROM.get(name)
    suffix = f" — derived from `{source}`" if source else ""
    return f"### {name} ({', '.join(parts)}){suffix}"


def _metric_section(name: str, level) -> list[str]:
    """One metric's heading plus its band table."""
    lines = [_heading(name, level), ""]
    lines.append("| Range | Severity | Interpretation |")
    lines.append("|-------|----------|----------------|")
    for range_text, severity in _bands(name, level):
        interpretation = level.means.get(severity, "")
        lines.append(f"| {range_text} | {severity} | {interpretation} |")
    lines.append("")
    return lines


def _metric_tables(registry: ThresholdRegistry) -> list[str]:
    """The node-based and serverless threshold sections."""
    lines: list[str] = []
    all_levels = registry._thresholds

    node_names = [n for n in all_levels if n not in SERVERLESS_METRICS]
    serverless_names = [n for n in all_levels if n in SERVERLESS_METRICS]

    lines.append("## Node-Based Thresholds")
    lines.append("")
    for name in node_names:
        lines.extend(_metric_section(name, all_levels[name]))

    lines.append("## Serverless Thresholds")
    lines.append("")
    lines.append(
        "These grade a cache against limits *you configured*, not against "
        "hardware capacity. A cache with no configured maximum has no "
        "percentage to measure, so Stage 3 derives no series and these bands "
        "do not apply — the absence is reported as Unknown rather than as "
        "headroom."
    )
    lines.append("")
    for name in serverless_names:
        lines.extend(_metric_section(name, all_levels[name]))

    return lines


def _trend_table() -> list[str]:
    """The trend section, from TrendModel and the severity classifier.

    The committed table listed EngineCPUUtilization, which TRENDABLE_METRICS
    does not contain — so the agent was told to expect a CPU trend entry that
    Stage 3 never emits — and omitted BytesUsedForCache and CurrItems, which it
    does. It also stated one severity per metric where the classifier escalates
    by slope. Both are generated here.
    """
    lines = [
        "## Trend-Based Thresholds",
        "",
        "Beyond point-in-time values, Stage 3 fits a linear regression to daily "
        "averages over the 14-day window. A trend is only reported when "
        "R² > 0.4 *and* the slope clears the minimum below — a confident fit "
        "on an operationally irrelevant slope is not a finding.",
        "",
        "Rising is the concerning direction for every metric here except "
        "`CacheHitRate`, where it is declining.",
        "",
        "The metrics named here are the **raw** CloudWatch series, not the "
        "derived ones the point-in-time bands above grade. `Evictions` is the "
        "case to watch: its threshold band is per minute, but its trend is "
        "fitted to the per-period Sum, so the two rows are about series with "
        "different units and the same name.",
        "",
        "| Metric | Reported when | Severity |",
        "|--------|---------------|----------|",
    ]

    missing = [m for m in TrendModel.TRENDABLE_METRICS if m not in SLOPE_UNITS]
    if missing:
        raise ValueError(
            f"SLOPE_UNITS has no entry for {missing}. Without one the row reads "
            "'> N per week' with no unit, which is the ambiguity this file "
            "exists to remove. Add the unit the raw series is counted in."
        )

    classify = FindingsGenerator._classify_trend_severity

    for metric in TrendModel.TRENDABLE_METRICS:
        slope_min = TrendModel.MEANINGFUL_SLOPE_THRESHOLDS.get(metric, 0.0)
        direction = "declining" if metric == "CacheHitRate" else "rising"
        slope_unit = SLOPE_UNITS.get(metric, "per week")

        if slope_min > 0:
            condition = f"{direction} > {_fmt_slope(slope_min, slope_unit)}"
        else:
            condition = f"any {direction} trend"

        # Ask the classifier rather than restating it: it escalates on slope for
        # three metrics, and a hand-written column is where that gets lost. The
        # probe slopes bracket every internal boundary (5, 10, 1).
        verdicts = {
            classify(metric, slope, direction)
            for slope in (0.01, 0.5, 2.0, 6.0, 11.0)
        }
        if verdicts == {"MEDIUM"}:
            severity = "MEDIUM"
        else:
            # Find where it escalates, so the doc names the number.
            escalation = None
            for boundary in (1.0, 5.0, 10.0):
                if (classify(metric, boundary - 0.01, direction) == "MEDIUM"
                        and classify(metric, boundary + 0.01,
                                     direction) == "HIGH"):
                    escalation = boundary
                    break
            if escalation is None:
                severity = " / ".join(sorted(verdicts))
            else:
                severity = (
                    f"MEDIUM, escalating to HIGH above "
                    f"{_fmt_slope(escalation, slope_unit)}"
                )

        lines.append(f"| {metric} | {condition} | {severity} |")

    lines.append("")
    return lines


def _config_table() -> list[str]:
    """The Stage 3.5 configuration checks, from the CHECKS registry.

    Generated because the committed table disagreed with the registry on four of
    fourteen severities, listed two Graviton rows that are not checks at all,
    and omitted two that are. Every column here is a field on `Check`.
    """
    lines = [
        "## Configuration Assessment (Non-Metric)",
        "",
        "Stage 3.5 (`check_configuration.py`) evaluates these from "
        "inventory.json alone — no CloudWatch, no API calls. A check that does "
        "not apply to a cluster type is reported in `checks_skipped` rather "
        "than passed, so a clean report cannot be confused with an unevaluated "
        "one.",
        "",
        "| Check | Title | Severity | Pillar | Applies to |",
        "|-------|-------|----------|--------|------------|",
    ]
    for check in CHECKS:
        applies = ", ".join(check.applies_to)
        pillar = check.pillar.replace("_", " ")
        severity = check.severity
        if check.needs_review_date:
            # SEC-06's severity is a function of the review date, so a single
            # value would be wrong two thirds of the time.
            severity = f"{severity}–HIGH (by proximity to the date)"
        lines.append(
            f"| {check.check_id} | {check.title} | {severity} | {pillar} "
            f"| {applies} |"
        )
    lines.append("")
    return lines


def render() -> str:
    """The full generated block, marker to marker."""
    registry = ThresholdRegistry()

    lines = [
        BEGIN_MARKER,
        "<!-- Generated by scripts/generate_thresholds_doc.py from the code -->",
        "<!-- Do not edit between the markers; edit the registry and re-run. -->",
        "",
    ]
    lines.extend(_metric_tables(registry))
    lines.extend(_trend_table())
    lines.extend(_config_table())
    lines.append(END_MARKER)
    return "\n".join(lines)


def splice(existing: str, generated: str) -> str:
    """Replace the marked region of `existing` with `generated`."""
    start = existing.find(BEGIN_MARKER)
    end = existing.find(END_MARKER)
    if start == -1 or end == -1:
        raise ValueError(
            f"{DOC_PATH} has no {BEGIN_MARKER} / {END_MARKER} pair. The "
            "generated tables have nowhere to go; add the markers where they "
            "should appear."
        )
    if end < start:
        raise ValueError(
            f"{DOC_PATH} has {END_MARKER} before {BEGIN_MARKER}."
        )
    return existing[:start] + generated + existing[end + len(END_MARKER):]


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate the threshold tables in references/thresholds.md from "
            "the code. That document is agent-facing input, not "
            "documentation: a row it carries that the registry does not is a "
            "wrong instruction."
        ),
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help=(
            "Do not write. Exit 1 if the committed doc differs from what the "
            "code would generate. This is what CI runs."
        ),
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    with open(DOC_PATH, encoding="utf-8") as handle:
        existing = handle.read()

    updated = splice(existing, render())

    if args.check:
        if updated != existing:
            print(
                f"{os.path.relpath(DOC_PATH, REPO_ROOT)} is out of date. Run "
                "python3 scripts/generate_thresholds_doc.py",
                file=sys.stderr,
            )
            return 1
        return 0

    if updated == existing:
        print(f"{os.path.relpath(DOC_PATH, REPO_ROOT)} is already current.")
        return 0

    with open(DOC_PATH, "w", encoding="utf-8") as handle:
        handle.write(updated)
    print(f"Wrote {os.path.relpath(DOC_PATH, REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
