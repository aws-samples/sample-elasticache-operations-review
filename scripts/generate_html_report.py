#!/usr/bin/env python3
"""Render a self-contained HTML report from the review's JSON outputs.

Reads inventory.json, metrics.json and analysis.json — plus config_findings.json
when Stage 3.5 ran — and emits a single HTML file with the metric time-series
embedded as JSON. No network access, no CDN, no build step — the output opens
straight from disk.

The three metric stages are required; the configuration findings are optional and
picked up automatically from output/config_findings.json when present. A report
built without them says so, rather than showing an empty configuration section
that would read as a clean result.

`--notes` adds the reviewing agent's own judgement: a fleet-level assessment, a
verdict and reasoning per finding, and a ranked priority list. It is the only
input here a language model writes, so every figure in its prose is checked
against a value under a path the note cites, and the render fails naming any
figure that is not there. Agent sections are boxed and attributed so a reader can
tell judgement from measurement.

Charts are hand-rolled inline SVG drawn by a small vanilla-JS layer so the file
stays dependency-free. Every chart ships a table-view twin, so no value is
reachable only by hovering.

Usage:
    python3 scripts/generate_html_report.py \
        --inventory output/inventory.json \
        --metrics output/metrics.json \
        --analysis output/analysis.json \
        --config-findings output/config_findings.json \
        --notes output/notes.json \
        --output output/report.html
"""

from __future__ import annotations

import argparse
import datetime
import html
import json
import logging
import math
import os
import re
import sys

# The shared metrics seam (Phase 9): lets the reducer and the render fallback
# stream one cluster shard at a time instead of loading the whole metrics file.
from _metrics_store import (  # noqa: E402
    iter_cluster_ids,
    load_cluster_metrics,
)

# Stage 3.5's check registry, imported so the report's per-check glossary is
# derived from the same source the checker runs, never a second hand-kept copy.
from check_configuration import CHECKS  # noqa: E402

logger = logging.getLogger(__name__)

# Stage 3.5's output. Picked up automatically when present so the ordinary
# pipeline run includes configuration findings without an extra flag.
DEFAULT_CONFIG_FINDINGS = "output/config_findings.json"

# Phase 7c pricing. Written by the agent from price_calculator.py (live rates
# never enter the reproducible pipeline), picked up automatically when present.
DEFAULT_PRICING = "output/pricing.json"

# Phase 9a: the report-facing reduction of metrics.json (per-cluster chart
# bundles + cost + metadata). Emitted by run_review.py after Stage 3 and picked
# up automatically here, so the render never loads the multi-GB raw file.
DEFAULT_REPORT_DATA = "output/report_data.json"

# The agent's AI review (assessment, per-finding verdicts, priorities). The skill
# is run by an LLM agent, and SKILL.md Step 4 REQUIRES that agent to write this
# before rendering, so the review is present on every agent-run report. Picked up
# automatically here -- no flag needed. Staleness (a notes.json left from an
# earlier run over changed data) is caught by verify_notes: a note whose cited
# figures no longer match the data fails the render rather than attaching quietly.
DEFAULT_NOTES = "output/notes.json"

# ---------------------------------------------------------------------------
# Palette — the dataviz reference instance's documented slot order, all eight
# slots, in that order. The ORDER is the colourblind-safety mechanism, not a
# preference, so slots are added by extending the published sequence and never
# by picking favourites.
#
# The per-cluster detail panels each hold a single series and read slot 0, so no
# two hues are ever on screen together and telling marks apart never arises. The
# full slot order is kept as the reference publishes it, in the published order,
# for any future chart that does render multiple series.
#
# Light-mode aqua sits below 3:1 contrast on the light surface, so the relief rule
# applies: every chart carries visible labels and a table view. It does.
# ---------------------------------------------------------------------------
SERIES_LIGHT = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100",
                "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
SERIES_DARK = ["#3987e5", "#d95926", "#199e70", "#c98500",
               "#d55181", "#008300", "#9085e9", "#e66767"]

# Hourly buckets from 5-minute datapoints (12 samples per hour).
SAMPLES_PER_HOUR = 12

# ---------------------------------------------------------------------------
# Cost Explorer region billing codes -> region id.
#
# Cost Explorer prefixes usage types with a short region code (USE1-, APS3-).
# Recognising the code is how out-of-scope spend gets identified, so the map has
# to be complete: a code that is absent here is silently read as home-region
# spend, which understates what the review failed to account for.
#
# Transcribed verbatim from the AWS Region billing codes reference:
# https://docs.aws.amazon.com/global-infrastructure/latest/regions/aws-region-billing-codes.html
# Two things are NOT guessable from the pattern and were the bugs in the
# previous six-entry map:
#   - eu-west-1 is EU, not EUW1 (it predates the numbering convention).
#   - the APS* codes do not track the ap-south*/ap-southeast* split at all
#     (APS3 is ap-south-1, APS5 is ap-south-2, the rest are ap-southeast-*).
# So this is carried as data, never derived from the region string.
#
# GovCloud (us-gov-*) and the European Sovereign Cloud (eusc-*) are absent from
# that reference. They are deliberately not invented here -- see
# _region_for_billing_code.
# ---------------------------------------------------------------------------
REGION_BILLING_CODES = {
    # North America
    "USE1": "us-east-1", "USE2": "us-east-2",
    "USW1": "us-west-1", "USW2": "us-west-2",
    "CAN1": "ca-central-1", "CAN2": "ca-west-1",
    "MXC1": "mx-central-1",
    # Africa
    "AFS1": "af-south-1",
    # Asia Pacific
    "APE1": "ap-east-1", "APE2": "ap-east-2",
    "APN1": "ap-northeast-1", "APN2": "ap-northeast-2", "APN3": "ap-northeast-3",
    "APS1": "ap-southeast-1", "APS2": "ap-southeast-2", "APS3": "ap-south-1",
    "APS4": "ap-southeast-3", "APS5": "ap-south-2", "APS6": "ap-southeast-4",
    "APS7": "ap-southeast-5", "APS8": "ap-southeast-6", "APS9": "ap-southeast-7",
    # Europe
    "EU": "eu-west-1", "EUC1": "eu-central-1", "EUC2": "eu-central-2",
    "EUW2": "eu-west-2", "EUW3": "eu-west-3", "EUN1": "eu-north-1",
    "EUS1": "eu-south-1", "EUS2": "eu-south-2",
    # Middle East
    "ILC1": "il-central-1", "MEC1": "me-central-1", "MES1": "me-south-1",
    # South America
    "SAE1": "sa-east-1",
}

# Reverse lookup. Built once; the forward map is the source of truth.
BILLING_CODE_FOR_REGION = {r: c for c, r in REGION_BILLING_CODES.items()}


def _region_for_billing_code(usage_type: str) -> str | None:
    """Resolve the region a Cost Explorer usage type is billed against.

    Args:
        usage_type: A Cost Explorer usage type, e.g. "USE2-NodeUsage:cache.t4g.micro".

    Returns:
        The region id when the usage type carries a recognised billing-code
        prefix, otherwise None.

    None means "cannot be attributed from this string", which covers two very
    different cases and is deliberately not split:

      - No prefix at all. Older ElastiCache usage types are unprefixed
        ("NodeUsage:cache.m5.large" appears alongside "USE1-CachedData:Redis"
        in the same account), so absence of a prefix is not evidence of any
        particular region.
      - A prefix this map does not know -- a new region, GovCloud, or a
        sovereign partition.

    Both must resolve to "unknown", never to the home region: naming a region
    the string does not establish would put a fabricated attribution in front of
    a reviewer who can check it.
    """
    head = usage_type.split("-", 1)[0]
    return REGION_BILLING_CODES.get(head)


# ---------------------------------------------------------------------------
# Agent notes: the no-new-numbers check.
#
# Notes are the one input to this report that a language model writes, so they
# are the one input that can contain a figure nothing measured. Everything else
# here is a number a script computed; a note is prose, and prose is where a
# plausible-looking "CPU peaked around 80%" gets written next to real data and
# inherits its authority.
#
# The check is mechanical rather than an instruction to be careful, because
# "be careful with numbers" is exactly the sort of guardrail that holds until
# the one time it matters. Every figure in note prose must be traceable to a
# value under a path the note itself cites, or the render fails and names the
# figure.
#
# It is scoped to the cited paths, not to the documents as a whole, and that
# scoping is the whole check. metrics.json for the seven-cluster example fleet
# holds ~1.9 million distinct numbers, which saturates the two-decimal grid
# between 0 and 100 completely: all 10,001 of those values occur somewhere in the
# document. "This figure appears somewhere in the source JSON" would therefore
# pass *every* fabricated percentage a note could quote to two decimals -- not a
# weak check, no check at all. analysis.json alone still admits 4.3% of the grid.
# ---------------------------------------------------------------------------

# A note whose cites between them authorise more than this many distinct numbers
# is rejected as too broad to mean anything. The budget is shared across a
# note's cites, so twenty narrow cites cannot be stacked to the same effect.
#
# Sized from the example fleet rather than guessed. Cited whole, the derived
# documents admit these fractions of the 10,001 two-decimal values between 0
# and 100 -- the grid a fabricated percentage would be drawn from:
#
#   one percentiles entry (9 numbers)            0.09%
#   a cluster's utilization block (6)            0.06%
#   a cluster's findings (35)                    0.34%
#   a cluster's whole analysis entry (229)       1.31%
#   every cluster, all documents (846)           3.95%
#
# 250 admits any single cluster's analysis and rejects a cite of the whole
# fleet, which is the line worth drawing: a claim about one cluster is checkable
# against that cluster.
MAX_CITE_NUMBERS = 250

# Coverage invariant (the "no silent drop" rule). A finding that fires on this
# fraction of the fleet or more is a *systemic* finding: the same problem on a
# majority of clusters, not a one-off. Those are exactly the findings an agent's
# top-severity triage drops silently -- an individually LOW note (e.g. "engine is
# Redis OSS rather than Valkey", COST-01) repeated across every cluster is a real
# fleet-wide recommendation, but reads as noise one row at a time. So a
# fleet-wide finding must be explicitly accounted for in the notes (a
# finding_note verdict + reasoning) or the render fails, the same way a
# fabricated figure fails. This is the completeness half of the notes contract;
# no-new-numbers is the other half. It does NOT require a note for every
# finding -- a lone high-severity finding is already what the assessment leads
# with; the enforced floor is the systemic pattern, which is the auditable case
# and the one that was actually being lost.
FLEET_PATTERN_FRACTION = 0.5

# Documents a cite may resolve against. Raw metrics.json is deliberately absent.
#
# It is not a matter of degree. One raw metric series on this fleet is 8,064
# datapoints admitting 46.1% of that same grid, so citing a single series would
# pass a fabricated percentage about half the time -- the check would run, report
# success, and mean nothing. The whole of metrics.json holds ~1.8 million
# distinct numbers.
#
# Nothing is lost by excluding it. A raw 5-minute datapoint is not a fact worth
# quoting in an assessment anyway; the summary of it is, and that lives in
# analysis.json, which is also what the report shows. An agent wanting to say
# "CPU peaked at 94.23" cites the percentiles entry that establishes it.
CITABLE_SOURCES = ("inventory", "analysis", "config", "pricing")

# Units a figure may wear and still be a figure. Stripped before the numeric
# test so "4.5GB" reads as 4.5, while "cache.m5.large" and "p95" stay names.
FIGURE_UNITS = ("%", "kb", "mb", "gb", "tb", "kib", "mib", "gib", "tib",
                "ms", "s", "m", "h", "d", "x", "k", "b")

# Trailing/leading characters that punctuate a figure without being part of it,
# including the hedges an assessment tends to wear ("~80%", ">90%").
_FIGURE_TRIM = "\"'“”()[]{},.;:!?—-"
_FIGURE_LEAD = "$£€~<>≈≤≥+"

_PURE_NUMBER = re.compile(r"^\d{1,3}(?:,\d{3})+(?:\.\d+)?$|^\d+(?:\.\d+)?$")


class NotesError(ValueError):
    """A notes file that cannot be rendered without asserting something unmeasured.

    Raised rather than logged-and-skipped. A note that fails this check is not
    a cosmetic problem: it is prose about to be published beside real
    measurements, and dropping it silently would leave the report looking
    complete while the reason it is incomplete appears only in a log nobody
    reads. Same principle as absent cost becoming None rather than 0.0.
    """


def prose_figures(text: str) -> list[str]:
    """Extract the numeric claims from a sentence, ignoring names that contain digits.

    A *figure* is a standalone numeric token: "80.55", "1,024", "4.5GB", "~12%".
    A *name* that happens to contain digits is not a figure -- "p95", "SEC-06",
    "cache.m5.large", "redis 7.1.0", "us-east-1", "EngineCPUUtilization" -- and
    a check that flagged those would be a check the note author routes around by
    never naming a metric, which costs more than it saves.

    The rule that separates them: strip punctuation, a leading currency or
    hedge symbol, and one trailing unit; whatever is left must be *entirely*
    numeric. A letter anywhere else means the token names something.

    Args:
        text: Prose from a notes file.

    Returns:
        The figures as they were written, in order, duplicates included -- the
        caller reports them back to a human, so "80.6" must not come back as
        "80.60" or the error names a figure that is not in the file.
    """
    figures = []
    for raw in str(text).split():
        token = raw.strip(_FIGURE_TRIM).lstrip(_FIGURE_LEAD).strip(_FIGURE_TRIM)
        if not token:
            continue
        # One unit, longest first: "mib" must not be stripped as "b".
        lowered = token.lower()
        for unit in sorted(FIGURE_UNITS, key=len, reverse=True):
            if lowered.endswith(unit) and len(token) > len(unit):
                token = token[:-len(unit)]
                break
        token = token.strip(_FIGURE_TRIM)
        if _PURE_NUMBER.match(token):
            figures.append(token)
    return figures


def _figure_matches(figure: str, value: float) -> bool:
    """Does a written figure round to this value, at the precision it was written?

    The tolerance comes from the figure's own decimal places, so "80.6" accepts
    80.55 and "81" accepts 80.55, but "80.55" accepts only 80.545-80.555.
    Rounding for readability is what prose is for; a note forced to quote
    80.54999999999999 would be less honest, not more.

    Derived from the written form rather than by rounding the source value,
    because round() breaks ties to even: round(80.55, 1) is 80.5 in binary
    floating point, so a note that correctly wrote "80.6" would have failed.

    Small integers are admitted freely by this rule -- "1" matches anything in
    [0.5, 1.5] -- and that is accepted rather than tightened. Prose is full of
    small counts ("2 of 7 clusters"), they are the figures least able to
    mislead, and a check that rejected them would be argued with rather than
    obeyed.
    """
    try:
        written = float(figure.replace(",", ""))
    except ValueError:  # pragma: no cover - guarded by _PURE_NUMBER
        return False
    decimals = len(figure.split(".")[1]) if "." in figure else 0
    return abs(value - written) <= 0.5 * 10 ** -decimals


def _harvest_numbers(node, out: set, budget: list) -> None:
    """Collect every number reachable from a node, including keys and sizes.

    Keys are harvested as well as values because a note citing a percentiles
    entry to say "p95" is citing a number that lives in the key. Container
    sizes are harvested because "all 7 clusters" is a figure derived from the
    structure and nowhere written in it.

    Booleans are excluded: True is not the measurement 1, and admitting it
    would let a note quote "1" against a cite of a TLS flag.

    Args:
        node: Any JSON value.
        out: Accumulator of allowed numbers.
        budget: Single-element list used as a mutable countdown, so a cite of a
            whole document stops early instead of walking 1.8 million values.
    """
    if budget[0] <= 0:
        return
    if isinstance(node, dict):
        out.add(float(len(node)))
        for key, value in node.items():
            _harvest_numbers(key, out, budget)
            _harvest_numbers(value, out, budget)
    elif isinstance(node, list):
        out.add(float(len(node)))
        for value in node:
            _harvest_numbers(value, out, budget)
    elif isinstance(node, bool):
        return
    elif isinstance(node, (int, float)):
        budget[0] -= 1
        out.add(float(node))
    elif isinstance(node, str):
        for match in _PURE_NUMBER.finditer(node):
            budget[0] -= 1
            out.add(float(match.group().replace(",", "")))
        # Numbers embedded in a string that is not purely numeric -- a version,
        # a date, a node type. Harvested too: a note quoting an engine version
        # is quoting the source.
        for match in re.finditer(r"\d+(?:\.\d+)?", node):
            budget[0] -= 1
            out.add(float(match.group()))


_MISSING = object()


def _walk(doc, path: str) -> object:
    """Follow a dotted path into one document, or _MISSING."""
    node = doc
    for part in path.split("."):
        if isinstance(node, dict) and part in node:
            node = node[part]
        elif isinstance(node, list) and part.isdigit() and int(part) < len(node):
            node = node[int(part)]
        else:
            return _MISSING
    return node


def _resolve_cite(path: str, sources: dict) -> list:
    """Resolve a dotted cite path against every citable document that has it.

    A cite names a path, not a file ("clusters.prod-api-cache.percentiles"),
    which is how the data reads and how a reviewer would say it.

    Every match is returned, not the first. The same path frequently exists in
    more than one document -- `clusters.prod-api-cache` is in analysis.json and
    config_findings.json, and `clusters` is in three of them -- so returning the
    first match made the answer depend on dict insertion order. A cite would
    have silently resolved against whichever document happened to be listed
    first, and a figure genuinely present in the other one would have been
    reported as fabricated.

    Args:
        path: Dotted path, list indices numeric.
        sources: Documents keyed by label. Only CITABLE_SOURCES are consulted;
            raw metrics are excluded, see that constant.

    Returns:
        A list of (label, node) for each document containing the path. Empty
        when nothing does -- which is an error to the caller, because a citation
        to nothing reads as evidence to anyone who does not follow it.
    """
    found = []
    for label in CITABLE_SOURCES:
        node = _walk(sources.get(label) or {}, path)
        if node is not _MISSING:
            found.append((label, node))
    return found


def verify_notes(notes: dict, sources: dict, known_finding_ids=None) -> None:
    """Check every figure in note prose against the paths that note cites.

    Args:
        notes: Parsed notes.json.
        sources: Documents keyed by label. Only the CITABLE_SOURCES labels
            (inventory/analysis/config) are consulted; passing raw metrics is
            harmless but they are not citable, for the reason given there.
        known_finding_ids: Every finding_id in the report. A note keyed on an id
            that is not there annotates nothing, and would vanish from the
            report with no error -- the failure mode that made Stage 3's
            per-cluster ids a bug rather than a wart. None skips the check.

    Raises:
        NotesError: Naming every problem found, not just the first. An author
            fixing one fabricated figure per render would rather see all four.
    """
    problems = []
    blocks = _notes_blocks(notes)
    if not blocks:
        return

    for label, prose, cites in blocks:
        figures = prose_figures(prose)

        allowed: set = set()
        # One budget for the whole note, not one per cite. Per-cite budgets
        # would let twenty narrow cites authorise as much as one broad one,
        # which is the same hole by a longer route.
        budget = [MAX_CITE_NUMBERS]
        too_broad = False
        for cite in cites:
            matches = _resolve_cite(cite, sources)
            if not matches:
                problems.append(
                    f"{label}: cites {cite!r}, which no citable source has. "
                    f"A citation to nothing reads as evidence to anyone who "
                    f"does not follow it. Citable: "
                    f"{', '.join(CITABLE_SOURCES)} (raw metrics are not "
                    f"citable -- cite the analysis entry that summarises them).")
                continue
            before = budget[0]
            for _, node in matches:
                _harvest_numbers(node, allowed, budget)
            if budget[0] <= 0:
                too_broad = True
                problems.append(
                    f"{label}: the cites in this note reach more than "
                    f"{MAX_CITE_NUMBERS} distinct numbers by {cite!r}, which "
                    f"authorises almost any figure. Cite the specific value "
                    f"that establishes the claim.")
                break
            if before == budget[0] and figures:
                problems.append(
                    f"{label}: cite {cite!r} resolves to no numbers at all, so "
                    f"it cannot support the figures in this note.")

        # Skipped when the cites were rejected as too broad: `allowed` is a
        # truncated harvest at that point, so every figure would be reported as
        # fabricated too and bury the one problem that needs fixing.
        if too_broad:
            continue
        # Truncated: a note with fifty cites would otherwise produce an error
        # message longer than the notes file, which is a message nobody reads.
        if not cites:
            where = "nothing (the note cites no path)"
        elif len(cites) > 4:
            where = (", ".join(repr(c) for c in cites[:4])
                     + f" and {len(cites) - 4} other cites")
        else:
            where = ", ".join(repr(c) for c in cites)
        for figure in figures:
            if not any(_figure_matches(figure, value) for value in allowed):
                problems.append(
                    f"{label}: the figure {figure!r} does not appear under "
                    f"{where}. Every number in note prose must come from the "
                    f"data the note cites.")

    if known_finding_ids is not None:
        known = set(known_finding_ids)
        for note in notes.get("finding_notes") or []:
            fid = note.get("finding_id")
            if fid not in known:
                problems.append(
                    f"finding_notes: {fid!r} is not a finding in this report, "
                    f"so this note would annotate nothing and disappear "
                    f"silently.")

    if problems:
        raise NotesError(
            "notes.json asserts figures the data does not support:\n  - "
            + "\n  - ".join(problems))


def _notes_blocks(notes: dict) -> list:
    """Flatten a notes file into (label, prose, cites) triples for checking.

    Every prose-bearing block carries its own cites. The alternative -- one
    document-level cite list -- would let a paragraph about one cluster draw on
    another cluster's numbers, which is the same scoping hole as checking
    against the whole document.
    """
    blocks = []
    assessment = notes.get("assessment")
    if isinstance(assessment, dict) and assessment.get("prose"):
        blocks.append(("assessment", assessment["prose"],
                       assessment.get("cites") or []))
    context = notes.get("context")
    if isinstance(context, dict) and context.get("environment"):
        # Short and rarely numeric, but it is prose that appears in the report
        # under the agent's name, so it is checked like the rest.
        blocks.append(("context", context["environment"],
                       context.get("cites") or []))
    for note in notes.get("finding_notes") or []:
        if note.get("reasoning"):
            blocks.append((f"finding_notes[{note.get('finding_id')}]",
                           note["reasoning"], note.get("cites") or []))
    for item in notes.get("priorities") or []:
        cites = item.get("cites") or []
        for field in ("action", "why"):
            if item.get(field):
                blocks.append((f"priorities[{item.get('rank')}].{field}",
                               item[field], cites))
    return blocks


VERDICTS = ("confirmed", "false_positive", "needs_data")


def _pattern_key(finding: dict) -> str:
    """The identity a finding shares with the same problem on another cluster.

    A configuration check has a stable ``check_id`` (COST-01 is COST-01 on every
    cluster). Metric findings have no check id, so they group by title -- which
    means two metric findings only count as the same pattern when their titles
    match verbatim. That is deliberately strict: a title that embeds the node
    type or a per-cluster value will not collapse, so only genuinely identical
    findings form a fleet-wide pattern.
    """
    return finding.get("check_id") or finding.get("title") or ""


def fleet_wide_patterns(findings: list, n_clusters: int) -> dict:
    """Findings that recur across a majority of the fleet, grouped by pattern.

    A single-cluster fleet has no fleet-wide pattern by definition (a threshold
    of one would make every finding "systemic"), so this returns empty when
    ``n_clusters < 2``.

    Returns:
        ``{pattern_key: {check_id, title, severity, clusters, finding_ids}}``
        for each pattern firing on at least ``ceil(FLEET_PATTERN_FRACTION *
        n_clusters)`` distinct clusters (floored at 2). ``severity`` is the
        worst the pattern reaches; ``clusters``/``finding_ids`` are sorted for
        deterministic output.
    """
    if n_clusters < 2:
        return {}
    groups: dict = {}
    for f in findings:
        key = _pattern_key(f)
        if not key:
            continue
        g = groups.setdefault(key, {
            "check_id": f.get("check_id"),
            "title": f.get("title"),
            "rank": 9,
            "severity": None,
            "clusters": set(),
            "finding_ids": set(),
        })
        g["clusters"].add(f.get("cluster"))
        if f.get("finding_id"):
            g["finding_ids"].add(f["finding_id"])
        r = _SEVERITY_RANK.get(f.get("severity"), 9)
        if r < g["rank"]:
            g["rank"] = r
            g["severity"] = f.get("severity")
    threshold = max(2, math.ceil(FLEET_PATTERN_FRACTION * n_clusters))
    material = {}
    for key, g in groups.items():
        if len(g["clusters"]) >= threshold:
            g["clusters"] = sorted(c for c in g["clusters"] if c)
            g["finding_ids"] = sorted(g["finding_ids"])
            material[key] = g
    return material


def _pattern_addressed(pattern: dict, noted_ids: set) -> bool:
    """A pattern is accounted for if any of its findings carries an agent note.

    One note covers the whole pattern -- the agent writes a single verdict for
    "engine is Redis OSS across the fleet", not one per cluster. The note is the
    accounting channel; featuring the pattern in the priorities as well is
    encouraged but not what this checks, because prose mention cannot be
    verified mechanically the way a finding_id match can.
    """
    return any(fid in noted_ids for fid in pattern["finding_ids"])


def notes_with_reasoning(notes: dict) -> set:
    """finding_ids the agent actually wrote a reasoned note for.

    A finding_note with an empty ``reasoning`` is not accounting for anything,
    so it does not count toward coverage.
    """
    return {
        note.get("finding_id")
        for note in (notes.get("finding_notes") or [])
        if note.get("finding_id") and (note.get("reasoning") or "").strip()
    }


def verify_coverage(notes: dict, findings: list, n_clusters: int) -> list:
    """Fail the render when a fleet-wide finding is left unaccounted for.

    The completeness half of the notes contract. ``verify_notes`` stops the
    agent inventing numbers; this stops the agent silently dropping a systemic
    finding from its review. A fleet-wide pattern (see ``fleet_wide_patterns``)
    must have at least one finding_note with reasoning, or this raises.

    Returns:
        The coverage ledger: one row per fleet-wide pattern, in worst-severity
        then key order, each marked ``addressed`` true/false with the agent's
        verdict when present. Returned even when everything passes, so the
        report can show a reader that every systemic finding got a call.

    Raises:
        NotesError: Naming every unaddressed fleet-wide pattern.
    """
    patterns = fleet_wide_patterns(findings, n_clusters)
    noted_ids = notes_with_reasoning(notes)
    verdict_by_id = {
        note.get("finding_id"): note.get("verdict")
        for note in (notes.get("finding_notes") or [])
    }
    ledger = []
    unaddressed = []
    for key, p in sorted(patterns.items(),
                         key=lambda kv: (kv[1]["rank"], kv[0])):
        addressed = _pattern_addressed(p, noted_ids)
        verdict = next((verdict_by_id.get(fid) for fid in p["finding_ids"]
                        if fid in noted_ids), None)
        ledger.append({
            "label": p["check_id"] or p["title"],
            "title": p["title"],
            "severity": p["severity"],
            "cluster_count": len(p["clusters"]),
            "addressed": addressed,
            "verdict": verdict,
        })
        if not addressed:
            unaddressed.append(
                f"{p['check_id'] or p['title']!r} "
                f"({p['severity']}) fires on {len(p['clusters'])} of "
                f"{n_clusters} clusters but no finding_note accounts for it. A "
                f"finding that recurs across the fleet is a systemic "
                f"recommendation; add a finding_note (verdict + reasoning) on "
                f"one of its findings, or address it in the priorities and note "
                f"it, so it is not silently dropped from the review.")
    if unaddressed:
        raise NotesError(
            "notes.json leaves fleet-wide findings unaccounted for:\n  - "
            + "\n  - ".join(unaddressed))
    return ledger


def _build_notes(notes: dict | None, findings: list,
                 coverage: list | None = None) -> dict | None:
    """Attach agent notes to the findings and shape them for the renderer.

    Mutates each matching finding row with `verdict` and `note`, so the
    annotation travels with the row it annotates rather than being joined in JS
    -- a join the renderer could silently fail.

    A `false_positive` verdict does not remove the row. The finding is still
    what the pipeline produced, and a report that quietly deleted it would hide
    a real Stage 3 bug behind an agent's judgement: the row stays, struck
    through, with the reasoning beside it. That is also the only form in which
    a reader can disagree with the suppression.

    Returns:
        The renderer's notes section, or None when no notes were supplied.
    """
    if not notes:
        return None

    by_id = {f["finding_id"]: f for f in findings if f.get("finding_id")}
    annotated = 0
    for note in notes.get("finding_notes") or []:
        row = by_id.get(note.get("finding_id"))
        if row is None:
            # verify_notes rejects this, so reaching it means notes were not
            # verified. Skipped rather than raised here to keep this function
            # a pure shaping step.
            logger.warning("Note for unknown finding %s ignored",
                           note.get("finding_id"))
            continue
        verdict = note.get("verdict")
        if verdict not in VERDICTS:
            logger.warning("Note for %s has unrecognised verdict %r; the "
                           "reasoning is kept but no verdict is shown",
                           note.get("finding_id"), verdict)
            verdict = None
        row["verdict"] = verdict
        row["note"] = note.get("reasoning") or None
        annotated += 1

    assessment = notes.get("assessment")
    context = notes.get("context") or {}
    return {
        "assessment": (assessment.get("prose")
                       if isinstance(assessment, dict) else None),
        "environment": context.get("environment"),
        "environment_source": context.get("source"),
        "priorities": [
            {"rank": p.get("rank"), "action": p.get("action"),
             "why": p.get("why")}
            for p in sorted((notes.get("priorities") or []),
                            key=lambda p: p.get("rank") or 99)
        ],
        # Rendered as an attribution line. A reader deciding how much weight to
        # give a struck-through finding needs to know how much of the table was
        # reviewed at all: 3 of 48 annotated is a different document from 48 of
        # 48, and the table looks identical either way.
        "annotated": annotated,
        "findings_total": len(findings),
        # Coverage ledger: one row per fleet-wide (systemic) finding, each shown
        # as addressed by the analyst or not. Present so a reader can see the
        # review accounted for every systemic finding -- the render would have
        # failed otherwise, so every row here reads "addressed", but showing the
        # set is what makes that auditable rather than asserted.
        "coverage": coverage or [],
    }


def _mean(values: list) -> float | None:
    """Arithmetic mean over non-null values, or None when there are none."""
    clean = [v for v in values if v is not None]
    if not clean:
        return None
    return sum(clean) / len(clean)


def _max(values: list) -> float | None:
    """Maximum over non-null values, or None when there are none."""
    clean = [v for v in values if v is not None]
    if not clean:
        return None
    return max(clean)


def downsample(values: list, timestamps: list, bucket: int = SAMPLES_PER_HOUR,
               agg: str = "max") -> tuple[list, list]:
    """Reduce a 5-minute series to one point per bucket.

    Keeps the embedded payload small enough to inline (4,032 points becomes
    336) without hiding peaks — the default aggregation is max, so spikes
    survive the reduction.

    Args:
        values: Metric values, may contain None.
        timestamps: Matching ISO timestamps.
        bucket: Samples per output point.
        agg: "max" or "mean".

    Returns:
        Tuple of (bucketed values, first timestamp of each bucket).
    """
    fn = _max if agg == "max" else _mean
    out_vals: list = []
    out_ts: list = []
    for i in range(0, len(values), bucket):
        chunk = values[i:i + bucket]
        val = fn(chunk)
        if val is None:
            continue
        out_vals.append(round(val, 3))
        out_ts.append(timestamps[i] if i < len(timestamps) else "")
    return out_vals, out_ts


def cluster_series(cluster: dict, metric: str, statistic: str,
                   agg: str = "max") -> tuple[list, list]:
    """Extract one series per cluster, taking the worst node at each point.

    Node-based clusters report per node; for a fleet-level view we want the
    hottest node at each timestamp rather than an average that would dilute a
    single hot shard.

    Args:
        cluster: A cluster entry from metrics.json.
        metric: CloudWatch metric name.
        statistic: Statistic key (e.g. "Maximum").
        agg: Bucket aggregation passed to downsample.

    Returns:
        Tuple of (values, timestamps). Empty lists when the metric is absent.
    """
    timestamps = cluster.get("timestamps_5min") or []

    if "nodes" in cluster:
        per_node = []
        for node_data in cluster["nodes"].values():
            series = (node_data.get(metric) or {}).get(statistic)
            if series:
                per_node.append(series)
        if not per_node:
            return [], []
        length = max(len(s) for s in per_node)
        merged = []
        for i in range(length):
            point = [s[i] for s in per_node if i < len(s) and s[i] is not None]
            merged.append(max(point) if point else None)
    else:
        merged = ((cluster.get("metrics") or {}).get(metric) or {}).get(statistic)
        if not merged:
            return [], []

    return downsample(merged, timestamps, agg=agg)


def _derive_facts(clusters_inv: dict, analysis_clusters: dict,
                  by_type: dict) -> dict:
    """Derive the narrative callout figures from the data, not from hardcoding.

    The report's prose asserts three specific things: a volatile-lru cluster
    with no TTL-bearing keys, a serverless cache billed for storage it isn't
    using, and spend attributed to a region this review never scanned. Each
    only belongs in the report if the JSON actually supports it, so each is
    resolved here and the corresponding callout is skipped when it is absent.

    Args:
        clusters_inv: Inventory entries keyed by cluster id.
        analysis_clusters: Analysis entries keyed by cluster id.
        by_type: Usage-type totals over the window.

    Returns:
        Dict with optional "ttl_risk", "serverless_minimum" and
        "foreign_region" entries.
    """
    facts: dict = {}
    scanned = {c.get("region") for c in clusters_inv.values() if c.get("region")}

    for cid, inv in sorted(clusters_inv.items()):
        pct = (analysis_clusters.get(cid, {}).get("percentiles") or {})

        def peak(key: str, default=None):
            entry = pct.get(key)
            if isinstance(entry, dict):
                return entry.get("max", default)
            return default

        # volatile-* policies can only evict keys carrying a TTL. Items present
        # but zero volatile items means nothing is evictable.
        policy = (inv.get("parameters") or {}).get("maxmemory-policy") or ""
        items = peak("CurrItems_Maximum") or 0
        volatile = peak("CurrVolatileItems_Maximum")
        if (policy.startswith("volatile-") and items > 0
                and (volatile is None or volatile == 0)):
            facts["ttl_risk"] = {"cluster": cid, "policy": policy,
                                 "items": int(items)}

        # A serverless cache storing nothing is still billed the engine's
        # metered storage minimum.
        #
        # The finding rests on the bytes metric, not on the cost, so it still
        # holds when no cost was collected -- it just cannot quote a figure.
        # None, not 0.0: summing an empty by_type gave 0.0 and the callout then
        # said the cache "accrued $0.00 in storage charges", which contradicts
        # the very point it is making.
        if inv.get("cluster_type") == "serverless":
            stored = peak("BytesUsedForCache_Maximum")
            if stored is not None and stored == 0:
                storage_lines = [v for k, v in by_type.items()
                                 if "CachedData" in k]
                facts["serverless_minimum"] = {
                    "cluster": cid,
                    "engine": inv.get("engine") or "redis",
                    "cost": (round(sum(storage_lines), 2)
                             if storage_lines else None),
                }

    # Cost Explorer prefixes usage types with a region billing code (USE2-,
    # APS3-, EU-). Spend tagged for a region this review never scanned is spend
    # the review cannot account for, so it is called out rather than folded into
    # the totals above.
    #
    # Compared on region ids, not on codes: the codes are irregular (eu-west-1
    # is EU) and comparing them directly meant an unlisted region resolved to
    # None on both sides and matched nothing.
    if not scanned & set(BILLING_CODE_FOR_REGION):
        # Every scanned region is outside the published billing-code table
        # (GovCloud, sovereign partitions). Foreign-region detection cannot work
        # there, and reporting "no foreign spend" would assert a completeness
        # the data does not support.
        logger.warning(
            "No scanned region (%s) has a published Cost Explorer billing code; "
            "out-of-region spend cannot be identified for this account.",
            ", ".join(sorted(scanned)) or "none",
        )

    foreign: dict = {}
    for usage_type, amount in by_type.items():
        region = _region_for_billing_code(usage_type)
        if region is None:
            # Unprefixed, or a code newer than the table. Not attributable, and
            # not evidence of home-region spend either.
            logger.debug("Usage type %s carries no known region code", usage_type)
            continue
        if region not in scanned:
            foreign[region] = round(foreign.get(region, 0.0) + amount, 2)
    if foreign:
        facts["foreign_region"] = foreign

    return facts


# ---------------------------------------------------------------------------
# Per-cluster panel charts.
#
# Each entry is (metric, statistic, label, unit, divisor). The statistic is part
# of the identity of the series, not a detail: the same metric read with a
# different statistic is a different measurement, and reading one with the wrong
# statistic is the defect class this pipeline has hit most often. Every pair here
# is the one references/metrics-catalog.md says Stage 2 collects, and a pair Stage
# 2 does not collect yields no chart rather than an empty one.
#
# CacheHitRate is charted as a percentage because that is what CloudWatch emits
# for ElastiCache (the example fleet's values run 75-99, not 0.75-0.99) even
# though the metric's definition reads as a ratio. Asserted in the tests, since
# a 0-1 ratio on a 0-100 axis would draw a flat line at the bottom and read as a
# cache that never hits.
PANEL_METRICS = (
    ("EngineCPUUtilization", "Maximum", "Engine CPU", "%", 1),
    ("DatabaseMemoryUsagePercentage", "Maximum", "Memory used", "%", 1),
    ("NetworkBaselineUsageInPercentage", "Maximum",
     "Network baseline used (in)", "%", 1),
    ("CacheHitRate", "Average", "Cache hit rate", "%", 1),
    ("CurrConnections", "Maximum", "Connections", "", 1),
    # Sums, per 5-minute datapoint. Labelled as such: a bare "Evictions" axis
    # invites the reader to take the number as a rate.
    ("Evictions", "Sum", "Evictions (per 5 min)", "", 1),
    ("ReplicationLag", "Maximum", "Replication lag", "s", 1),
    # 1 on the shard's primary, 0 on a replica -- the per-node lines show which
    # node is primary, and a failover appears as the 1 moving between them.
    ("IsMaster", "Maximum", "Primary node (1 = primary)", "", 1),
    ("BytesUsedForCache", "Maximum", "Memory in use", " MB", 1048576),
    # Serverless-only. Absent metrics are skipped per cluster, so listing both
    # families here needs no cluster_type branch.
    ("ElastiCacheProcessingUnits", "Sum", "ECPUs (per 5 min)", "", 1),
    ("ThrottledCmds", "Sum", "Throttled commands (per 5 min)", "", 1),
)

# ---------------------------------------------------------------------------
# All-metrics charting (per node, grouped by category).
#
# An operations review is only complete if the operator can see every series
# that was collected, so the panels chart every metric+statistic pair a cluster
# actually reported -- not the curated shortlist above. Each chart overlays one
# line per node in the replication group, so a hot shard shows against its peers
# rather than being hidden inside a fleet-level max. The curated PANEL_METRICS
# still lead, under a "Key metrics" group, so the series an operator reaches for
# first are one glance away; every pair (these included) also appears under its
# category group for full detail.

# The category a metric falls in, resolved by ordered substring match on its
# name. First match wins, so specific rules precede general ones.
_CATEGORY_RULES = (
    # Topology first, so IsMaster / MasterLink group together at the top rather
    # than matching a broader rule.
    ("Topology", ("IsMaster", "MasterLink")),
    ("Compute", ("CPUCredit", "CPUUtilization", "EngineCPUUtilization")),
    ("Memory", ("Memory", "BytesUsedForCache", "Freeable", "Swap",
                "Fragmentation", "Defrag", "CurrItems", "CurrVolatileItems",
                "Reclaimed", "Eviction", "PageFaults", "KeysTracked",
                "AverageTTL", "DatabaseCapacityUsage")),
    ("Cache", ("CacheHit", "CacheMiss")),
    ("Network", ("Network",)),
    ("Connections", ("Connection", "TrafficManagementActive")),
    ("Commands", ("Cmds", "Cmd", "PubSub", "ProcessedCommands")),
    ("Replication", ("Replication", "SaveInProgress", "Durability")),
    ("Security", ("Authentication", "Authorization", "Iam")),
    ("Errors", ("Error",)),
    ("Serverless", ("ElastiCacheProcessingUnits", "Throttled")),
)

# Group render order. "Key metrics" leads, "Topology" second (which node is
# primary is a first question), "Other" catches anything unmatched.
_CATEGORY_ORDER = ("Key metrics", "Topology", "Compute", "Memory", "Cache",
                   "Network", "Connections", "Commands", "Replication",
                   "Security", "Errors", "Serverless", "Other")


def _metric_category(metric: str) -> str:
    """The category group a metric name falls in, or 'Other' when none match."""
    for name, needles in _CATEGORY_RULES:
        if any(n in metric for n in needles):
            return name
    return "Other"


def _split_camel(metric: str) -> str:
    """'EngineCPUUtilization' -> 'Engine CPU Utilization' for a chart label.

    Inserts a space before a capital that begins a new word, keeping runs of
    capitals (CPU, IAM, ECPU) together rather than exploding them letter by
    letter.
    """
    out = []
    for i, ch in enumerate(metric):
        if (i and ch.isupper()
                and (not metric[i - 1].isupper()
                     or (i + 1 < len(metric) and metric[i + 1].islower()))):
            out.append(" ")
        out.append(ch)
    return "".join(out)


# (metric, statistic) -> curated (label, unit, divisor), derived from
# PANEL_METRICS so the curated labels cannot drift from the shortlist.
_CURATED = {(m, s): (label, unit, divisor)
            for m, s, label, unit, divisor in PANEL_METRICS}


def _label_unit_divisor(metric: str, statistic: str) -> tuple:
    """Label, axis unit and byte divisor for any metric+statistic pair.

    Curated pairs keep their hand-written label and unit; every other pair gets
    a label split from its metric name and a unit inferred from the name, so a
    percentage axis reads 0-100 and a bytes axis reads in MB. A Sum is labelled
    "per 5 min" (the datapoint period) so the number is never read as a rate.
    """
    if (metric, statistic) in _CURATED:
        return _CURATED[(metric, statistic)]
    name = _split_camel(metric)
    if statistic == "Sum" and "Percentage" not in metric:
        name += " (per 5 min)"
    # Percentage before bytes, so a "...UsagePercentage" is a % axis, not a byte
    # count.
    if ("Percentage" in metric or "Utilization" in metric
            or metric.endswith("Rate")):
        return name, "%", 1
    if ("Bytes" in metric
            or metric in ("FreeableMemory", "SwapUsage", "BytesUsedForCache",
                          "UsedMemoryDataset")):
        return name, " MB", 1048576
    if "Latency" in metric:
        return name, " µs", 1
    if metric == "ReplicationLag":
        return name, " s", 1
    return name, "", 1


def _collect_pairs(cdata: dict) -> list:
    """Every (metric, statistic) pair a cluster reported, deduped across nodes.

    Ordered for a stable, diffable report: by category (the group render order),
    then metric name, then statistic. Node-based clusters union the pairs across
    their nodes; a serverless cache reads its single ``metrics`` map.
    """
    metric_map: dict = {}
    if "nodes" in cdata:
        for node_data in cdata["nodes"].values():
            for metric, stats in node_data.items():
                if isinstance(stats, dict):
                    metric_map.setdefault(metric, set()).update(stats)
    else:
        for metric, stats in (cdata.get("metrics") or {}).items():
            if isinstance(stats, dict):
                metric_map.setdefault(metric, set()).update(stats)
    pairs = [(m, s) for m, stats in metric_map.items() for s in stats]
    order = {c: i for i, c in enumerate(_CATEGORY_ORDER)}
    pairs.sort(key=lambda p: (order.get(_metric_category(p[0]), 99), p[0], p[1]))
    return pairs


def _node_series(cdata: dict, metric: str, statistic: str,
                 bucket: int = SAMPLES_PER_HOUR, agg: str = "max") -> tuple:
    """One downsampled series per node, aligned to a shared bucket grid.

    Unlike ``cluster_series`` (which merges to the hottest node at each point),
    this keeps every node distinct so the panel can overlay them. Empty buckets
    stay ``None`` rather than being dropped, so every node's array lines up with
    the single shared timestamp axis returned alongside them.

    Returns:
        ``(shared_timestamps, [(node_id, values), ...])`` -- an empty entry list
        when no node reported the pair.
    """
    timestamps = cdata.get("timestamps_5min") or []
    shared_ts = [timestamps[i] for i in range(0, len(timestamps), bucket)]
    fn = _max if agg == "max" else _mean

    def bucketize(series: list) -> list:
        out = []
        for i in range(0, len(timestamps), bucket):
            val = fn(series[i:i + bucket]) if series else None
            out.append(round(val, 3) if val is not None else None)
        return out

    entries = []
    if "nodes" in cdata:
        for node_id, node_data in cdata["nodes"].items():
            series = (node_data.get(metric) or {}).get(statistic)
            if series:
                entries.append((node_id, bucketize(series)))
    else:
        series = ((cdata.get("metrics") or {}).get(metric) or {}).get(statistic)
        if series:
            entries.append(("serverless", bucketize(series)))
    return shared_ts, entries


_SEVERITY_RANK = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}

# SATURATED first deliberately: a saturated cluster is the one about to page
# someone. An unknown classification ranks last rather than first -- the absence
# of a classification is not a signal about the cluster.
_CLASS_RANK = {"SATURATED": 0, "NETWORK-BOUND": 1, "OVER-PROVISIONED": 2,
               "BALANCED": 3, "IDLE": 4}


def _findings_by_cluster(findings: list) -> dict:
    """Group the merged findings list by cluster id."""
    out: dict = {}
    for finding in findings:
        out.setdefault(finding["cluster"], []).append(finding)
    return out


def _worst_severity(findings: list) -> str | None:
    """The most severe severity in a findings list, or None when empty."""
    ranked = [f.get("severity") for f in findings or []
              if f.get("severity") in _SEVERITY_RANK]
    if not ranked:
        return None
    return min(ranked, key=lambda s: _SEVERITY_RANK[s])


def _attention_key(cid: str, findings_by_cluster: dict,
                   analysis_clusters: dict) -> tuple:
    """Sort key ordering clusters by how much they warrant a reader's attention.

    Worst finding severity first, then utilization class, then cluster id.

    Severity is read from the **merged** findings -- Stage 3 and Stage 3.5
    together. Ranking on analysis.json alone put staging-redis-legacy, whose only
    CRITICALs are configuration findings (TLS off, no auth, a security group open
    to the world), below a cluster whose worst finding is a HIGH. That would have
    ordered the least secure cluster on the fleet below a healthier one, so a
    reader working top-down would meet it last.

    The cluster-id tiebreak keeps the order total, and therefore keeps the panel
    order stable between two runs of an unchanged fleet: without it, two clusters
    with the same severity and class would order by dict iteration and could swap
    places run to run -- the reproducibility guarantee failing in one of the few
    places a reader would actually notice.

    Args:
        cid: Cluster id.
        findings_by_cluster: cluster id -> that cluster's merged findings.
        analysis_clusters: analysis.json's clusters map, for the class.

    Returns:
        A tuple usable as a sort key; lower sorts earlier.
    """
    util = ((analysis_clusters.get(cid) or {}).get("utilization") or {})
    worst = _worst_severity(findings_by_cluster.get(cid) or [])
    return (_SEVERITY_RANK.get(worst, 9),
            _CLASS_RANK.get(util.get("classification"), 9),
            cid)


def _panel_metrics_bundle(cdata: dict) -> dict:
    """One cluster's charts, grouped by category, with one line per node.

    Charts every metric+statistic pair the cluster reported that carries
    non-zero data, grouped into ``_CATEGORY_ORDER`` with the curated key metrics
    leading. Each chart overlays one downsampled series per node, so a hot shard
    is visible against its peers rather than merged away. Pairs that arrived flat
    at zero, and the expected key metrics that never arrived, are named in
    ``flat_zero`` / ``not_reported`` rather than charted -- absence and zero
    stated separately, never as a silently missing chart a reader takes as fine.

    Extracted from ``_cluster_panels`` so this reduction can run ahead of render
    time and be written to ``report_data.json`` (Phase 9a): the render step then
    reads the few-hundred-KB reduction instead of loading the multi-GB raw
    ``metrics.json``. It is the same code either way, so the charts match.

    Args:
        cdata: One cluster's entry from metrics.json's clusters map, or {} for a
            cluster that reported no datapoints.

    Returns:
        ``{groups, flat_zero, not_reported, metrics_reported, chart_count}``.
    """
    def make_chart(metric, statistic, label, unit, divisor, shared_ts, entries):
        series = []
        for node_id, values in entries:
            if divisor != 1:
                values = [round(v / divisor, 3) if v is not None else None
                          for v in values]
            series.append({"node": node_id, "values": values})
        return {
            "metric": metric,
            # Carried so a reader can check a figure against CloudWatch without
            # guessing which statistic produced it.
            "statistic": statistic,
            "label": label,
            "unit": unit,
            # One shared x-axis; every node series aligns to it.
            "timestamps": shared_ts,
            "series": series,
        }

    # Absent and flat-zero are both omitted from the charts, but they are not the
    # same fact and the panel names which is which. A chart missing with no
    # explanation is the absence-rendered-as-measurement reading inverted: the
    # reader assumes the metric was fine.
    charts_by_cat: dict = {}
    flat_zero: list = []
    for metric, statistic in _collect_pairs(cdata):
        shared_ts, entries = _node_series(cdata, metric, statistic)
        if not entries:
            continue  # _collect_pairs only lists pairs that are present
        label, unit, divisor = _label_unit_divisor(metric, statistic)
        peak = max((v for _, vals in entries for v in vals if v is not None),
                   default=0)
        if peak <= 0:
            flat_zero.append(label)
            continue
        charts_by_cat.setdefault(_metric_category(metric), []).append(
            make_chart(metric, statistic, label, unit, divisor,
                       shared_ts, entries))

    # Key metrics: the curated shortlist, charted first. A hero pair that never
    # arrived is named in not_reported so its absence is explicit -- the only
    # source of not_reported, since non-curated metrics have no expected set for
    # a pair to be missing from. A hero pair flat at zero is already in flat_zero
    # from the pass above.
    key_charts: list = []
    not_reported: list = []
    for metric, statistic, label, unit, divisor in PANEL_METRICS:
        shared_ts, entries = _node_series(cdata, metric, statistic)
        if not entries:
            not_reported.append(label)
            continue
        peak = max((v for _, vals in entries for v in vals if v is not None),
                   default=0)
        if peak <= 0:
            continue
        key_charts.append(make_chart(metric, statistic, label, unit, divisor,
                                     shared_ts, entries))

    groups = []
    if key_charts:
        groups.append({"name": "Key metrics", "charts": key_charts})
    for cat in _CATEGORY_ORDER:
        if cat == "Key metrics":
            continue
        if charts_by_cat.get(cat):
            groups.append({"name": cat, "charts": charts_by_cat[cat]})

    return {
        "groups": groups,
        # Named, not merely omitted. "Zero for the whole window" is a
        # measurement; "the metric never arrived" is a monitoring gap. A chart
        # silently absent would read as neither.
        "flat_zero": flat_zero,
        "not_reported": not_reported,
        # False when the cluster reported no datapoints -- a fact about
        # monitoring, not about the cluster.
        "metrics_reported": bool(cdata),
        "chart_count": sum(len(g["charts"]) for g in groups),
    }


def build_report_data(metrics: dict, metrics_path: str | None = None) -> dict:
    """Reduce raw metrics to the report-facing report_data.json (Phase 9a).

    The render step needs only three things out of the ~1.7 GB raw
    ``metrics.json``: the per-cluster chart series (already downsampled to
    hourly), the cost section, and the collection metadata. This produces
    exactly those, so ``generate_html_report.py`` can render from a
    few-hundred-KB file and never open the raw one.

    Per cluster it carries the finished, downsampled chart bundle from
    ``_panel_metrics_bundle`` -- the same reduction the panels used before this
    split, so the charts are byte-identical. Cost and metadata are copied
    verbatim. No analysis happens here (the report performs none); this only
    downsamples for charts and passes cost/metadata through.

    Clusters are streamed one shard at a time through the shared loader (Phase
    9): ``metrics`` may be the small manifest or a legacy single file, and under
    the manifest each cluster's series are loaded, reduced, then freed before the
    next -- so this holds one shard in memory, not the whole fleet.

    Args:
        metrics: Parsed metrics -- the sharded manifest or a legacy single file.
        metrics_path: Path ``metrics`` was read from, needed to resolve shard
            files under the manifest shape. ``None`` is fine for a legacy
            in-memory dict.

    Returns:
        ``{clusters: {cid: bundle}, cost: {...}, metadata: {...}}``.
    """
    clusters = {}
    # Sorted so report_data.json is byte-identical whatever order the source
    # listed clusters in -- a legacy inline map preserves insertion order while
    # the manifest is already sorted; the panels re-sort by attention regardless.
    for cid in sorted(iter_cluster_ids(metrics, metrics_path)):
        cdata = load_cluster_metrics(metrics, metrics_path, cid)
        clusters[cid] = _panel_metrics_bundle(cdata or {})
    return {
        "clusters": clusters,
        "cost": metrics.get("cost") or {},
        "metadata": metrics.get("metadata") or {},
    }


def _cluster_panels(clusters_inv: dict, report_clusters: dict,
                    analysis_clusters: dict, findings: list) -> list:
    """Build one collapsible panel per cluster: its own charts, its own findings.

    Each panel charts one cluster's own metrics, grouped by category with the
    key metrics first, and overlays one line per node in the replication group
    so a hot shard is visible against its peers rather than merged away.

    A panel is built for every cluster in the *inventory*, not only those with
    metrics. A cluster that reported no datapoints at all still has a panel
    saying so -- silently omitting it would make a monitoring gap look like a
    cluster that does not exist.

    The metrics-derived half of each panel (charts + absence lists) comes from
    ``report_clusters`` -- the already-reduced bundles from
    ``build_report_data`` -- rather than from raw metrics, so this function never
    touches the multi-GB ``metrics.json`` (Phase 9a).

    Args:
        clusters_inv: cluster id -> inventory entry.
        report_clusters: report_data's clusters map (cid -> reduced bundle).
        analysis_clusters: analysis.json's clusters map.
        findings: The merged, sorted findings list. Counted per cluster so the
            summary line can carry a severity without the reader opening the
            panel.

    Returns:
        One dict per cluster, ordered by _attention_key so the panel a reader
        should open first is the one at the top.
    """
    by_cluster = _findings_by_cluster(findings)
    panels = []
    for cid, inv in clusters_inv.items():
        bundle = report_clusters.get(cid)
        if bundle is None:
            # In the inventory but absent from the reduction: no datapoints
            # arrived (or the cluster was discovered after the reducer ran).
            # This is the same bundle build_report_data would emit for an empty
            # entry, computed here so the cluster still gets a "not reported"
            # panel -- and it reads nothing, so the no-metrics.json guarantee
            # holds.
            bundle = _panel_metrics_bundle({})
        stats = analysis_clusters.get(cid) or {}
        own = by_cluster.get(cid) or []
        util = stats.get("utilization") or {}
        panels.append({
            "cluster": cid,
            "type": inv.get("cluster_type"),
            "engine": f"{inv.get('engine')} {inv.get('engine_version')}",
            "node_type": inv.get("node_type") or "serverless",
            "region": inv.get("region"),
            "classification": util.get("classification") or "unknown",
            # Steadiness (steady/variable/spiky) is the orthogonal axis to the
            # activity classification; the summary shows both. None/not_measured
            # is omitted rather than shown, since it is moot on an idle cluster.
            "steadiness": (stats.get("steadiness") or {}).get("label"),
            # Read/write mix for the characterization line. Carries its own
            # not_measured, so the panel states "no command traffic" rather than
            # rendering a default split.
            "read_write": (stats.get("efficiency") or {}).get(
                "read_write_ratio") or {},
            "recommendation": util.get("recommendation"),
            "worst_severity": _worst_severity(own),
            "finding_count": len(own),
            "groups": bundle["groups"],
            "chart_count": bundle["chart_count"],
            "flat_zero": bundle["flat_zero"],
            "not_reported": bundle["not_reported"],
            "metrics_reported": bundle["metrics_reported"],
        })

    # Ordered by _attention_key, so the panel a reader should open first -- the
    # cluster most in need of attention -- is at the top.
    panels.sort(key=lambda p: _attention_key(p["cluster"], by_cluster,
                                             analysis_clusters))
    return panels


def _check_glossary() -> list[dict]:
    """Every configuration check in the registry, as id -> meaning + pillar.

    Built from `check_configuration.CHECKS` so it cannot drift from what the
    checker evaluates: a reader who meets "SEC-03" in a finding row (or wonders
    about a check that passed and therefore has no row) can look up what it means
    without console access or ElastiCache expertise. Sorted by id so the output
    is deterministic and diffable.

    Returns:
        One dict per check, `{"check_id", "title", "pillar"}`, sorted by id.
    """
    return [
        {"check_id": c.check_id, "title": c.title, "pillar": c.pillar}
        for c in sorted(CHECKS, key=lambda c: c.check_id)
    ]


def _config_coverage(config: dict | None,
                     inventory_cluster_ids: list[str] | None = None) -> dict | None:
    """Summarise which configuration checks ran, and against what.

    This is the part of `config_findings.json` that has no equivalent anywhere
    else in the pipeline. Stage 3's findings say what is wrong; only Stage 3.5
    can say what was *looked at* — and the difference matters, because a check
    that never ran and a check that passed are indistinguishable in a report
    that only lists failures. The reader who scans the Findings table and sees
    no TLS row deserves to know whether that means TLS is fine or TLS was never
    evaluated.

    Stage 3.5 runs every check in its registry against every cluster it is given,
    so `metadata.check_ids` is the whole registry and a check is only ever
    *evaluated* or *skipped with a reason*. The gap is therefore not a missing
    check but a missing cluster: a record with no `cluster_id`, or a cluster
    discovered after Stage 3.5 ran, is absent from `clusters` entirely and its
    posture was never graded. That is the silent case, so it is computed here by
    difference against the inventory rather than trusted to be empty.

    Args:
        config: Parsed config_findings.json, or None when not supplied.
        inventory_cluster_ids: Every cluster in the inventory, used to find the
            ones Stage 3.5 never saw. Omitted means the check is skipped, not
            that nothing is missing.

    Returns:
        Dict with the registry size, the evaluated check ids, the policy the
        checks were graded against, per-check skip reasons, and any unchecked
        clusters. None when no config findings were supplied, which the renderer
        branches on rather than reporting zero coverage as full coverage.
    """
    if not config:
        return None

    meta = config.get("metadata") or {}
    clusters = config.get("clusters") or {}

    # Skips are reported per reason rather than per cluster: "COST-06 does not
    # apply to 5 node-based clusters" is one fact, and repeating it five times
    # buries the one skip a reader should care about.
    skipped: dict[str, dict] = {}
    for cid, entry in sorted(clusters.items()):
        for skip in entry.get("checks_skipped") or []:
            check_id = skip.get("check_id") or "unknown"
            row = skipped.setdefault(
                check_id, {"check_id": check_id,
                           "reason": skip.get("reason") or "",
                           "clusters": []})
            row["clusters"].append(cid)

    return {
        "checks_in_registry": meta.get("checks_in_registry"),
        "check_ids": meta.get("check_ids") or [],
        "clusters_checked": meta.get("clusters_checked"),
        "review_date": meta.get("review_date"),
        # The policy the findings were graded against. A reader cannot judge an
        # OE-04 tag finding without knowing which tags were required, and the
        # standard is a per-customer input (see D5) rather than a universal.
        "policy": meta.get("policy") or {},
        "skipped": sorted(skipped.values(), key=lambda r: r["check_id"]),
        # Normally empty. When it is not, an entire cluster's security and
        # reliability posture is ungraded and no findings table can show that.
        "unchecked_clusters": sorted(
            set(inventory_cluster_ids or []) - set(clusters)),
    }


def _gate_options(price: dict, classification: str | None,
                  steadiness: str | None) -> list:
    """Turn a cluster's priced options into the ones honest to show it.

    The gate is the honesty-critical half of Phase 7c. It joins pricing with
    the analysis verdict so the report never contradicts itself:

    - **IDLE** -> the only saving is to decommission. Every keep-and-optimize
      lever (Valkey/Reserved/DSP/Graviton) is suppressed: recommending a
      multi-year commitment on a cluster the same report says to switch off is
      advice that locks spend onto waste.
    - **Active** (not IDLE) -> the eligible levers, decommission not offered.
      A commitment (Reserved/DSP) on a *steady* cluster is sound and shown
      plainly; on a *variable*/*spiky* one it is listed but annotated as risky
      (serverless is the better fit); on *not_measured* it is listed without the
      annotation, since there is no steadiness signal to warn from.
    - An **ineligible** option (e.g. DSP needing Gen7+) renders as its note with
      no dollar. A **Graviton** option with no positive on-demand saving says
      "no on-demand saving" rather than a negative one.

    These are ALTERNATIVES, never summed.
    """
    # A serverless / unpriced cluster has no node levers. Its cluster-level note
    # is surfaced separately by the caller; here it simply has no options.
    if price.get("node_type") is None:
        return []

    if classification == "IDLE":
        current = price.get("current_monthly")
        return [{
            "label": "Decommission -- recover current spend",
            "monthly": None,
            "saving": current,
            "commitment": False,
            "note": None,
            "risk_note": None,
            "is_decommission": True,
        }]

    display = []
    for opt in price.get("options") or []:
        commitment = bool(opt.get("commitment"))
        saving = opt.get("saving_monthly")
        note = opt.get("note")

        if not opt.get("eligible"):
            # No dollar; the note carries the reason it does not apply.
            display.append({
                "label": opt.get("label"), "monthly": None, "saving": None,
                "commitment": commitment, "note": note, "risk_note": None,
                "is_decommission": False})
            continue

        if opt.get("key") == "graviton" and (saving is None or saving <= 0):
            # A generation change that does not lower the on-demand rate. Shown
            # as such, never as a negative "saving".
            display.append({
                "label": opt.get("label"), "monthly": opt.get("monthly"),
                "saving": None, "commitment": commitment,
                "note": note or ("No on-demand saving; Graviton is a "
                                 "price/performance change, not a discount."),
                "risk_note": None, "is_decommission": False})
            continue

        risk_note = None
        if commitment and steadiness in ("variable", "spiky"):
            risk_note = (f"load is {steadiness}; a multi-year commitment "
                         f"carries risk -- consider serverless.")
        display.append({
            "label": opt.get("label"), "monthly": opt.get("monthly"),
            "saving": saving, "commitment": commitment, "note": note,
            "risk_note": risk_note, "is_decommission": False})
    return display


# ---------------------------------------------------------------------------
# Well-Architected pillar scoring.
#
# Implements the skill's published scoring methodology so a reader can recompute
# a score by hand from the findings. A score is a health *ratio* (0-100) per
# pillar, not a part-to-whole share -- the widget renders it as a gauge + bars
# (fill fraction), never as pie slices. Scores are DERIVED from the deterministic
# findings, so they are reproducible: same findings -> same number.
# ---------------------------------------------------------------------------

# Weights sum to 1.0, so a cluster score is a weighted mean of its pillar scores.
_PILLAR_WEIGHTS = {
    "reliability": 0.25,
    "performance": 0.25,
    "security": 0.20,
    "cost_optimization": 0.15,
    "operational_excellence": 0.10,
    "sustainability": 0.05,
}

# Display order matches _PILLAR_WEIGHTS; labels use the canonical WA pillar names.
_PILLAR_LABELS = {
    "reliability": "Reliability",
    "performance": "Performance Efficiency",
    "security": "Security",
    "cost_optimization": "Cost Optimization",
    "operational_excellence": "Operational Excellence",
    "sustainability": "Sustainability",
}

# Penalty per finding, by the severity ON the finding (not the check registry's
# default -- SEC-06 differs). Matches the skill's scoring table.
_SCORE_PENALTY = {"CRITICAL": 25, "HIGH": 15, "MEDIUM": 8, "LOW": 3}


def _score_band(score: float) -> str:
    """Map a 0-100 score to its assessment band."""
    if score >= 90:
        return "Excellent"
    if score >= 70:
        return "Good"
    if score >= 50:
        return "Needs Improvement"
    return "At Risk"


def _analysis_pillar(finding: dict) -> str | None:
    """Which pillar an analysis.json finding lands in, by its model_source.

    config_findings carry an explicit ``pillar``; analysis findings do not, so
    they are assigned here. A ``utilization`` finding classified IDLE or
    OVER-PROVISIONED is *waste*, so it scores against **sustainability**; every
    other analysis finding scores against **performance**. A ``correlation`` is a
    diagnostic that explains another finding, so it is excluded from scoring
    (returns None) -- scoring it would penalise a cluster twice for one problem.
    """
    model_source = finding.get("model_source")
    if model_source == "correlation":
        return None
    if model_source == "utilization":
        if finding.get("classification") in ("IDLE", "OVER-PROVISIONED"):
            return "sustainability"
        return "performance"
    # percentile, breach, efficiency, shard_balance, trend, workload, and any
    # other analysis model all score against performance.
    return "performance"


def _score_cluster(analysis_findings: list, config_findings: list) -> dict:
    """Compute one cluster's six pillar scores and its weighted overall score.

    Args:
        analysis_findings: this cluster's ``findings`` from analysis.json.
        config_findings: this cluster's ``findings`` from config_findings.json.

    Returns:
        ``{overall, band, pillars: [{key,label,score,band}, ...]}`` with pillars
        in _PILLAR_WEIGHTS order.
    """
    penalty = {pillar: 0 for pillar in _PILLAR_WEIGHTS}
    for finding in analysis_findings or []:
        pillar = _analysis_pillar(finding)
        if pillar in penalty:
            penalty[pillar] += _SCORE_PENALTY.get(finding.get("severity"), 0)
    for finding in config_findings or []:
        pillar = finding.get("pillar")
        if pillar in penalty:
            penalty[pillar] += _SCORE_PENALTY.get(finding.get("severity"), 0)

    pillars = []
    overall = 0.0
    for key, weight in _PILLAR_WEIGHTS.items():
        score = max(0, 100 - penalty[key])
        overall += score * weight
        pillars.append({"key": key, "label": _PILLAR_LABELS[key],
                        "score": score, "band": _score_band(score)})
    overall = round(overall, 1)
    return {"overall": overall, "band": _score_band(overall), "pillars": pillars}


def _build_scores(analysis_clusters: dict, config_findings: dict | None,
                  clusters_inv: dict) -> dict:
    """Fleet and per-cluster Well-Architected pillar scores.

    Scores each cluster first, then averages -- pooling penalties fleet-wide
    would let one bad staging box drive a pillar to zero for the whole fleet. The
    fleet score is the mean of the cluster scores; each fleet pillar score is the
    mean of that pillar across clusters.

    Args:
        analysis_clusters: analysis.json ``clusters`` map (findings per cluster).
        config_findings: parsed config_findings.json, or None.
        clusters_inv: inventory cluster-id -> entry, defining the cluster set.

    Returns:
        ``{fleet:{overall,band,pillars:[...]}, clusters:{cid:{...}}}``. Fleet is
        None-safe: an empty fleet scores 100 (nothing wrong observed), stated as
        such by the band.
    """
    config_clusters = (config_findings or {}).get("clusters", {})
    per_cluster: dict = {}
    for cid in sorted(clusters_inv):
        per_cluster[cid] = _score_cluster(
            (analysis_clusters.get(cid) or {}).get("findings"),
            (config_clusters.get(cid) or {}).get("findings"),
        )

    # Fleet pillar score = mean of that pillar across clusters; fleet overall =
    # mean of cluster overalls (score-then-average, never pooled).
    fleet_pillars = []
    if per_cluster:
        for i, (key, _weight) in enumerate(_PILLAR_WEIGHTS.items()):
            mean = sum(c["pillars"][i]["score"] for c in per_cluster.values())
            mean = round(mean / len(per_cluster), 1)
            fleet_pillars.append({"key": key, "label": _PILLAR_LABELS[key],
                                  "score": mean, "band": _score_band(mean)})
        fleet_overall = round(
            sum(c["overall"] for c in per_cluster.values()) / len(per_cluster), 1)
    else:
        fleet_pillars = [{"key": key, "label": _PILLAR_LABELS[key],
                          "score": 100, "band": _score_band(100)}
                         for key in _PILLAR_WEIGHTS]
        fleet_overall = 100.0

    return {
        "fleet": {"overall": fleet_overall, "band": _score_band(fleet_overall),
                  "pillars": fleet_pillars},
        "clusters": per_cluster,
    }


def _build_savings(pricing: dict | None, analysis_clusters: dict) -> dict | None:
    """The savings payload: per-cluster gated options and two fleet KPIs.

    None when there is no pricing.json, so the renderer omits the savings KPIs
    and the per-cluster cost-options blocks entirely rather than showing zeros --
    the same null-branch discipline config and notes follow. Never fabricates:
    a missing figure is None, which the renderer draws as an em dash.

    KPIs:
      - **Idle recoverable** -- the current monthly spend on IDLE clusters, which
        is recoverable in full by decommissioning. None when nothing is idle
        (or no idle cluster carries a priced current), so the tile reads "--".
      - **Best committed saving** -- the largest eligible commitment saving over
        the clusters actually being kept AND steady. None on an all-idle or
        all-spiky fleet, which is the honest "no commitment belongs here".
    """
    if not pricing:
        return None

    clusters: dict = {}
    idle_recoverable = 0.0
    best_committed = None
    for cid, price in sorted((pricing.get("clusters") or {}).items()):
        stats = analysis_clusters.get(cid) or {}
        classification = (stats.get("utilization") or {}).get("classification")
        steadiness = (stats.get("steadiness") or {}).get("label")
        current = price.get("current_monthly")

        clusters[cid] = {
            "current_monthly": current,
            "classification": classification,
            "steadiness": steadiness,
            "options": _gate_options(price, classification, steadiness),
            # Surfaced when there are no node options (serverless / unpriced).
            "note": price.get("note"),
        }

        if classification == "IDLE" and current is not None:
            idle_recoverable += current

        # Best committed saving is scoped to clusters being kept and steady:
        # never an idle cluster (decommission it), never a spiky one (serverless
        # fits better). That scoping is what encodes the recommendation.
        if classification and classification != "IDLE" and steadiness == "steady":
            for opt in price.get("options") or []:
                if (opt.get("eligible") and opt.get("commitment")
                        and opt.get("saving_monthly") is not None):
                    if (best_committed is None
                            or opt["saving_monthly"] > best_committed):
                        best_committed = opt["saving_monthly"]

    return {
        "clusters": clusters,
        "idle_recoverable": (round(idle_recoverable, 2)
                             if idle_recoverable > 0 else None),
        "best_committed_saving": (round(best_committed, 2)
                                  if best_committed is not None else None),
        "metadata": pricing.get("metadata") or {},
    }


def build_payload(inventory: dict, metrics: dict | None, analysis: dict,
                  generated_at: str | None = None,
                  config_findings: dict | None = None,
                  notes: dict | None = None,
                  pricing: dict | None = None,
                  report_data: dict | None = None,
                  metrics_path: str | None = None) -> dict:
    """Assemble the JSON object embedded in the report.

    Args:
        inventory: Parsed inventory.json.
        metrics: Parsed metrics.json, or None when report_data is supplied.
            Only used as the fallback source for the chart reduction, cost and
            metadata when report_data is absent -- with report_data present the
            raw metrics file is never opened (Phase 9a).
        analysis: Parsed analysis.json.
        generated_at: Render timestamp to stamp in the footer. Defaults to
            now, but is injectable so the same inputs produce byte-identical
            output -- which is what makes two reports diffable, and lets a
            test assert the whole document rather than parts of it.
        config_findings: Parsed config_findings.json (Stage 3.5), or None.
            Optional because the report must render from Stages 1-3 alone:
            a customer whose Stage 3.5 run failed still gets the metric
            sections rather than no report. When absent, the configuration
            coverage statement is omitted rather than claiming zero findings —
            "nothing was checked" and "everything passed" must never render
            the same way.
        notes: Parsed notes.json (the agent's own judgement), or None. The
            metric and configuration sections are the floor; agent prose is
            additive, so a report without notes is a complete report with one
            fewer section rather than a degraded one.
        report_data: Parsed report_data.json (Phase 9a) -- the pre-reduced
            per-cluster chart bundles plus the cost and metadata sections. When
            supplied, all three come from here and metrics.json is never
            opened. When None it is derived from metrics on the spot, so callers
            that still pass raw metrics behave exactly as before.
        metrics_path: Path ``metrics`` was read from, threaded to the fallback
            reducer so it can stream shards under the manifest shape (Phase 9).
            Unused when report_data is supplied or metrics is a legacy dict.

    Returns:
        A JSON-serialisable dict consumed by the report's JS layer.

    Raises:
        NotesError: When the notes assert a figure the cited data does not
            support, or annotate a finding that is not in the report. Checked
            here rather than left to the caller so no code path can embed
            unverified agent prose -- and checked against this function's own
            findings list, so the ids a note may use cannot drift from the ids
            the report actually renders.
    """
    if generated_at is None:
        generated_at = (datetime.datetime.now(datetime.timezone.utc)
                        .strftime("%Y-%m-%d %H:%M UTC"))
    clusters_inv = {c["cluster_id"]: c for c in inventory.get("clusters", [])}
    # Phase 9a: the render reads its chart series, cost and metadata from the
    # small report_data.json. When it was not supplied, reduce the raw metrics
    # here so callers that still pass metrics.json behave exactly as before --
    # the reduction is the same code either way, so the charts match byte for
    # byte.
    if report_data is None:
        if metrics is None:
            raise ValueError(
                "build_payload needs either metrics or report_data")
        report_data = build_report_data(metrics, metrics_path)
    report_clusters = report_data.get("clusters") or {}
    cost = report_data.get("cost") or {}
    analysis_clusters = analysis.get("clusters", {})

    # --- Cost: totals per usage type over the collected window ---
    #
    # Whether cost was collected at all is NOT the same question as whether it
    # was zero, and the report has to be able to tell a reader which it is.
    # fetch_metrics.py omits the "cost" key entirely under --skip-cost (a
    # documented flag, and the only option for a customer without Cost Explorer
    # permissions) and also when the Cost Explorer call raises. Both left every
    # projection below computing to 0.0, which rendered as "$0.00" monthly and
    # annual spend: a fabricated figure telling the customer their fleet is free.
    #
    # Same bug class as reading a metric with the wrong statistic -- absence
    # arriving as a confident number -- so it is fixed the same way: absence
    # becomes None, never 0.0, and the renderer is forced to branch on it.
    cost_collected = bool(cost.get("daily"))
    daily = cost.get("daily") or []
    by_type: dict = {}
    total_cost = 0.0
    for day in daily:
        total_cost += day.get("total_usd") or 0.0
        for usage_type, amount in (day.get("by_usage_type") or {}).items():
            by_type[usage_type] = by_type.get(usage_type, 0.0) + amount

    # --- Per-cluster summary table ---
    rows = []
    for cid, inv in sorted(clusters_inv.items()):
        stats = analysis_clusters.get(cid, {})
        pct = stats.get("percentiles") or {}

        def peak(metric_key: str):
            entry = pct.get(metric_key)
            if isinstance(entry, dict) and "max" in entry:
                return entry["max"]
            return None

        util = stats.get("utilization") or {}
        rows.append({
            "cluster": cid,
            "type": inv.get("cluster_type"),
            "engine": f"{inv.get('engine')} {inv.get('engine_version')}",
            "node_type": inv.get("node_type") or "serverless",
            "nodes": inv.get("total_nodes"),
            "tls": bool(inv.get("tls_enabled")),
            "at_rest": bool(inv.get("encryption_at_rest")),
            "auth": inv.get("auth_mode") or "none",
            "multi_az": bool(inv.get("multi_az")),
            "failover": bool(inv.get("automatic_failover")),
            "backups": inv.get("snapshot_retention_days"),
            "classification": util.get("classification") or "unknown",
            "cpu_peak": peak("EngineCPUUtilization_Maximum"),
            "items_peak": peak("CurrItems_Maximum"),
            "bytes_peak": peak("BytesUsedForCache_Maximum"),
            "conn_peak": peak("CurrConnections_Maximum"),
            "tags": inv.get("tags") or {},
        })

    # --- Findings, flattened across clusters and across both sources ---
    #
    # Stage 3 (metrics) and Stage 3.5 (configuration) are merged into one list
    # on purpose. Until this change the configuration findings were not rendered
    # at all -- three CRITICALs, auth_mode "none" on every cluster, existed only
    # in config_findings.json, which nothing downstream opened. A finding nobody
    # sees is close to a finding that does not exist, and worse than a missing
    # one, because the pipeline reported success.
    #
    # Merged rather than shown in a second table, because the reader's question
    # is "what is worst on this fleet", and that question does not care which
    # stage produced the answer. An open security group outranks a MEDIUM
    # eviction rate whichever file it came from. `source` and `check_id` are
    # carried so the row can still say where it came from and stay traceable to
    # the Well-Architected mapping.
    findings = []
    for cid, stats in sorted(analysis_clusters.items()):
        for finding in stats.get("findings") or []:
            findings.append({
                "cluster": cid,
                # Carried so an agent note can be attached to a specific row.
                # Fleet-unique as of the 5b prerequisite: Stage 3 numbered
                # findings per cluster, so three clusters each had a
                # "percentile-001" and a note would have landed on whichever
                # was read first.
                "finding_id": finding.get("finding_id"),
                "severity": finding.get("severity"),
                "title": finding.get("title"),
                "description": finding.get("description"),
                "recommendation": finding.get("recommendation"),
                "metric": finding.get("metric_name"),
                "value": finding.get("current_value"),
                "source": "metrics",
                "check_id": None,
                "pillar": None,
            })
    for cid, entry in sorted((config_findings or {}).get("clusters", {}).items()):
        for finding in entry.get("findings") or []:
            findings.append({
                "cluster": cid,
                "finding_id": finding.get("finding_id"),
                "severity": finding.get("severity"),
                "title": finding.get("title"),
                "description": finding.get("description"),
                "recommendation": finding.get("recommendation"),
                # Configuration checks read the API, not CloudWatch, so
                # metric_name is null and current_value is a config value
                # (often a bool). Kept as-is rather than coerced: rendering
                # False as "0" would read as a measurement.
                "metric": finding.get("metric_name"),
                "value": finding.get("current_value"),
                "source": "configuration",
                "check_id": finding.get("check_id"),
                "pillar": finding.get("pillar"),
            })

    # Sorted on (severity, cluster, check_id/title) rather than severity alone.
    # Python's sort is stable, so severity-only ordering left ties in input
    # order -- which meant every Stage 3 finding preceded every configuration
    # finding of the same severity, and the report's worst rows were grouped by
    # the accident of which file was read first. Ties now break on the cluster,
    # so one cluster's problems read together.
    findings.sort(key=lambda f: (_SEVERITY_RANK.get(f["severity"], 9),
                                 f["cluster"] or "",
                                 f["check_id"] or "",
                                 f["title"] or ""))

    # After the sort, so a panel's worst-severity summary is computed from the
    # same rows the findings table renders.
    panels = _cluster_panels(clusters_inv, report_clusters, analysis_clusters,
                             findings)

    # Well-Architected pillar scores (fleet + per-cluster), derived from the
    # deterministic findings. Attached to each panel so a cluster carries its own
    # health verdict alongside its charts.
    scores = _build_scores(analysis_clusters, config_findings, clusters_inv)
    for panel in panels:
        panel["scores"] = scores["clusters"].get(panel["cluster"])

    # Savings: the two fleet-level KPIs. None when no pricing.json was supplied,
    # so the KPIs are omitted rather than shown as zeros. The per-cluster cost
    # options that used to hang off each panel were removed: cost belongs in the
    # fleet savings section, not repeated inside every per-cluster detail block.
    savings = _build_savings(pricing, analysis_clusters)

    # Verified before it is shaped, and against this function's own findings
    # list. A note may only cite the documents this report was built from --
    # pricing.json included, so a savings figure can be cited in the assessment.
    coverage = None
    if notes:
        verify_notes(
            notes,
            {"inventory": inventory, "analysis": analysis,
             "config": config_findings or {}, "pricing": pricing or {}},
            known_finding_ids=[f["finding_id"] for f in findings
                               if f.get("finding_id")],
        )
        # Completeness half of the contract: a systemic (fleet-wide) finding may
        # not be silently dropped from the review. Raises here -- like the
        # figure check above -- so no code path can embed a review that quietly
        # ignored a fleet-wide recommendation. The ledger it returns is shown in
        # the report so a reader can see every systemic finding got a call.
        coverage = verify_coverage(notes, findings, len(clusters_inv))
    # After the sort, so annotations attach to the rows in their final order.
    # _build_notes writes verdict/note onto the rows themselves.
    notes_section = _build_notes(notes, findings, coverage)

    meta = report_data.get("metadata") or {}
    inv_meta = inventory.get("metadata", {})
    days = max(len(daily), 1)

    return {
        "meta": {
            "account": inv_meta.get("account_id"),
            "regions": inv_meta.get("regions_scanned") or [],
            "generated": generated_at,
            "period_start": meta.get("period_start"),
            "period_end": meta.get("period_end"),
            "datapoints": meta.get("total_datapoints"),
            "cluster_count": inv_meta.get("total_clusters"),
            # None, not 0.0, when nothing was collected -- including cost_days,
            # which otherwise reported a 1-day window over a window that was
            # never observed.
            "cost_collected": cost_collected,
            "cost_days": days if cost_collected else None,
            "cost_total": round(total_cost, 2) if cost_collected else None,
            "cost_monthly": (round(total_cost / days * 30.4, 2)
                             if cost_collected else None),
            "cost_annual": (round(total_cost / days * 365, 2)
                            if cost_collected else None),
        },
        "facts": _derive_facts(clusters_inv, analysis_clusters, by_type),
        "clusters": rows,
        # One entry per inventory cluster, always -- including any that reported
        # no metrics at all.
        "panels": panels,
        "findings": findings,
        # None when Stage 3.5's output was not supplied. The renderer must
        # branch on that rather than treating it as "no configuration problems".
        "config": _config_coverage(config_findings, list(clusters_inv)),
        # Every check id in the registry -> its plain-English meaning and pillar,
        # so a reader who meets a check id in a finding row (or wonders about one
        # that passed and left no row) can look it up. Derived from
        # check_configuration.CHECKS, so it cannot drift from what actually ran.
        "check_glossary": _check_glossary(),
        # None when the agent supplied no notes. The renderer omits the
        # assessment section entirely rather than showing an empty one, for the
        # same reason absent cost is not $0.00: an empty section attributed to
        # an analyst reads as "the analyst had nothing to say".
        "notes": notes_section,
        # None when no pricing.json was supplied; the renderer then shows no
        # savings KPIs and no cost-options blocks, and the rest is unchanged.
        "savings": savings,
        # Well-Architected pillar scores: fleet verdict + per-cluster. Derived
        # from the findings by the documented formula, reproducible by hand.
        "scores": scores,
        "palette": {"light": SERIES_LIGHT, "dark": SERIES_DARK},
    }


# ---------------------------------------------------------------------------
# HTML / CSS / JS template
# ---------------------------------------------------------------------------

TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ElastiCache Operations Review &mdash; __ACCOUNT__</title>
<style>
  /* Palette roles from the dataviz reference instance. Dark values are
     declared under both the media query (OS setting) and the [data-theme]
     scope (explicit toggle), so the toggle wins either way. */
  :root {
    color-scheme: light;
    --surface-1: #fcfcfb;
    --page: #f9f9f7;
    --text-primary: #0b0b0b;
    --text-secondary: #52514e;
    --text-muted: #898781;
    --grid: #e1e0d9;
    --axis: #c3c2b7;
    --border: rgba(11,11,11,0.10);
    --series-1: #2a78d6;
    --series-2: #eb6834;
    --series-3: #1baf7a;
    --good: #0ca30c;
    --warning: #fab219;
    --serious: #ec835a;
    --critical: #d03b3b;
    --success-text: #006300;
    --emphasis: #898781;
    /* Score-band ramp (green -> lime -> amber -> red). A status scale, always
       rendered WITH the band label, so it never rests on colour alone. Shared
       across themes like the other status tokens. */
    --band-excellent: #0a8f3f;
    --band-good: #7f9e26;
    --band-needs: #e08a00;
    --band-risk: #d03b3b;
  }
  @media (prefers-color-scheme: dark) {
    :root:where(:not([data-theme="light"])) {
      color-scheme: dark;
      --surface-1: #1a1a19;
      --page: #0d0d0d;
      --text-primary: #ffffff;
      --text-secondary: #c3c2b7;
      --text-muted: #898781;
      --grid: #2c2c2a;
      --axis: #383835;
      --border: rgba(255,255,255,0.10);
      --series-1: #3987e5;
      --series-2: #d95926;
      --series-3: #199e70;
      --success-text: #0ca30c;
    }
  }
  :root[data-theme="dark"] {
    color-scheme: dark;
    --surface-1: #1a1a19;
    --page: #0d0d0d;
    --text-primary: #ffffff;
    --text-secondary: #c3c2b7;
    --text-muted: #898781;
    --grid: #2c2c2a;
    --axis: #383835;
    --border: rgba(255,255,255,0.10);
    --series-1: #3987e5;
    --series-2: #d95926;
    --series-3: #199e70;
    --success-text: #0ca30c;
  }

  * { box-sizing: border-box; }
  body {
    margin: 0;
    background: var(--page);
    color: var(--text-primary);
    font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
    font-size: 15px;
    line-height: 1.55;
  }
  .wrap { max-width: 1080px; margin: 0 auto; padding: 32px 24px 72px; }
  header.top { display: flex; justify-content: space-between; align-items: flex-start; gap: 16px; flex-wrap: wrap; }
  h1 { font-size: 24px; font-weight: 600; margin: 0 0 4px; letter-spacing: -0.01em; }
  .sub { color: var(--text-secondary); font-size: 13px; margin: 0; }
  h2 { font-size: 17px; font-weight: 600; margin: 40px 0 4px; letter-spacing: -0.01em; }
  h2:first-of-type { margin-top: 32px; }
  .section-note { color: var(--text-secondary); font-size: 13px; margin: 0 0 16px; }
  .sub-head { font-size: 15px; font-weight: 600; margin: 30px 0 4px; }

  button.theme {
    background: var(--surface-1); color: var(--text-secondary);
    border: 1px solid var(--border); border-radius: 8px;
    padding: 7px 13px; font: inherit; font-size: 13px; cursor: pointer;
  }
  button.theme:hover { color: var(--text-primary); }

  .hero {
    background: var(--surface-1); border: 1px solid var(--border);
    border-radius: 12px; padding: 24px 26px; margin: 24px 0 20px;
  }
  .hero .label { color: var(--text-secondary); font-size: 13px; margin-bottom: 6px; }
  .hero .figure { font-size: 52px; font-weight: 600; line-height: 1.05; letter-spacing: -0.02em; }
  .hero .qualifier { color: var(--text-secondary); font-size: 13px; margin-top: 8px; }

  .kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 14px; }
  .tile {
    background: var(--surface-1); border: 1px solid var(--border);
    border-radius: 12px; padding: 16px 18px;
  }
  .tile .label { color: var(--text-secondary); font-size: 12.5px; }
  .tile .value { font-size: 27px; font-weight: 600; margin-top: 3px; letter-spacing: -0.01em; }
  .tile .foot { color: var(--text-muted); font-size: 12px; margin-top: 3px; }

  .card {
    background: var(--surface-1); border: 1px solid var(--border);
    border-radius: 12px; padding: 20px 22px 16px; margin-bottom: 20px;
  }
  .card h3 { font-size: 15px; font-weight: 600; margin: 0 0 2px; }
  .card .desc { color: var(--text-secondary); font-size: 13px; margin: 0 0 14px; }
  .card-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }
  button.toggle {
    background: none; border: 1px solid var(--border); border-radius: 7px;
    color: var(--text-secondary); font: inherit; font-size: 12px;
    padding: 5px 10px; cursor: pointer; white-space: nowrap;
  }
  button.toggle:hover { color: var(--text-primary); }

  svg { display: block; width: 100%; height: auto; overflow: visible; }
  svg text { font-family: system-ui, -apple-system, "Segoe UI", sans-serif; }
  .tick { font-size: 11px; fill: var(--text-muted); font-variant-numeric: tabular-nums; }
  .dlabel { font-size: 11.5px; fill: var(--text-secondary); font-variant-numeric: tabular-nums; }
  .gridline { stroke: var(--grid); stroke-width: 1; }
  .axisline { stroke: var(--axis); stroke-width: 1; }

  table { border-collapse: collapse; width: 100%; font-size: 13px; }
  caption { text-align: left; color: var(--text-secondary); font-size: 12.5px; padding-bottom: 8px; }
  th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid var(--border); }
  th { color: var(--text-secondary); font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: 0.04em; }
  td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
  tbody tr:last-child td { border-bottom: none; }
  .tableview[hidden] { display: none; }
  .scroll { overflow-x: auto; }

  /* Stands in for a chart whose data is absent. Deliberately looks like a
     stated fact rather than an error or an empty frame: the section keeps its
     place in the document, and the reason the data is missing is the content.
     Dashed, so it does not read as a rendered result. */
  .absent {
    border: 1px dashed var(--axis); border-radius: 10px;
    padding: 18px 20px; color: var(--text-secondary); font-size: 13px;
  }
  .absent strong { color: var(--text-primary); font-weight: 600; }

  /* Per-cluster panels. Collapsed by default: seven clusters x up to eight
     charts is not a thing to scroll past on the way to the findings, and <details>
     keeps every cluster present in the document (and in Ctrl-F) without
     rendering it. print forces them open -- a paper copy has no disclosure. */
  details.panel {
    background: var(--surface-1); border: 1px solid var(--border);
    border-radius: 12px; margin-bottom: 10px;
  }
  details.panel > summary {
    cursor: pointer; padding: 14px 18px; display: flex; flex-wrap: wrap;
    align-items: center; gap: 10px; font-size: 14px; list-style: none;
  }
  details.panel > summary::-webkit-details-marker { display: none; }
  /* The caret is text, not an image, so it survives forced-colors mode. */
  details.panel > summary::before {
    content: "\\u25b8"; color: var(--text-muted); font-size: 11px;
    flex: none; transition: transform .12s;
  }
  details.panel[open] > summary::before { transform: rotate(90deg); }
  details.panel > summary .cid { font-weight: 600; }
  details.panel > summary .meta { color: var(--text-secondary); font-size: 12.5px; }
  details.panel > summary .count { color: var(--text-muted); font-size: 12.5px; margin-left: auto; }
  .panel-body { padding: 0 18px 16px; }
  .panel-body .desc { color: var(--text-secondary); font-size: 13px; margin: 0 0 14px; }
  .panel-grid {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(460px, 1fr));
    gap: 18px 22px;
  }
  /* Charts are authored at 980px wide. Three columns squeezed them to ~320px
     (a 0.33x downscale that made axis labels illegible), so secondary groups
     render 2-up (~485px) and the "Key metrics" hero group spans the full width
     (~996px, near authoring size). */
  .panel-grid--wide { grid-template-columns: 1fr; }
  .panel-chart h4 {
    margin: 0 0 2px; font-size: 13px; font-weight: 600;
    display: flex; justify-content: space-between; align-items: baseline; gap: 10px;
  }
  .panel-chart h4 .stat { color: var(--text-muted); font-size: 11.5px; font-weight: 400; }
  .panel-chart .tableview { margin-top: 4px; }
  .panel-chart .chartmount[hidden] { display: none; }
  .panel-toggle { display: flex; justify-content: flex-end; margin-bottom: 8px; }
  /* Well-Architected score widget: overall gauge donut + pillar bars. */
  .scorecard {
    display: flex; flex-wrap: wrap; gap: 20px 28px; align-items: center;
    background: var(--surface-1); border: 1px solid var(--border);
    border-radius: 12px; padding: 20px 22px;
  }
  .scorecard .gauge { flex: 0 0 auto; width: 132px; }
  .scorecard.compact { padding: 14px 16px; gap: 16px 20px; }
  .scorecard.compact .gauge { width: 104px; }
  .gauge text.g-num { fill: var(--text-primary); font-weight: 600; font-variant-numeric: tabular-nums; }
  .gauge text.g-band { fill: var(--text-secondary); }
  .gauge text.g-cap { fill: var(--text-muted); }
  .pillar-bars { flex: 1 1 340px; min-width: 260px; display: grid; gap: 7px; }
  .pillar-row {
    display: grid; grid-template-columns: minmax(120px, 40%) 1fr auto;
    gap: 12px; align-items: center; font-size: 12.5px;
  }
  .pillar-row .pname { color: var(--text-secondary); }
  .pillar-track { height: 8px; border-radius: 5px; background: var(--grid); overflow: hidden; }
  .pillar-fill { height: 100%; border-radius: 5px; }
  .pillar-val { color: var(--text-primary); font-variant-numeric: tabular-nums; white-space: nowrap; }
  .pillar-val .band { color: var(--text-secondary); font-size: 11px; margin-left: 6px; }
  @media (max-width: 560px) { .pillar-row { grid-template-columns: 1fr auto; } .pillar-row .pillar-track { grid-column: 1 / -1; } }

  .metric-absence { margin: 0 0 10px; }
  .metric-absence summary {
    cursor: pointer; color: var(--text-secondary); font-size: 12.5px;
    padding: 2px 0;
  }
  .metric-absence summary:hover { color: var(--text-primary); }
  .metric-absence p {
    margin: 4px 0 0; color: var(--text-secondary); font-size: 12.5px;
    line-height: 1.5;
  }
  .panel-group-title {
    margin: 18px 0 8px; font-size: 12px; font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.05em;
    color: var(--text-secondary);
    border-bottom: 1px solid var(--border); padding-bottom: 4px;
  }
  .filters { display: flex; flex-wrap: wrap; gap: 12px 16px; align-items: flex-end; margin-bottom: 14px; }
  .filters .filter { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--text-secondary); }
  .filters select, .filters input {
    font: inherit; font-size: 13px; color: var(--text-primary);
    background: var(--surface-1); border: 1px solid var(--border);
    border-radius: 7px; padding: 5px 8px; min-width: 140px;
  }
  .filters .filter-count { color: var(--text-secondary); font-size: 12px; padding-bottom: 6px; }
  @media (max-width: 640px) { .panel-grid { grid-template-columns: 1fr; } }

  .badge {
    display: inline-flex; align-items: center; gap: 5px;
    font-size: 11.5px; font-weight: 600; padding: 2px 8px 2px 6px;
    border-radius: 999px; border: 1px solid var(--border); white-space: nowrap;
  }
  .badge .dot { width: 7px; height: 7px; border-radius: 50%; flex: none; }
  .ok { color: var(--success-text); }
  .no { color: var(--critical); }

  .tip {
    position: fixed; pointer-events: none; opacity: 0; transition: opacity .1s;
    background: var(--surface-1); border: 1px solid var(--border);
    border-radius: 9px; padding: 9px 11px; font-size: 12.5px;
    box-shadow: 0 6px 22px rgba(0,0,0,0.13); z-index: 50; min-width: 132px;
  }
  .tip .when { color: var(--text-muted); font-size: 11.5px; margin-bottom: 5px; }
  .tip .row { display: flex; align-items: center; gap: 7px; margin-top: 3px; }
  .tip .k { width: 12px; height: 2px; border-radius: 1px; flex: none; }
  .tip .v { font-weight: 600; font-variant-numeric: tabular-nums; }
  .tip .n { color: var(--text-secondary); }

  .callout {
    border: 1px solid var(--border); border-left: 3px solid var(--critical);
    background: var(--surface-1); border-radius: 10px;
    padding: 14px 18px; margin-bottom: 14px;
  }
  .callout.warn { border-left-color: var(--warning); }
  .callout.info { border-left-color: var(--series-1); }
  .callout h4 { margin: 0 0 5px; font-size: 14px; font-weight: 600; }
  .callout p { margin: 0; color: var(--text-secondary); font-size: 13px; }
  .callout code, code {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 12.5px; background: var(--page); padding: 1px 5px;
    border-radius: 4px; border: 1px solid var(--border);
  }
  ul.tight { margin: 8px 0 0; padding-left: 20px; color: var(--text-secondary); font-size: 13px; }
  ul.tight li { margin-bottom: 5px; }

  /* Agent-written sections. Marked off with a dashed rule and an explicit
     attribution line so a reader can tell judgement from measurement at a
     glance -- prose that looks like the rest of the report borrows its
     authority. */
  .analyst {
    border: 1px dashed var(--emphasis); border-radius: 10px;
    background: var(--surface-1); padding: 16px 18px; margin-bottom: 14px;
  }
  .analyst .attrib {
    font-size: 11.5px; text-transform: uppercase; letter-spacing: 0.04em;
    color: var(--text-muted); margin: 0 0 8px;
  }
  .analyst p { margin: 0 0 10px; color: var(--text-secondary); font-size: 13.5px; }
  .analyst p:last-child { margin-bottom: 0; }
  .analyst ol { margin: 8px 0 0; padding-left: 20px; color: var(--text-secondary); font-size: 13px; }
  .analyst ol li { margin-bottom: 7px; }
  .analyst ol strong { color: var(--text-primary); font-weight: 600; }

  /* A finding the agent judged a false positive. Struck through, never
     removed: the row is still what the pipeline produced, and deleting it
     would hide a pipeline bug behind an agent's opinion. Opacity alone would
     leave the judgement resting on a visual cue, so the verdict is also
     written out in the note column. */
  tr.dismissed td:not(.note) { text-decoration: line-through; }
  tr.dismissed { color: var(--text-muted); }
  td.note { font-size: 12.5px; color: var(--text-secondary); max-width: 320px; }
  td.note .verdict {
    display: block; font-size: 11px; text-transform: uppercase;
    letter-spacing: 0.04em; color: var(--text-muted); margin-bottom: 2px;
    text-decoration: none;
  }
  footer { color: var(--text-muted); font-size: 12px; margin-top: 40px; border-top: 1px solid var(--border); padding-top: 16px; }
  /* On paper there is no disclosure widget to click, so a collapsed panel would
     simply be missing data. CSS cannot open a <details>, so the beforeprint
     handler does it (and draws the charts) -- this only handles the styling. */
  @media print {
    button, .theme, .toggle, .filters { display: none !important; }
    .tableview { display: block !important; }
    details.panel > summary::before { display: none; }
    .panel-chart { break-inside: avoid; }
  }
</style>
</head>
<body>
<div class="wrap">

  <header class="top">
    <div>
      <h1>ElastiCache Operations Review</h1>
      <p class="sub" id="subtitle"></p>
    </div>
    <button class="theme" id="themeBtn" type="button">Dark mode</button>
  </header>

  <!-- ================= TIER 1 — VERDICT ================= -->
  <!-- The report leads with the fleet's Well-Architected health: the score a
       reader quotes, then the AI reading, then the supporting KPIs (cost is one
       of them now, no longer the hero). Inverted pyramid: conclusion first. -->
  <h2>Fleet health</h2>
  <p class="section-note">
    Well-Architected pillar scores, derived from the findings by the documented
    formula (penalty per finding, weighted by pillar) &mdash; reproducible by hand.
  </p>
  <div id="scores"></div>

  <!-- The AI review leads the reading: the fleet as a reviewer sees it, marked
       off and attributed. When no review pass ran it renders an explicit
       "no AI review" note in its place, so the section is present every run.
       Every figure in the AI prose was checked against its cited source. -->
  <div id="assessment"></div>

  <div class="kpis" id="kpis"></div>

  <!-- ================= TIER 2 — WHAT TO DO ================= -->
  <h2>What to do</h2>
  <p class="section-note">
    Prioritised actions. The AI ranking (what to do first, a judgement) leads;
    the list beneath it is derived from the findings by script.
  </p>
  <div id="priorities"></div>
  <div id="actions"></div>

  <!-- ================= TIER 3 — WHY ================= -->
  <h2>Findings</h2>
  <p class="section-note" id="findingsNote"></p>
  <div class="filters" id="findingsFilters"></div>
  <div class="card scroll" id="findingsTable"></div>

  <!-- Derived observations: mechanical facts read straight from the metrics
       (unbounded-growth risk, a serverless storage minimum, out-of-region
       spend). Folded in beneath the findings rather than floated to the top as
       a separate band, since they are findings of a different kind. -->
  <h3 class="sub-head">Derived observations</h3>
  <p class="section-note">
    Facts derived mechanically from the measured metrics; each cluster's own
    metrics are charted in the cluster detail below.
  </p>
  <div id="callouts"></div>

  <!-- ================= TIER 4 — DETAIL ================= -->
  <h2>Cluster detail</h2>
  <p class="section-note" id="panelsNote"></p>
  <div id="panels"></div>

  <h2>Fleet inventory &amp; posture</h2>
  <p class="section-note">
    Security and reliability settings straight from the ElastiCache API, with
    observed peaks from the analysis stage.
  </p>
  <div class="card scroll" id="inventoryTable"></div>

  <h2>Configuration checks &amp; coverage</h2>
  <p class="section-note" id="coverageNote"></p>
  <div class="card" id="coverage"></div>

  <!-- Glossary of every check id in the registry, so a reader who meets "SEC-03"
       in a finding row can learn what it means -- including checks that passed
       and therefore have no row. Script-derived from check_configuration.CHECKS,
       so it cannot drift from what the checker actually evaluates. -->
  <div class="card" id="checkGlossary"></div>

  <!-- ================= FOOTER — METHOD ================= -->
  <h2>Method &amp; caveats</h2>
  <div class="card" id="method"></div>

  <footer id="footer"></footer>
</div>

<div class="tip" id="tip" role="status" aria-live="polite"></div>

<script id="reportData" type="application/json">__DATA__</script>
<script>
(function () {
  "use strict";

  const DATA = JSON.parse(document.getElementById("reportData").textContent);
  const tip = document.getElementById("tip");

  // --- helpers ---------------------------------------------------------
  const SVG_NS = "http://www.w3.org/2000/svg";
  function el(name, attrs, parent) {
    const node = document.createElementNS(SVG_NS, name);
    if (attrs) for (const k in attrs) node.setAttribute(k, attrs[k]);
    if (parent) parent.appendChild(node);
    return node;
  }
  function svgRoot(parent, w, h) {
    parent.textContent = "";
    const s = el("svg", {viewBox: "0 0 " + w + " " + h,
                         preserveAspectRatio: "xMidYMid meet",
                         role: "img"}, parent);
    return s;
  }
  // An em dash for a figure that was never collected. Guarded here as well as
  // at each call site so a null can never reach toFixed and render "$NaN" --
  // and, more importantly, so it can never render "$0.00" and read as measured.
  function money(v) {
    if (v === null || v === undefined) return "\\u2014";
    const abs = Math.abs(v);
    if (abs >= 1000) return "$" + v.toLocaleString("en-US", {maximumFractionDigits: 0});
    return "$" + v.toFixed(2);
  }
  function fmt(v, digits) {
    if (v === null || v === undefined) return "\\u2014";
    return v.toLocaleString("en-US", {maximumFractionDigits: digits === undefined ? 2 : digits});
  }
  function bytes(v) {
    if (v === null || v === undefined) return "\\u2014";
    if (v >= 1073741824) return (v / 1073741824).toFixed(2) + " GB";
    if (v >= 1048576) return (v / 1048576).toFixed(1) + " MB";
    if (v >= 1024) return (v / 1024).toFixed(1) + " KB";
    return v + " B";
  }
  function shortDate(iso) {
    if (!iso) return "";
    const d = new Date(iso);
    if (isNaN(d)) return iso;
    return d.toLocaleDateString("en-US", {month: "short", day: "numeric"});
  }
  function dateTime(iso) {
    if (!iso) return "";
    const d = new Date(iso);
    if (isNaN(d)) return iso;
    return d.toLocaleString("en-US", {month: "short", day: "numeric",
                                      hour: "numeric", minute: "2-digit"});
  }
  function darkNow() {
    return document.documentElement.getAttribute("data-theme") === "dark" ||
      (!document.documentElement.getAttribute("data-theme") &&
       window.matchMedia("(prefers-color-scheme: dark)").matches);
  }
  function seriesColors() {
    return darkNow() ? DATA.palette.dark : DATA.palette.light;
  }
  // A series' colour comes from the slot the payload assigned it, never from
  // its index in this chart's list. Every per-cluster panel holds a single
  // series on slot 0, so index-based colouring (which once cycled and gave one
  // cluster different hues in different charts) cannot arise -- see build_payload.
  function colorOf(s) {
    const slots = darkNow() ? DATA.palette.dark : DATA.palette.light;
    // Clamped, not wrapped. Wrapping is what cycled; a slot past the end of the
    // palette is a bug in build_payload, and repeating the last hue makes it
    // visible rather than making it look like a legitimate extra cluster.
    return slots[Math.min(s.slot || 0, slots.length - 1)];
  }
  function niceMax(v) {
    if (v <= 0) return 1;
    const mag = Math.pow(10, Math.floor(Math.log10(v)));
    // Fine-grained steps: a coarse ladder rounds $104 up to $200 and leaves
    // half the plot empty.
    const steps = [1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10];
    for (const s of steps) if (v <= s * mag) return s * mag;
    return 10 * mag;
  }
  // Untrusted labels (cluster ids, usage types) always go in via textContent.
  function tipRows(when, rows) {
    tip.textContent = "";
    if (when) {
      const w = document.createElement("div");
      w.className = "when";
      w.textContent = when;
      tip.appendChild(w);
    }
    rows.forEach(function (r) {
      const line = document.createElement("div");
      line.className = "row";
      if (r.color) {
        const k = document.createElement("span");
        k.className = "k";
        k.style.background = r.color;
        line.appendChild(k);
      }
      const v = document.createElement("span");
      v.className = "v";
      v.textContent = r.value;
      line.appendChild(v);
      const n = document.createElement("span");
      n.className = "n";
      n.textContent = r.name;
      line.appendChild(n);
      tip.appendChild(line);
    });
  }
  function showTip(evt) {
    tip.style.opacity = "1";
    const pad = 14;
    const rect = tip.getBoundingClientRect();
    let x = evt.clientX + pad;
    let y = evt.clientY + pad;
    if (x + rect.width > window.innerWidth - 8) x = evt.clientX - rect.width - pad;
    if (y + rect.height > window.innerHeight - 8) y = evt.clientY - rect.height - pad;
    tip.style.left = Math.max(8, x) + "px";
    tip.style.top = Math.max(8, y) + "px";
  }
  function hideTip() { tip.style.opacity = "0"; }

  // Replaces a chart with a statement of why its data is absent, keeping the
  // section in place. `heading` names the cause in a few words; `detail` is the
  // full explanation. Both go in via textContent -- never innerHTML.
  function absent(container, heading, detail) {
    container.textContent = "";
    const box = document.createElement("div");
    box.className = "absent";
    const h = document.createElement("strong");
    h.textContent = heading;
    box.appendChild(h);
    box.appendChild(document.createTextNode(" \\u2014 " + detail));
    container.appendChild(box);
  }

  function table(container, caption, headers, rows) {
    container.textContent = "";
    const t = document.createElement("table");
    if (caption) {
      const c = document.createElement("caption");
      c.textContent = caption;
      t.appendChild(c);
    }
    const thead = document.createElement("thead");
    const hr = document.createElement("tr");
    headers.forEach(function (h) {
      const th = document.createElement("th");
      if (h.num) th.className = "num";
      th.textContent = h.label;
      hr.appendChild(th);
    });
    thead.appendChild(hr);
    t.appendChild(thead);
    const tbody = document.createElement("tbody");
    rows.forEach(function (r) {
      const tr = document.createElement("tr");
      // A row may be a bare array of cells, or {cells, className} when it needs
      // to carry state -- a finding the agent dismissed is struck through, and
      // that belongs on the row rather than on each cell.
      const cells = Array.isArray(r) ? r : r.cells;
      if (!Array.isArray(r) && r.className) tr.className = r.className;
      cells.forEach(function (cell, i) {
        const td = document.createElement("td");
        const classes = [];
        if (headers[i] && headers[i].num) classes.push("num");
        if (headers[i] && headers[i].cls) classes.push(headers[i].cls);
        if (classes.length) td.className = classes.join(" ");
        if (cell && cell.nodeType) td.appendChild(cell);
        else td.textContent = cell;
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    t.appendChild(tbody);
    container.appendChild(t);
  }

  // --- line chart with crosshair --------------------------------------
  function lineChart(mount, series, opts) {
    const W = 980, H = 300;
    const M = {t: 14, r: 74, b: 34, l: 52};
    const svg = svgRoot(mount, W, H);
    svg.setAttribute("aria-label", opts.aria || "line chart");

    const n = Math.max.apply(null, series.map(function (s) { return s.values.length; }));
    const allVals = [];
    series.forEach(function (s) {
      s.values.forEach(function (v) { if (v !== null) allVals.push(v); });
    });
    if (!allVals.length) return;
    const dataMax = Math.max.apply(null, allVals);
    const yMax = opts.yMax || niceMax(dataMax * 1.35);
    const plotW = W - M.l - M.r, plotH = H - M.t - M.b;
    const X = function (i) { return M.l + (n <= 1 ? 0 : (i / (n - 1)) * plotW); };
    const Y = function (v) { return M.t + plotH - (v / yMax) * plotH; };

    // gridlines + y ticks (solid hairlines, recessive)
    const ticks = 4;
    const step = yMax / ticks;
    // Precision must come from the step, not the value: rounding a 0.25 step
    // to one decimal prints "0.3" on the gridline actually sitting at 0.25.
    let digits = 0;
    while (digits < 4 && Math.abs(step * Math.pow(10, digits) -
                                  Math.round(step * Math.pow(10, digits))) > 1e-9) {
      digits++;
    }
    for (let i = 0; i <= ticks; i++) {
      const val = step * i;
      const y = Y(val);
      el("line", {x1: M.l, y1: y, x2: M.l + plotW, y2: y, class: "gridline"}, svg);
      const t = el("text", {x: M.l - 9, y: y + 4, class: "tick",
                            "text-anchor": "end"}, svg);
      t.textContent = val.toLocaleString("en-US", {minimumFractionDigits: digits,
                                                   maximumFractionDigits: digits}) +
                      (opts.unit || "");
    }
    // x axis
    el("line", {x1: M.l, y1: M.t + plotH, x2: M.l + plotW, y2: M.t + plotH,
                class: "axisline"}, svg);
    const stamps = series[0].timestamps || [];
    const xTicks = 7;
    for (let i = 0; i < xTicks; i++) {
      const idx = Math.round((n - 1) * (i / (xTicks - 1)));
      const t = el("text", {x: X(idx), y: M.t + plotH + 20, class: "tick",
                            "text-anchor": i === 0 ? "start" :
                                           (i === xTicks - 1 ? "end" : "middle")}, svg);
      t.textContent = shortDate(stamps[idx]);
    }

    // series paths — 2px, round join/cap
    const ends = [];
    series.forEach(function (s, si) {
      let d = "";
      s.values.forEach(function (v, i) {
        if (v === null) return;
        d += (d ? " L " : "M ") + X(i).toFixed(2) + " " + Y(v).toFixed(2);
      });
      el("path", {d: d, fill: "none", stroke: colorOf(s),
                  "stroke-width": 2, "stroke-linejoin": "round",
                  "stroke-linecap": "round"}, svg);
      let lastIdx = -1;
      for (let i = s.values.length - 1; i >= 0; i--) {
        if (s.values[i] !== null) { lastIdx = i; break; }
      }
      if (lastIdx >= 0) {
        // end marker: >=8px, 2px surface ring so overlapping dots stay legible
        el("circle", {cx: X(lastIdx), cy: Y(s.values[lastIdx]), r: 4,
                      fill: colorOf(s),
                      stroke: "var(--surface-1)", "stroke-width": 2}, svg);
        ends.push({si: si, y: Y(s.values[lastIdx]), x: X(lastIdx),
                   value: s.values[lastIdx]});
      }
    });

    // Direct labels: end-of-line when the series separate there. When they
    // converge, stacking or nudging the labels detaches them from their lines,
    // so label the chart's extreme instead and let legend + table carry the
    // rest (marks-and-anatomy.md).
    const sortedEnds = ends.slice().sort(function (a, b) { return a.y - b.y; });
    let collides = false;
    for (let i = 1; i < sortedEnds.length; i++) {
      if (sortedEnds[i].y - sortedEnds[i - 1].y < 15) collides = true;
    }
    if (!collides) {
      ends.forEach(function (e) {
        const lab = el("text", {x: e.x + 9, y: e.y + 4, class: "dlabel"}, svg);
        lab.textContent = fmt(e.value, 2) + (opts.unit || "");
      });
    } else {
      let best = null;
      series.forEach(function (s, si) {
        s.values.forEach(function (v, i) {
          if (v !== null && (!best || v > best.v)) best = {v: v, i: i, si: si};
        });
      });
      if (best) {
        const cx = X(best.i), cy = Y(best.v);
        el("circle", {cx: cx, cy: cy, r: 4, fill: colorOf(series[best.si]),
                      stroke: "var(--surface-1)", "stroke-width": 2}, svg);
        // Flip the label inward near the right edge so it never overflows.
        const flip = cx > M.l + plotW * 0.8;
        const lab = el("text", {x: cx + (flip ? -9 : 9), y: cy - 8,
                                class: "dlabel",
                                "text-anchor": flip ? "end" : "start"}, svg);
        lab.textContent = "peak " + fmt(best.v, 2) + (opts.unit || "") + " \\u00b7 " +
                          series[best.si].name;
      }
    }

    // crosshair + hover layer
    const cross = el("line", {y1: M.t, y2: M.t + plotH, class: "axisline",
                              opacity: 0}, svg);
    const dots = series.map(function (s, si) {
      return el("circle", {r: 4.5, fill: colorOf(s),
                           stroke: "var(--surface-1)", "stroke-width": 2,
                           opacity: 0}, svg);
    });
    const hit = el("rect", {x: M.l, y: M.t, width: plotW, height: plotH,
                            fill: "transparent", tabindex: 0,
                            role: "application"}, svg);

    function atIndex(idx, evt) {
      const x = X(idx);
      cross.setAttribute("x1", x);
      cross.setAttribute("x2", x);
      cross.setAttribute("opacity", 1);
      const rows = [];
      series.forEach(function (s, si) {
        const v = s.values[idx];
        if (v === null || v === undefined) {
          dots[si].setAttribute("opacity", 0);
          return;
        }
        dots[si].setAttribute("cx", x);
        dots[si].setAttribute("cy", Y(v));
        dots[si].setAttribute("opacity", 1);
        rows.push({color: colorOf(s), name: s.name,
                   value: fmt(v, 2) + (opts.unit || "")});
      });
      tipRows(dateTime((series[0].timestamps || [])[idx]), rows);
      if (evt) showTip(evt);
    }
    hit.addEventListener("pointermove", function (evt) {
      const box = svg.getBoundingClientRect();
      const rel = (evt.clientX - box.left) / box.width * W;
      let idx = Math.round((rel - M.l) / plotW * (n - 1));
      idx = Math.max(0, Math.min(n - 1, idx));
      atIndex(idx, evt);
    });
    hit.addEventListener("pointerleave", function () {
      cross.setAttribute("opacity", 0);
      dots.forEach(function (d) { d.setAttribute("opacity", 0); });
      hideTip();
    });
    let focusIdx = 0;
    hit.addEventListener("focus", function () {
      atIndex(focusIdx);
      const box = hit.getBoundingClientRect();
      showTip({clientX: box.left + box.width / 2, clientY: box.top});
    });
    hit.addEventListener("blur", function () {
      cross.setAttribute("opacity", 0);
      dots.forEach(function (d) { d.setAttribute("opacity", 0); });
      hideTip();
    });
    hit.addEventListener("keydown", function (evt) {
      if (evt.key !== "ArrowLeft" && evt.key !== "ArrowRight") return;
      evt.preventDefault();
      focusIdx += (evt.key === "ArrowRight" ? 1 : -1);
      focusIdx = Math.max(0, Math.min(n - 1, focusIdx));
      atIndex(focusIdx);
      const box = hit.getBoundingClientRect();
      showTip({clientX: box.left + box.width / 2, clientY: box.top});
    });
  }

  // --- Well-Architected score widget: gauge donut + pillar bars ---------
  //
  // Score is a health ratio (0-100), NOT a part-to-whole share, so it is drawn
  // as fill fraction (an arc filled to score/100, and bars filled to each
  // pillar's score) -- never as pie slices. Colour follows the band, but the
  // band label is always printed beside it, so identity never rests on colour.
  const bandColor = {
    "Excellent": "var(--band-excellent)", "Good": "var(--band-good)",
    "Needs Improvement": "var(--band-needs)", "At Risk": "var(--band-risk)"
  };
  function scoreWidget(container, scoreObj, opts) {
    opts = opts || {};
    container.textContent = "";
    if (!scoreObj) return;
    const card = document.createElement("div");
    card.className = "scorecard" + (opts.compact ? " compact" : "");

    // Overall gauge: an SVG ring, background track + an arc stroked to the
    // score fraction, starting at 12 o'clock. Centre holds the number + band.
    const gauge = document.createElement("div");
    gauge.className = "gauge";
    const S = 120, cx = S / 2, cy = S / 2, r = 48, sw = 12;
    const C = 2 * Math.PI * r;
    // Taller viewBox than the ring is wide: the ring's outer edge reaches
    // y = cy + r + sw/2 = 114, so a square 120 box leaves no room for the
    // caption below it and the text collides with the bottom stroke. The extra
    // height lands entirely under the ring (which stays centred at cy) and
    // preserveAspectRatio letterboxes cleanly, so the ring does not shrink.
    const H = 134;
    const svg = svgRoot(gauge, S, H);
    svg.setAttribute("aria-label",
      (opts.caption || "Overall") + " score " + fmt(scoreObj.overall, 0) +
      " of 100, " + scoreObj.band);
    el("circle", {cx: cx, cy: cy, r: r, fill: "none", stroke: "var(--grid)",
                  "stroke-width": sw}, svg);
    const frac = Math.max(0, Math.min(1, scoreObj.overall / 100));
    const arc = el("circle", {cx: cx, cy: cy, r: r, fill: "none",
                   stroke: bandColor[scoreObj.band] || "var(--emphasis)",
                   "stroke-width": sw, "stroke-linecap": "round",
                   "stroke-dasharray": (frac * C).toFixed(2) + " " + C.toFixed(2),
                   transform: "rotate(-90 " + cx + " " + cy + ")"}, svg);
    if (frac === 0) arc.setAttribute("stroke", "none");
    const num = el("text", {x: cx, y: cy - 2, "text-anchor": "middle",
                   "font-size": 26, class: "g-num"}, svg);
    num.textContent = fmt(scoreObj.overall, 0);
    const band = el("text", {x: cx, y: cy + 16, "text-anchor": "middle",
                    "font-size": 10.5, class: "g-band"}, svg);
    band.textContent = scoreObj.band;
    if (opts.caption) {
      const cap = el("text", {x: cx, y: H - 5, "text-anchor": "middle",
                     "font-size": 10, class: "g-cap"}, svg);
      cap.textContent = opts.caption;
    }
    card.appendChild(gauge);

    // Pillar bars: one row per pillar, fill width = score, value + band beside.
    const bars = document.createElement("div");
    bars.className = "pillar-bars";
    (scoreObj.pillars || []).forEach(function (p) {
      const row = document.createElement("div");
      row.className = "pillar-row";
      const name = document.createElement("span");
      name.className = "pname";
      name.textContent = p.label;
      const track = document.createElement("div");
      track.className = "pillar-track";
      const fill = document.createElement("div");
      fill.className = "pillar-fill";
      fill.style.width = Math.max(0, Math.min(100, p.score)) + "%";
      fill.style.background = bandColor[p.band] || "var(--emphasis)";
      track.appendChild(fill);
      const val = document.createElement("span");
      val.className = "pillar-val";
      val.appendChild(document.createTextNode(fmt(p.score, 0)));
      const b = document.createElement("span");
      b.className = "band";
      b.textContent = p.band;
      val.appendChild(b);
      // Hover: the full label + score + band, so a truncated name is recoverable.
      row.setAttribute("title", p.label + ": " + fmt(p.score, 0) + " \\u2014 " + p.band);
      row.appendChild(name);
      row.appendChild(track);
      row.appendChild(val);
      bars.appendChild(row);
    });
    card.appendChild(bars);
    container.appendChild(card);
  }

  // --- render ----------------------------------------------------------
  const M = DATA.meta;

  // Fleet Well-Architected verdict — the report's headline (Tier 1).
  if (DATA.scores && DATA.scores.fleet) {
    scoreWidget(document.getElementById("scores"), DATA.scores.fleet,
                {caption: "Fleet"});
  }

  // Said the same way everywhere cost is missing, so a reader who sees one
  // instance knows what the others mean. It names both causes because the
  // remedy differs: --skip-cost is the reviewer's own choice, a permissions
  // failure is not, and a customer who did not pass the flag needs to know to
  // check ce:GetCostAndUsage rather than assume the fleet is free.
  const COST_ABSENT =
    "Cost Explorer data was not collected for this run \\u2014 either " +
    "--skip-cost was passed or the ce:GetCostAndUsage call did not succeed. " +
    "This is not a $0 bill: no spend figure in this report, and no savings " +
    "estimate anywhere in it, is available for this fleet.";

  document.getElementById("subtitle").textContent =
    "Account " + M.account + " \\u00b7 " + M.regions.join(", ") +
    " \\u00b7 " + M.cluster_count + " clusters \\u00b7 " +
    shortDate(M.period_start) + " \\u2013 " + shortDate(M.period_end) +
    " (" + Number(M.datapoints).toLocaleString("en-US") + " datapoints)";

  // Cost is now a KPI tile (below), not the hero -- the fleet health verdict
  // leads the report instead. COST_ABSENT is reused as the tile's footnote so
  // "not collected" never reads as $0.

  const idle = DATA.clusters.filter(function (c) {
    return c.classification === "IDLE";
  }).length;
  const critical = DATA.findings.filter(function (f) {
    return f.severity === "CRITICAL";
  }).length;
  const kpis = [
    // The verdict's headline stat, restated as a tile for the scan-the-KPIs
    // reader; the gauge above carries the same number and band.
    {label: "Fleet score",
     value: DATA.scores ? fmt(DATA.scores.fleet.overall, 0) : "\\u2014",
     foot: DATA.scores ? DATA.scores.fleet.band : "not scored"},
    {label: "Critical findings", value: String(critical),
     foot: critical ? "act on these first" : "none"},
    {label: "Classified idle", value: idle + " of " + M.cluster_count,
     foot: idle ? "no traffic served" : "none idle"},
    // Cost demoted from hero to a tile. Monthly projection is the headline; the
    // footnote carries the observed total, window and annual -- or the absent
    // note, so "not collected" is never mistaken for $0.
    {label: "Monthly spend (proj.)",
     value: M.cost_collected ? money(M.cost_monthly) : "\\u2014",
     foot: M.cost_collected
       ? money(M.cost_total) + " over " + M.cost_days + " days \\u00b7 ~" +
         money(M.cost_annual) + "/yr"
       : "not collected"},
    // The footer names what the count covers, because the count's meaning
    // depends on whether Stage 3.5 ran. It used to claim every pillar
    // unconditionally while the configuration findings went unread -- so the
    // tile asserted Security and Reliability coverage from a number that held
    // only metric findings.
    {label: "Open findings", value: String(DATA.findings.length),
     foot: DATA.config ? "metrics and configuration checks"
                       : "metrics only \\u2014 no configuration check ran"}
  ];
  // Savings KPIs only when pricing.json was supplied. A "/mo" figure or an em
  // dash -- never $0.00, and never fabricated: "Best committed saving" is "--"
  // precisely when no cluster is both kept and steady, which is the honest
  // result on an all-idle fleet.
  if (DATA.savings) {
    const sv = DATA.savings;
    const perMo = function (v) {
      return (v === null || v === undefined) ? "\\u2014" : money(v) + "/mo";
    };
    kpis.push({label: "Idle recoverable", value: perMo(sv.idle_recoverable),
               foot: "decommission clusters classified idle"});
    kpis.push({label: "Best committed saving",
               value: perMo(sv.best_committed_saving),
               foot: "reserved / savings plan on a kept, steady cluster"});
  }
  const kpiWrap = document.getElementById("kpis");
  kpis.forEach(function (k) {
    const d = document.createElement("div");
    d.className = "tile";
    const l = document.createElement("div"); l.className = "label"; l.textContent = k.label;
    const v = document.createElement("div"); v.className = "value"; v.textContent = k.value;
    const f = document.createElement("div"); f.className = "foot"; f.textContent = k.foot;
    d.appendChild(l); d.appendChild(v); d.appendChild(f);
    kpiWrap.appendChild(d);
  });

  // Callouts — each is emitted only when the data supports the claim.
  const F = DATA.facts || {};
  const callouts = [];
  if (F.ttl_risk) {
    callouts.push({cls: "", h: F.ttl_risk.policy +
      " with no TTL keys \\u2014 writes will fail with OOM",
      p: F.ttl_risk.cluster + " holds " +
         F.ttl_risk.items.toLocaleString("en-US") + " items with " +
         "CurrVolatileItems at 0, so no key carries a TTL. Its maxmemory-policy is " +
         F.ttl_risk.policy + ", which can only evict keys that have one. AWS " +
         "documents this combination as returning \\u201cOOM command not " +
         "allowed\\u201d once memory fills. Set TTLs on the keys, or switch to " +
         "allkeys-lru."});
  }
  if (F.serverless_minimum) {
    callouts.push({cls: "warn",
      h: "Serverless cache bills a storage minimum while storing 0 bytes",
      p: F.serverless_minimum.cluster + " reported 0 bytes stored for the entire " +
         "window yet " +
         // cost is null when Cost Explorer was not queried. The metered minimum
         // is documented behaviour, so the claim holds without a figure.
         (F.serverless_minimum.cost === null
           ? "is still billed the engine's metered storage minimum (no Cost " +
             "Explorer data was collected for this run, so the charge is not " +
             "quantified here)"
           : "accrued " + money(F.serverless_minimum.cost) +
             " in storage charges") +
         ". Redis OSS serverless meters a 1 GB minimum; Valkey lowers that " +
         "to 100 MB at 33% lower price. Storing nothing all window, deleting or " +
         "right-sizing it is the real fix."});
  }
  if (F.foreign_region) {
    const parts = Object.keys(F.foreign_region).map(function (r) {
      return money(F.foreign_region[r]) + " to " + r;
    });
    callouts.push({cls: "info", h: "Spend outside the reviewed region",
      p: "Cost Explorer attributes " + parts.join(", ") + " over this window \\u2014 " +
         "regions this review did not scan, so nothing above accounts for it. " +
         "Worth a separate look."});
  }
  const coWrap = document.getElementById("callouts");
  callouts.forEach(function (c) {
    const d = document.createElement("div");
    d.className = "callout " + c.cls;
    const h = document.createElement("h4"); h.textContent = c.h;
    const p = document.createElement("p"); p.textContent = c.p;
    d.appendChild(h); d.appendChild(p);
    coWrap.appendChild(d);
  });

  // Inventory table
  function yesNo(flag, goodWhenTrue) {
    const span = document.createElement("span");
    const good = goodWhenTrue ? flag : !flag;
    span.className = good ? "ok" : "no";
    span.textContent = flag ? "yes" : "no";
    return span;
  }
  table(document.getElementById("inventoryTable"),
        "Configuration and observed peaks per cluster",
        [{label: "Cluster"}, {label: "Type"}, {label: "Engine"},
         {label: "Nodes"}, {label: "TLS"}, {label: "At rest"}, {label: "Auth"},
         {label: "Multi-AZ"}, {label: "Failover"}, {label: "Backups", num: true},
         {label: "CPU peak %", num: true}, {label: "Items", num: true},
         {label: "Data held", num: true}],
        DATA.clusters.map(function (c) {
          return [
            c.cluster,
            c.type,
            c.engine,
            c.node_type + (c.nodes ? " \\u00d7" + c.nodes : ""),
            yesNo(c.tls, true),
            yesNo(c.at_rest, true),
            c.auth,
            yesNo(c.multi_az, true),
            yesNo(c.failover, true),
            c.backups === 0 ? "none" : c.backups + "d",
            c.cpu_peak === null || c.cpu_peak === undefined ? "\\u2014" : fmt(c.cpu_peak, 2),
            c.items_peak === null || c.items_peak === undefined ? "\\u2014" : fmt(c.items_peak, 0),
            bytes(c.bytes_peak)
          ];
        }));

  // Findings
  const sevColor = {CRITICAL: "var(--critical)", HIGH: "var(--serious)",
                    MEDIUM: "var(--warning)", LOW: "var(--emphasis)"};
  const cfgCount = DATA.findings.filter(function (f) {
    return f.source === "configuration";
  }).length;
  const metCount = DATA.findings.length - cfgCount;
  // "0 from configuration checks" would be the same absence-as-a-measurement
  // reading this section exists to remove: it says the checks ran and found
  // nothing. With no Stage 3.5 output the count is not zero, it is undefined.
  const split = DATA.config
    ? metCount + " from measured metrics and " + cfgCount +
      " from configuration checks"
    : "all from measured metrics; no configuration check ran (see below)";
  const NOTES = DATA.notes;
  // The reviewed count, not a claim of review. An unannotated row means the
  // agent did not comment on it, which is not the same as agreeing with it --
  // the note below says so, because a table with three annotations reads as a
  // fully reviewed table if nothing states otherwise.
  const reviewNote = NOTES
    ? " The AI review examined " + NOTES.annotated + " of these " +
      DATA.findings.length + "; a row with no note was not commented on, " +
      "which is not the same as confirmed. A row struck through was judged a " +
      "false positive and is kept, with the reasoning, rather than removed."
    : "";
  document.getElementById("findingsNote").textContent =
    DATA.findings.length + " findings, most severe first: " + split + ". " +
    "Severity is each check's own classification, not a re-scoring; the badge " +
    "pairs a dot with the label so severity never rests on color alone. The " +
    "Check column cites the Well-Architected check id where there is one, so " +
    "any row can be traced back to the mapping it came from." + reviewNote;
  const verdictLabel = {confirmed: "Confirmed by AI review",
                        false_positive: "AI review: false positive",
                        needs_data: "AI review: needs more data"};
  const findingHeaders = [{label: "Severity"}, {label: "Cluster"},
                          {label: "Check"}, {label: "Finding"},
                          {label: "Recommendation"}];
  // The column exists only when notes do. An always-present "AI note"
  // column full of em dashes would read as an AI review that examined
  // everything and had nothing to say.
  if (NOTES) findingHeaders.push({label: "AI note", cls: "note"});
  // A row's Check cell cites its check id (configuration) or the metric it
  // measured (metrics); it is also the value the Check filter matches on.
  function findingCheck(f) { return f.check_id || f.metric || ""; }
  function buildFindingRow(f) {
    const badge = document.createElement("span");
    badge.className = "badge";
    const dot = document.createElement("span");
    dot.className = "dot";
    dot.style.background = sevColor[f.severity] || "var(--emphasis)";
    badge.appendChild(dot);
    badge.appendChild(document.createTextNode(f.severity || "?"));
    const cite = findingCheck(f) || "\\u2014";
    const cells = [badge, f.cluster, cite, f.title || "",
                   f.recommendation || "\\u2014"];
    if (!NOTES) return cells;
    const note = document.createElement("div");
    if (f.verdict) {
      const v = document.createElement("span");
      v.className = "verdict";
      v.textContent = verdictLabel[f.verdict] || f.verdict;
      note.appendChild(v);
    }
    note.appendChild(document.createTextNode(
      f.note || (f.verdict ? "" : "\\u2014")));
    cells.push(note);
    return {cells: cells,
            className: f.verdict === "false_positive" ? "dismissed" : ""};
  }

  // Filter bar: severity, cluster and check are exact-match dropdowns built
  // from the values actually present; "Finding" is a free-text search across a
  // row's title, description, recommendation, cluster and check. The table is
  // re-rendered on every change, and a count states how many of the total are
  // shown so a filtered-down table never looks like the whole fleet.
  const findingsFilters = document.getElementById("findingsFilters");
  function uniqSorted(vals) {
    return Array.from(new Set(vals.filter(Boolean))).sort();
  }
  function makeFilterSelect(labelText, values) {
    const wrap = document.createElement("label");
    wrap.className = "filter";
    const span = document.createElement("span");
    span.textContent = labelText;
    wrap.appendChild(span);
    const sel = document.createElement("select");
    const all = document.createElement("option");
    all.value = "";
    all.textContent = "All";
    sel.appendChild(all);
    values.forEach(function (v) {
      const o = document.createElement("option");
      o.value = v;
      o.textContent = v;
      sel.appendChild(o);
    });
    wrap.appendChild(sel);
    findingsFilters.appendChild(wrap);
    return sel;
  }
  // Severity in rank order (not alphabetical), and only those present.
  const sevPresent = ["CRITICAL", "HIGH", "MEDIUM", "LOW"].filter(
    function (s) { return DATA.findings.some(function (f) {
      return f.severity === s; }); });
  const sevSel = makeFilterSelect("Severity", sevPresent);
  const clusterSel = makeFilterSelect("Cluster",
    uniqSorted(DATA.findings.map(function (f) { return f.cluster; })));
  const checkSel = makeFilterSelect("Check",
    uniqSorted(DATA.findings.map(findingCheck)));
  const textWrap = document.createElement("label");
  textWrap.className = "filter";
  const textSpan = document.createElement("span");
  textSpan.textContent = "Finding";
  textWrap.appendChild(textSpan);
  const textInput = document.createElement("input");
  textInput.type = "search";
  textInput.placeholder = "Search text\\u2026";
  textWrap.appendChild(textInput);
  findingsFilters.appendChild(textWrap);
  const filterCount = document.createElement("span");
  filterCount.className = "filter-count";
  findingsFilters.appendChild(filterCount);

  function applyFindingFilters() {
    const sv = sevSel.value, cl = clusterSel.value, ck = checkSel.value;
    const q = textInput.value.trim().toLowerCase();
    const rows = DATA.findings.filter(function (f) {
      if (sv && f.severity !== sv) return false;
      if (cl && f.cluster !== cl) return false;
      if (ck && findingCheck(f) !== ck) return false;
      if (q) {
        const hay = [f.title, f.description, f.recommendation, f.cluster,
                     findingCheck(f)].filter(Boolean).join(" ").toLowerCase();
        if (hay.indexOf(q) < 0) return false;
      }
      return true;
    });
    table(document.getElementById("findingsTable"), null, findingHeaders,
          rows.map(buildFindingRow));
    filterCount.textContent = rows.length === DATA.findings.length
      ? "" : "Showing " + rows.length + " of " + DATA.findings.length;
  }
  [sevSel, clusterSel, checkSel].forEach(function (s) {
    s.addEventListener("change", applyFindingFilters);
  });
  textInput.addEventListener("input", applyFindingFilters);
  applyFindingFilters();

  // --- per-cluster panels ---------------------------------------------
  //
  // Every cluster in the inventory gets one.
  //
  // Each chart here holds a single series, so it needs no palette slot at all
  // -- which is why the cycling bug cannot come back through this section.
  function drawPanelChart(box, chart, cid) {
    // One line per node in the replication group, each its own palette slot, so
    // a hot shard reads against its peers. A serverless cache has one series;
    // the shared timestamp axis is carried on the chart, not per node.
    const unitSuffix = chart.unit ? " (" + chart.unit.trim() + ")" : "";
    const series = chart.series.map(function (s, i) {
      return {name: s.node, slot: i, values: s.values,
              timestamps: chart.timestamps};
    });
    lineChart(box.querySelector(".chartmount"), series,
              {unit: chart.unit,
               aria: chart.label + " for " + cid + ", " + chart.statistic +
                     ", " + series.length +
                     (series.length === 1 ? " node" : " nodes")});
    // One table column per node, so the numbers behind every line are legible
    // in the table view too.
    const headers = [{label: "Hour"}].concat(chart.series.map(function (s) {
      return {label: s.node + unitSuffix, num: true};
    }));
    table(box.querySelector(".tableview"), null, headers,
          chart.timestamps.map(function (ts, i) {
            return [dateTime(ts)].concat(chart.series.map(function (s) {
              return fmt(s.values[i], 2);
            }));
          }));
  }

  // Charts are drawn when a panel is opened, not up front: charting every
  // metric a cluster reported runs to dozens per panel, and building them all
  // on load costs the reader the first paint for charts nobody has asked to
  // see. Drawn on each open
  // rather than once, so a theme switch that happened while the panel was
  // closed cannot leave stale hues behind.
  function drawPanel(details) {
    const cid = details.getAttribute("data-cluster");
    details.querySelectorAll(".panel-chart").forEach(function (box) {
      drawPanelChart(box, JSON.parse(box.getAttribute("data-chart")), cid);
    });
  }

  function drawOpenPanels() {
    document.querySelectorAll("details.panel[open]").forEach(drawPanel);
  }

  const panelWrap = document.getElementById("panels");
  const PANELS = DATA.panels || [];

  document.getElementById("panelsNote").textContent =
    "One panel per cluster, most in need of attention first \\u2014 every " +
    "cluster in the inventory. Every metric the cluster reported is charted, " +
    "grouped by category with the key metrics first, and each chart overlays " +
    "one line per node so a hot shard shows against its peers. Metrics that " +
    "were flat at zero or never reported are named rather than charted. Panels " +
    "are collapsed to keep the findings reachable; open one to draw its charts.";

  PANELS.forEach(function (p) {
    const d = document.createElement("details");
    d.className = "panel";
    d.setAttribute("data-cluster", p.cluster);

    const sum = document.createElement("summary");
    // The flex gap spaces these visually, but a whitespace-only text node
    // between them is what keeps them separate words to a screen reader and to
    // Ctrl-F -- without it the summary reads "prod-api-cacheCRITICALnode-based".
    // A whitespace-only anonymous flex item is not rendered, so the layout is
    // unaffected.
    const gap = function () { sum.appendChild(document.createTextNode(" ")); };
    const cid = document.createElement("span");
    cid.className = "cid";
    cid.textContent = p.cluster;
    sum.appendChild(cid);
    gap();

    if (p.worst_severity) {
      const badge = document.createElement("span");
      badge.className = "badge";
      const dot = document.createElement("span");
      dot.className = "dot";
      dot.style.background = sevColor[p.worst_severity] || "var(--emphasis)";
      badge.appendChild(dot);
      badge.appendChild(document.createTextNode(p.worst_severity));
      sum.appendChild(badge);
      gap();
    }

    const meta = document.createElement("span");
    meta.className = "meta";
    // Activity and steadiness are orthogonal axes -- show both ("BALANCED
    // \\u00b7 steady") when steadiness was measured; omit it when it was not
    // (an idle cluster has no steadiness to speak of).
    const activity = (p.steadiness && p.steadiness !== "not_measured")
      ? p.classification + " \\u00b7 " + p.steadiness
      : p.classification;
    meta.textContent = [p.type, p.engine, p.node_type, p.region,
                        activity].filter(Boolean).join(" \\u00b7 ");
    sum.appendChild(meta);
    gap();

    const count = document.createElement("span");
    count.className = "count";
    count.textContent = p.finding_count + (p.finding_count === 1 ? " finding"
                                                                : " findings") +
      " \\u00b7 " + p.chart_count +
      (p.chart_count === 1 ? " chart" : " charts");
    sum.appendChild(count);
    d.appendChild(sum);

    const body = document.createElement("div");
    body.className = "panel-body";

    const desc = document.createElement("p");
    desc.className = "desc";
    const bits = [];
    // analyze_metrics.py writes recommendations as unpunctuated fragments
    // ("Immediate scaling needed"), so the sentence that follows would run
    // straight into it.
    if (p.recommendation) {
      bits.push(/[.!?]$/.test(p.recommendation) ? p.recommendation
                                                : p.recommendation + ".");
    }
    desc.textContent = bits.join(" ");
    if (bits.length) body.appendChild(desc);

    // This cluster's Well-Architected verdict (compact gauge + pillar bars),
    // right under the summary so a reader sees the cluster's health before its
    // charts. Rendered on every panel that has a score.
    if (p.scores) {
      const scoreBox = document.createElement("div");
      scoreWidget(scoreBox, p.scores, {compact: true, caption: "Cluster"});
      body.appendChild(scoreBox);
    }

    // Flat-zero and not-reported metrics are named but collapsed. With every
    // collected metric charted these lists run long, and the panel note already
    // explains the convention -- so the count stays visible and the names are
    // one click away. Absent and zero stay separate: a metric that never
    // arrived is a monitoring gap, not a measurement of zero.
    function absenceDetails(summaryText, labels) {
      const det = document.createElement("details");
      det.className = "metric-absence";
      const sm = document.createElement("summary");
      sm.textContent = summaryText;
      det.appendChild(sm);
      const list = document.createElement("p");
      list.textContent = labels.join(", ") + ".";
      det.appendChild(list);
      body.appendChild(det);
    }
    if (p.flat_zero && p.flat_zero.length) {
      absenceDetails(
        p.flat_zero.length + (p.flat_zero.length === 1 ? " metric was" :
          " metrics were") + " flat at zero for the whole window (measured, " +
          "not charted)", p.flat_zero);
    }
    if (p.not_reported && p.not_reported.length) {
      absenceDetails(
        p.not_reported.length + (p.not_reported.length === 1 ? " metric" :
          " metrics") + " not reported by this cluster (no datapoint arrived " +
          "\\u2014 not a measurement of zero)", p.not_reported);
    }

    // Read/write mix. Only for clusters that reported metrics -- a cluster with
    // no metrics at all is covered by the notice below. "not_measured" is stated
    // plainly (no command traffic), never rendered as a default 50/50 split.
    if (p.metrics_reported) {
      const rw = p.read_write || {};
      const rwLine = document.createElement("p");
      rwLine.className = "desc";
      if (rw.class && rw.class !== "not_measured") {
        const nice = {"read-heavy": "Read-heavy", "write-heavy": "Write-heavy",
                      "balanced": "Balanced"}[rw.class] || rw.class;
        rwLine.textContent = "Read/write mix: " + nice + " \\u2014 " +
          fmt(rw.read_pct, 0) + "% reads / " + fmt(rw.write_pct, 0) + "% writes.";
      } else {
        rwLine.textContent = "Read/write mix: not measured \\u2014 no command " +
          "traffic served in the window.";
      }
      body.appendChild(rwLine);
    }

    if (!p.metrics_reported) {
      absent(body, "No metrics collected",
             p.cluster + " is in the inventory but returned no CloudWatch " +
             "datapoints for this window. Nothing here is a statement about " +
             "the cluster's health -- it is a gap in what was collected.");
    } else if (!p.chart_count) {
      absent(body, "Every metric flat at zero",
             p.cluster + " reported datapoints, but every metric above sat at " +
             "zero for the whole window. That is a measurement, not a gap.");
    } else {
      // One toggle for the whole panel rather than one per chart: dozens of
      // buttons inside one panel is dozens of decisions to read the same data
      // as numbers. It lives in the body, not the summary -- a button in a
      // <summary> is still a click on the summary, so it would collapse the
      // panel.
      const head = document.createElement("div");
      head.className = "panel-toggle";
      const btn = document.createElement("button");
      btn.className = "toggle";
      btn.type = "button";
      btn.textContent = "Table view";
      btn.addEventListener("click", function () {
        const show = d.querySelector(".panel-chart .tableview")
                      .hasAttribute("hidden");
        d.querySelectorAll(".panel-chart").forEach(function (box) {
          const tbl = box.querySelector(".tableview");
          const mount = box.querySelector(".chartmount");
          if (show) {
            tbl.removeAttribute("hidden");
            mount.setAttribute("hidden", "");
          } else {
            tbl.setAttribute("hidden", "");
            mount.removeAttribute("hidden");
          }
        });
        btn.textContent = show ? "Chart view" : "Table view";
      });
      head.appendChild(btn);
      body.appendChild(head);

      // Charts grouped by category, key metrics first. Each group gets a
      // heading, then its own grid; every chart box carries its own data so the
      // open handler and the theme redraw read the same source.
      p.groups.forEach(function (group) {
        const gt = document.createElement("h4");
        gt.className = "panel-group-title";
        gt.textContent = group.name;
        body.appendChild(gt);

        const grid = document.createElement("div");
        grid.className = "panel-grid" +
          (group.name === "Key metrics" ? " panel-grid--wide" : "");
        group.charts.forEach(function (chart) {
          const box = document.createElement("div");
          box.className = "panel-chart";
          box.setAttribute("data-chart", JSON.stringify(chart));
          const h = document.createElement("h4");
          const name = document.createElement("span");
          name.textContent = chart.label + (chart.unit ? " (" +
                              chart.unit.trim() + ")" : "");
          h.appendChild(name);
          // The statistic is part of the number's identity: a Maximum and an
          // Average of the same metric are different facts, and a reader
          // checking this against CloudWatch needs to know which one to ask for.
          const stat = document.createElement("span");
          stat.className = "stat";
          stat.textContent = chart.metric + " \\u00b7 " + chart.statistic;
          h.appendChild(stat);
          box.appendChild(h);
          const mount = document.createElement("div");
          mount.className = "chartmount";
          box.appendChild(mount);
          const tbl = document.createElement("div");
          tbl.className = "tableview scroll";
          tbl.setAttribute("hidden", "");
          box.appendChild(tbl);
          grid.appendChild(box);
        });
        body.appendChild(grid);
      });
    }

    d.appendChild(body);
    panelWrap.appendChild(d);

    d.addEventListener("toggle", function () {
      // Redrawn on every open, not only the first: a theme switch while the
      // panel was closed leaves the cached SVG in the other theme's hues.
      if (d.open) drawPanel(d);
    });
  });

  // Printing a collapsed panel would print a summary line with no data. CSS
  // cannot open a <details>, so this does, and draws the charts that were never
  // built.
  window.addEventListener("beforeprint", function () {
    document.querySelectorAll("details.panel").forEach(function (d) {
      if (!d.open) { d.open = true; drawPanel(d); }
    });
  });

  // Configuration checks & coverage
  //
  // This section exists to answer "what was NOT checked", which no other
  // section can. A findings table lists failures, so silence in it is
  // ambiguous: the check passed, or the check never ran. Absent Stage 3.5
  // output is stated as absent rather than rendered as full coverage.
  const CFG = DATA.config;
  const coverage = document.getElementById("coverage");
  if (!CFG) {
    document.getElementById("coverageNote").textContent =
      "Not available for this run.";
    const p = document.createElement("p");
    p.className = "desc";
    p.textContent =
      "No configuration findings file was supplied, so no configuration check " +
      "ran for this report. The security and reliability settings in the " +
      "inventory table above are reported as collected, but nothing graded " +
      "them. Treat this as absent coverage, not as a clean result.";
    coverage.appendChild(p);
  } else {
    document.getElementById("coverageNote").textContent =
      "All " + CFG.checks_in_registry + " checks in the registry were run " +
      "against each of " + CFG.clusters_checked + " clusters" +
      (CFG.review_date ? ", graded as of " + CFG.review_date : "") +
      ". A check listed below as not applicable produced no verdict for those " +
      "clusters, so its absence from the findings table is not a pass.";

    const ran = document.createElement("p");
    ran.className = "desc";
    ran.textContent = "Evaluated: " + CFG.check_ids.join(", ") + ".";
    coverage.appendChild(ran);

    // The policy is a per-customer input, so a finding graded against it is
    // only checkable if the report says which values were used.
    const pol = CFG.policy || {};
    const polBits = [];
    if (pol.required_tags && pol.required_tags.length) {
      polBits.push("required tags " + pol.required_tags.join(", "));
    }
    if (pol.min_replicas_per_shard !== undefined &&
        pol.min_replicas_per_shard !== null) {
      polBits.push("at least " + pol.min_replicas_per_shard +
                   " replicas per shard");
    }
    if (pol.min_snapshot_retention_days !== undefined &&
        pol.min_snapshot_retention_days !== null) {
      polBits.push("at least " + pol.min_snapshot_retention_days +
                   " days of snapshot retention");
    }
    if (polBits.length) {
      const p = document.createElement("p");
      p.className = "desc";
      p.textContent = "Graded against: " + polBits.join("; ") +
        ". These are Well-Architected defaults unless this review was told " +
        "otherwise \\u2014 a finding against a standard you do not use is a " +
        "finding to override, not to action.";
      coverage.appendChild(p);
    }

    if (CFG.skipped.length) {
      const p = document.createElement("p");
      p.className = "desc";
      p.textContent = "Not applicable:";
      coverage.appendChild(p);
      const ul = document.createElement("ul");
      ul.className = "tight";
      CFG.skipped.forEach(function (s) {
        const li = document.createElement("li");
        li.textContent = s.check_id + " \\u2014 " + s.reason + " (" +
          s.clusters.length + " cluster" +
          (s.clusters.length === 1 ? "" : "s") + ": " +
          s.clusters.join(", ") + ")";
        ul.appendChild(li);
      });
      coverage.appendChild(ul);
    }

    // Normally empty. A cluster missing from Stage 3.5 has no configuration
    // verdict at all, and nothing else in the report would reveal it -- the
    // findings table would simply have no rows for it, which reads as clean.
    if (CFG.unchecked_clusters.length) {
      const p = document.createElement("p");
      p.className = "desc";
      p.textContent = "Not checked at all: " +
        CFG.unchecked_clusters.join(", ") + ". " +
        (CFG.unchecked_clusters.length === 1 ? "This cluster is" : "These are") +
        " in the inventory but absent from the configuration stage, so no " +
        "security or reliability setting on " +
        (CFG.unchecked_clusters.length === 1 ? "it" : "them") +
        " was graded. Findings above cover the checked clusters only.";
      coverage.appendChild(p);
    }
  }

  // Check glossary
  //
  // Every check id in the registry, with its plain-English meaning and pillar,
  // so a reader who meets "SEC-03" in a finding row -- or wonders about a check
  // that passed and left no row -- can look it up without console access. Built
  // from check_configuration.CHECKS in build_payload, so it stays in step with
  // what the checker actually evaluates. Rendered regardless of whether Stage
  // 3.5 ran: the meaning of a check id does not depend on this run's coverage.
  const glossary = document.getElementById("checkGlossary");
  const GLOSS = DATA.check_glossary || [];
  const gnote = document.createElement("p");
  gnote.className = "desc";
  gnote.textContent = "What each check id means. A check that passed leaves no " +
    "finding row above, so this is the only place its id is explained.";
  glossary.appendChild(gnote);
  table(glossary, "Configuration check glossary",
        [{label: "Check"}, {label: "What it checks"}, {label: "Pillar"}],
        GLOSS.map(function (g) { return [g.check_id, g.title, g.pillar]; }));

  // AI-generated assessment — the lead of "What the data shows" when a review
  // pass ran. When none did, the slot renders an explicit "no AI review" note
  // rather than vanishing: the section is present every run, and the derived
  // facts below are never mistaken for the whole reading. Saying "no review ran"
  // is a fact; an empty heading would be the different, false claim that the AI
  // reviewed the fleet and had nothing to say.
  const assessmentWrap = document.getElementById("assessment");
  if (NOTES && (NOTES.assessment || NOTES.environment)) {
    const wrap = assessmentWrap;
    const box = document.createElement("div");
    box.className = "analyst";
    const attrib = document.createElement("p");
    attrib.className = "attrib";
    // Names both what wrote it and what constrains it. Every figure in this
    // prose was checked against the data it cites before the report rendered,
    // and that is the reason a reader can weigh it at all.
    attrib.textContent =
      "AI-generated assessment \\u2014 written by the reviewing AI agent from " +
      "the data below. Every figure in it was checked against the cited source " +
      "before this report was generated.";
    box.appendChild(attrib);
    if (NOTES.environment) {
      const env = document.createElement("p");
      // Where the environment claim came from matters: "the user told us this
      // is non-production" and "we inferred it from a tag" carry different
      // weight, and every recommendation below leans on it.
      env.textContent = "Environment: " + NOTES.environment +
        (NOTES.environment_source
          ? " (established from: " + NOTES.environment_source + ")"
          : " (source not stated)");
      box.appendChild(env);
    }
    if (NOTES.assessment) {
      // Split on blank lines so the agent can write more than one paragraph.
      // textContent per paragraph, never innerHTML: this is the one string in
      // the document a language model wrote.
      NOTES.assessment.split(/\\n\\s*\\n/).forEach(function (para) {
        if (!para.trim()) return;
        const p = document.createElement("p");
        p.textContent = para.trim();
        box.appendChild(p);
      });
    }
    wrap.appendChild(box);
  } else {
    const note = document.createElement("p");
    note.className = "section-note";
    note.textContent = "No AI review was generated for this run \\u2014 what " +
      "follows is the mechanical analysis only. Re-run the review with agent " +
      "notes (--notes) to lead this section with the AI reading of the fleet.";
    assessmentWrap.appendChild(note);
  }

  // AI-generated priorities — the AI agent's ranking, above the derived actions
  // and marked as a different kind of claim. The derived list below says what
  // the data implies; this says what to do first, which is a judgement the data
  // alone cannot make.
  if (NOTES && NOTES.priorities.length) {
    const box = document.createElement("div");
    box.className = "analyst";
    const attrib = document.createElement("p");
    attrib.className = "attrib";
    attrib.textContent = "AI-generated priorities \\u2014 the reviewing AI " +
      "agent's ranking. The list below it is derived from the data by script.";
    box.appendChild(attrib);
    const ol = document.createElement("ol");
    NOTES.priorities.forEach(function (p) {
      const li = document.createElement("li");
      const strong = document.createElement("strong");
      strong.textContent = p.action || "";
      li.appendChild(strong);
      if (p.why) li.appendChild(document.createTextNode(" \\u2014 " + p.why));
      ol.appendChild(li);
    });
    box.appendChild(ol);
    document.getElementById("priorities").appendChild(box);
  }

  // Actions — derived from each cluster's classification and findings, so the
  // prose cannot drift from the data. Nothing here asserts a fleet-wide condition
  // (e.g. "all clusters are idle"): a decommission line names only the IDLE
  // clusters, a scale-up line only the SATURATED ones, and a compliant busy fleet
  // gets only its real config actions.
  const idleClusters = DATA.clusters.filter(function (c) {
    return c.classification === "IDLE";
  });
  const saturated = DATA.clusters.filter(function (c) {
    return c.classification === "SATURATED";
  });
  const idleRecoverable = DATA.savings ? DATA.savings.idle_recoverable : null;
  const noFailover = DATA.clusters.filter(function (c) {
    return c.nodes > 1 && !c.failover;
  });
  const noBackup = DATA.clusters.filter(function (c) { return !c.backups; });
  const untagged = DATA.clusters.filter(function (c) {
    const keys = Object.keys(c.tags || {}).map(function (k) {
      return k.toLowerCase();
    });
    return !keys.some(function (k) {
      return k === "owner" || k === "environment" || k === "application";
    });
  });

  const actions = [];
  if (idleClusters.length) {
    const idleWithData = idleClusters.filter(function (c) {
      return (c.items_peak || 0) > 0;
    });
    actions.push({h: actions.length + 1 + ". Decommission the idle cluster" +
      (idleClusters.length > 1 ? "s" : ""),
      p: idleClusters.length + " of " + M.cluster_count + " cluster" +
         (idleClusters.length > 1 ? "s are" : " is") + " classified IDLE: " +
         idleClusters.map(function (c) { return c.cluster; }).join(", ") +
         ". Snapshot first" +
         (idleWithData.length
           ? " \\u2014 " + idleWithData.map(function (c) {
               return c.cluster + " holds " + fmt(c.items_peak, 0) + " real items" +
                      (c.backups ? " on " + c.backups + "-day retention" : "");
             }).join("; ") + "."
           : ".") +
         // Idle-scoped recoverable spend from pricing (7c) when present; never the
         // whole-fleet bill, which would overstate on a partly-idle fleet.
         (idleRecoverable !== null && idleRecoverable !== undefined
           ? " Recovering " + (idleClusters.length > 1 ? "them" : "it") +
             " frees about " + money(idleRecoverable) + " a month."
           : (M.cost_collected ? ""
               : " The saving is not quantified here: no Cost Explorer data was " +
                 "collected for this run."))});
  }
  if (saturated.length) {
    actions.push({h: actions.length + 1 + ". Relieve the saturated cluster" +
      (saturated.length > 1 ? "s" : ""),
      p: saturated.map(function (c) { return c.cluster; }).join(", ") + " " +
         (saturated.length > 1 ? "are" : "is") + " SATURATED. Scale the node type " +
         "up or add a shard before the next traffic peak, not after."});
  }
  if (F.ttl_risk) {
    actions.push({h: actions.length + 1 + ". If " + F.ttl_risk.cluster + " stays, fix the eviction policy",
      p: "Set TTLs on its keys or move maxmemory-policy from " + F.ttl_risk.policy +
         " to allkeys-lru. Left as it is, writes fail once memory fills."});
  }
  if (noFailover.length || noBackup.length) {
    const parts = [];
    if (noFailover.length) {
      parts.push(noFailover.map(function (c) { return c.cluster; }).join(", ") +
        " " + (noFailover.length > 1 ? "have" : "has") +
        " automatic failover disabled, so the replica will not promote");
    }
    if (noBackup.length) {
      parts.push(noBackup.map(function (c) { return c.cluster; }).join(", ") +
        " " + (noBackup.length > 1 ? "have" : "has") + " no backups at all");
    }
    actions.push({h: actions.length + 1 +
      ". If any cluster stays, close the reliability gaps",
      p: parts.join("; ") + "."});
  }
  if (untagged.length) {
    actions.push({h: actions.length + 1 + ". Tag whatever survives",
      p: untagged.length + " of " + M.cluster_count + " clusters carry no Owner, " +
         "Environment or Application tag. Without an owner, unused infrastructure " +
         "has nobody to retire it."});
  }
  // A compliant, busy fleet legitimately has no fleet-level action. Say so rather
  // than leaving an empty section, which would read as an omission.
  if (!actions.length) {
    actions.push({h: "No fleet-level actions",
      p: "No cluster is idle or saturated, and no cross-cutting reliability or " +
         "tagging gap was found. Anything to act on is in the per-cluster findings " +
         "and the configuration checks above."});
  }
  const actWrap = document.getElementById("actions");
  actions.forEach(function (a) {
    const d = document.createElement("div");
    d.className = "callout info";
    const h = document.createElement("h4"); h.textContent = a.h;
    const p = document.createElement("p"); p.textContent = a.p;
    d.appendChild(h); d.appendChild(p);
    actWrap.appendChild(d);
  });

  // Method
  const method = document.getElementById("method");
  const mp = document.createElement("p");
  mp.className = "desc";
  mp.textContent =
    "Inventory from the ElastiCache API; metrics from CloudWatch GetMetricData at " +
    "5-minute resolution over the window shown" +
    (M.cost_collected ? "; charges from Cost Explorer. "
                      : ". No Cost Explorer data was collected. ") +
    "Charts plot hourly peaks (max within each hour) so the reduction from " +
    Number(M.datapoints).toLocaleString("en-US") + " raw datapoints cannot hide a spike. " +
    // The Method section is where a reader decides how much of the report to
    // trust, so which stages ran belongs here and not only in the coverage
    // section further up.
    (DATA.config
      ? "Configuration findings are deterministic checks against the API's own " +
        "description of each cluster \\u2014 no metric is involved, so they hold " +
        "regardless of the observation window."
      : "No configuration check ran for this report, so every finding below rests " +
        "on metrics alone; security and reliability settings were collected but " +
        "not graded.") +
    // Whether an AI judgement layer exists at all is a Method fact: it changes
    // how a reader should read a finding nobody struck through.
    (NOTES
      ? " An AI review pass then reviewed the findings; its prose is boxed and " +
        "attributed as AI-generated wherever it appears, and no figure in it " +
        "survived unless it matched the source it cites."
      : " No AI review pass reviewed these findings: every row is the raw output " +
        "of a deterministic check, unfiltered for false positives.");
  method.appendChild(mp);
  const caveats = document.createElement("ul");
  caveats.className = "tight";
  [
    // The Method section is where a reader checks whether to trust the rest, so
    // the absence of cost data is stated here as plainly as its presence.
    M.cost_collected
      ? "Cost figures are actual Cost Explorer charges for this account, not list-price estimates. " +
        "Savings projections assume the resource is removed outright."
      : "No cost figures appear in this report: Cost Explorer was not queried for this run " +
        "(--skip-cost, or the ce:GetCostAndUsage call did not succeed). Absent spend data is " +
        "shown as \\u201cnot collected\\u201d rather than as $0.",
    "Serverless caches expose no CPU or per-node metric, so they are absent from the CPU chart.",
    "This review is read-only. It scanned only the regions listed above."
  ].forEach(function (text) {
    const li = document.createElement("li");
    li.textContent = text;
    caveats.appendChild(li);
  });
  method.appendChild(caveats);

  // Review-coverage ledger — a Method fact, not a finding: it records that the
  // AI review accounted for every systemic (fleet-wide) finding, since the
  // render fails otherwise. Demoted here (collapsed) rather than sitting beside
  // the prioritized actions, where it read as process noise competing with real
  // findings; a reader auditing completeness opens it, everyone else skims past.
  if (NOTES && NOTES.coverage && NOTES.coverage.length) {
    const det = document.createElement("details");
    det.className = "coverage-ledger";
    const sum = document.createElement("summary");
    sum.textContent = "Review coverage \\u2014 " + NOTES.coverage.length +
      " fleet-wide findings, each accounted for in the AI review";
    det.appendChild(sum);
    const ul = document.createElement("ul");
    ul.className = "tight";
    NOTES.coverage.forEach(function (c) {
      const li = document.createElement("li");
      const mark = document.createElement("strong");
      mark.textContent = c.addressed ? "\\u2713 " : "\\u2717 ";
      li.appendChild(mark);
      const parts = [c.label];
      if (c.severity) parts.push(c.severity);
      parts.push("on " + c.cluster_count + " clusters");
      if (c.verdict) parts.push("verdict: " + c.verdict);
      li.appendChild(document.createTextNode(parts.join(" \\u00b7 ")));
      ul.appendChild(li);
    });
    det.appendChild(ul);
    method.appendChild(det);
  }

  document.getElementById("footer").textContent =
    "Generated " + M.generated + " by the elasticache-operations-review skill \\u00b7 " +
    "Pricing reference: https://aws.amazon.com/elasticache/pricing/";

  // --- table-view toggles ---------------------------------------------
  document.querySelectorAll("button[data-toggle]").forEach(function (btn) {
    btn.addEventListener("click", function () {
      const key = btn.getAttribute("data-toggle");
      const tbl = document.getElementById(key + "Table");
      const chart = document.getElementById(key + "Chart");
      const legendEl = document.getElementById(key + "Legend");
      const showTable = tbl.hasAttribute("hidden");
      if (showTable) {
        tbl.removeAttribute("hidden");
        chart.setAttribute("hidden", "");
        if (legendEl) legendEl.setAttribute("hidden", "");
        btn.textContent = "Chart view";
      } else {
        tbl.setAttribute("hidden", "");
        chart.removeAttribute("hidden");
        if (legendEl) legendEl.removeAttribute("hidden");
        btn.textContent = "Table view";
      }
    });
  });

  // --- theme toggle ----------------------------------------------------
  const themeBtn = document.getElementById("themeBtn");
  function prefersDark() {
    return window.matchMedia("(prefers-color-scheme: dark)").matches;
  }
  function currentlyDark() {
    const stamp = document.documentElement.getAttribute("data-theme");
    return stamp ? stamp === "dark" : prefersDark();
  }
  function redraw() {
    themeBtn.textContent = currentlyDark() ? "Light mode" : "Dark mode";
    // Only the panels a reader has opened; the rest are redrawn by the toggle
    // handler when they are opened, so no closed panel keeps the old hues.
    drawOpenPanels();
  }
  themeBtn.addEventListener("click", function () {
    document.documentElement.setAttribute("data-theme",
      currentlyDark() ? "light" : "dark");
    redraw();
  });
  window.matchMedia("(prefers-color-scheme: dark)")
        .addEventListener("change", function () {
    if (!document.documentElement.getAttribute("data-theme")) redraw();
  });
  themeBtn.textContent = currentlyDark() ? "Light mode" : "Dark mode";
})();
</script>
</body>
</html>
"""


def render(payload: dict) -> str:
    """Substitute the payload and account label into the HTML template.

    Args:
        payload: Output of build_payload.

    Returns:
        The complete HTML document as a string.
    """
    # </script> inside the JSON would close the host <script> tag early.
    data_json = json.dumps(payload, separators=(",", ":")).replace("</", "<\\/")
    account = html.escape(str(payload["meta"].get("account") or "unknown"))
    return TEMPLATE.replace("__DATA__", data_json).replace("__ACCOUNT__", account)


def _emit_report_data(metrics_path: str, out_path: str) -> int:
    """Reduce metrics.json to report_data.json and return an exit code.

    The Phase 9a reduction step. Reads the raw metrics file (the only place in
    the ordinary pipeline that still does), writes the small report-facing file
    deterministically so two runs over the same metrics are byte-identical, and
    returns 0 on success.
    """
    if not os.path.exists(metrics_path):
        logger.error("metrics file not found: %s", metrics_path)
        return 1
    try:
        with open(metrics_path, "r", encoding="utf-8") as handle:
            metrics = json.load(handle)
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Cannot read metrics (%s): %s", metrics_path, exc)
        return 1

    # Under the sharded format ``metrics`` is the small manifest and the per-
    # cluster series stream in one shard at a time; a legacy single file is read
    # whole (unchanged). Either way the reduction is byte-identical.
    data = build_report_data(metrics, metrics_path)

    out_dir = os.path.dirname(os.path.abspath(out_path))
    os.makedirs(out_dir, exist_ok=True)
    tmp = out_path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as handle:
            # No sort_keys on purpose: the render serialises the payload in dict
            # insertion order, so the file must round-trip in that same order or
            # a render from report_data.json would differ byte-for-byte from one
            # off the raw metrics (the chart dict keys reorder). build_report_data
            # builds in a fixed order, so this is still byte-stable across runs.
            json.dump(data, handle, indent=2)
            handle.write("\n")
        os.replace(tmp, out_path)
    except OSError as exc:
        logger.error("Cannot write report data: %s", exc)
        if os.path.exists(tmp):
            os.unlink(tmp)
        return 1

    size_kb = os.path.getsize(out_path) / 1024
    logger.info("Report data written: %s (%.0f KB, %d clusters) from %s",
                out_path, size_kb, len(data.get("clusters") or {}),
                metrics_path)
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Args:
        argv: Argument vector, defaults to sys.argv[1:].

    Returns:
        Exit code: 0 on success, 1 on failure.
    """
    parser = argparse.ArgumentParser(
        description="Render an HTML report from the review's JSON outputs."
    )
    parser.add_argument("--inventory", default="output/inventory.json")
    parser.add_argument(
        "--metrics",
        default="output/metrics.json",
        help=("Raw metrics.json. Only read when --report-data is absent -- with "
              "report_data present the render never opens this file (Phase "
              "9a)."),
    )
    parser.add_argument("--analysis", default="output/analysis.json")
    parser.add_argument(
        "--report-data",
        help=("Report data (Phase 9a): the per-cluster chart bundles plus the "
              f"cost and metadata sections, default {DEFAULT_REPORT_DATA} if "
              "that file exists. When present the render reads its charts, cost "
              "and metadata from here and NEVER opens metrics.json. Optional: "
              "with no file it falls back to reducing --metrics. A path given "
              "explicitly must exist."),
    )
    parser.add_argument(
        "--emit-report-data",
        metavar="PATH",
        help=("Reduce --metrics to a report_data.json at PATH and exit without "
              "rendering. This is the Phase 9a reduction step: run_review.py "
              "calls it after Stage 3 so the render can decouple from the "
              "multi-GB metrics.json."),
    )
    parser.add_argument(
        "--config-findings",
        help=("Stage 3.5 configuration findings, default "
              f"{DEFAULT_CONFIG_FINDINGS} if that file exists. Optional: with "
              "no file the report renders from the metric stages alone and "
              "states that no configuration check ran. A path given explicitly "
              "must exist -- silently skipping one that was asked for would "
              "read as full coverage."),
    )
    parser.add_argument(
        "--notes",
        help=("The agent's AI review (assessment, per-finding verdicts, "
              f"priorities) as JSON. Defaults to {DEFAULT_NOTES} if that file "
              "exists — the skill's Step 4 writes it, so an agent-run report "
              "always includes it. With no file the report renders from the "
              "scripts' output alone and says that no AI review pass ran. Every "
              "figure in the prose must be traceable to a value under a path "
              "the note cites, or rendering fails naming the figure."),
    )
    parser.add_argument(
        "--pricing",
        help=("Priced savings levers as JSON (Phase 7c), default "
              f"{DEFAULT_PRICING} if that file exists. Optional: with no file "
              "the report renders without the savings KPIs or per-cluster cost "
              "options. A path given explicitly must exist. Live rates never "
              "enter the pipeline -- this file is written by the agent from "
              "price_calculator.py and carries its own provenance."),
    )
    parser.add_argument("--output", default="output/report.html")
    parser.add_argument(
        "--generated-at",
        help=("Timestamp for the footer (e.g. '2026-08-11 09:57 UTC'). "
              "Set this to make the output byte-reproducible for the same "
              "inputs; defaults to the current UTC time."),
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    # Phase 9a emit mode: reduce metrics.json to report_data.json and exit. This
    # is the only mode that reads the raw metrics file in the ordinary pipeline;
    # the render below does not.
    if args.emit_report_data:
        return _emit_report_data(args.metrics, args.emit_report_data)

    loaded = {}
    for label, path in (("inventory", args.inventory),
                        ("analysis", args.analysis)):
        if not os.path.exists(path):
            logger.error("%s file not found: %s", label, path)
            return 1
        try:
            with open(path, "r", encoding="utf-8") as handle:
                loaded[label] = json.load(handle)
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Cannot read %s (%s): %s", label, path, exc)
            return 1

    # Phase 9a: prefer report_data.json and never open metrics.json when it is
    # present. Optional by default, required when named -- same rule as
    # --config-findings and --pricing.
    report_data = None
    report_data_path = args.report_data or DEFAULT_REPORT_DATA
    if os.path.exists(report_data_path):
        try:
            with open(report_data_path, "r", encoding="utf-8") as handle:
                report_data = json.load(handle)
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Cannot read report data (%s): %s",
                         report_data_path, exc)
            return 1
        logger.info("Rendering from %s; metrics.json will not be opened.",
                    report_data_path)
    elif args.report_data:
        logger.error("report data file not found: %s", report_data_path)
        return 1

    # Metrics are the fallback source only. With report_data in hand the raw
    # file is never touched -- that decoupling is the whole point of Phase 9a.
    metrics = None
    if report_data is None:
        if not os.path.exists(args.metrics):
            logger.error(
                "Neither report data (%s) nor metrics (%s) found; one is "
                "required to render.", report_data_path, args.metrics)
            return 1
        try:
            with open(args.metrics, "r", encoding="utf-8") as handle:
                metrics = json.load(handle)
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Cannot read metrics (%s): %s", args.metrics, exc)
            return 1

    # Optional by default, required when named. Stages 1-3 are the floor: the
    # report must render without Stage 3.5. But a --config-findings path that
    # does not exist is a typo, and warning-and-continuing there would produce
    # a report whose coverage section says "not available" for a run where the
    # checks did in fact happen.
    config_findings = None
    config_path = args.config_findings or DEFAULT_CONFIG_FINDINGS
    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as handle:
                config_findings = json.load(handle)
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Cannot read config findings (%s): %s",
                         config_path, exc)
            return 1
    elif args.config_findings:
        logger.error("config findings file not found: %s", config_path)
        return 1
    else:
        logger.info("No configuration findings at %s; the report will state "
                    "that no configuration check ran.", config_path)

    # The AI review, picked up automatically from output/notes.json (or a path
    # given explicitly). The skill is agent-run and SKILL.md Step 4 requires the
    # agent to write this before rendering, so every agent-run report carries it;
    # a bare-scripts run with no agent has no notes and the report states that no
    # AI review ran. Staleness is not silent: verify_notes rejects a note whose
    # cited figures no longer match the data, so an out-of-date notes.json fails
    # the render rather than attaching last week's judgement to this week's data.
    notes = None
    notes_path = args.notes or DEFAULT_NOTES
    if os.path.exists(notes_path):
        try:
            with open(notes_path, "r", encoding="utf-8") as handle:
                notes = json.load(handle)
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Cannot read notes (%s): %s", notes_path, exc)
            return 1
    elif args.notes:
        logger.error("notes file not found: %s", notes_path)
        return 1
    else:
        logger.info("No AI review at %s; the report will state that no AI "
                    "review pass ran. The skill's Step 4 writes this — a report "
                    "without it means the agent step was skipped.", notes_path)

    # Optional by default, required when named -- same rule as --config-findings.
    # Without pricing the report simply carries no savings KPIs or cost blocks;
    # a named path that does not exist is a typo, not a silent omission.
    pricing = None
    pricing_path = args.pricing or DEFAULT_PRICING
    if os.path.exists(pricing_path):
        try:
            with open(pricing_path, "r", encoding="utf-8") as handle:
                pricing = json.load(handle)
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Cannot read pricing (%s): %s", pricing_path, exc)
            return 1
    elif args.pricing:
        logger.error("pricing file not found: %s", pricing_path)
        return 1
    else:
        logger.info("No pricing at %s; the report will omit the savings KPIs "
                    "and per-cluster cost options.", pricing_path)

    try:
        payload = build_payload(loaded["inventory"], metrics,
                                loaded["analysis"], args.generated_at,
                                config_findings, notes, pricing,
                                report_data=report_data,
                                metrics_path=args.metrics)
    except NotesError as exc:
        # Fail the render rather than dropping the notes. A report that
        # silently omitted the analyst sections would look like a run where no
        # analyst pass happened, and the reason would exist only in a log.
        logger.error("%s", exc)
        logger.error("No report was written. Fix the notes or omit --notes.")
        return 1
    document = render(payload)

    out_dir = os.path.dirname(os.path.abspath(args.output))
    os.makedirs(out_dir, exist_ok=True)
    tmp = args.output + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as handle:
            handle.write(document)
        os.replace(tmp, args.output)
    except OSError as exc:
        logger.error("Cannot write report: %s", exc)
        return 1

    size_kb = os.path.getsize(args.output) / 1024
    logger.info("Report written: %s (%.0f KB, %d clusters, %d findings)",
                args.output, size_kb, len(payload["clusters"]),
                len(payload["findings"]))
    if payload["notes"]:
        logger.info("Analyst notes included: %d of %d findings annotated.",
                    payload["notes"]["annotated"],
                    payload["notes"]["findings_total"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
