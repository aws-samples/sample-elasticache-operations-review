"""Pins references/metrics-catalog.md against what fetch_metrics.py collects.

The catalog is the agent's knowledge base: it is what an agent consults to say
"NewConnections is a Sum over 300 seconds" when interpreting a number. When the
table disagrees with the collector, the agent reasons correctly from a false
premise, which produces a confident wrong answer rather than an error.

That had happened to eight rows. The serverless table claimed a 60-second period
for `ThrottledCmds`, `NewConnections`, `CurrConnections`, both latency metrics and
`ElastiCacheProcessingUnits`, and the node-based table claimed it for the two
latency metrics — while `MetricDefinition.period` is 300 for every one of them.
The only 60-second data the pipeline collects is the separate 24-hour latency
percentile window, which is a different query with its own metadata key.

Nothing caught it because nothing was looking: the catalog is prose, the
collector is code, and a unit test of either passes while they contradict each
other. Same defect class as `tests/test_wa_mapping_accuracy.py` — the test parses
the document rather than restating it, so there is one place to update.
"""
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from fetch_metrics import MetricDefinitionRegistry  # noqa: E402

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CATALOG = os.path.join(REPO_ROOT, "references", "metrics-catalog.md")

# The catalog documents each metric twice for metrics that apply to both cluster
# types -- once in the node-based table, once in the serverless one. Both rows
# describe the same collection, so both must match.
ROW = re.compile(
    r"^\|\s*(?P<name>[A-Za-z][A-Za-z0-9]*)\s*"
    r"\|\s*(?P<unit>[^|]+?)\s*"
    r"\|\s*(?P<statistic>[^|]+?)\s*"
    r"\|\s*(?P<period>\d+)s\s*"
    r"\|(?P<significance>.*)\|\s*$",
    re.M,
)


def _catalog_text():
    with open(CATALOG, encoding="utf-8") as handle:
        return handle.read()


def _documented_rows():
    """Every metric row in the catalog, as (name, statistics, period) tuples.

    A metric can appear more than once, so this returns a list rather than a
    dict -- collapsing duplicates would hide exactly the case where the two
    tables disagree with each other.
    """
    rows = []
    for match in ROW.finditer(_catalog_text()):
        statistics = [s.strip() for s in match.group("statistic").split(",")]
        rows.append((
            match.group("name"),
            statistics,
            int(match.group("period")),
        ))
    return rows


def _collected():
    """{metric_name: MetricDefinition} from the collector's own registry."""
    return {m.name: m for m in MetricDefinitionRegistry()._metrics}


def test_the_catalog_parses_at_all():
    """Guards the premise: a regex that matches nothing makes every test pass."""
    rows = _documented_rows()
    assert len(rows) > 40, (
        f"only {len(rows)} metric rows parsed out of metrics-catalog.md. Either "
        "the table format changed or ROW no longer matches it -- in which case "
        "every assertion in this file is silently vacuous."
    )


@pytest.mark.parametrize("row", _documented_rows(), ids=lambda r: f"{r[0]}-{r[2]}s")
def test_documented_period_matches_the_collector(row):
    name, _statistics, period = row
    collected = _collected().get(name)
    if collected is None:
        pytest.skip(f"{name} is documented but not collected (aspirational row)")

    assert period == collected.period, (
        f"metrics-catalog.md documents {name} at {period}s; "
        f"fetch_metrics.py collects it at {collected.period}s. An agent reading "
        "the catalog will convert a per-period total by the wrong factor."
    )


@pytest.mark.parametrize("row", _documented_rows(), ids=lambda r: f"{r[0]}-{r[2]}s")
def test_documented_statistic_matches_the_collector(row):
    """The statistic is half of what makes a number meaningful.

    A Sum read as an Average is the same class of defect as a per-period total
    read as a rate, and four of this pipeline's bugs were one or the other.
    """
    name, statistics, _period = row
    collected = _collected().get(name)
    if collected is None:
        pytest.skip(f"{name} is documented but not collected")

    # Percentile statistics (p50/p95/p99) come from the separate latency window,
    # not from MetricDefinition.statistics, so they are not expected to appear.
    documented = {s for s in statistics if not re.fullmatch(r"p\d+", s)}
    actual = set(collected.statistics)

    assert documented == actual, (
        f"metrics-catalog.md documents {name} with statistics "
        f"{sorted(documented)}; fetch_metrics.py collects {sorted(actual)}."
    )


def test_no_row_claims_the_60s_latency_window_as_its_own_period():
    """The 60s window is one extra query, not a per-metric collection period.

    This is the specific confusion that produced the eight wrong rows: the
    pipeline does collect 60-second data, so "60s" looked plausible on any
    latency or serverless row. It belongs only to the 24-hour percentile sweep.
    """
    offenders = [(name, period) for name, _s, period in _documented_rows()
                 if period == 60]
    assert not offenders, (
        f"rows claiming a 60s collection period: {offenders}. Every metric in "
        "the main sweep is collected at 300s; the 60s data is the separate "
        "24-hour latency percentile window (metadata.latency_resolution_seconds)."
    )


def test_the_count_metrics_say_they_are_period_totals():
    """The three rate metrics must warn, in the catalog, that they are not rates.

    An agent that reads "Count / Sum / 300s" and reports it as a per-second or
    per-minute rate is making the same mistake the thresholds made. The row has
    to say so, because the agent reads the row and not this test.
    """
    text = _catalog_text()
    for name in ("Evictions", "NewConnections", "ThrottledCmds"):
        rows = [m.group(0) for m in ROW.finditer(text)
                if m.group("name") == name]
        assert rows, f"{name} has no row in metrics-catalog.md"
        assert any("PerMinute" in row for row in rows), (
            f"no row for {name} mentions the derived PerMinute series. The "
            "catalog is where an agent learns that the raw Sum is a five-minute "
            f"total; without it, a reported {name} figure is ambiguous."
        )
