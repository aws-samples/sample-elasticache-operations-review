"""Pins references/thresholds.md to the registry it is generated from.

That file is not documentation. SKILL.md Step 3 tells the agent to read it before
interpreting analysis.json, so every row is an instruction, and a row describing a
band `classify_value` cannot return tells the agent to expect a severity that
never arrives — which the agent reads as a pass. Same family as the vacuous check
and the silently-absent chart: **absence rendered as a measurement**, except
produced by a sentence instead of by code, so no correctness test can see it.

Seven such defects were in the committed version:

1. A trend row for `EngineCPUUtilization`, which `TRENDABLE_METRICS` does not
   contain — Stage 3 emits seven trend entries and no CPU one.
2. Headings naming the raw metrics (`Evictions`, `NewConnections`,
   `ThrottledCmds`, `BytesUsedForCache`, ECPU) whose thresholds are registered
   under derived names, inviting a regrade of the raw Sum — D11 again.
3. A `## Composite Scoring` section specifying the pillar formula for a scoring
   engine that was deliberately deleted.
4. A config table disagreeing with the Stage 3.5 registry on four of fourteen
   severities, listing two Graviton rows that are not checks, omitting SEC-06 and
   REL-04b.
5. `Evictions 0.2-20/min = LOW`, where the code returns HEALTHY.
6. `ECPU < 20% = LOW`, where the code returns HEALTHY.
7. `ThrottledCmds` sporadic = MEDIUM, unreachable: the boundary is zero.

So the tables are generated, and this file asserts three separate things:

* the committed file is what the generator produces (the drift check);
* every band the doc describes is one `classify_value` can actually return, and
  every band it can return is described (the round trip);
* the hand-written prose outside the markers survives generation.

The second is the one that matters. A generator can faithfully render a wrong
model, so the round trip is checked against `classify_value` itself — the function
whose answers the doc claims to explain — by probing it either side of every
boundary rather than by re-reading the same fields the generator read.
"""
import os
import re
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from analyze_metrics import ThresholdRegistry, TrendModel  # noqa: E402
from check_configuration import CHECKS  # noqa: E402
from generate_thresholds_doc import (  # noqa: E402
    BEGIN_MARKER,
    DOC_PATH,
    END_MARKER,
    render,
    splice,
)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
GENERATOR = os.path.join(REPO_ROOT, "scripts", "generate_thresholds_doc.py")

# A severity cell in a generated band table.
SEVERITIES = ("CRITICAL", "HIGH", "MEDIUM", "LOW", "HEALTHY")


def _doc_text():
    with open(DOC_PATH, encoding="utf-8") as handle:
        return handle.read()


def _generated_region(text):
    start = text.index(BEGIN_MARKER)
    end = text.index(END_MARKER)
    return text[start:end]


def _band_sections(text):
    """{metric_name: [severity, ...]} for every `###` band table in the doc.

    Keyed on the metric name in the heading, which is the name the threshold is
    registered under -- that agreement is itself one of the things under test.
    """
    sections = {}
    current = None
    for line in _generated_region(text).splitlines():
        heading = re.match(r"^###\s+(\S+)\s*\(", line)
        if heading:
            current = heading.group(1)
            sections[current] = []
            continue
        if current and line.startswith("|"):
            cells = [c.strip() for c in line.strip("|").split("|")]
            if len(cells) >= 2 and cells[1] in SEVERITIES:
                sections[current].append(cells[1])
    return sections


class TestTheCommittedDocIsWhatTheCodeGenerates:
    def test_running_the_generator_changes_nothing(self):
        """The drift check, as CI runs it.

        If this fails, a threshold moved and the instruction the agent reads did
        not. Run `python3 scripts/generate_thresholds_doc.py`.
        """
        existing = _doc_text()
        assert splice(existing, render()) == existing, (
            "references/thresholds.md is out of date with the registry. Run "
            "python3 scripts/generate_thresholds_doc.py"
        )

    def test_the_check_mode_agrees_and_exits_zero(self):
        """--check is what a hook or CI step invokes, so its exit code is API."""
        result = subprocess.run(
            [sys.executable, GENERATOR, "--check"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0, result.stderr

    def test_check_mode_fails_on_a_tampered_doc(self, tmp_path, monkeypatch):
        """Mutation test: the drift check must actually detect drift.

        Without this, a generator whose splice silently no-ops would pass every
        assertion above — the doc would agree with itself forever.
        """
        import generate_thresholds_doc as gen

        tampered = _doc_text().replace("> 90%", "> 95%", 1)
        assert tampered != _doc_text(), "the fixture edit found nothing to change"

        target = tmp_path / "thresholds.md"
        target.write_text(tampered, encoding="utf-8")
        monkeypatch.setattr(gen, "DOC_PATH", str(target))

        assert gen.main(["--check"]) == 1

    def test_a_doc_without_markers_is_an_error_not_a_silent_skip(self):
        with pytest.raises(ValueError, match="no <!-- BEGIN GENERATED"):
            splice("# just prose\n", render())


class TestEveryDocumentedBandIsReachable:
    """The round trip: what the doc promises, `classify_value` can return.

    Probed against the function rather than the dataclass, so a generator that
    renders the fields correctly but describes the wrong comparison still fails.
    """

    def test_every_metric_with_a_threshold_has_a_section(self):
        sections = _band_sections(_doc_text())
        registered = set(ThresholdRegistry()._thresholds)
        assert registered == set(sections), (
            "the doc and the registry disagree about which metrics are graded; "
            f"only in registry: {sorted(registered - set(sections))}, only in "
            f"doc: {sorted(set(sections) - registered)}"
        )

    def test_no_section_names_a_raw_rate_metric(self):
        """D11's shape: a heading naming the series nobody grades.

        The raw Sum is what every model reads by default, so a heading that says
        "Evictions" over a per-minute table is an invitation to compare a
        five-minute total against a one-minute boundary.
        """
        sections = _band_sections(_doc_text())
        for raw in ThresholdRegistry.RATE_METRICS:
            assert raw not in sections, (
                f"the doc has a band table headed {raw}, but the threshold is "
                f"registered under {raw}PerMinute and grades a derived series."
            )

    @pytest.mark.parametrize(
        "metric", sorted(ThresholdRegistry()._thresholds)
    )
    def test_the_documented_severities_are_exactly_the_reachable_ones(
        self, metric
    ):
        """Both directions at once: nothing promised that cannot happen, and
        nothing that can happen left undocumented.

        Reachability is established by probing `classify_value` either side of
        every registered boundary plus the extremes, which is how the three
        unreachable bands in the committed doc would have been caught: no probe
        value returns LOW for EvictionsPerMinute, because `low` is None.
        """
        registry = ThresholdRegistry()
        level = registry._thresholds[metric]

        boundaries = [
            b for b in (level.critical, level.high, level.medium, level.low)
            if b is not None
        ]
        probes = [-1.0, 0.0, 1e9]
        for b in boundaries:
            probes.extend([b - 0.01, b, b + 0.01])

        reachable = {registry.classify_value(metric, p) for p in probes}
        documented = set(_band_sections(_doc_text())[metric])

        assert documented == reachable, (
            f"{metric}: the doc describes {sorted(documented)} but "
            f"classify_value can return {sorted(reachable)}. A band that "
            "cannot be returned tells the agent to expect a severity that "
            "never arrives; a band that can be returned but is undocumented "
            "leaves it unexplained."
        )

    def test_every_reachable_band_carries_an_interpretation(self):
        """An empty Interpretation cell is a row with no instruction in it."""
        text = _generated_region(_doc_text())
        for line in text.splitlines():
            if not line.startswith("|"):
                continue
            cells = [c.strip() for c in line.strip("|").split("|")]
            if len(cells) == 3 and cells[1] in SEVERITIES:
                assert cells[2], (
                    f"band row {line!r} has no interpretation. Add it to the "
                    "metric's ThresholdLevel.means."
                )


class TestTheTrendTableMatchesTheTrendModel:
    def test_only_trendable_metrics_appear(self):
        """The committed table listed EngineCPUUtilization. Stage 3 emits none.

        A documented trend for a metric TrendModel never fits is absence
        rendered as a pass: the agent looks for the entry, finds nothing, and
        concludes the metric is flat.
        """
        region = _generated_region(_doc_text())
        table = region[region.index("## Trend-Based Thresholds"):]
        table = table[:table.index("## Configuration Assessment")]

        listed = [
            line.strip("|").split("|")[0].strip()
            for line in table.splitlines()
            if line.startswith("|") and not line.startswith("|---")
            and not line.startswith("| Metric")
        ]
        assert listed == list(TrendModel.TRENDABLE_METRICS)

    def test_every_row_states_the_unit_of_its_slope(self):
        """"> 2 per week" is two percentage points for memory, two for lag.

        The one thing a reader cannot infer, and the recurring defect class in
        this pipeline stated for trends: a number is a scalar plus a unit.
        """
        from generate_thresholds_doc import SLOPE_UNITS

        for metric in TrendModel.TRENDABLE_METRICS:
            assert metric in SLOPE_UNITS, (
                f"{metric} is trendable but SLOPE_UNITS has no unit for it, so "
                "its row would read '> N per week' with no unit."
            )


class TestTheConfigTableMatchesTheCheckRegistry:
    def test_every_check_appears_exactly_once(self):
        """The committed table omitted SEC-06 and REL-04b and invented two rows.

        An omitted check is a check a reader cannot know ran; an invented row is
        one they will believe ran when nothing evaluates it.
        """
        region = _generated_region(_doc_text())
        ids = re.findall(r"^\| (SEC-\S+|REL-\S+|OE-\S+|COST-\S+) \|", region,
                         re.M)
        assert ids == [c.check_id for c in CHECKS]

    def test_severities_match_the_registry(self):
        """Four of fourteen disagreed. The doc is what an agent grades against."""
        region = _generated_region(_doc_text())
        for check in CHECKS:
            row = re.search(rf"^\| {re.escape(check.check_id)} \| (.*)$",
                            region, re.M)
            assert row, f"{check.check_id} has no row"
            cells = [c.strip() for c in row.group(1).split("|")]
            assert cells[1].startswith(check.severity), (
                f"{check.check_id} is {check.severity} in the registry, and the "
                f"doc says {cells[1]!r}"
            )


class TestTheHandWrittenProseSurvives:
    """Generation must not eat the part that explains why.

    The severity ladder and the note on units are the only content here that no
    dataclass holds; a generator that overwrote the whole file would delete the
    reasoning and leave the tables looking complete.
    """

    def test_the_prose_sections_are_outside_the_markers(self):
        text = _doc_text()
        prose = text[:text.index(BEGIN_MARKER)]
        assert "## Severity Levels" in prose
        assert "## A Note on Units" in prose

    def test_the_prose_is_untouched_by_a_regeneration(self):
        text = _doc_text()
        regenerated = splice(text, render())
        assert (regenerated[:regenerated.index(BEGIN_MARKER)]
                == text[:text.index(BEGIN_MARKER)])

    def test_the_deleted_scoring_engine_is_not_specified_here(self):
        """`## Composite Scoring` documented a stage that no longer exists.

        SKILL.md's Scoring Methodology is the single legitimate copy of that
        formula: the agent computes it on request and shows the arithmetic. A
        second copy in an input document is a second thing to keep in step, and
        the deleted engine is proof of which copy loses.
        """
        text = _doc_text()
        assert "## Composite Scoring" not in text
        assert "pillar_score = 100 -" not in text
        assert "Scoring Methodology" in text, (
            "the pointer to SKILL.md's formula is gone, so a reader looking for "
            "how scores are computed finds nothing at all"
        )
