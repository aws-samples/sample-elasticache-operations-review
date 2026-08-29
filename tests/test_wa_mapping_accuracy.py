"""Pins references/well-architected-mapping.md to what the code actually does.

The mapping table is the document a reviewer uses to answer "was this cluster
checked for X?". It shipped claiming ✅ Implemented on eleven rows that no code
evaluated -- OE-03, OE-06, PERF-01 through PERF-06, and SEC-06 before it existed.
Nothing failed, because the table is prose and prose is not executed.

That is a different defect class from a wrong number: a mapping document that
overstates coverage invites a reader to conclude a cluster passed a check that
never ran, which is strictly worse than telling them nothing. So the table's
strongest claim -- "this is a Stage 3.5 check" -- is now mechanically checked
against the registry in both directions.

Weaker claims are deliberately not pinned. A row marked "📊 Stage 3 threshold"
names a metric threshold rather than a check_id, and analyze_metrics.py has no
check_id concept to compare against; asserting on those would mean encoding the
same guess the table already makes. What this file guarantees is narrower and
worth more: the ✅ set and the registry are the same set.
"""
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from check_configuration import CHECKS  # noqa: E402

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MAPPING_DOC = os.path.join(REPO_ROOT, "references", "well-architected-mapping.md")

# The exact marker the table uses for "this is a Check in the Stage 3.5 registry".
IMPLEMENTED_MARKER = "✅ Stage 3.5 check"


def _doc_text():
    with open(MAPPING_DOC, encoding="utf-8") as handle:
        return handle.read()


def _rows():
    """(check_id, status_cell) for every table row that has a check ID."""
    rows = []
    for line in _doc_text().splitlines():
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 6:
            continue
        check_id = cells[0]
        if not re.fullmatch(r"(OE|SEC|REL|PERF|COST|SUS)-\d+[a-z]?", check_id):
            continue
        rows.append((check_id, cells[-1]))
    return rows


def _claimed_implemented():
    return {cid for cid, status in _rows() if IMPLEMENTED_MARKER in status}


def test_the_table_is_actually_parsed():
    # Guards the guard. A table reformat that broke this regex would make every
    # assertion below vacuously true, which is how the original wrong claims
    # survived: nothing was reading them.
    rows = _rows()
    assert len(rows) > 30, (
        f"only {len(rows)} check rows parsed out of the mapping table; its shape "
        "changed and the tests below are now inert"
    )


def test_check_ids_are_unique_in_the_table():
    ids = [cid for cid, _ in _rows()]
    assert len(ids) == len(set(ids)), (
        "a check ID appears twice in the mapping table: "
        f"{sorted({i for i in ids if ids.count(i) > 1})}"
    )


def test_every_registry_check_appears_in_the_table():
    registry = {c.check_id for c in CHECKS}
    documented = {cid for cid, _ in _rows()}
    missing = registry - documented
    assert not missing, (
        "these checks run but are absent from "
        f"references/well-architected-mapping.md: {sorted(missing)}. A finding "
        "a reviewer cannot trace back to a pillar is an unattributable finding."
    )


def test_every_implemented_claim_is_a_real_registry_check():
    registry = {c.check_id for c in CHECKS}
    overclaimed = _claimed_implemented() - registry
    assert not overclaimed, (
        f"the mapping table marks these '{IMPLEMENTED_MARKER}' but they are not "
        f"in check_configuration.py's registry: {sorted(overclaimed)}. Mark them "
        "🔲 Planned, or 📊 Stage 3 threshold if analyze_metrics.py evaluates the "
        "condition without emitting this check_id."
    )


def test_every_registry_check_is_claimed_implemented():
    # The other direction: a check that runs but is documented as Planned makes
    # the table under-report, which sends a reviewer looking for coverage that
    # is already there.
    registry = {c.check_id for c in CHECKS}
    understated = registry - _claimed_implemented()
    assert not understated, (
        "these checks are in the Stage 3.5 registry but the mapping table does "
        f"not mark them '{IMPLEMENTED_MARKER}': {sorted(understated)}"
    )


def test_the_pillar_column_matches_the_registry_pillar():
    # A check filed under the wrong pillar skews that pillar's score. The table
    # encodes the pillar as the section a row sits in, so the section headings
    # are walked rather than a cell being read.
    section = None
    heading = re.compile(r"^## Pillar \d+: (.+)$")
    doc_pillar = {}
    for line in _doc_text().splitlines():
        match = heading.match(line)
        if match:
            section = match.group(1).strip().lower()
            continue
        if section and line.startswith("|"):
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if cells and re.fullmatch(
                r"(OE|SEC|REL|PERF|COST|SUS)-\d+[a-z]?", cells[0]
            ):
                doc_pillar[cells[0]] = section

    # The registry's pillar values are short keys; map them onto the doc's
    # pillar headings.
    expected_section = {
        "operational_excellence": "operational excellence",
        "security": "security",
        "reliability": "reliability",
        "performance": "performance efficiency",
        "cost_optimization": "cost optimization",
        "sustainability": "sustainability",
    }

    mismatched = []
    for check in CHECKS:
        want = expected_section.get(check.pillar)
        got = doc_pillar.get(check.check_id)
        if want is None:
            mismatched.append(
                f"{check.check_id}: registry pillar {check.pillar!r} has no "
                "corresponding section in the mapping doc"
            )
        elif got is not None and got != want:
            mismatched.append(
                f"{check.check_id}: registry says {check.pillar!r} "
                f"(-> '{want}') but the table files it under '{got}'"
            )
    assert not mismatched, "\n  ".join([""] + mismatched)


def test_sec06_points_at_its_date_source():
    # SEC-06 is the only check whose correctness depends on a vendored table.
    # A reader who cannot find that table cannot audit the date.
    row = next(status for cid, status in _rows() if cid == "SEC-06")
    text = _doc_text()
    sec06_line = next(
        line for line in text.splitlines()
        if line.startswith("| SEC-06 |")
    )
    assert "engine-support-lifecycle.md" in sec06_line, (
        "SEC-06's row must name references/engine-support-lifecycle.md, the "
        "vendored schedule its dates come from"
    )
    assert IMPLEMENTED_MARKER in row


def test_stage3_rows_that_name_a_threshold_key_name_a_real_one():
    """Closes the 📊 gap where the row is specific enough to check.

    The module docstring above explains why 📊 rows are not pinned in general:
    they name a metric, not a check_id, and a metric name is a guess about which
    threshold applies. But after the D11 unit fix, some rows name the registry
    key in backticks -- `NewConnectionsPerMinute` rather than "NewConnections" --
    and a key either exists in ThresholdRegistry or it does not.

    This matters because those names are precisely the ones that changed. A row
    left saying "NewConnections" would describe a metric with no threshold, which
    reads as coverage the pipeline does not have.
    """
    from analyze_metrics import ThresholdRegistry  # noqa: E402

    registry = ThresholdRegistry()
    unknown = []
    for line in _doc_text().splitlines():
        if not line.startswith("|") or "📊" not in line:
            continue
        for name in re.findall(r"`([A-Za-z][A-Za-z0-9]*)`", line):
            # Model class names (ShardBalanceModel) are not threshold keys.
            if name.endswith("Model"):
                continue
            if registry.get_threshold(name) is None:
                unknown.append(f"{line.split('|')[1].strip()}: `{name}`")

    assert not unknown, (
        "mapping rows name threshold keys that ThresholdRegistry does not "
        "register, so the documented check does not exist:\n  "
        + "\n  ".join(unknown)
    )


def test_the_status_legend_exists():
    # The markers are only meaningful if the table explains them, and the tests
    # above assert on the exact marker string.
    text = _doc_text()
    for marker in (IMPLEMENTED_MARKER, "📊 Stage 3 threshold", "🔲 Planned"):
        assert text.count(marker) >= 2, (
            f"the status marker {marker!r} is used without being defined in the "
            "legend, or is defined but never used"
        )
