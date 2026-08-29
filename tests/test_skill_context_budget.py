"""Pins SKILL.md's instructions about which outputs the agent may read whole.

``metrics.json`` is the pipeline's raw time-series: 5-minute resolution x 14 days x
every metric x every node. That is 15 MB for three clusters and over 100 MB for
seven, against a context window measured in low hundreds of KB. An agent that opens
it does not get a degraded review, it gets no review -- the failure lands before the
first finding is read.

Nothing in the pipeline can prevent that, because the reader is a language model
following prose. So the prose is what gets tested. This file checks that SKILL.md
still tells the agent not to read the file, still ships an extraction snippet that
works, and -- the part most likely to rot -- that the snippet's own assumptions
about the JSON shape still hold.

This is the same defect class as ``tests/test_wa_mapping_accuracy.py``:
documentation-to-code agreement, where the document is an instruction rather than a
claim. A correctness test cannot catch a stale instruction, because the code is
right and the sentence about it is wrong.
"""
import json
import os
import re
import subprocess
import sys

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SKILL_DOC = os.path.join(REPO_ROOT, "SKILL.md")

# The outputs small enough to read whole, with the ceiling each is claimed to sit
# under in SKILL.md's Step 2 list. Generous by ~3x: the point is to catch a file
# that has changed category (KB -> MB), not to police growth.
SMALL_OUTPUTS = {
    "inventory.json": 200 * 1024,
    "analysis.json": 500 * 1024,
    "config_findings.json": 200 * 1024,
}

# Below this, "do not read it" would be advice about nothing and this whole file
# could go. Deliberately far under the real 15 MB.
BIG_ENOUGH_TO_MATTER = 2 * 1024 * 1024


def _skill_text():
    with open(SKILL_DOC, encoding="utf-8") as handle:
        return handle.read()


def _example_fleet_dir():
    """The generated example fleet, or a skip.

    ``examples/`` is gitignored and costs ~30s to build, so a developer who has not
    run the generator gets a skip rather than a failure. The size assertions below
    are the ones that need real files; everything else reads only SKILL.md.
    """
    path = os.path.join(REPO_ROOT, "examples")
    if not os.path.isfile(os.path.join(path, "metrics.json")):
        pytest.skip(
            "examples/ not generated; run "
            "python3 scripts/make_example_fleet.py --output examples/"
        )
    return path


class TestTheInstructionIsStillThere:
    """The prose must keep saying it, in the place the agent acts on it."""

    def test_step_2_warns_against_reading_metrics_json(self):
        text = _skill_text()
        step2 = text[text.index("### Step 2"):text.index("### Step 3")]
        assert re.search(r"[Dd]o not read `metrics\.json`", step2), (
            "SKILL.md Step 2 no longer tells the agent not to read metrics.json. "
            "That warning is the only thing standing between a review and a "
            "context overflow on the first tool call."
        )

    def test_step_3_repeats_it_where_the_reading_happens(self):
        """Step 2 runs the pipeline; Step 3 is where an agent decides what to open.

        The warning has to be at the point of action, not only at the point the
        file was created -- by Step 3 the Step 2 text may be far up the transcript.
        """
        text = _skill_text()
        step3 = text[text.index("### Step 3"):text.index("### Step 4")]
        assert "metrics.json" in step3 and re.search(
            r"[Dd]o not read `metrics\.json`|never by reading the file", step3), (
            "SKILL.md Step 3 tells the agent to read the outputs but no longer "
            "excludes metrics.json at that point."
        )

    def test_the_size_asymmetry_is_stated_not_implied(self):
        """An agent obeys a reason better than a rule, and the reason is the size.

        Without a number, "do not read it" reads as style advice and loses to a
        user asking for detail.
        """
        text = _skill_text()
        assert re.search(r"15 MB", text) and re.search(r"100 MB", text), (
            "SKILL.md no longer states metrics.json's actual size range, so the "
            "instruction not to read it has lost its justification."
        )


class TestTheSnippetActuallyWorks:
    """A documented command that does not run is worse than none."""

    def test_step_2_ships_an_extraction_snippet(self):
        text = _skill_text()
        step2 = text[text.index("### Step 2"):text.index("### Step 3")]
        assert "m['cost']" in step2 or 'm["cost"]' in step2, (
            "SKILL.md Step 2 no longer shows how to extract the cost section, so "
            "the only way left to reach it is reading the whole file."
        )

    def test_the_snippet_runs_against_real_output(self):
        """Runs the documented command verbatim, as a subprocess, on real data.

        Extracted from the doc rather than retyped here: a copy in this file would
        pass while the published one was broken, which is the failure mode this
        test exists to prevent.
        """
        fleet = _example_fleet_dir()
        text = _skill_text()
        step2 = text[text.index("### Step 2"):text.index("### Step 3")]
        # Step 2 has two bash blocks -- the pipeline invocation and this one. Select
        # by content rather than by position, so adding a third does not silently
        # start testing the wrong command.
        blocks = [b for b in re.findall(r"```bash\n(.*?)```", step2, re.DOTALL)
                  if "cost" in b and "python3 -c" in b]
        assert len(blocks) == 1, (
            f"expected exactly one cost-extraction bash block in Step 2, "
            f"found {len(blocks)}"
        )

        snippet = blocks[0].strip()
        code = re.search(r'python3 -c "\n?(.*?)"\s*$', snippet, re.DOTALL).group(1)
        code = code.replace("output/metrics.json",
                            os.path.join(fleet, "metrics.json"))

        result = subprocess.run([sys.executable, "-c", code],
                                capture_output=True, text=True, cwd=REPO_ROOT)
        assert result.returncode == 0, result.stderr

        extracted = json.loads(result.stdout)
        assert set(extracted) == {"metadata", "cost"}, sorted(extracted)
        assert extracted["cost"]["daily"], "extracted an empty cost section"
        # The whole point: what comes back is small enough to read.
        assert len(result.stdout) < 64 * 1024, (
            f"the extraction returned {len(result.stdout) // 1024} KB; it is "
            "supposed to be a few KB"
        )


class TestTheSizesTheDocClaims:
    """Reality check on the categories, using the generated fleet."""

    @pytest.mark.parametrize("name,ceiling", sorted(SMALL_OUTPUTS.items()))
    def test_small_outputs_are_still_small(self, name, ceiling):
        size = os.path.getsize(os.path.join(_example_fleet_dir(), name))
        assert size < ceiling, (
            f"{name} is {size // 1024} KB, past the {ceiling // 1024} KB this "
            "suite assumes. SKILL.md tells the agent to read it whole; if it has "
            "changed category, that instruction needs revisiting."
        )

    def test_the_raw_shards_are_still_the_big_ones(self):
        """Guards the premise. Since Phase 9 the raw bulk lives in per-cluster
        shards under metrics/ rather than in metrics.json (now a KB manifest), so
        the size check follows the series to their new home. If the shards ever
        became small, the warning would be stale advice and should be removed."""
        shard_dir = os.path.join(_example_fleet_dir(), "metrics")
        total = sum(
            os.path.getsize(os.path.join(shard_dir, f))
            for f in os.listdir(shard_dir)
            if f.endswith(".json")
        )
        assert total > BIG_ENOUGH_TO_MATTER, (
            f"the raw shards total only {total // 1024} KB. SKILL.md warns at "
            "length that they are 15-100 MB; if collection changed, update the doc."
        )

    def test_a_single_shard_is_too_big_to_read(self):
        """The doc's fallback -- open one cluster's shard -- has to be worth doing.

        If one shard were small, "open only that cluster's shard" would be needless
        ceremony. It is not: the doc claims 1-40 MB apiece, and the largest single
        shard in the example fleet is tens of MB -- on its own more than a context
        window holds.
        """
        shard_dir = os.path.join(_example_fleet_dir(), "metrics")
        biggest = max(
            os.path.getsize(os.path.join(shard_dir, f))
            for f in os.listdir(shard_dir)
            if f.endswith(".json")
        )
        assert biggest > 512 * 1024, (
            f"the largest shard is {biggest // 1024} KB; SKILL.md's advice to "
            "open a single shard assumes it is megabytes"
        )


class TestTheSchemaBlocksDescribeRealOutput:
    """SKILL.md's `analysis.json Structure` block against the real file.

    The committed version was wrong six ways: it documented `breach_counts` and
    `command_mix`, neither of which Stage 3 emits, and omitted `breaches`,
    `findings`, `workload_class`, and `errors`, which it does. It also described
    `utilization` as three percentages when those live nested under
    `utilization_scores`, and titled the section "Statistics Only — No Findings"
    while the file's largest per-cluster key is `findings`.

    Every one of those is the same failure as a wrong threshold row: the agent is
    told to read a field, does not find it, and treats the absence as a
    measurement of nothing. `command_mix` is the worst of them — an agent looking
    for the read/write ratio there finds nothing and may report the workload as
    unclassified, when `workload_class` sits two lines away.

    Only top-level per-cluster keys are pinned. The nested shapes are illustrated
    with real values but not asserted field-by-field: they change as models gain
    outputs, and pinning them would make every model improvement a doc failure
    without catching anything the key set does not.
    """

    def _schema_block(self):
        text = _skill_text()
        start = text.index("### analysis.json Structure")
        end = text.index("### metrics.json Structure", start)
        return text[start:end]

    def test_every_documented_cluster_key_exists_in_real_output(self):
        with open(os.path.join(_example_fleet_dir(), "analysis.json"),
                  encoding="utf-8") as handle:
            analysis = json.load(handle)
        real = set(next(iter(analysis["clusters"].values())))

        block = self._schema_block()
        # Keys at the per-cluster level of the JSON sample: six spaces of indent.
        documented = set(re.findall(r'^      "(\w+)":', block, re.M))

        assert documented <= real, (
            f"SKILL.md documents analysis.json fields that Stage 3 does not "
            f"emit: {sorted(documented - real)}. An agent told to read a field "
            "that is absent reads the absence as a measurement."
        )

    def test_every_real_cluster_key_is_documented(self):
        with open(os.path.join(_example_fleet_dir(), "analysis.json"),
                  encoding="utf-8") as handle:
            analysis = json.load(handle)
        real = set(next(iter(analysis["clusters"].values())))

        block = self._schema_block()
        documented = set(re.findall(r'^      "(\w+)":', block, re.M))

        assert real <= documented, (
            f"Stage 3 emits analysis.json fields SKILL.md does not document: "
            f"{sorted(real - documented)}. Undocumented output is output the "
            "agent has no instruction to use."
        )

    def test_the_heading_does_not_deny_the_findings_it_documents(self):
        """The heading said "Statistics Only — No Findings". It emits findings.

        Two sentences in one document disagreeing about whether a key exists is
        worse than either being wrong alone: the agent picks whichever it read
        last, and there is no way to tell which that was.
        """
        block = self._schema_block()
        heading = block.splitlines()[0]
        assert "No Findings" not in heading, (
            f"the schema heading is {heading!r}, but analysis.json carries a "
            "`findings` array per cluster -- 35 of them on the example fleet."
        )
        assert '"findings"' in block


class TestNoRecalledPrices:
    """The other half of the same rule: numbers must be derived, not stated.

    SKILL.md's Step 5 follow-up table used to answer "estimate cost savings" with
    "node_type x count x hourly rate x savings %" -- the exact from-memory
    multiplication the Cost Estimation Guide forbids fifty lines later. A doc that
    contradicts itself lets the agent pick whichever line it read last.
    """

    def test_the_followup_table_does_not_teach_rate_multiplication(self):
        """Looks for the multiplication *chain*, not the phrase "hourly rate".

        Step 5 legitimately says "**Never a recalled hourly rate**", and a naive
        pattern flags the sentence that forbids the thing -- which would make this
        test unfixable except by deleting the warning. What is actually wrong is a
        formula: two or more factors multiplied into a price.
        """
        text = _skill_text()
        step5 = text[text.index("### Step 5"):text.index("### Step 6")]
        chain = re.search(
            r"(node_type|nodes?|count|rate)\s*[x×*]\s*\w+\s*[x×*]\s*\w+", step5)
        assert not chain, (
            "Step 5 teaches the agent to multiply out a price "
            f"({chain.group(0)!r}), which the Cost Estimation Guide forbids. "
            "Point it at price_calculator.py instead."
        )

    def test_the_followup_table_points_at_the_calculator(self):
        text = _skill_text()
        step5 = text[text.index("### Step 5"):text.index("### Step 6")]
        assert "price_calculator.py" in step5, (
            "Step 5's cost answer no longer names the tool that fetches live "
            "rates, leaving the agent to recall them."
        )


class TestValidateBeforeNarrate:
    """Step 3 must tell the agent to sanity-check the pipeline first (Phase 4).

    This is what caught the four false CRITICALs -- a 0% hit rate at zero
    traffic, a percentage over 100 -- and it only works if it is the agent's
    first move, before it starts explaining figures. A correctness test cannot
    enforce it: the instruction lives in prose the model reads. So the prose is
    pinned, the same way this file pins the do-not-read-metrics.json rule.
    """

    def _step3(self):
        text = _skill_text()
        return text[text.index("### Step 3"):text.index("### Step 4")]

    def test_step_3_says_to_validate_before_narrating(self):
        step3 = self._step3()
        assert "sanity-check the pipeline" in step3, (
            "Step 3 no longer tells the agent to validate the data before "
            "narrating it -- the check that caught the four false CRITICALs."
        )

    def test_the_check_comes_before_the_reading_instruction(self):
        """First move, not a footnote. If it lands after 'read the outputs' the
        agent has already started interpreting before it is told to doubt."""
        step3 = self._step3()
        assert (step3.index("sanity-check the pipeline")
                < step3.index("Read three of the four outputs whole")), (
            "The validate-before-narrate block must precede the reading "
            "instruction; an agent validates first or not at all."
        )

    def test_step_3_names_the_specific_pipeline_artifacts(self):
        """The named signals are the ones with a history in this pipeline; a
        vague 'check the data' would not have caught any of the real bugs."""
        step3 = self._step3()
        for signal in ("hit rate", "above 100%", "insufficient_data",
                       "total_datapoints", "Unknown"):
            assert signal in step3, (
                f"Step 3's validation block no longer names {signal!r}, one of "
                "the four pipeline-artifact signals it must call out."
            )


class TestNoNewNumbersGuardrail:
    """The conversational path has no renderer to enforce no-new-numbers.

    5b's check guards figures on their way into the *report*; nothing guards
    figures the agent states in chat. Phase 4 adds the guardrail line for that
    path, worded as 5b enforces it: a number must trace to the specific value it
    is about, not merely appear somewhere in the JSON (which 99.96% of plausible
    fabricated percentages do -- see PLAN.md Gap 5b).
    """

    def _guardrails(self):
        text = _skill_text()
        return text[text.index("## Guardrails"):text.index("## Pipeline Scripts")]

    def test_a_guardrail_requires_numbers_to_be_traceable(self):
        guardrails = self._guardrails()
        assert "specific value" in guardrails and "recalled" in guardrails, (
            "No guardrail requires stated numbers to trace to the specific "
            "value they describe; the conversational path is unguarded."
        )

    def test_the_guardrail_rejects_the_appears_somewhere_reading(self):
        """The rule is scoped, not 'present in the JSON'. The wording must say
        so, because the unscoped reading passes almost any plausible figure."""
        guardrails = self._guardrails()
        assert "merely present somewhere" in guardrails, (
            "The traceability guardrail must reject the 'appears somewhere' "
            "reading explicitly -- that reading is a formality, not a check."
        )

    def test_step_4_reinforces_it_and_points_at_the_renderer(self):
        text = _skill_text()
        step4 = text[text.index("### Step 4"):text.index("### Step 5")]
        assert "Cite the specific value" in step4, (
            "Step 4 no longer carries the cite-the-specific-value habit."
        )
        assert "--notes" in step4, (
            "Step 4's number habit should point at the --notes renderer, which "
            "enforces in the report exactly what chat cannot."
        )

    def test_the_read_only_guardrail_is_still_present(self):
        """Adding rows must not displace the load-bearing ones."""
        guardrails = self._guardrails()
        assert "READ-ONLY" in guardrails and "explicit user confirmation" in guardrails


class TestProvenanceInstruction:
    """SKILL.md must tell the agent to quote provenance, and name a real field.

    Documentation-to-code agreement: the instruction names ``pipeline_version``,
    so if that field were renamed in the collectors the doc would be a stale
    instruction pointing at a key that no longer exists. The second test closes
    that loop against the generated fleet.
    """

    def test_the_doc_tells_the_agent_to_quote_provenance(self):
        text = _skill_text()
        assert "Quote provenance from" in text and "pipeline_version" in text, (
            "SKILL.md no longer tells the agent to quote provenance from "
            "metadata; it will recall region/window/version instead."
        )

    def test_the_named_provenance_field_actually_exists_in_output(self):
        path = _example_fleet_dir()
        for name in ("inventory.json", "analysis.json", "config_findings.json"):
            with open(os.path.join(path, name)) as handle:
                meta = json.load(handle)["metadata"]
            assert "pipeline_version" in meta, (
                f"SKILL.md tells the agent to quote pipeline_version, but "
                f"{name} does not emit it")
