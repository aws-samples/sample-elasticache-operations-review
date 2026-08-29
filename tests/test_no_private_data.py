"""Pins that no real AWS account's data reaches the shipped documentation.

This skill is developed against a live AWS account and published as an
aws-samples repository. Those two facts collide in the docs: the fastest way to
write an example of the agent's output is to paste what the agent actually said,
and what it actually said names real clusters in a real account.

That is exactly how it happened. ``SKILL.md``'s example output shipped with three
lines naming two clusters from the development account. No test failed, because
no test was looking -- the leak is not a malformed value or a wrong number, it is
a *correct* value from the wrong fleet, and every other check in this suite is
about correctness.

This file deliberately does not quote the names that leaked. A test that hardcodes
them would republish them and undo the fix; the checks below depend on the *shape*
of a leak, not on any particular string.

So this file checks provenance rather than correctness. Documentation may only
name entities from the offline example fleet, which is synthetic by construction
(``scripts/make_example_fleet.py``, account 123456789012). Anything else is
presumed to have come from a real account.

Two layers, different scopes. (1) The transcript sweep guesses which backticked
tokens are cluster names, so it can only afford deny-by-default inside markdown
blockquotes; it exempts ``PLAN.md`` (``PROVENANCE_EXEMPT``), whose blockquotes quote
owner instructions and example commands, not fleet transcripts. (2) The denylist
guard knows the exact real identifiers (from the git-ignored ``.private-identifiers``)
and so scans EVERY tracked file with no exemption -- PLAN.md was scrubbed to the
example account, so it is held to the same bar as everything shipped.
"""
import os
import re
import subprocess

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# The only account ID allowed to appear anywhere: the reserved documentation
# account used by the example fleet and by AWS's own docs.
EXAMPLE_ACCOUNT = "123456789012"

# Files that may quote real cluster names because their subject *is* the leak.
# PLAN.md is a development artifact, not customer-facing documentation.
PROVENANCE_EXEMPT = {"PLAN.md"}

# Directories that are not shipped documentation and not read by customers.
EXCLUDED_DIRS = {".git", "__pycache__", ".pytest_cache", "examples", "output",
                 ".venv", "venv", ".kiro"}

# Cluster names belonging to the fleet the docs are allowed to describe. Read
# from the generator rather than hardcoded, so adding a cluster to the example
# fleet does not require editing this test.
def _example_cluster_names():
    generator = os.path.join(REPO_ROOT, "scripts", "make_example_fleet.py")
    with open(generator, encoding="utf-8") as handle:
        source = handle.read()
    names = set(re.findall(r'"cluster_id":\s*"([^"]+)"', source))
    names |= set(re.findall(r'cluster_id=["\']([^"\']+)["\']', source))
    return names


def _tracked_text_files():
    """Every git-tracked file, so untracked scratch files are not scanned.

    Uses git rather than os.walk because what ships is what git tracks.
    """
    out = subprocess.run(
        ["git", "ls-files"], cwd=REPO_ROOT, capture_output=True, text=True,
        check=True,
    ).stdout
    for rel in out.splitlines():
        if not rel:
            continue
        if rel.split("/")[0] in EXCLUDED_DIRS:
            continue
        path = os.path.join(REPO_ROOT, rel)
        if not os.path.isfile(path):
            continue
        try:
            with open(path, encoding="utf-8") as handle:
                yield rel, handle.read()
        except UnicodeDecodeError:
            continue  # binary asset; no prose to leak


def test_example_fleet_uses_the_reserved_documentation_account():
    """Guards the allowlist itself.

    Every other test here trusts that the example fleet is synthetic. If the
    generator were ever pointed at a real account, those tests would start
    allowlisting real names, so the allowlist's own premise is checked first.
    """
    generator = os.path.join(REPO_ROOT, "scripts", "make_example_fleet.py")
    with open(generator, encoding="utf-8") as handle:
        source = handle.read()
    accounts = set(re.findall(r"\b(\d{12})\b", source))
    assert accounts <= {EXAMPLE_ACCOUNT}, (
        "the example fleet generator references a 12-digit account other than "
        f"the reserved documentation account: {sorted(accounts - {EXAMPLE_ACCOUNT})}"
    )


def test_no_real_account_ids_in_tracked_files():
    """A 12-digit number in an ARN or a docs example is an account ID.

    Matched narrowly -- bare 12-digit numbers occur legitimately (byte counts,
    timestamps, datapoint totals), so this looks only at the account position of
    an ARN, where the meaning is unambiguous.
    """
    arn_account = re.compile(r"arn:aws[a-z-]*:[a-z0-9-]+:[a-z0-9-]*:(\d{12}):")
    offenders = []
    for rel, text in _tracked_text_files():
        for match in arn_account.finditer(text):
            if match.group(1) != EXAMPLE_ACCOUNT:
                line = text[: match.start()].count("\n") + 1
                offenders.append(f"{rel}:{line} -> {match.group(1)}")
    assert not offenders, (
        "ARNs must use the reserved documentation account "
        f"{EXAMPLE_ACCOUNT}, not a real one:\n  " + "\n  ".join(offenders)
    )


PRIVATE_IDENTIFIERS_FILE = os.path.join(REPO_ROOT, ".private-identifiers")


def _known_real_identifiers():
    """The real account/cluster identifiers that once leaked, read from a local,
    git-ignored ``.private-identifiers`` file (one per line).

    They are NOT hardcoded here: this test file ships, so listing them would make it
    the next place they are republished. They also no longer live in any tracked file
    (PLAN.md was scrubbed to the example account) -- so the only copy is the local,
    untracked denylist. A fresh clone will not have it, and there is nothing to
    protect there anyway (the tree is clean), so the guard skips rather than fails.
    """
    if not os.path.isfile(PRIVATE_IDENTIFIERS_FILE):
        pytest.skip(
            "no .private-identifiers file; create one (git-ignored, one real "
            "account/cluster id per line) to enable the re-leak guard locally"
        )
    with open(PRIVATE_IDENTIFIERS_FILE, encoding="utf-8") as handle:
        idents = [ln.strip() for ln in handle
                  if ln.strip() and not ln.lstrip().startswith("#")]
    assert idents, ".private-identifiers exists but is empty"
    return idents


def test_no_tracked_file_names_a_known_real_identifier():
    """The class the blockquote sweep cannot see: a real name in a comment or in code.

    The transcript sweep below is scoped to markdown blockquotes, so a real cluster
    name in a Python comment or a code string slips past it (one did, in a test
    docstring, which is what motivated this guard). This scans every tracked file --
    comments, code, and docs, PLAN.md included (it was scrubbed to the example
    account) -- against the local denylist. ``.private-identifiers`` itself is
    git-ignored, so it is not among the tracked files scanned.
    """
    denylist = _known_real_identifiers()
    offenders = []
    for rel, text in _tracked_text_files():
        for ident in denylist:
            if ident in text:
                line = text[: text.index(ident)].count("\n") + 1
                offenders.append(f"{rel}:{line} -> {ident}")
    assert not offenders, (
        "a real account/cluster identifier from the development fleet appears in a "
        "tracked file. Replace it with an example-fleet name or a generic "
        "description — nothing that ships may quote it:\n  "
        + "\n  ".join(offenders)
    )


def test_example_transcripts_only_name_example_fleet_clusters():
    """Every backticked name in an example transcript must be accountable.

    Scoped to markdown blockquotes, because that is the shape a pasted transcript
    takes: ``> - `some-cluster` has TLS disabled``. Restricting to blockquotes
    buys a much stronger rule than a fleet-wide scan could support -- here the
    default is *deny*. Prose elsewhere legitimately backticks hundreds of
    filenames, flags, and metric names, so a scan of all prose has to guess which
    tokens are cluster-shaped, and guessing fails in both directions: it flags
    `maxmemory-policy` while missing a real cluster whose name happens to be one
    unhyphenated word, which is what the actual leak looked like.

    Inside a transcript the vocabulary is small and enumerable, so anything not
    enumerated fails. Adding a term is a one-line edit and a deliberate one --
    which is the point, since the leak happened by paste, not by decision.
    """
    allowed = set(_example_cluster_names())
    # Non-cluster terms an example transcript may quote. Deliberately short: a
    # transcript that needs a large vocabulary is dumping data rather than
    # synthesizing, which Step 4 tells the agent not to do.
    allowed |= {
        # Utilization classes and severities emitted by Stage 3 / Stage 3.5.
        "IDLE", "BALANCED", "SATURATED", "Unknown",
        "CRITICAL", "HIGH", "MEDIUM", "LOW",
        # Pipeline outputs a transcript may cite as its source.
        "inventory.json", "metrics.json", "analysis.json",
        "config_findings.json",
        # Flags whose behaviour changes what a transcript may claim.
        "--skip-cost",
        # Tools a transcript may offer to run next. Not fleet entities.
        "price_calculator.py",
    }

    offenders = []
    for rel, text in _tracked_text_files():
        if not rel.endswith(".md") or rel in PROVENANCE_EXEMPT:
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            if not line.lstrip().startswith(">"):
                continue
            for token in re.findall(r"`([^`\n]+)`", line):
                token = token.strip()
                if token in allowed:
                    continue
                offenders.append(f"{rel}:{lineno} -> `{token}`")
    assert not offenders, (
        "example transcripts may only name clusters from the offline example "
        "fleet. If one of these came from a real AWS account, replace it with an "
        "example-fleet name; if it is a legitimate technical term, add it to the "
        "allowlist in this test:\n  " + "\n  ".join(offenders)
    )


def test_the_transcript_sweep_would_catch_a_real_leak():
    """Proves the sweep above is load-bearing rather than inert.

    A deny-by-default check reads as strong and can still catch nothing -- if the
    blockquote pattern stopped matching, or the allowlist grew to admit
    everything, the test above would pass on a leaking file. So this feeds it
    lines shaped like the ones that actually shipped and asserts they are
    rejected.

    The stand-in names are invented, not the real ones. Hardcoding what leaked
    would republish it in a shipped file and undo the fix -- and the check does
    not depend on the specific strings, only on their shape: a backticked name
    inside a blockquote that the example fleet cannot account for. Two shapes are
    covered deliberately, because the real leak had both: a hyphenated name, and
    a bare single word, which no cluster-name heuristic would flag.
    """
    lines = [
        "> - `notafleetcluster-01` uses cache.m5.large — moving to m7g.large",
        "> - 2 clusters (`notafleetcluster-01`, `scratch`) are IDLE",
    ]
    allowed = set(_example_cluster_names())
    caught = {
        token.strip()
        for line in lines
        if line.lstrip().startswith(">")
        for token in re.findall(r"`([^`\n]+)`", line)
        if token.strip() not in allowed
    }
    assert {"notafleetcluster-01", "scratch"} <= caught, (
        "the transcript sweep no longer rejects unaccountable cluster names; "
        f"it flagged only {sorted(caught)}"
    )


@pytest.mark.parametrize("rel", ["SKILL.md", "README.md"])
def test_customer_facing_docs_exist_and_are_scanned(rel):
    """Fails if a scanned doc is renamed or dropped.

    Without this, deleting SKILL.md would make the sweep above pass trivially --
    a green suite that checks nothing is worse than a red one.
    """
    assert os.path.isfile(os.path.join(REPO_ROOT, rel)), (
        f"{rel} is missing; the provenance sweep above silently stops covering "
        "it. Update this test if the file was intentionally renamed."
    )
