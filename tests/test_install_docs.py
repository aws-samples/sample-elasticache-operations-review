"""Pins the install instructions to the skill's actual name (D12).

Claude Code discovers a skill by the directory it lives under
(`~/.claude/skills/<name>/SKILL.md`), and that directory name is the skill's
invocation name. README documents a symlink whose link name must therefore equal
`SKILL.md`'s frontmatter `name:` — or the documented `/… ` command silently does not
resolve to this skill. Nothing else fails when the two drift, so this test is the
only thing that catches a rename that updates one but not the other.

This is the documentation-to-code class again (cf. test_skill_context_budget.py),
one layer out: the "code" here is the frontmatter, the "doc" is the README install
block, and a mismatch ships an install command that does not work.
"""
import os
import re

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SKILL_DOC = os.path.join(REPO_ROOT, "SKILL.md")
README = os.path.join(REPO_ROOT, "README.md")


def _read(path):
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def _skill_name():
    """The `name:` from SKILL.md's YAML frontmatter."""
    text = _read(SKILL_DOC)
    assert text.startswith("---"), "SKILL.md has no YAML frontmatter"
    frontmatter = text.split("---", 2)[1]
    match = re.search(r"^name:\s*(\S+)\s*$", frontmatter, re.M)
    assert match, "SKILL.md frontmatter has no `name:` field"
    return match.group(1)


def test_skill_declares_a_name():
    assert _skill_name() == "elasticache-operations-review"


def test_readme_install_uses_the_skill_name_as_the_directory():
    """The documented `~/.claude/skills/<dir>` link name must equal the frontmatter
    name, since that directory is the invocation name. A rename that touches only
    one side lands here."""
    name = _skill_name()
    readme = _read(README)
    assert f"~/.claude/skills/{name}" in readme, (
        f"README's install instructions do not create "
        f"~/.claude/skills/{name}; the frontmatter name and the documented "
        "install directory have drifted, so the documented skill command will "
        "not resolve to this skill."
    )


def test_readme_documents_the_claude_code_discovery_path():
    """Guard the premise: if the install section were dropped, the test above
    could pass vacuously only if the name also vanished -- so assert the section's
    load-bearing pieces are present."""
    readme = _read(README)
    assert "~/.claude/skills/" in readme, (
        "README no longer documents where Claude Code discovers skills"
    )
    assert "ln -s" in readme, (
        "README no longer shows the symlink install command"
    )
