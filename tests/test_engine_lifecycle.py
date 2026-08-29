"""Tests for SEC-06, the engine end-of-support check.

SEC-06 is unlike every other check in the registry: its answer depends on *when
the review runs*, not only on the configuration. That introduces two failure
modes nothing else in this suite can catch.

**The check can rot.** The dates come from a published AWS schedule, vendored
into ``references/engine-support-lifecycle.md`` so the pipeline stays offline and
deterministic. Vendored data goes stale silently, so the tests below pin the code
to that file and fail when the file's ``Last verified`` date is more than a year
old. A wrong date here is worse than no check: a customer reschedules real work
around it.

**The check can invent a deadline.** Valkey, Memcached, and Redis OSS 7.x have no
announced end of standard support. The correct output for them is *nothing*. An
extrapolated date would look exactly as authoritative as a real one, so silence
gets its own tests rather than being assumed.

The severity boundaries are tested against an injected review date, never the
real clock. A test that read ``date.today()`` would pass today, grade the same
fleet differently next quarter, and eventually fail on its own -- which is the
same non-reproducibility the pipeline as a whole is built to avoid.
"""
import datetime
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from check_configuration import (  # noqa: E402
    CHECKS,
    ENGINE_SUPPORT_SCHEDULE,
    EXTENDED_SUPPORT_PREMIUM,
    SUPPORT_END_WARNING_DAYS,
    VALID_SEVERITIES,
    ConfigurationChecker,
    _major_version,
    _sec06_extended_support,
)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LIFECYCLE_DOC = os.path.join(REPO_ROOT, "references", "engine-support-lifecycle.md")

# How long the vendored schedule may go un-reverified. AWS revises the list
# roughly once a year, so a year is the point past which "verified" stops meaning
# anything.
MAX_DOC_AGE_DAYS = 365

SEC06 = next(c for c in CHECKS if c.check_id == "SEC-06")


def _doc_text():
    with open(LIFECYCLE_DOC, encoding="utf-8") as handle:
        return handle.read()


def _doc_schedule():
    """The (engine, major) -> end-of-standard-support map as the doc states it.

    Parsed from the markdown table rather than duplicated here, so this test
    cannot itself become the stale copy it exists to prevent.
    """
    schedule = {}
    row = re.compile(
        r"^\|\s*Redis OSS\s+(\d+)\s*\|\s*(\d{4}-\d{2}-\d{2})\s*\|", re.MULTILINE
    )
    for major, date in row.findall(_doc_text()):
        schedule[("redis", int(major))] = date
    return schedule


def cluster(engine="redis", version="6.2", **overrides):
    """Minimal cluster record -- SEC-06 reads only engine and engine_version."""
    record = {
        "cluster_id": "c1",
        "cluster_type": "node-based",
        "engine": engine,
        "engine_version": version,
    }
    record.update(overrides)
    return record


# The end-of-support date SEC-06 grades against for the fixtures below. Derived
# from the schedule instead of written as a literal, so these tests keep testing
# the boundaries after AWS publishes a new date.
REDIS6_EOS = datetime.date.fromisoformat(ENGINE_SUPPORT_SCHEDULE[("redis", 6)])


def on(days_before_eos):
    """A review date the given number of days before Redis OSS 6's EOS."""
    return REDIS6_EOS - datetime.timedelta(days=days_before_eos)


# ---------------------------------------------------------------------------
# The vendored schedule must match its source of truth, and must not rot
# ---------------------------------------------------------------------------


class TestVendoredScheduleAgreesWithTheReference:
    def test_reference_doc_exists(self):
        # Without this the parsing tests below would pass on an empty parse.
        assert os.path.isfile(LIFECYCLE_DOC), (
            "references/engine-support-lifecycle.md is the source of truth for "
            "SEC-06's dates. If it was renamed, update this test -- do not "
            "delete the check's provenance."
        )

    def test_doc_table_is_actually_parsed(self):
        # Guards the guard: a regex that stopped matching would make every
        # agreement test below vacuously true.
        assert _doc_schedule(), (
            "no Redis OSS rows parsed out of the schedule table; the table's "
            "shape changed and the agreement tests below are now inert"
        )

    def test_code_and_doc_agree_exactly(self):
        assert ENGINE_SUPPORT_SCHEDULE == _doc_schedule(), (
            "ENGINE_SUPPORT_SCHEDULE in check_configuration.py has drifted from "
            "the table in references/engine-support-lifecycle.md. Fix both, and "
            "re-verify against the AWS URLs at the top of that file."
        )

    def test_premium_multipliers_appear_in_the_doc(self):
        # Stated as percentages in prose ("80% for Y1 and Y2, 160% for Y3").
        text = _doc_text()
        for year, multiplier in EXTENDED_SUPPORT_PREMIUM.items():
            assert f"{multiplier:.0%}" in text, (
                f"the year-{year} premium {multiplier:.0%} is not stated in "
                "references/engine-support-lifecycle.md"
            )

    def test_warning_window_is_documented(self):
        assert str(SUPPORT_END_WARNING_DAYS) in _doc_text(), (
            f"the {SUPPORT_END_WARNING_DAYS}-day warning window is not "
            "explained in references/engine-support-lifecycle.md, so a reader "
            "cannot tell why a finding is MEDIUM rather than LOW"
        )

    def test_schedule_is_not_stale(self):
        match = re.search(r"\*\*Last verified:\*\*\s*(\d{4}-\d{2}-\d{2})", _doc_text())
        assert match, (
            "references/engine-support-lifecycle.md has no `Last verified:` "
            "date, so nothing can tell whether its dates are still current"
        )
        verified = datetime.date.fromisoformat(match.group(1))
        age = (datetime.datetime.now(datetime.timezone.utc).date() - verified).days
        assert age <= MAX_DOC_AGE_DAYS, (
            f"the vendored engine support schedule was last verified {age} days "
            f"ago ({verified}). Re-check the AWS URLs at the top of "
            "references/engine-support-lifecycle.md, update the table, "
            "ENGINE_SUPPORT_SCHEDULE, and the `Last verified` date. AWS adds "
            "versions to this list; a stale table under-reports."
        )

    def test_only_redis_oss_has_a_scheduled_date(self):
        # Encodes the current published state. If AWS announces a Valkey or
        # Memcached date, this fails and the silence tests below need revisiting
        # rather than the schedule being extended quietly.
        assert {engine for engine, _ in ENGINE_SUPPORT_SCHEDULE} == {"redis"}


# ---------------------------------------------------------------------------
# Silence -- the check must never invent a deadline
# ---------------------------------------------------------------------------


class TestNoAnnouncedDateMeansSilence:
    @pytest.mark.parametrize("engine,version", [
        ("valkey", "7.2"),
        ("valkey", "8.0"),
        ("valkey", "8"),
        ("memcached", "1.6.22"),
        ("memcached", "1.4.34"),   # old, but still no announced date
        ("redis", "7.0"),
        ("redis", "7.1"),
        ("redis", "8.0"),
    ])
    def test_unannounced_versions_produce_nothing(self, engine, version):
        assert _sec06_extended_support(
            cluster(engine=engine, version=version), on(0)
        ) is None, (
            f"{engine} {version} has no AWS-announced end of standard support. "
            "Reporting one invents a deadline a customer would schedule real "
            "work around."
        )

    @pytest.mark.parametrize("version", [None, "", "unknown", "latest", "v6"])
    def test_unreadable_version_produces_nothing(self, version):
        # Guessing a major version here would either fabricate a deadline or
        # hide a real one. Silence is the only safe default.
        assert _sec06_extended_support(cluster(version=version), on(0)) is None

    def test_missing_engine_produces_nothing(self):
        record = cluster()
        del record["engine"]
        assert _sec06_extended_support(record, on(0)) is None

    def test_a_far_future_date_is_not_silence(self):
        # The inverse mistake: "not urgent" is not "not reportable". A dated
        # obligation is always stated, just at LOW.
        finding = _sec06_extended_support(cluster(version="6.2"), on(3650))
        assert finding is not None
        assert finding["severity"] == "LOW"

    def test_major_version_parsing(self):
        assert _major_version("6.2") == 6
        assert _major_version("8") == 8
        assert _major_version("8.0.1") == 8
        assert _major_version(None) is None
        assert _major_version("latest") is None


# ---------------------------------------------------------------------------
# Severity is graded against the injected review date
# ---------------------------------------------------------------------------


class TestSeverityGrading:
    def test_more_than_the_warning_window_out_is_low(self):
        f = _sec06_extended_support(cluster(), on(SUPPORT_END_WARNING_DAYS + 1))
        assert f["severity"] == "LOW"

    def test_exactly_the_warning_window_out_is_medium(self):
        # Boundary is inclusive: at exactly 90 days the upgrade has to start.
        f = _sec06_extended_support(cluster(), on(SUPPORT_END_WARNING_DAYS))
        assert f["severity"] == "MEDIUM"

    def test_the_day_of_end_of_support_is_still_medium(self):
        # Standard support ends *on* this date; the premium starts the day after.
        f = _sec06_extended_support(cluster(), on(0))
        assert f["severity"] == "MEDIUM"
        assert "Extended Support" not in f["title"]

    def test_the_day_after_end_of_support_is_high(self):
        f = _sec06_extended_support(cluster(), on(-1))
        assert f["severity"] == "HIGH"
        assert f["title"] == "Engine is in Extended Support (paid)"

    def test_the_same_cluster_grades_differently_at_different_dates(self):
        # The whole reason severity is a per-finding override rather than a fixed
        # registry value. One configuration, three answers.
        grades = [
            _sec06_extended_support(cluster(), on(days))["severity"]
            for days in (400, 30, -30)
        ]
        assert grades == ["LOW", "MEDIUM", "HIGH"]

    def test_every_severity_it_can_emit_is_rankable(self):
        for days in (400, 30, 0, -1, -400, -800, -5000):
            f = _sec06_extended_support(cluster(), on(days))
            assert f["severity"] in VALID_SEVERITIES


class TestPremiumYear:
    @pytest.mark.parametrize("days_past,year", [
        (1, 1),
        (364, 1),
        (366, 2),
        (740, 3),
        (5000, 3),   # capped: year 3 is the last, then the version is EOL
    ])
    def test_premium_year_advances_with_elapsed_time(self, days_past, year):
        f = _sec06_extended_support(cluster(), on(-days_past))
        expected = f"{EXTENDED_SUPPORT_PREMIUM[year]:.0%}"
        assert f"year {year}" in f["detail"]
        assert expected in f["detail"]


# ---------------------------------------------------------------------------
# What the finding may and may not say
# ---------------------------------------------------------------------------


class TestFindingContent:
    def test_the_finding_states_the_date_and_the_day_count(self):
        # So a reader can check the grading instead of trusting it.
        f = _sec06_extended_support(cluster(), on(120))
        assert ENGINE_SUPPORT_SCHEDULE[("redis", 6)] in f["detail"]
        assert "120 days" in f["detail"]

    def test_the_finding_names_the_engine_and_version_it_read(self):
        f = _sec06_extended_support(cluster(version="6.0"), on(120))
        assert "redis 6.0" in f["detail"]

    @pytest.mark.parametrize("days", [400, 30, -30, -800])
    def test_the_finding_never_states_a_dollar_figure(self, days):
        # The premium is a percentage of a region-dependent On-Demand rate this
        # stage does not have. Quoting a dollar amount would be the same defect
        # class as the hardcoded price table: a number stated rather than
        # derived. Percentages and dates only; the agent converts to money.
        f = _sec06_extended_support(cluster(), on(days))
        blob = f"{f['detail']} {f['recommendation']} {f['title']}"
        assert "$" not in blob

    def test_the_current_value_is_the_date_not_a_boolean(self):
        # Downstream renders current_value verbatim; "True" would tell a reader
        # nothing actionable.
        f = _sec06_extended_support(cluster(), on(120))
        assert f["value"] == ENGINE_SUPPORT_SCHEDULE[("redis", 6)]

    def test_the_upgrade_target_is_named(self):
        for days in (120, -30):
            f = _sec06_extended_support(cluster(), on(days))
            assert "Valkey" in f["recommendation"]


# ---------------------------------------------------------------------------
# Evaluator wiring -- the review date must be injected, never read from the clock
# ---------------------------------------------------------------------------


class TestReviewDateWiring:
    def test_sec06_declares_that_it_needs_the_review_date(self):
        assert SEC06.needs_review_date is True

    def test_the_predicate_refuses_to_run_without_a_review_date(self):
        # No default argument, deliberately. A default would let the predicate
        # fall back to the wall clock the moment the evaluator forgot to pass the
        # date, producing non-reproducible output that nothing would flag.
        with pytest.raises(TypeError):
            _sec06_extended_support(cluster())

    def test_predicate_arity_matches_its_declared_flags(self):
        # Keeps the declaration honest in the other direction: a check that
        # quietly grew a second parameter without setting the flag would be
        # called with one argument and get swallowed by the per-check error
        # handler, silently disappearing from every report. The evaluator passes
        # `today` when needs_review_date and `policy` when needs_policy, so the
        # required-arg count must be exactly 1 (cluster) plus one per flag set.
        import inspect
        for check in CHECKS:
            params = inspect.signature(check.predicate).parameters
            required = [p for p in params.values() if p.default is inspect.Parameter.empty]
            expected = 1 + int(check.needs_review_date) + int(check.needs_policy)
            assert len(required) == expected, (
                f"{check.check_id}'s predicate takes {len(required)} required "
                f"argument(s) but needs_review_date={check.needs_review_date}, "
                f"needs_policy={check.needs_policy}"
            )

    def test_the_evaluator_grades_against_the_injected_date(self):
        checker = ConfigurationChecker(today=on(-30))
        findings, _ = checker.check_cluster(cluster())
        sec06 = [f for f in findings if f["check_id"] == "SEC-06"]
        assert len(sec06) == 1
        assert sec06[0]["severity"] == "HIGH"

    def test_the_severity_override_reaches_the_finding(self):
        # The registry value is LOW; a past-EOS cluster must not be reported as
        # LOW just because that is what the Check declares.
        assert SEC06.severity == "LOW"
        findings, _ = ConfigurationChecker(today=on(-30)).check_cluster(cluster())
        assert [f["severity"] for f in findings if f["check_id"] == "SEC-06"] == ["HIGH"]

    def test_the_title_and_recommendation_overrides_reach_the_finding(self):
        findings, _ = ConfigurationChecker(today=on(-30)).check_cluster(cluster())
        f = next(f for f in findings if f["check_id"] == "SEC-06")
        assert f["title"] == "Engine is in Extended Support (paid)"
        assert f["recommendation"] != SEC06.recommendation
        assert "zero-downtime" in f["recommendation"]

    def test_the_registry_values_are_used_when_no_override_is_returned(self):
        # Every other check returns no overrides; they must be unaffected.
        findings, _ = ConfigurationChecker(today=on(0)).check_cluster(
            cluster(engine="valkey", version="8.0", tls_enabled=False)
        )
        sec01 = next(f for f in findings if f["check_id"] == "SEC-01")
        assert sec01["severity"] == "CRITICAL"
        assert sec01["title"] == "In-transit encryption (TLS) disabled"

    def test_an_unrankable_severity_override_falls_back_to_the_registry(self):
        # The scoring formula and the severity counters both key on
        # VALID_SEVERITIES, so an unknown string would either crash the run or
        # score as nothing. Fall back and log rather than emit it.
        import dataclasses
        broken = dataclasses.replace(
            SEC06, predicate=lambda c, today: {"severity": "URGENT", "detail": "x"}
        )
        checker = ConfigurationChecker(checks=(broken,), today=on(0))
        out = checker.run({"metadata": {}, "clusters": [cluster()]})
        finding = out["clusters"]["c1"]["findings"][0]
        assert finding["severity"] == "LOW"
        assert out["metadata"]["findings_by_severity"]["LOW"] == 1

    def test_the_review_date_is_recorded_in_metadata(self):
        # Without it the grading cannot be reproduced: the same fleet legitimately
        # grades differently next quarter.
        out = ConfigurationChecker(today=on(0)).run(
            {"metadata": {}, "clusters": [cluster()]}
        )
        assert out["metadata"]["review_date"] == on(0).isoformat()

    def test_a_pinned_date_makes_the_output_byte_stable(self):
        inv = {"metadata": {}, "clusters": [cluster()]}
        import json
        a = ConfigurationChecker(today=on(45)).run(inv)
        b = ConfigurationChecker(today=on(45)).run(inv)
        assert json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)

    def test_the_default_review_date_is_today_in_utc(self):
        # The production path takes no --as-of. Pinned to UTC rather than local
        # time so two engineers in different timezones get the same grading.
        expected = datetime.datetime.now(datetime.timezone.utc).date()
        out = ConfigurationChecker().run({"metadata": {}, "clusters": []})
        assert out["metadata"]["review_date"] == expected.isoformat()


class TestCLIReviewDate:
    def test_as_of_pins_the_grading(self, tmp_path):
        import json

        from check_configuration import main
        inv = tmp_path / "inventory.json"
        inv.write_text(json.dumps({"metadata": {}, "clusters": [cluster()]}))
        out = tmp_path / "config_findings.json"
        assert main(["--inventory", str(inv), "--output", str(out),
                     "--as-of", on(-30).isoformat()]) == 0
        data = json.loads(out.read_text())
        assert data["metadata"]["review_date"] == on(-30).isoformat()
        sec06 = [f for f in data["clusters"]["c1"]["findings"]
                 if f["check_id"] == "SEC-06"]
        assert sec06[0]["severity"] == "HIGH"

    def test_a_malformed_as_of_exits_nonzero(self, tmp_path):
        import json

        from check_configuration import main
        inv = tmp_path / "inventory.json"
        inv.write_text(json.dumps({"metadata": {}, "clusters": []}))
        # Silently ignoring this would grade against today while the operator
        # believed they had pinned the date.
        assert main(["--inventory", str(inv),
                     "--output", str(tmp_path / "o.json"),
                     "--as-of", "last Tuesday"]) == 1
