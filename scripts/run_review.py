#!/usr/bin/env python3
"""
ElastiCache Operations Review — Data Collection Pipeline

Runs Stages 1-3.5 (discovery, metrics, statistics, configuration checks) and
produces JSON outputs for the AI agent to analyze. The agent handles
interpretation, prioritization, remediation advice, and presentation.

Usage:
  # Collect data for all clusters in a region:
  python3 scripts/run_review.py --regions us-east-1

  # Specific clusters only:
  python3 scripts/run_review.py --regions us-east-1 --replication-groups my-rg-1 my-rg-2

  # Multi-region:
  python3 scripts/run_review.py --regions us-east-1 eu-west-1

  # With specific profile:
  python3 scripts/run_review.py --regions us-east-1 --profile readonly-prod

  # Skip cost data:
  python3 scripts/run_review.py --regions us-east-1 --skip-cost

Output: inventory.json, metrics.json, analysis.json, config_findings.json in the
output directory. The AI agent reads these to produce the assessment.
"""

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

logger = logging.getLogger("run_review")

SCRIPT_DIR = Path(__file__).resolve().parent


def setup_logging(verbose: bool) -> None:
    """Configure logging with timestamps."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def parse_args(argv=None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "ElastiCache Operations Review — data collection pipeline. "
            "Runs discovery, metrics collection, statistical analysis, and "
            "configuration checks. Produces JSON outputs for the AI agent to "
            "interpret and present."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --regions us-east-1
  %(prog)s --regions us-east-1 eu-west-1
  %(prog)s --regions us-east-1 --replication-groups prod-session prod-cache
  %(prog)s --regions us-east-1 --output ./reports/ --verbose
        """,
    )

    # Required
    parser.add_argument(
        "--regions",
        nargs="+",
        required=True,
        help="AWS region(s) to scan (e.g., us-east-1 eu-west-1).",
    )

    # Scope filters
    parser.add_argument(
        "--replication-groups",
        nargs="+",
        default=None,
        help="Specific replication group IDs to review (default: all).",
    )
    parser.add_argument(
        "--serverless-caches",
        nargs="+",
        default=None,
        help="Specific serverless cache names to review (default: all).",
    )

    # Pipeline options
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Metric lookback period in days (default: 14).",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="AWS CLI profile name.",
    )
    parser.add_argument(
        "--output",
        default="./output",
        help="Output directory for all artifacts (default: ./output/).",
    )
    parser.add_argument(
        "--skip-cost",
        action="store_true",
        help="Skip Cost Explorer queries.",
    )
    parser.add_argument(
        "--as-of",
        default=None,
        metavar="YYYY-MM-DD",
        help=(
            "Review date used to grade time-sensitive checks (SEC-06 engine "
            "end-of-support). Defaults to today (UTC). Pin it to reproduce an "
            "earlier review's grading exactly."
        ),
    )

    # Policy overrides (Stage 3.5) — customer-specific standards. Omit to use the
    # Well-Architected defaults; the effective values are recorded in
    # config_findings.json's metadata.policy so the report can state what it graded.
    parser.add_argument(
        "--required-tags",
        nargs="*",
        default=None,
        metavar="TAG",
        help=(
            "Tag keys OE-04 requires on every cluster (default: Environment Owner "
            "Application). Pass your tagging standard; pass with no values to "
            "require none."
        ),
    )
    parser.add_argument(
        "--min-replicas-per-shard",
        type=int,
        default=None,
        metavar="N",
        help="Replica floor REL-03 grades against (default: 2).",
    )
    parser.add_argument(
        "--min-snapshot-retention-days",
        type=int,
        default=None,
        metavar="DAYS",
        help="Backup-retention floor REL-04b grades against (default: 7).",
    )

    # General
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable detailed logging.",
    )

    return parser.parse_args(argv)


def run_stage(name: str, cmd: list[str], verbose: bool) -> bool:
    """Run a pipeline stage as a subprocess.

    Args:
        name: Human-readable stage name for logging.
        cmd: Command and arguments to execute.
        verbose: Whether to stream stdout/stderr.

    Returns:
        True if stage succeeded, False otherwise.
    """
    logger.info("━━━ %s ━━━", name)
    logger.debug("Command: %s", " ".join(cmd))

    start = time.time()
    try:
        # Safe: cmd is a list (no shell=True), built from sys.executable + this
        # repo's own stage scripts + the operator's CLI args. Nothing is shell-
        # interpreted, so a non-literal command is not a command-injection vector.
        result = subprocess.run(  # nosemgrep: dangerous-subprocess-use-audit  # nosec B603
            cmd,
            capture_output=not verbose,
            text=True,
            cwd=str(SCRIPT_DIR.parent),
        )
        elapsed = time.time() - start

        if result.returncode != 0:
            logger.error("%s failed (exit code %d, %.1fs)", name, result.returncode, elapsed)
            if not verbose and result.stderr:
                # Show last 20 lines of stderr
                lines = result.stderr.strip().split("\n")
                for line in lines[-20:]:
                    logger.error("  %s", line)
            return False

        logger.info("✓ %s completed (%.1fs)", name, elapsed)
        return True

    except FileNotFoundError:
        logger.error("%s: python3 not found", name)
        return False
    except Exception as e:
        logger.error("%s: unexpected error: %s", name, e)
        return False


def main(argv=None) -> int:
    """Run the full ElastiCache Operations Review pipeline."""
    args = parse_args(argv)
    setup_logging(args.verbose)

    # Resolve output directory to absolute path before building child commands
    output_dir = Path(args.output).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    # Output file paths
    inventory_path = str(output_dir / "inventory.json")
    metrics_path = str(output_dir / "metrics.json")
    analysis_path = str(output_dir / "analysis.json")
    config_path = str(output_dir / "config_findings.json")
    report_data_path = str(output_dir / "report_data.json")

    python = sys.executable  # Use same Python interpreter

    logger.info("ElastiCache Operations Review")
    logger.info("Regions: %s", ", ".join(args.regions))
    logger.info("Output:  %s", output_dir.resolve())
    if args.replication_groups:
        logger.info("Scope:   %s", ", ".join(args.replication_groups))
    logger.info("")

    pipeline_start = time.time()

    # ── Stage 1: Inventory Discovery ──
    cmd = [
        python, str(SCRIPT_DIR / "discover_inventory.py"),
        "--regions", *args.regions,
        "--output", inventory_path,
    ]
    if args.replication_groups:
        cmd.extend(["--replication-groups", *args.replication_groups])
    if args.serverless_caches:
        cmd.extend(["--serverless-caches", *args.serverless_caches])
    if args.profile:
        cmd.extend(["--profile", args.profile])
    if args.verbose:
        cmd.append("--verbose")

    if not run_stage("Stage 1: Inventory Discovery", cmd, args.verbose):
        return 1

    # Verify inventory was created
    if not Path(inventory_path).exists():
        logger.error("Inventory file not found at %s", inventory_path)
        return 1

    # ── Stage 2: Metrics Collection ──
    cmd = [
        python, str(SCRIPT_DIR / "fetch_metrics.py"),
        "--inventory", inventory_path,
        "--output", metrics_path,
        "--days", str(args.days),
    ]
    if args.profile:
        cmd.extend(["--profile", args.profile])
    if args.skip_cost:
        cmd.append("--skip-cost")
    if args.verbose:
        cmd.append("--verbose")

    if not run_stage("Stage 2: Metrics Collection", cmd, args.verbose):
        return 1

    # ── Stage 3: Metrics Analysis ──
    cmd = [
        python, str(SCRIPT_DIR / "analyze_metrics.py"),
        "--metrics", metrics_path,
        "--inventory", inventory_path,
        "--output", analysis_path,
    ]
    if args.verbose:
        cmd.append("--verbose")

    if not run_stage("Stage 3: Metrics Analysis", cmd, args.verbose):
        return 1

    # ── Stage 3.5: Configuration Checks ──
    # Reads inventory only — no AWS calls, no metrics. Independent of Stage 3,
    # so a metrics failure must not cost us the security and reliability checks.
    cmd = [
        python, str(SCRIPT_DIR / "check_configuration.py"),
        "--inventory", inventory_path,
        "--output", config_path,
    ]
    if args.as_of:
        cmd += ["--as-of", args.as_of]
    if args.required_tags is not None:
        # Pass through even when empty ("require no tags"); argparse nargs="*"
        # preserves the distinction from the flag being absent (WA default).
        cmd += ["--required-tags", *args.required_tags]
    if args.min_replicas_per_shard is not None:
        cmd += ["--min-replicas-per-shard", str(args.min_replicas_per_shard)]
    if args.min_snapshot_retention_days is not None:
        cmd += ["--min-snapshot-retention-days", str(args.min_snapshot_retention_days)]
    if args.verbose:
        cmd.append("--verbose")

    if not run_stage("Stage 3.5: Configuration Checks", cmd, args.verbose):
        return 1

    # ── Report data reduction (Phase 9a) ──
    # Reduce the raw metrics into the small report_data.json the renderer reads,
    # so the render step never loads the multi-GB metrics.json. Depends only on
    # Stage 2's metrics; placed here so an ordinary run always leaves an
    # output/report_data.json ready for generate_html_report.py.
    cmd = [
        python, str(SCRIPT_DIR / "generate_html_report.py"),
        "--metrics", metrics_path,
        "--emit-report-data", report_data_path,
    ]
    if not run_stage("Report data reduction", cmd, args.verbose):
        return 1

    # ── Summary ──
    total_elapsed = time.time() - pipeline_start
    logger.info("")
    logger.info("═══ Data Collection Complete (%.1fs) ═══", total_elapsed)
    logger.info("")
    logger.info("Outputs (ready for agent analysis):")
    logger.info("  Inventory:  %s", inventory_path)
    logger.info("  Metrics:    %s", metrics_path)
    logger.info("  Analysis:   %s", analysis_path)
    logger.info("  Config:     %s", config_path)
    logger.info("  ReportData: %s", report_data_path)
    logger.info("")
    logger.info("The AI agent will now read these files to produce the assessment.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
