"""Unit tests for ResultAssembler and main() entry point (Task 9).

Tests the assemble method (flattening, counting, errors_count), atomic write,
JSON output format, directory creation, and the main() function.
"""

import json
import os
import sys

import pytest

# Add scripts to path so we can import the module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from discover_inventory import (
    AccountIdentity,
    RegionResult,
    ResultAssembler,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def assembler():
    """Create a ResultAssembler instance."""
    return ResultAssembler()


@pytest.fixture
def account():
    """Create a test AccountIdentity."""
    return AccountIdentity(
        account_id="123456789012",
        caller_arn="arn:aws:iam::123456789012:user/test-user",
    )


@pytest.fixture
def region_results_no_errors():
    """Create RegionResult list with no errors."""
    return [
        RegionResult(
            region="us-east-1",
            clusters=[
                {
                    "cluster_id": "rg-1",
                    "cluster_type": "node-based",
                    "region": "us-east-1",
                    "errors": [],
                },
                {
                    "cluster_id": "rg-2",
                    "cluster_type": "node-based",
                    "region": "us-east-1",
                    "errors": [],
                },
            ],
            errors=[],
            rg_count=2,
            serverless_count=0,
        ),
        RegionResult(
            region="eu-west-1",
            clusters=[
                {
                    "cluster_id": "sc-1",
                    "cluster_type": "serverless",
                    "region": "eu-west-1",
                    "errors": [],
                },
            ],
            errors=[],
            rg_count=0,
            serverless_count=1,
        ),
    ]


@pytest.fixture
def region_results_with_errors():
    """Create RegionResult list with both region-level and cluster-level errors."""
    return [
        RegionResult(
            region="us-east-1",
            clusters=[
                {
                    "cluster_id": "rg-1",
                    "cluster_type": "node-based",
                    "region": "us-east-1",
                    "errors": [
                        "DescribeCacheParameters failed for pg-custom: AccessDeniedException",
                        "ListTagsForResource failed: ThrottlingException",
                    ],
                },
                {
                    "cluster_id": "rg-2",
                    "cluster_type": "node-based",
                    "region": "us-east-1",
                    "errors": [],
                },
            ],
            errors=["DescribeReplicationGroups partially failed: Throttling"],
            rg_count=2,
            serverless_count=0,
        ),
        RegionResult(
            region="eu-west-1",
            clusters=[
                {
                    "cluster_id": "sc-1",
                    "cluster_type": "serverless",
                    "region": "eu-west-1",
                    "errors": ["ListTagsForResource failed: AccessDenied"],
                },
            ],
            errors=[],
            rg_count=0,
            serverless_count=1,
        ),
    ]


# ---------------------------------------------------------------------------
# Task 9.1: ResultAssembler.assemble
# ---------------------------------------------------------------------------


class TestAssemble:
    """Tests for ResultAssembler.assemble method."""

    def test_flattens_clusters_from_all_regions(
        self, assembler, account, region_results_no_errors
    ):
        """Assemble flattens all clusters into one list."""
        result = assembler.assemble(
            account, ["us-east-1", "eu-west-1"], region_results_no_errors, 10.0
        )
        assert len(result["clusters"]) == 3

    def test_metadata_total_clusters(
        self, assembler, account, region_results_no_errors
    ):
        """total_clusters equals the actual number of clusters."""
        result = assembler.assemble(
            account, ["us-east-1", "eu-west-1"], region_results_no_errors, 10.0
        )
        assert result["metadata"]["total_clusters"] == 3

    def test_metadata_node_based_count(
        self, assembler, account, region_results_no_errors
    ):
        """node_based_count sums rg_count from all regions."""
        result = assembler.assemble(
            account, ["us-east-1", "eu-west-1"], region_results_no_errors, 10.0
        )
        assert result["metadata"]["node_based_count"] == 2

    def test_metadata_serverless_count(
        self, assembler, account, region_results_no_errors
    ):
        """serverless_count sums serverless_count from all regions."""
        result = assembler.assemble(
            account, ["us-east-1", "eu-west-1"], region_results_no_errors, 10.0
        )
        assert result["metadata"]["serverless_count"] == 1

    def test_errors_count_zero_when_no_errors(
        self, assembler, account, region_results_no_errors
    ):
        """errors_count is 0 when there are no errors anywhere."""
        result = assembler.assemble(
            account, ["us-east-1", "eu-west-1"], region_results_no_errors, 10.0
        )
        assert result["metadata"]["errors_count"] == 0

    def test_errors_count_includes_region_and_cluster_errors(
        self, assembler, account, region_results_with_errors
    ):
        """errors_count counts both region-level AND per-cluster errors."""
        result = assembler.assemble(
            account, ["us-east-1", "eu-west-1"], region_results_with_errors, 10.0
        )
        # Region-level: 1 (us-east-1 has 1)
        # Cluster-level: rg-1 has 2, rg-2 has 0, sc-1 has 1 => total 3
        # Grand total: 1 + 3 = 4
        assert result["metadata"]["errors_count"] == 4

    def test_metadata_has_all_required_fields(
        self, assembler, account, region_results_no_errors
    ):
        """metadata contains all required fields per the spec."""
        result = assembler.assemble(
            account, ["us-east-1", "eu-west-1"], region_results_no_errors, 12.5
        )
        metadata = result["metadata"]
        assert metadata["account_id"] == "123456789012"
        assert (
            metadata["caller_identity_arn"]
            == "arn:aws:iam::123456789012:user/test-user"
        )
        assert metadata["regions_scanned"] == ["us-east-1", "eu-west-1"]
        assert "scan_timestamp" in metadata
        assert metadata["total_clusters"] == 3
        assert metadata["node_based_count"] == 2
        assert metadata["serverless_count"] == 1
        assert metadata["scan_duration_seconds"] == 12.5
        assert metadata["errors_count"] == 0

    def test_output_has_metadata_and_clusters_keys(
        self, assembler, account, region_results_no_errors
    ):
        """Output dict has exactly 'metadata' and 'clusters' top-level keys."""
        result = assembler.assemble(
            account, ["us-east-1", "eu-west-1"], region_results_no_errors, 10.0
        )
        assert set(result.keys()) == {"metadata", "clusters"}

    def test_scan_timestamp_is_iso8601(
        self, assembler, account, region_results_no_errors
    ):
        """scan_timestamp is in ISO 8601 format."""
        result = assembler.assemble(
            account, ["us-east-1", "eu-west-1"], region_results_no_errors, 10.0
        )
        ts = result["metadata"]["scan_timestamp"]
        # Should end with Z and contain a T separator
        assert "T" in ts
        assert ts.endswith("Z")

    def test_duration_rounded_to_two_decimals(self, assembler, account):
        """scan_duration_seconds is rounded to 2 decimal places."""
        results = [
            RegionResult(
                region="us-east-1", clusters=[], errors=[], rg_count=0, serverless_count=0
            )
        ]
        result = assembler.assemble(account, ["us-east-1"], results, 12.3456789)
        assert result["metadata"]["scan_duration_seconds"] == 12.35

    def test_empty_region_results(self, assembler, account):
        """Assemble handles empty region results gracefully."""
        result = assembler.assemble(account, ["us-east-1"], [], 5.0)
        assert result["metadata"]["total_clusters"] == 0
        assert result["metadata"]["errors_count"] == 0
        assert result["clusters"] == []


# ---------------------------------------------------------------------------
# Task 9.2: ResultAssembler.write_atomic
# ---------------------------------------------------------------------------


class TestWriteAtomic:
    """Tests for ResultAssembler.write_atomic method."""

    def test_writes_valid_json(self, assembler, tmp_path):
        """write_atomic produces a parseable JSON file."""
        output_path = str(tmp_path / "output.json")
        inventory = {"metadata": {"test": True}, "clusters": []}
        assembler.write_atomic(inventory, output_path)

        with open(output_path) as f:
            loaded = json.load(f)
        assert loaded == inventory

    def test_creates_output_directory(self, assembler, tmp_path):
        """write_atomic creates the output directory if it doesn't exist."""
        output_path = str(tmp_path / "subdir" / "deep" / "output.json")
        inventory = {"metadata": {}, "clusters": []}
        assembler.write_atomic(inventory, output_path)

        assert os.path.exists(output_path)

    def test_atomic_write_no_partial_file(self, assembler, tmp_path):
        """On success, only the final file exists (no .tmp file left behind)."""
        output_path = str(tmp_path / "output.json")
        inventory = {"metadata": {}, "clusters": []}
        assembler.write_atomic(inventory, output_path)

        # Check no .tmp files in the directory
        tmp_files = [f for f in os.listdir(tmp_path) if f.endswith(".tmp")]
        assert len(tmp_files) == 0

    def test_overwrites_existing_file(self, assembler, tmp_path):
        """write_atomic overwrites an existing file atomically."""
        output_path = str(tmp_path / "output.json")

        # Write initial content
        with open(output_path, "w") as f:
            json.dump({"old": True}, f)

        # Overwrite with new content
        new_inventory = {"metadata": {"new": True}, "clusters": []}
        assembler.write_atomic(new_inventory, output_path)

        with open(output_path) as f:
            loaded = json.load(f)
        assert loaded == new_inventory

    def test_json_uses_indent_2(self, assembler, tmp_path):
        """Output JSON is indented with 2 spaces for readability."""
        output_path = str(tmp_path / "output.json")
        inventory = {"metadata": {"key": "value"}, "clusters": []}
        assembler.write_atomic(inventory, output_path)

        with open(output_path) as f:
            content = f.read()
        # Verify indentation (2 spaces)
        assert '  "metadata"' in content

    def test_json_serializes_datetimes(self, assembler, tmp_path):
        """Output JSON handles datetime objects via default=str."""
        from datetime import datetime, timezone

        output_path = str(tmp_path / "output.json")
        dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        inventory = {"metadata": {"timestamp": dt}, "clusters": []}
        assembler.write_atomic(inventory, output_path)

        with open(output_path) as f:
            loaded = json.load(f)
        assert "2024-01-15" in loaded["metadata"]["timestamp"]

    def test_write_atomic_relative_path(self, assembler, tmp_path, monkeypatch):
        """write_atomic handles relative output paths correctly."""
        monkeypatch.chdir(tmp_path)
        output_path = "inventory.json"
        inventory = {"metadata": {}, "clusters": []}
        assembler.write_atomic(inventory, output_path)

        assert os.path.exists(os.path.join(str(tmp_path), output_path))


# ---------------------------------------------------------------------------
# Task 9.5: main() function
# ---------------------------------------------------------------------------


class TestMain:
    """Tests for the main() entry point."""

    def test_main_exists_as_callable(self):
        """main is defined as a top-level callable function."""
        from discover_inventory import main

        assert callable(main)

    def test_if_name_main_calls_main(self):
        """The if __name__ == '__main__' block calls main()."""
        import inspect

        import discover_inventory

        source = inspect.getsource(discover_inventory)
        assert 'if __name__ == "__main__"' in source or "if __name__ == '__main__'" in source
        assert "main()" in source
