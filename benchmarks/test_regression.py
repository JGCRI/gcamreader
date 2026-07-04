"""Regression tests that lock gcamreader behavior to the v1.4.0 baseline.

These tests re-run the operations captured by
:mod:`benchmarks.generate_baseline` and assert that the results are identical
to the committed reference fixtures in ``benchmarks/baseline/``. They form the
gate for the v1.5.0 modernization: the modernized package must continue to
reproduce the v1.4.0 outputs exactly.

The tests require Java and the bundled sample BaseX database. When Java is not
available, the database-backed tests are skipped (the structural
``parse_batch_query`` test still runs).
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

import gcamreader

DATA_DIR = Path(gcamreader.sample_data_dir())
SAMPLE_DB_NAME = "sample_basexdb"
LAND_QUERY = DATA_DIR / "queries" / "query_land_reg32_basin235_gcam5p0.xml"

BASELINE_DIR = Path(__file__).resolve().parent / "baseline"
QUERY_OUTPUT_DIR = BASELINE_DIR / "query_outputs"
CLI_OUTPUT_DIR = BASELINE_DIR / "cli_outputs"
MANIFEST_PATH = BASELINE_DIR / "manifest.json"

LAND_SORT_COLUMNS = ["region", "land-allocation", "Year"]

java_available = shutil.which("java") is not None
requires_java = pytest.mark.skipif(
    not java_available, reason="Java runtime not available"
)
requires_baseline = pytest.mark.skipif(
    not MANIFEST_PATH.exists(),
    reason="baseline not generated; run benchmarks/generate_baseline.py",
)


def _sha256(path: Path) -> str:
    """Return the hex SHA-256 digest of a file.

    Args:
        path: File to hash.

    Returns:
        Hexadecimal SHA-256 digest.
    """
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


@pytest.fixture(scope="module")
def manifest() -> dict:
    """Load the baseline manifest.

    Returns:
        The parsed manifest dictionary.
    """
    return json.loads(MANIFEST_PATH.read_text())


@pytest.fixture(scope="module")
def connection() -> gcamreader.LocalDBConn:
    """Create a local database connection to the bundled sample database.

    Returns:
        An initialized :class:`gcamreader.LocalDBConn`.
    """
    return gcamreader.LocalDBConn(str(DATA_DIR), SAMPLE_DB_NAME, suppress_gabble=True)


@requires_baseline
def test_parse_batch_query_matches_baseline() -> None:
    """parse_batch_query structure must match the recorded baseline."""
    expected = json.loads((BASELINE_DIR / "parse_batch_query.json").read_text())
    queries = gcamreader.parse_batch_query(str(LAND_QUERY))

    assert len(queries) == expected["count"]
    actual = [
        {"title": q.title, "regions": q.regions, "querystr": q.querystr}
        for q in queries
    ]
    assert actual == expected["queries"]


@requires_baseline
@requires_java
def test_list_scenarios_matches_baseline(
    connection: gcamreader.LocalDBConn,
) -> None:
    """listScenariosInDB output must match the recorded baseline."""
    expected = pd.read_csv(QUERY_OUTPUT_DIR / "list_scenarios.csv")
    actual = connection.listScenariosInDB()
    pd.testing.assert_frame_equal(
        actual.reset_index(drop=True), expected.reset_index(drop=True)
    )


@requires_baseline
@requires_java
def test_land_query_matches_baseline(
    connection: gcamreader.LocalDBConn,
) -> None:
    """runQuery for the land-allocation query must match the baseline."""
    expected = pd.read_csv(QUERY_OUTPUT_DIR / "land_query.csv")
    query = gcamreader.parse_batch_query(str(LAND_QUERY))[0]
    actual = connection.runQuery(query).sort_values(
        by=LAND_SORT_COLUMNS, ignore_index=True
    )
    expected = expected.sort_values(by=LAND_SORT_COLUMNS, ignore_index=True)
    pd.testing.assert_frame_equal(actual, expected)


@requires_baseline
@requires_java
def test_importdata_matches_baseline() -> None:
    """importdata results must match the recorded baseline."""
    expected = pd.read_csv(QUERY_OUTPUT_DIR / "importdata_land.csv")
    results = gcamreader.importdata(str(DATA_DIR / SAMPLE_DB_NAME), str(LAND_QUERY))
    title = next(iter(results))
    actual = results[title].sort_values(by=LAND_SORT_COLUMNS, ignore_index=True)
    expected = expected.sort_values(by=LAND_SORT_COLUMNS, ignore_index=True)
    pd.testing.assert_frame_equal(actual, expected)


@requires_baseline
@requires_java
def test_cli_output_matches_baseline(tmp_path: Path, manifest: dict) -> None:
    """CLI local-query CSV output must match the baseline byte-for-byte."""
    subprocess.run(
        [
            sys.executable,
            "-m",
            "gcamreader",
            "local",
            "-d",
            str(DATA_DIR / SAMPLE_DB_NAME),
            "-q",
            str(LAND_QUERY),
            "-o",
            str(tmp_path),
            "-f",
        ],
        check=True,
    )
    produced = sorted(tmp_path.glob("*.csv"))
    assert produced, "CLI produced no CSV output"

    for csv_file in produced:
        key = f"cli_outputs/{csv_file.name}"
        assert key in manifest["fixtures"], f"unexpected CLI output {csv_file.name}"
        assert _sha256(csv_file) == manifest["fixtures"][key]
