"""Generate the v1.4.0 behavioral baseline for gcamreader.

This script exercises the full public surface of the *current* (pre-1.5.0)
``gcamreader`` package against the bundled sample BaseX database and writes the
results to ``benchmarks/baseline/`` as reference fixtures. A ``manifest.json``
records the package version, Python version, Java version, and a SHA-256
checksum for every fixture so later changes can be proven identical.

Run this once, against the unmodified v1.4.0 code, before any refactoring:

    python benchmarks/generate_baseline.py

The companion :mod:`benchmarks.test_regression` test re-runs the same
operations and asserts equality against these fixtures.
"""

from __future__ import annotations

import hashlib
import json
import platform
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

import gcamreader

# ---------------------------------------------------------------------------
# Locate the bundled sample data regardless of how the package was installed.
# The current package stores test data under gcamreader/tests/data.
# ---------------------------------------------------------------------------
DATA_DIR = Path(gcamreader.sample_data_dir())
SAMPLE_DB_NAME = "sample_basexdb"
LAND_QUERY = DATA_DIR / "queries" / "query_land_reg32_basin235_gcam5p0.xml"

BASELINE_DIR = Path(__file__).resolve().parent / "baseline"
QUERY_OUTPUT_DIR = BASELINE_DIR / "query_outputs"
CLI_OUTPUT_DIR = BASELINE_DIR / "cli_outputs"

# Stable sort keys so row order never drives a false mismatch.
LAND_SORT_COLUMNS = ["region", "land-allocation", "Year"]


def sha256_of_file(path: Path) -> str:
    """Return the hex SHA-256 digest of a file.

    Args:
        path: Path to the file to hash.

    Returns:
        The hexadecimal SHA-256 digest string.
    """
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def java_version() -> str:
    """Return the local ``java -version`` output, or a marker if unavailable.

    Returns:
        The captured Java version string, or ``"unavailable"`` when Java cannot
        be invoked.
    """
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return "unavailable"
    # java -version writes to stderr.
    return (result.stderr or result.stdout).strip()


def write_dataframe(df: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame to CSV deterministically.

    Args:
        df: The DataFrame to serialize.
        path: Destination CSV path.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def capture_parse_batch_query() -> dict:
    """Capture the structural output of :func:`gcamreader.parse_batch_query`.

    Returns:
        A JSON-serializable dict describing each parsed query (title, regions,
        and the exact query string).
    """
    queries = gcamreader.parse_batch_query(str(LAND_QUERY))
    return {
        "source": LAND_QUERY.name,
        "count": len(queries),
        "queries": [
            {
                "title": query.title,
                "regions": query.regions,
                "querystr": query.querystr,
            }
            for query in queries
        ],
    }


def capture_scenarios(conn: gcamreader.LocalDBConn) -> pd.DataFrame:
    """Capture :meth:`LocalDBConn.listScenariosInDB` output.

    Args:
        conn: An initialized local database connection.

    Returns:
        The scenarios DataFrame.
    """
    return conn.listScenariosInDB()


def capture_land_query(conn: gcamreader.LocalDBConn) -> pd.DataFrame:
    """Run the bundled land-allocation query and return a sorted DataFrame.

    Args:
        conn: An initialized local database connection.

    Returns:
        The query result sorted by the stable land sort columns.
    """
    query = gcamreader.parse_batch_query(str(LAND_QUERY))[0]
    df = conn.runQuery(query)
    return df.sort_values(by=LAND_SORT_COLUMNS, ignore_index=True)


def capture_importdata() -> pd.DataFrame:
    """Capture :func:`gcamreader.importdata` results for the land query.

    Returns:
        The single result DataFrame keyed by the land query title, sorted by
        the stable land sort columns.
    """
    dbspec = str(DATA_DIR / SAMPLE_DB_NAME)
    results = gcamreader.importdata(dbspec, str(LAND_QUERY))
    title = next(iter(results))
    df = results[title]
    return df.sort_values(by=LAND_SORT_COLUMNS, ignore_index=True)


def capture_cli() -> list[Path]:
    """Run the current CLI against the sample DB and capture the output CSVs.

    Returns:
        A list of paths to the CSV files the CLI produced.
    """
    CLI_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    # Clear stale outputs so the manifest reflects only this run.
    for stale in CLI_OUTPUT_DIR.glob("*.csv"):
        stale.unlink()
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
            str(CLI_OUTPUT_DIR),
            "-f",
        ],
        check=True,
    )
    return sorted(CLI_OUTPUT_DIR.glob("*.csv"))


def main() -> None:
    """Generate all baseline fixtures and write the manifest."""
    BASELINE_DIR.mkdir(parents=True, exist_ok=True)
    QUERY_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    fixtures: dict[str, str] = {}

    # 1. parse_batch_query structure.
    parsed = capture_parse_batch_query()
    parsed_path = BASELINE_DIR / "parse_batch_query.json"
    parsed_path.write_text(json.dumps(parsed, indent=2, sort_keys=True) + "\n")
    fixtures[parsed_path.name] = sha256_of_file(parsed_path)
    print(f"wrote {parsed_path.name}")

    # Establish a connection (validation also exercises listScenariosInDB).
    conn = gcamreader.LocalDBConn(str(DATA_DIR), SAMPLE_DB_NAME, suppress_gabble=True)

    # 2. listScenariosInDB.
    scenarios = capture_scenarios(conn)
    scenarios_path = QUERY_OUTPUT_DIR / "list_scenarios.csv"
    write_dataframe(scenarios, scenarios_path)
    fixtures[f"query_outputs/{scenarios_path.name}"] = sha256_of_file(scenarios_path)
    print(f"wrote {scenarios_path.name} shape={scenarios.shape}")

    # 3. runQuery for the land-allocation query.
    land = capture_land_query(conn)
    land_path = QUERY_OUTPUT_DIR / "land_query.csv"
    write_dataframe(land, land_path)
    fixtures[f"query_outputs/{land_path.name}"] = sha256_of_file(land_path)
    print(f"wrote {land_path.name} shape={land.shape}")

    # 4. importdata results.
    imported = capture_importdata()
    imported_path = QUERY_OUTPUT_DIR / "importdata_land.csv"
    write_dataframe(imported, imported_path)
    fixtures[f"query_outputs/{imported_path.name}"] = sha256_of_file(imported_path)
    print(f"wrote {imported_path.name} shape={imported.shape}")

    # 5. CLI outputs.
    cli_files = capture_cli()
    for cli_file in cli_files:
        fixtures[f"cli_outputs/{cli_file.name}"] = sha256_of_file(cli_file)
        print(f"wrote cli_outputs/{cli_file.name}")

    manifest = {
        "generated_at": datetime.now(UTC).isoformat(),
        "gcamreader_version": getattr(gcamreader, "__version__", "1.4.0"),
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "java_version": java_version(),
        "sort_columns": {"land_query": LAND_SORT_COLUMNS},
        "fixtures": fixtures,
    }
    manifest_path = BASELINE_DIR / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(f"wrote {manifest_path.name} with {len(fixtures)} fixtures")


if __name__ == "__main__":
    main()
