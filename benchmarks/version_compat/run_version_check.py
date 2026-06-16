"""Verify gcamreader against a single GCAM output database version.

This harness opens a :class:`gcamreader.LocalDBConn` against one GCAM output
database, lists its scenarios, and runs a benchmark query, timing each stage.
Failures are captured as status codes in a JSON record rather than raised, so a
single bad database cannot abort a Slurm array of many versions.

Usage::

    python run_version_check.py \
        --version gcam-v8.2 \
        --db-root /rcfs/projects/GCAM/gcam-ci-run \
        --basex-dir database_basexdbGCAM \
        --query queries/land_allocation.xml \
        --out results/per_version \
        --max-memory 16g

Status codes written to the JSON record:

- ``PASS``         connect + scenario list + benchmark query all returned data
- ``MISSING``      no ``*.basex`` files found under the expected directory
- ``CONNECT_FAIL`` ``LocalDBConn`` construction raised
- ``SCENARIO_FAIL````listScenariosInDB`` raised or returned ``None``
- ``QUERY_FAIL``   ``runQuery`` raised (e.g., schema/XQuery incompatibility)
- ``QUERY_EMPTY``  query ran but returned no rows (possible schema drift)
"""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import time
import traceback
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import gcamreader


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


def timed(fn: Callable[[], Any]) -> tuple[Any, float, str | None]:
    """Run ``fn`` and capture its result, elapsed time, and any error.

    Args:
        fn: A zero-argument callable to execute and time.

    Returns:
        A tuple ``(result, elapsed_seconds, error)`` where ``result`` is the
        return value of ``fn`` (or ``None`` on failure), ``elapsed_seconds`` is
        the wall-clock duration, and ``error`` is a formatted traceback string
        on failure or ``None`` on success.
    """
    start = time.perf_counter()
    try:
        result = fn()
        return result, time.perf_counter() - start, None
    except Exception:  # noqa: BLE001 - failures are recorded as data, not raised
        return None, time.perf_counter() - start, traceback.format_exc()


def _write(out_dir: Path, version: str, record: dict) -> None:
    """Write a result record to ``<out_dir>/<version>.json``.

    Args:
        out_dir: Directory in which to write the JSON record.
        version: The GCAM version label (used as the filename stem).
        record: The result record to serialize.
    """
    path = out_dir / f"{version}.json"
    path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    print(f"[{record['status']}] {version} -> {path}")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        The parsed argument namespace.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--version",
        required=True,
        help="GCAM version label, e.g. gcam-v8.2",
    )
    parser.add_argument(
        "--db-root",
        required=True,
        help="root containing <version>/output directories",
    )
    parser.add_argument(
        "--basex-dir",
        default="database_basexdbGCAM",
        help="name of the BaseX database directory (parent of *.basex files)",
    )
    parser.add_argument(
        "--query",
        required=True,
        help="path to the benchmark query XML file",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="directory in which to write the per-version JSON record",
    )
    parser.add_argument(
        "--max-memory",
        default="16g",
        help="maximum Java heap (passed as -Xmx); keep <= Slurm --mem",
    )
    return parser.parse_args()


def main() -> None:
    """Run the single-version compatibility check and write a JSON record."""
    args = parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    db_parent = Path(args.db_root) / args.version / "output"
    basex_path = db_parent / args.basex_dir

    record: dict[str, Any] = {
        "version": args.version,
        "db_parent": str(db_parent),
        "basex_dir": args.basex_dir,
        "query_file": str(args.query),
        "max_memory": args.max_memory,
        "generated_at": datetime.now(UTC).isoformat(),
        "python_version": platform.python_version(),
        "java_version": java_version(),
        "gcamreader_version": getattr(gcamreader, "__version__", "unknown"),
        "platform": platform.platform(),
        "status": "UNKNOWN",
        "timings_sec": {},
        "scenario_count": None,
        "scenarios": None,
        "query_rows": None,
        "query_cols": None,
        "errors": {},
    }

    # Stage 0: existence check.
    if not basex_path.is_dir() or not list(basex_path.glob("*.basex")):
        record["status"] = "MISSING"
        record["errors"]["existence"] = f"no *.basex under {basex_path}"
        _write(out_dir, args.version, record)
        return

    # Stage 1: connect. We use validatedb=False so each stage is timed
    # independently and a failure becomes recorded data rather than an
    # exception that would terminate the Slurm task.
    conn, t_connect, err = timed(
        lambda: gcamreader.LocalDBConn(
            str(db_parent),
            args.basex_dir,
            suppress_gabble=True,
            maxMemory=args.max_memory,
            validatedb=False,
        )
    )
    record["timings_sec"]["connect"] = round(t_connect, 4)
    if err is not None:
        record["status"] = "CONNECT_FAIL"
        record["errors"]["connect"] = err
        _write(out_dir, args.version, record)
        return

    # Stage 2: list scenarios.
    scen_df, t_scen, err = timed(conn.listScenariosInDB)
    record["timings_sec"]["list_scenarios"] = round(t_scen, 4)
    if err is not None or scen_df is None:
        record["status"] = "SCENARIO_FAIL"
        record["errors"]["list_scenarios"] = err or "returned None"
        _write(out_dir, args.version, record)
        return
    record["scenario_count"] = int(len(scen_df))
    record["scenarios"] = scen_df["name"].tolist()

    # Stage 3: run the benchmark query.
    queries = gcamreader.parse_batch_query(args.query)
    query = queries[0]
    df, t_query, err = timed(lambda: conn.runQuery(query, warn_empty=False))
    record["timings_sec"]["run_query"] = round(t_query, 4)
    if err is not None:
        record["status"] = "QUERY_FAIL"
        record["errors"]["run_query"] = err
        _write(out_dir, args.version, record)
        return
    if df is None or len(df) == 0:
        record["status"] = "QUERY_EMPTY"
        _write(out_dir, args.version, record)
        return

    record["query_rows"] = int(len(df))
    record["query_cols"] = list(df.columns)
    record["status"] = "PASS"
    _write(out_dir, args.version, record)


if __name__ == "__main__":
    main()
