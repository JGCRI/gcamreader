"""Aggregate per-version JSON results into a CSV summary and markdown report.

Run this once after the Slurm array completes. It merges every per-version JSON
record produced by :mod:`run_version_check` into a tidy CSV and a human-readable
``VERIFIED_VERSIONS.md`` containing a status matrix and per-stage timing
figures.

Usage::

    python aggregate_results.py \
        --in results/per_version \
        --out results
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pandas as pd


def version_sort_key(label: str) -> tuple[int, ...]:
    """Return a natural-sort key for a GCAM version label.

    Splits a label such as ``gcam-v8.10`` into numeric components so that it
    sorts after ``gcam-v8.2`` rather than before it (which a naive string or
    float sort would get wrong).

    Args:
        label: A GCAM version label, e.g. ``"gcam-v8.10"``.

    Returns:
        A tuple of integers extracted from the label.
    """
    return tuple(int(n) for n in re.findall(r"\d+", label))


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        The parsed argument namespace.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--in",
        dest="indir",
        required=True,
        help="directory containing the per-version JSON records",
    )
    parser.add_argument(
        "--out",
        dest="outdir",
        required=True,
        help="directory in which to write the CSV and markdown report",
    )
    return parser.parse_args()


def build_dataframe(records: list[dict]) -> pd.DataFrame:
    """Flatten per-version records into a sorted summary DataFrame.

    Args:
        records: The loaded per-version JSON records.

    Returns:
        A DataFrame with one row per version, sorted by natural version order.
    """
    rows = []
    for r in records:
        t = r.get("timings_sec", {})
        rows.append(
            {
                "version": r["version"],
                "status": r["status"],
                "scenario_count": r.get("scenario_count"),
                "query_rows": r.get("query_rows"),
                "connect_s": t.get("connect"),
                "list_scenarios_s": t.get("list_scenarios"),
                "run_query_s": t.get("run_query"),
                "gcamreader_version": r.get("gcamreader_version"),
                "python_version": r.get("python_version"),
                "max_memory": r.get("max_memory"),
            }
        )

    df = pd.DataFrame(rows)
    df = df.sort_values(by="version", key=lambda s: s.map(version_sort_key))
    return df.reset_index(drop=True)


def write_markdown(df: pd.DataFrame, records: list[dict], out_dir: Path) -> Path:
    """Write the ``VERIFIED_VERSIONS.md`` status/timing report.

    Args:
        df: The summary DataFrame from :func:`build_dataframe`.
        records: The loaded per-version JSON records (for header metadata).
        out_dir: Directory in which to write the markdown file.

    Returns:
        The path to the written markdown file.
    """
    java_full = records[0].get("java_version", "unknown")
    java = java_full.splitlines()[0] if java_full else "unknown"
    n_pass = int((df["status"] == "PASS").sum())

    lines = [
        "# Verified GCAM Versions",
        "",
        f"- gcamreader: `{records[0].get('gcamreader_version')}`",
        f"- Python: `{records[0].get('python_version')}`",
        f"- Java: `{java}`",
        f"- Versions passing: **{n_pass} / {len(df)}**",
        "",
        "| Version | Status | Scenarios | Query rows | Connect (s) | "
        "Scenarios (s) | Query (s) |",
        "|---|---|---|---|---|---|---|",
    ]
    for _, row in df.iterrows():
        lines.append(
            f"| {row['version']} | {row['status']} | "
            f"{row['scenario_count']} | {row['query_rows']} | "
            f"{row['connect_s']} | {row['list_scenarios_s']} | "
            f"{row['run_query_s']} |"
        )
    lines.append("")

    md_path = out_dir / "VERIFIED_VERSIONS.md"
    md_path.write_text("\n".join(lines))
    return md_path


def main() -> None:
    """Aggregate per-version JSON records into a CSV and markdown report."""
    args = parse_args()

    records = [
        json.loads(path.read_text())
        for path in sorted(Path(args.indir).glob("*.json"))
    ]
    if not records:
        raise SystemExit(f"no JSON files found in {args.indir}")

    df = build_dataframe(records)

    out = Path(args.outdir)
    out.mkdir(parents=True, exist_ok=True)

    csv_path = out / "version_compat_summary.csv"
    df.to_csv(csv_path, index=False)
    print(f"wrote {csv_path}")

    md_path = write_markdown(df, records, out)
    print(f"wrote {md_path}")


if __name__ == "__main__":
    main()
