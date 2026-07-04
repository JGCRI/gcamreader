"""Generate diagnostic figures for the GCAM version-compatibility results.

This script reads ``results/version_compat_summary.csv`` and renders a
self-contained SVG bar/step chart of the benchmark query's row count across
GCAM versions. It deliberately avoids matplotlib so the figure can be
regenerated anywhere (including a minimal docs-build environment) using only
the standard library plus pandas, which is already a project dependency.

Usage::

    python plot_query_rows.py \
        --csv results/version_compat_summary.csv \
        --out ../../docs/_static/query_rows_by_version.svg
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

# Visual constants for the SVG canvas.
WIDTH = 920
HEIGHT = 460
MARGIN_LEFT = 90
MARGIN_RIGHT = 30
MARGIN_TOP = 60
MARGIN_BOTTOM = 110
BAR_COLOR = "#2a7ae2"
BAR_COLOR_NEW = "#e2662a"  # bars that introduce a new row-count tier
AXIS_COLOR = "#444444"
GRID_COLOR = "#dddddd"
TEXT_COLOR = "#222222"


def version_sort_key(label: str) -> tuple[int, ...]:
    """Return a natural-sort key for a GCAM version label.

    Args:
        label: A GCAM version label, e.g. ``"gcam-v8.10"``.

    Returns:
        A tuple of integers extracted from the label so that ``v8.10`` sorts
        after ``v8.2``.
    """
    return tuple(int(n) for n in re.findall(r"\d+", label))


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        The parsed argument namespace.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv",
        required=True,
        help="path to version_compat_summary.csv",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="path to the output SVG file",
    )
    return parser.parse_args()


def load_passing(csv_path: Path) -> pd.DataFrame:
    """Load the summary CSV and return only passing versions, sorted.

    Args:
        csv_path: Path to the summary CSV.

    Returns:
        A DataFrame of passing versions with an integer ``query_rows`` column,
        ordered by natural version order.
    """
    df = pd.read_csv(csv_path)
    df = df[df["status"] == "PASS"].copy()
    df["query_rows"] = df["query_rows"].astype(int)
    df = df.sort_values(by="version", key=lambda s: s.map(version_sort_key))
    return df.reset_index(drop=True)


def render_svg(df: pd.DataFrame) -> str:
    """Render the row-count-by-version bar chart as an SVG string.

    Bars that introduce a new (higher) row-count tier are highlighted so the
    "steps" in land-allocation detail are visually obvious.

    Args:
        df: The passing-versions DataFrame from :func:`load_passing`.

    Returns:
        The complete SVG document as a string.
    """
    versions = df["version"].tolist()
    rows = df["query_rows"].tolist()
    n = len(versions)

    plot_w = WIDTH - MARGIN_LEFT - MARGIN_RIGHT
    plot_h = HEIGHT - MARGIN_TOP - MARGIN_BOTTOM

    # Zoom the y-axis to the data range so small steps are visible, but keep a
    # little headroom and a sensible floor.
    y_max = max(rows)
    y_min = min(rows)
    span = max(y_max - y_min, 1)
    y_lo = y_min - span * 0.15
    y_hi = y_max + span * 0.15

    def y_pixel(value: float) -> float:
        frac = (value - y_lo) / (y_hi - y_lo)
        return MARGIN_TOP + plot_h * (1 - frac)

    slot = plot_w / n
    bar_w = slot * 0.62

    parts: list[str] = []
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" '
        f'height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}" '
        f'font-family="Helvetica, Arial, sans-serif">'
    )
    parts.append(f'<rect width="{WIDTH}" height="{HEIGHT}" fill="white"/>')

    # Title.
    parts.append(
        f'<text x="{WIDTH / 2}" y="30" text-anchor="middle" '
        f'font-size="18" fill="{TEXT_COLOR}" font-weight="bold">'
        f"Crop Land Allocation query rows by GCAM version</text>"
    )

    # Y gridlines + labels (5 ticks).
    for i in range(5):
        val = y_lo + (y_hi - y_lo) * i / 4
        yp = y_pixel(val)
        parts.append(
            f'<line x1="{MARGIN_LEFT}" y1="{yp:.1f}" '
            f'x2="{WIDTH - MARGIN_RIGHT}" y2="{yp:.1f}" '
            f'stroke="{GRID_COLOR}" stroke-width="1"/>'
        )
        parts.append(
            f'<text x="{MARGIN_LEFT - 8}" y="{yp + 4:.1f}" '
            f'text-anchor="end" font-size="11" fill="{TEXT_COLOR}">'
            f"{int(val):,}</text>"
        )

    # Axes.
    parts.append(
        f'<line x1="{MARGIN_LEFT}" y1="{MARGIN_TOP}" '
        f'x2="{MARGIN_LEFT}" y2="{MARGIN_TOP + plot_h}" '
        f'stroke="{AXIS_COLOR}" stroke-width="1.5"/>'
    )
    parts.append(
        f'<line x1="{MARGIN_LEFT}" y1="{MARGIN_TOP + plot_h}" '
        f'x2="{WIDTH - MARGIN_RIGHT}" y2="{MARGIN_TOP + plot_h}" '
        f'stroke="{AXIS_COLOR}" stroke-width="1.5"/>'
    )

    # Bars.
    prev = None
    for i, (ver, val) in enumerate(zip(versions, rows, strict=True)):
        x = MARGIN_LEFT + slot * i + (slot - bar_w) / 2
        yp = y_pixel(val)
        bar_h = MARGIN_TOP + plot_h - yp
        is_new = prev is None or val != prev
        color = BAR_COLOR_NEW if (prev is not None and val != prev) else BAR_COLOR
        parts.append(
            f'<rect x="{x:.1f}" y="{yp:.1f}" width="{bar_w:.1f}" '
            f'height="{bar_h:.1f}" fill="{color}"/>'
        )
        # Value label above bars that start a new tier (avoid clutter).
        if is_new:
            parts.append(
                f'<text x="{x + bar_w / 2:.1f}" y="{yp - 5:.1f}" '
                f'text-anchor="middle" font-size="10" fill="{TEXT_COLOR}">'
                f"{val:,}</text>"
            )
        # X tick label (rotated).
        label = ver.replace("gcam-", "")
        cx = x + bar_w / 2
        ty = MARGIN_TOP + plot_h + 12
        parts.append(
            f'<text x="{cx:.1f}" y="{ty:.1f}" text-anchor="end" '
            f'font-size="11" fill="{TEXT_COLOR}" '
            f'transform="rotate(-45 {cx:.1f} {ty:.1f})">{label}</text>'
        )
        prev = val

    # Axis titles.
    parts.append(
        f'<text x="{MARGIN_LEFT + plot_w / 2}" y="{HEIGHT - 8}" '
        f'text-anchor="middle" font-size="13" fill="{TEXT_COLOR}">'
        f"GCAM version</text>"
    )
    parts.append(
        f'<text x="18" y="{MARGIN_TOP + plot_h / 2}" '
        f'text-anchor="middle" font-size="13" fill="{TEXT_COLOR}" '
        f'transform="rotate(-90 18 {MARGIN_TOP + plot_h / 2})">'
        f"Query rows returned</text>"
    )

    # Legend.
    lx = WIDTH - MARGIN_RIGHT - 210
    ly = MARGIN_TOP + 6
    parts.append(
        f'<rect x="{lx}" y="{ly}" width="12" height="12" fill="{BAR_COLOR_NEW}"/>'
    )
    parts.append(
        f'<text x="{lx + 18}" y="{ly + 11}" font-size="11" '
        f'fill="{TEXT_COLOR}">row count changes vs. previous version</text>'
    )

    parts.append("</svg>")
    return "\n".join(parts)


def main() -> None:
    """Generate the query-rows-by-version SVG figure."""
    args = parse_args()
    df = load_passing(Path(args.csv))
    svg = render_svg(df)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(svg)
    print(f"wrote {out} ({len(df)} versions)")


if __name__ == "__main__":
    main()
