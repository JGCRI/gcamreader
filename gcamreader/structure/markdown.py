"""Render a :class:`~gcamreader.structure.model.ComparisonResult` as Markdown.

Two outputs are produced from one result:

* the **main report** (:func:`render_markdown`), always of bounded size: verdict,
  inputs, settings, counting definitions, per-comparison summaries, the
  matrix, findings grouped by branch, warnings and limitations. When a
  comparison has more findings than ``inline_limit`` the per-finding lists
  and the full matrix are replaced by aggregate tables and a link to the
  appendix;
* the **appendix** (:func:`iter_appendix`), every finding and every matrix
  row, yielded line by line so that hundreds of thousands of findings stream
  to disk without being held in memory.

:func:`write_report` writes both. The report body is byte-stable for a given
result; a ``Generated:`` line is appended only when a timestamp is supplied.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path

from gcamreader.structure.compare import HISTOGRAM_MIN_DESCENDANTS
from gcamreader.structure.model import (
    ComparisonResult,
    Finding,
    FindingKind,
    InputIdentity,
    PairComparison,
)
from gcamreader.structure.rules import ComparisonRules

#: Comparisons with more findings than this move their lists to the appendix.
DEFAULT_INLINE_LIMIT = 2000

#: Appended to the report's stem to name the appendix file.
APPENDIX_SUFFIX = ".appendix.md"

KIND_LABELS = {
    FindingKind.ELEMENT_ADDED: "added",
    FindingKind.ELEMENT_REMOVED: "removed",
    FindingKind.ATTRIBUTE_CHANGED: "attribute changed",
    FindingKind.COUNT_CHANGED: "count changed",
    FindingKind.METADATA_VARIANTS_CHANGED: "metadata variants changed",
}
KIND_ORDER = {kind: position for position, kind in enumerate(KIND_LABELS)}
ONE_SIDED = (FindingKind.ELEMENT_ADDED, FindingKind.ELEMENT_REMOVED)


# -- Markdown helpers --------------------------------------------------------


def code(text: str) -> str:
    """Wrap ``text`` in a code span safe for GFM tables."""
    text = text.replace("\n", " ").replace("|", "\\|")
    if "`" in text:
        return f"`` {text} ``"
    return f"`{text}`"


def cell(text: str) -> str:
    """Escape plain text for a GFM table cell."""
    return text.replace("\n", " ").replace("|", "\\|").replace("<", "&lt;")


def table(headers: Sequence[str], rows: Iterable[Sequence[str]]) -> Iterator[str]:
    yield "| " + " | ".join(headers) + " |"
    yield "|" + "|".join("---" for _ in headers) + "|"
    for row in rows:
        yield "| " + " | ".join(row) + " |"


def _n(value: int) -> str:
    return f"{value:,}"


def _label(item: InputIdentity) -> str:
    return f"{item.index}: {item.label}"


def _elements(finding: Finding) -> int:
    return finding.members if finding.kind in ONE_SIDED else 1


def _finding_line(finding: Finding) -> str:
    label = KIND_LABELS[finding.kind]
    if finding.is_grouped:
        label += f" ({_n(finding.members)} elements)"
    line = f"- **{label}** {code(finding.path)}"
    if finding.detail:
        line += f": {cell(finding.detail)}"
    if finding.descendant_tags:
        histogram = ", ".join(
            f"{tag} x{count}" for tag, count in finding.descendant_tags
        )
        line += f" Descendant tags: {cell(histogram)}."
    return line


def _by_branch(findings: Iterable[Finding]) -> dict[str, list[Finding]]:
    branches: dict[str, list[Finding]] = {}
    for finding in findings:
        branches.setdefault(finding.branch(), []).append(finding)
    return dict(sorted(branches.items()))


def _aggregate_rows(findings: Iterable[Finding]) -> list[list[str]]:
    counts: Counter[tuple[FindingKind, str]] = Counter()
    elements: Counter[tuple[FindingKind, str]] = Counter()
    descendants: Counter[tuple[FindingKind, str]] = Counter()
    for finding in findings:
        key = (finding.kind, finding.tag)
        counts[key] += 1
        elements[key] += _elements(finding)
        descendants[key] += finding.descendant_count
    rows = []
    for kind, tag in sorted(counts, key=lambda k: (KIND_ORDER[k[0]], k[1])):
        key = (kind, tag)
        rows.append(
            [
                KIND_LABELS[kind],
                code(tag),
                _n(counts[key]),
                _n(elements[key]),
                _n(descendants[key]),
            ]
        )
    return rows


def needs_appendix(result: ComparisonResult, inline_limit: int) -> bool:
    """Whether any comparison or the matrix exceeds the inline limit."""
    if len(result.matrix) > inline_limit:
        return True
    return any(len(c.findings) > inline_limit for c in result.comparisons)


# -- main report sections -----------------------------------------------------


def _verdict(result: ComparisonResult) -> Iterator[str]:
    yield "# Structural comparison of GCAM output databases"
    yield ""
    candidates = len(result.comparisons)
    noun = "candidate" if candidates == 1 else "candidates"
    if not result.has_differences:
        yield (
            f"**Result: no structural differences** between the baseline and "
            f"{candidates} {noun}. Numeric values were not compared."
        )
        return
    differing = sum(1 for c in result.comparisons if c.findings)
    findings = sum(c.summary.total for c in result.comparisons)
    added = sum(c.summary.elements["element_added"] for c in result.comparisons)
    removed = sum(c.summary.elements["element_removed"] for c in result.comparisons)
    yield (
        f"**Result: structural differences found** in {differing} of {candidates} "
        f"{noun}: {_n(findings)} findings covering {_n(added)} added and "
        f"{_n(removed)} removed elements. Numeric values were not compared."
    )


def _inputs(result: ComparisonResult) -> Iterator[str]:
    yield ""
    yield "## Inputs"
    yield ""
    rows = []
    for item in result.inputs:
        scenario = item.scenario
        rows.append(
            [
                str(item.index),
                cell(item.label),
                code(item.path),
                code(scenario.label),
                cell(scenario.version or "unknown"),
                _n(scenario.element_count) if scenario.element_count else "unknown",
                code(item.fingerprint[:12]),
                "baseline" if item.index == result.baseline else "",
            ]
        )
    yield from table(
        [
            "#",
            "Label",
            "Database",
            "Scenario",
            "GCAM version",
            "Elements",
            "Fingerprint",
            "Role",
        ],
        rows,
    )
    yield ""
    yield (
        "Each database contributes exactly one scenario run (`name@date`); runs are "
        "never merged. The fingerprint covers the BaseX metadata file and the names "
        "and sizes of the storage files."
    )


def _settings(result: ComparisonResult) -> Iterator[str]:
    yield ""
    yield "## Settings"
    yield ""
    yield f"- gcamreader version: `{result.package_version}`"
    if result.rules:
        rules = ComparisonRules.from_mapping(result.rules)
    else:
        rules = ComparisonRules.default()
    for line in rules.describe():
        yield f"- {cell(line)}"


def _definitions() -> Iterator[str]:
    yield ""
    yield "## Counting definitions"
    yield ""
    yield (
        "- **Path**: `tag[identity, ...]` segments joined by ` > `, from the "
        "`scenario` document element down. Identity attributes are those listed "
        "in the settings; all other attributes are metadata."
    )
    yield (
        "- **Count / occurrences**: how many sibling elements share a path and the "
        "same metadata. Children of repeated siblings are merged, so their counts "
        "are totals across the repeats."
    )
    yield (
        "- **Added / removed**: a path present in one input only. Nothing below it "
        "is listed separately; **descendants** gives the number of elements in "
        "that subtree and, for subtrees with at least "
        f"{HISTOGRAM_MIN_DESCENDANTS} descendants, a histogram of their tags."
    )
    yield (
        "- **Grouped finding**: added or removed siblings with the same parent and "
        "tag are reported together; **elements** is the number of members and the "
        "detail lists every member identity. Paths of grouped findings end in a "
        "summary such as `Non-CO2[year=2040..2100 (13 values)]`."
    )
    yield (
        "- **Attribute changed**: same path on both sides, one metadata variant "
        "each, with different attribute values (added, removed or changed)."
    )
    yield (
        "- **Count changed**: same path and metadata on both sides, different "
        "number of occurrences."
    )
    yield (
        "- **Metadata variants changed**: a path whose occurrences carry several "
        "metadata variants on at least one side; all variants are listed, nothing "
        "is collapsed."
    )
    yield "- Element text (numeric values) is never compared."


def _summary(result: ComparisonResult) -> Iterator[str]:
    yield ""
    yield "## Summary"
    for comparison in result.comparisons:
        yield ""
        yield f"### {_label(comparison.candidate)} vs baseline {_label(comparison.baseline)}"
        yield ""
        summary = comparison.summary
        rows = []
        for kind in KIND_LABELS:
            count = summary.counts[kind.value]
            elements = summary.elements.get(kind.value, count)
            rows.append([KIND_LABELS[kind], _n(count), _n(elements)])
        rows.append(["**total**", f"**{_n(summary.total)}**", ""])
        yield from table(["Kind", "Findings", "Elements"], rows)
        yield ""
        yield (
            f"Descendant elements summarized under added/removed paths: "
            f"{_n(summary.descendants_summarized)}. Warnings: {len(comparison.warnings)}."
        )


def _matrix(
    result: ComparisonResult, inline_limit: int, appendix_name: str | None
) -> Iterator[str]:
    yield ""
    yield "## Matrix"
    yield ""
    if not result.matrix:
        yield "No differences, so the matrix is empty."
        return
    labels = [_label(item) for item in result.inputs]
    yield (
        "One row per differing path (and attribute); one column per input. A cell "
        "shows the value on that input: `present`/`absent`, an attribute value, a "
        "count, or the metadata variants. A candidate cell equal to the baseline "
        "means that comparison found no difference at that exact path; a "
        "difference in an ancestor is its own row."
    )
    yield ""
    if len(result.matrix) <= inline_limit:
        rows = (
            [
                code(row.path),
                KIND_LABELS[row.kind],
                code(row.attribute) if row.attribute else "",
            ]
            + [cell(c) if c is not None else "" for c in row.cells]
            for row in result.matrix
        )
        yield from table(["Path", "Kind", "Attribute", *labels], rows)
        return
    yield (
        f"The full matrix has {_n(len(result.matrix))} rows and is in the appendix"
        + (f" (`{appendix_name}`)" if appendix_name else "")
        + ". Below, rows are aggregated by branch and kind; cells give the number "
        "of elements differing from the baseline in each candidate."
    )
    yield ""
    totals: dict[tuple[str, FindingKind], dict[int, int]] = {}
    for comparison in result.comparisons:
        column = comparison.candidate.index
        for finding in comparison.findings:
            key = (finding.branch(), finding.kind)
            per_input = totals.setdefault(key, {})
            per_input[column] = per_input.get(column, 0) + _elements(finding)
    rows = []
    for branch, kind in sorted(totals, key=lambda k: (k[0], KIND_ORDER[k[1]])):
        per_input = totals[(branch, kind)]
        cells = [
            (
                "baseline"
                if item.index == result.baseline
                else _n(per_input.get(item.index, 0))
            )
            for item in result.inputs
        ]
        rows.append([code(branch), KIND_LABELS[kind], *cells])
    yield from table(["Branch", "Kind", *labels], rows)


def _findings(
    comparison: PairComparison, inline_limit: int, appendix_name: str | None
) -> Iterator[str]:
    yield ""
    yield f"## Findings: {_label(comparison.candidate)} vs baseline {_label(comparison.baseline)}"
    if not comparison.findings:
        yield ""
        yield "No structural differences."
        return
    inline = len(comparison.findings) <= inline_limit
    if not inline:
        yield ""
        yield (
            f"{_n(len(comparison.findings))} findings exceed the inline limit of "
            f"{_n(inline_limit)}; every finding is listed in the appendix"
            + (f" (`{appendix_name}`)" if appendix_name else "")
            + ". Each branch below shows totals by kind and tag."
        )
    for branch, findings in _by_branch(comparison.findings).items():
        yield ""
        yield f"### {branch}"
        yield ""
        yield from table(
            ["Kind", "Tag", "Findings", "Elements", "Descendants"],
            _aggregate_rows(findings),
        )
        if inline:
            yield ""
            for finding in findings:
                yield _finding_line(finding)


def _warnings(result: ComparisonResult) -> Iterator[str]:
    yield ""
    yield "## Warnings and limitations"
    yield ""
    warnings = result.all_warnings
    if warnings:
        for warning in warnings:
            yield f"- Warning: {cell(warning)}"
    else:
        yield "- No warnings were raised during extraction or comparison."
    yield (
        "- Only structure and metadata are compared; numeric values are ignored. A "
        "present element whose value is `0` is still present."
    )
    yield (
        "- A renamed or moved element appears as a removal plus an addition; no "
        "rename detection is attempted."
    )
    yield (
        "- GCAM writes only non-zero results, and runs may cover different periods, "
        "so a path missing from one database can reflect output filtering or run "
        "coverage rather than a configuration change. Interpret added/removed "
        "leaves such as outputs, inputs and emissions with that in mind."
    )
    yield (
        "- Identity values are compared as text: `2020` and `2020.0` are different "
        "identities."
    )
    yield (
        "- Identical subtrees are recognised by a 128-bit additive fingerprint; an "
        "accidental collision hiding a difference is negligible for GCAM output."
    )
    yield (
        f"- Descendant tag histograms are given only for added/removed subtrees "
        f"with at least {HISTOGRAM_MIN_DESCENDANTS} descendants; descendant counts "
        "are always given."
    )
    yield "- Remote databases are not supported; inputs must be local BaseX directories."


def render_markdown(
    result: ComparisonResult,
    *,
    generated_at: str | None = None,
    appendix_name: str | None = None,
    inline_limit: int = DEFAULT_INLINE_LIMIT,
) -> str:
    """Render the main report.

    Args:
        result: The comparison result.
        generated_at: Timestamp text for a trailing ``Generated:`` line; omitted
            when ``None`` so the body stays byte-stable.
        appendix_name: File name mentioned where lists were moved to the
            appendix (see :func:`write_report`).
        inline_limit: Comparisons with more findings than this, or a matrix with
            more rows, are summarised with aggregate tables instead of lists.

    Returns:
        GitHub-flavoured Markdown ending in a newline.
    """
    lines: list[str] = []
    lines.extend(_verdict(result))
    lines.extend(_inputs(result))
    lines.extend(_settings(result))
    lines.extend(_definitions())
    lines.extend(_summary(result))
    lines.extend(_matrix(result, inline_limit, appendix_name))
    for comparison in result.comparisons:
        lines.extend(_findings(comparison, inline_limit, appendix_name))
    lines.extend(_warnings(result))
    if generated_at:
        lines.append("")
        lines.append(f"Generated: {generated_at}")
    return "\n".join(lines) + "\n"


# -- appendix -------------------------------------------------------------------


def iter_appendix(result: ComparisonResult) -> Iterator[str]:
    """Yield the appendix line by line: all findings and the full matrix.

    The generator holds no more than the current finding, so writing it to a
    file keeps memory flat regardless of the number of findings.
    """
    yield "# Appendix: all findings and the full matrix"
    yield ""
    yield (
        "Companion to the main report. Nothing is truncated here: every finding "
        "of every comparison is listed by branch, followed by the complete matrix."
    )
    for comparison in result.comparisons:
        yield ""
        yield (
            f"## Findings: {_label(comparison.candidate)} vs baseline "
            f"{_label(comparison.baseline)} ({_n(len(comparison.findings))})"
        )
        if not comparison.findings:
            yield ""
            yield "No structural differences."
            continue
        for branch, findings in _by_branch(comparison.findings).items():
            yield ""
            yield f"### {branch} ({_n(len(findings))})"
            yield ""
            yield from table(
                ["Kind", "Path", "Elements", "Descendants", "Detail"],
                (
                    [
                        KIND_LABELS[f.kind],
                        code(f.path),
                        _n(_elements(f)),
                        _n(f.descendant_count),
                        cell(f.detail)
                        + (
                            " Descendant tags: "
                            + cell(", ".join(f"{t} x{c}" for t, c in f.descendant_tags))
                            + "."
                            if f.descendant_tags
                            else ""
                        ),
                    ]
                    for f in findings
                ),
            )
    yield ""
    yield f"## Matrix ({_n(len(result.matrix))} rows)"
    yield ""
    if not result.matrix:
        yield "No differences, so the matrix is empty."
        return
    labels = [_label(item) for item in result.inputs]
    yield from table(
        ["Path", "Kind", "Attribute", *labels],
        (
            [
                code(row.path),
                KIND_LABELS[row.kind],
                code(row.attribute) if row.attribute else "",
            ]
            + [cell(c) if c is not None else "" for c in row.cells]
            for row in result.matrix
        ),
    )


def write_report(
    result: ComparisonResult,
    path: str | Path,
    *,
    inline_limit: int = DEFAULT_INLINE_LIMIT,
    generated_at: str | None = None,
) -> list[Path]:
    """Write the main report and, when needed, the appendix beside it.

    Args:
        result: The comparison result.
        path: Report file. The appendix, if any, is ``<stem>.appendix.md`` in
            the same directory.
        inline_limit: See :func:`render_markdown`.
        generated_at: See :func:`render_markdown`.

    Returns:
        The files written, report first.
    """
    report = Path(path)
    appendix = report.with_name(report.stem + APPENDIX_SUFFIX)
    with_appendix = needs_appendix(result, inline_limit)
    text = render_markdown(
        result,
        generated_at=generated_at,
        appendix_name=appendix.name if with_appendix else None,
        inline_limit=inline_limit,
    )
    report.write_text(text, encoding="utf-8", newline="\n")
    written = [report]
    if with_appendix:
        with open(appendix, "w", encoding="utf-8", newline="\n") as handle:
            for line in iter_appendix(result):
                handle.write(line)
                handle.write("\n")
        written.append(appendix)
    return written
