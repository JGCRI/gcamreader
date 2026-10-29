"""Tests for :mod:`gcamreader.structure.markdown`.

Snapshot tests compare the rendered report with files under
``tests/snapshots/structure``. Set ``GCAMREADER_UPDATE_SNAPSHOTS=1`` to rewrite them
after an intentional format change, then review the diff.

@license BSD 2-Clause
"""

from __future__ import annotations

import inspect
import io
import itertools
import os
from pathlib import Path

import pytest

from gcamreader.structure import compare, extract, markdown
from gcamreader.structure.model import (
    ComparisonResult,
    Finding,
    FindingKind,
    InputIdentity,
    NodeSnapshot,
    PairComparison,
    PathSegment,
    ScenarioRef,
)
from gcamreader.structure.rules import ComparisonRules
from tests.test_structure_compare import BASE

SNAPSHOTS = Path(__file__).parent / "snapshots" / "structure"
VERSION = "1.5.0-test"
_counter = itertools.count()


def _input(
    index: int, name: str, date: str = "2026-1-6T10:39:43-07:00"
) -> InputIdentity:
    return InputIdentity(
        index=index,
        path=f"data/database_{name}",
        label=name,
        scenario=ScenarioRef(name, date, "ver_9.0_rgcam-v9.0", 18),
        fingerprint=f"{index:02x}" * 32,
    )


def build_result(
    xml_by_input: list[tuple[str, str]],
    tmp_path: Path,
    rules: ComparisonRules | None = None,
    warnings: tuple[str, ...] = (),
) -> ComparisonResult:
    """Compare the first document with each other one and assemble a result."""
    rules = rules or ComparisonRules.default()
    inputs = tuple(_input(i + 1, name) for i, (name, _) in enumerate(xml_by_input))
    indexes = []
    for name, xml in xml_by_input:
        path = tmp_path / f"{name}{next(_counter)}.sqlite"
        indexes.append(
            extract.build_index_from_stream(
                io.BytesIO(xml.encode()), str(path), rules, source=name
            )
        )
    try:
        comparisons = tuple(
            compare.compare_indexes(
                indexes[0], other, baseline_input=inputs[0], candidate_input=inputs[i]
            )
            for i, other in enumerate(indexes)
            if i > 0
        )
    finally:
        for index in indexes:
            index.close()
    return ComparisonResult(
        package_version=VERSION,
        inputs=inputs,
        baseline=1,
        comparisons=comparisons,
        matrix=compare.build_matrix(inputs, comparisons),
        rules=rules.to_dict(),
        warnings=warnings,
    )


def check_snapshot(name: str, text: str) -> None:
    path = SNAPSHOTS / name
    if os.environ.get("GCAMREADER_UPDATE_SNAPSHOTS") == "1" or not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8", newline="\n")
    expected = path.read_text(encoding="utf-8")
    assert text == expected, f"snapshot {name} differs; see test docstring to update"


# --- fixtures ---------------------------------------------------------------

UNIT_CHANGED = BASE.replace('unit="EJ" vintage="2025"', 'unit="PJ" vintage="2025"')
RESTRUCTURED = (
    BASE.replace(
        '<region name="China">',
        '<region name="China"><sector name="new" type="z"><subsector name="s1"/></sector>',
    )
    .replace(
        '<physical-output unit="EJ" vintage="2020">1.5</physical-output>\n'
        '            <physical-output unit="EJ" vintage="2025">1.4</physical-output>',
        "",
    )
    .replace(
        '<subsector name="road"/>', "<subsector name='road'/><subsector name='rail'/>"
    )
)


@pytest.fixture
def three_inputs(tmp_path: Path) -> ComparisonResult:
    return build_result(
        [
            ("GCAM", BASE),
            ("GCAM_units", UNIT_CHANGED),
            ("GCAM_restructured", RESTRUCTURED),
        ],
        tmp_path,
    )


# --- snapshots ----------------------------------------------------------------


def test_snapshot_no_differences(tmp_path: Path) -> None:
    result = build_result([("GCAM", BASE), ("GCAM_copy", BASE)], tmp_path)
    assert not result.has_differences
    check_snapshot("report_no_differences.md", markdown.render_markdown(result))


def test_snapshot_three_inputs(three_inputs: ComparisonResult) -> None:
    assert three_inputs.has_differences
    check_snapshot("report_three_inputs.md", markdown.render_markdown(three_inputs))


def test_snapshot_warnings(tmp_path: Path) -> None:
    dup = "<r><s name='a'><t/></s><s name='a'><t/></s></r>"
    result = build_result(
        [("dup", dup), ("single", "<r><s name='a'><t/></s></r>")],
        tmp_path,
        warnings=("Java 21.0.1 is newer than the tested versions",),
    )
    text = markdown.render_markdown(result)
    assert "Warning: Java 21.0.1" in text
    assert "repeated sibling" in text
    check_snapshot("report_with_warnings.md", text)


def test_snapshot_over_limit(three_inputs: ComparisonResult) -> None:
    text = markdown.render_markdown(
        three_inputs, inline_limit=0, appendix_name="report.appendix.md"
    )
    assert "exceed the inline limit of 0" in text
    assert "`report.appendix.md`" in text
    for marker in ("- **added", "- **removed", "- **attribute changed"):
        assert marker not in text  # no per-finding lists inline
    check_snapshot("report_over_limit.md", text)
    appendix = "\n".join(markdown.iter_appendix(three_inputs)) + "\n"
    check_snapshot("report_over_limit.appendix.md", appendix)


# --- behaviour ----------------------------------------------------------------


def test_generated_line_only_when_requested(three_inputs: ComparisonResult) -> None:
    plain = markdown.render_markdown(three_inputs)
    stamped = markdown.render_markdown(three_inputs, generated_at="2026-10-29T12:00:00")
    assert "Generated:" not in plain
    assert stamped.startswith(plain.rstrip("\n"))
    assert stamped.endswith("\nGenerated: 2026-10-29T12:00:00\n")


def test_render_is_deterministic(three_inputs: ComparisonResult) -> None:
    assert markdown.render_markdown(three_inputs) == markdown.render_markdown(
        three_inputs
    )


def test_report_sections_in_order(three_inputs: ComparisonResult) -> None:
    text = markdown.render_markdown(three_inputs)
    headers = [line for line in text.splitlines() if line.startswith("## ")]
    assert headers == [
        "## Inputs",
        "## Settings",
        "## Counting definitions",
        "## Summary",
        "## Matrix",
        "## Findings: 2: GCAM_units vs baseline 1: GCAM",
        "## Findings: 3: GCAM_restructured vs baseline 1: GCAM",
        "## Warnings and limitations",
    ]
    assert text.startswith("# Structural comparison of GCAM output databases\n")
    assert "**Result: structural differences found** in 2 of 2 candidates" in text
    assert "| 1 | GCAM |" in text and "| baseline |" in text
    assert "Fallback: every other attribute is metadata" in text


def test_grouped_findings_rendered(three_inputs: ComparisonResult) -> None:
    text = markdown.render_markdown(three_inputs)
    assert "**removed (2 elements)**" in text
    assert "physical-output[vintage=2020..2025 (2 values)]" in text
    assert "physical-output[vintage=2020], physical-output[vintage=2025]" in text


def test_matrix_inline_and_aggregated(three_inputs: ComparisonResult) -> None:
    inline = markdown.render_markdown(three_inputs)
    assert (
        "| Path | Kind | Attribute | 1: GCAM | 2: GCAM_units | 3: GCAM_restructured |"
        in inline
    )
    assert "| EJ | PJ | EJ |" in inline
    aggregated = markdown.render_markdown(three_inputs, inline_limit=1)
    assert (
        "| Branch | Kind | 1: GCAM | 2: GCAM_units | 3: GCAM_restructured |"
        in aggregated
    )
    assert "| `region[name=China]` | added | baseline | 0 | 1 |" in aggregated
    assert "| `region[name=USA]` | removed | baseline | 0 | 2 |" in aggregated


def test_escaping_in_code_spans_and_cells(tmp_path: Path) -> None:
    tricky = "<r><x name='a|b'/><y name='tick`mark'/></r>"
    result = build_result([("plain", "<r/>"), ("tricky", tricky)], tmp_path)
    text = markdown.render_markdown(result)
    assert "`r > x[name=a\\|b]`" in text
    assert "`` r > y[name=tick`mark] ``" in text
    for line in text.splitlines():
        if line.startswith("|"):
            assert line.count("|") - line.count("\\|") >= 2
    assert markdown.code("plain") == "`plain`"
    assert markdown.code("a|b\nc") == "`a\\|b c`"
    assert markdown.cell("a|b") == "a\\|b"
    assert markdown.cell("a<b>") == "a&lt;b>"


def test_write_report_without_appendix(
    three_inputs: ComparisonResult, tmp_path: Path
) -> None:
    target = tmp_path / "out" / "report.md"
    target.parent.mkdir()
    written = markdown.write_report(three_inputs, target)
    assert written == [target]
    assert target.read_text(encoding="utf-8") == markdown.render_markdown(three_inputs)
    assert not (tmp_path / "out" / "report.appendix.md").exists()


def test_write_report_with_appendix(
    three_inputs: ComparisonResult, tmp_path: Path
) -> None:
    target = tmp_path / "report.md"
    written = markdown.write_report(
        three_inputs, target, inline_limit=1, generated_at="now"
    )
    appendix = tmp_path / "report.appendix.md"
    assert written == [target, appendix]
    text = target.read_text(encoding="utf-8")
    assert "(`report.appendix.md`)" in text
    assert text.endswith("Generated: now\n")
    appendix_text = appendix.read_text(encoding="utf-8")
    assert appendix_text == "\n".join(markdown.iter_appendix(three_inputs)) + "\n"
    assert appendix_text.startswith("# Appendix: all findings and the full matrix\n")
    assert "## Matrix (" in appendix_text
    assert "\r" not in text and "\r" not in appendix_text


def test_needs_appendix(three_inputs: ComparisonResult) -> None:
    assert not markdown.needs_appendix(three_inputs, 2000)
    assert markdown.needs_appendix(three_inputs, 1)


def _synthetic_result(count: int) -> ComparisonResult:
    inputs = (_input(1, "a"), _input(2, "b"))
    segments = (
        PathSegment("scenario"),
        PathSegment("world"),
        PathSegment("region", (("name", "USA"),)),
    )
    findings = [
        Finding(
            kind=FindingKind.ELEMENT_REMOVED,
            path=f"scenario > world > region[name=USA] > sector[name=s{i}]",
            segments=(*segments, PathSegment("sector", (("name", f"s{i}"),))),
            tag="sector",
            identity=(("name", f"s{i}"),),
            identities=((("name", f"s{i}"),),),
            baseline=NodeSnapshot(1, (), 1),
            detail="Removed: 0 descendant element(s)",
        )
        for i in range(count)
    ]
    comparisons = (PairComparison.build(inputs[0], inputs[1], findings),)
    return ComparisonResult(
        VERSION, inputs, 1, comparisons, compare.build_matrix(inputs, comparisons)
    )


def test_iter_appendix_streams() -> None:
    result = _synthetic_result(10_000)
    gen = markdown.iter_appendix(result)
    assert inspect.isgenerator(gen)
    first = next(gen)
    assert first.startswith("# Appendix")
    lines = sum(1 for _ in gen) + 1
    # header lines + one table row per finding + one per matrix row
    assert lines > 20_000
    main = markdown.render_markdown(result, inline_limit=100)
    assert "10,000 findings exceed the inline limit of 100" in main
    assert main.count("\n") < 200


def test_appendix_equals_inline_lists_for_small_result(
    three_inputs: ComparisonResult,
) -> None:
    appendix = "\n".join(markdown.iter_appendix(three_inputs))
    for comparison in three_inputs.comparisons:
        for finding in comparison.findings:
            assert markdown.code(finding.path) in appendix
    for row in three_inputs.matrix:
        assert markdown.code(row.path) in appendix
