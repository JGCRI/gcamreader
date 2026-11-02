"""Unit tests for :mod:`gcamreader.structure.model`.

@license BSD 2-Clause
"""

from __future__ import annotations

import json

import pytest

from gcamreader.structure.model import (
    AttributeChange,
    ComparisonResult,
    ComparisonSummary,
    Finding,
    FindingKind,
    InputIdentity,
    MatrixRow,
    NodeSnapshot,
    PairComparison,
    PathSegment,
    ScenarioRef,
    format_path,
    format_value,
    summarize_identities,
)

# --- paths ------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("USA", "USA"),
        ("coal (conv pul)", "coal (conv pul)"),
        ("2020", "2020"),
        ("a]b", '"a]b"'),
        ("a,b", '"a,b"'),
        ("a=b", '"a=b"'),
        ('say "hi"', '"say \\"hi\\""'),
        (" lead", '" lead"'),
        ("trail ", '"trail "'),
        ("", '""'),
        ("back\\slash", "back\\slash"),
        ('quote"and\\back', '"quote\\"and\\\\back"'),
        ("in side", "in side"),
        ("[open", "[open"),
    ],
)
def test_format_value(value: str, expected: str) -> None:
    assert format_value(value) == expected


def test_path_segment_format() -> None:
    assert PathSegment("world").format() == "world"
    assert PathSegment("region", (("name", "USA"),)).format() == "region[name=USA]"
    seg = PathSegment("technology", (("name", "coal (conv pul)"), ("year", "2020")))
    assert seg.format() == "technology[name=coal (conv pul), year=2020]"
    seg = PathSegment("x", (("name", "a,b"),))
    assert seg.format() == 'x[name="a,b"]'


def test_format_path() -> None:
    segments = (
        PathSegment("scenario"),
        PathSegment("world"),
        PathSegment("region", (("name", "USA"),)),
        PathSegment("sector", (("name", "electricity"),)),
        PathSegment("technology", (("name", "coal (conv pul)"), ("year", "2020"))),
    )
    assert format_path(segments) == (
        "scenario > world > region[name=USA] > sector[name=electricity] > "
        "technology[name=coal (conv pul), year=2020]"
    )
    assert format_path(()) == ""


def test_clark_notation_tag_passes_through() -> None:
    seg = PathSegment("{http://example.org/ns}item", (("name", "a"),))
    assert seg.format() == "{http://example.org/ns}item[name=a]"
    assert seg.to_dict() == {
        "tag": "{http://example.org/ns}item",
        "identity": [["name", "a"]],
    }


# --- fixtures ---------------------------------------------------------------


def _scenario(name: str = "GCAM") -> ScenarioRef:
    return ScenarioRef(
        name=name, date="2026-1-6T10:39:43-07:00", version="ver_9.0", element_count=10
    )


def _input(index: int, name: str = "GCAM") -> InputIdentity:
    return InputIdentity(
        index=index,
        path=f"data/db{index}",
        label=f"db{index}",
        scenario=_scenario(name),
        fingerprint=f"{index:064x}",
    )


def _segments(*tail: PathSegment) -> tuple[PathSegment, ...]:
    return (PathSegment("scenario"), PathSegment("world"), *tail)


def _finding(kind: FindingKind = FindingKind.ELEMENT_ADDED, **overrides) -> Finding:
    segments = _segments(
        PathSegment("region", (("name", "USA"),)),
        PathSegment("sector", (("name", "new"),)),
    )
    defaults = dict(
        kind=kind,
        path=format_path(segments),
        segments=segments,
        tag="sector",
        identity=(("name", "new"),),
        baseline=None,
        candidate=NodeSnapshot(count=1, metadata=(("type", "x"),), subtree_size=5),
        descendant_count=4,
        descendant_tags=(("subsector", 1), ("technology", 3)),
        detail="added",
    )
    defaults.update(overrides)
    return Finding(**defaults)


# --- model classes ----------------------------------------------------------


def test_scenario_ref_label_and_dict() -> None:
    ref = _scenario()
    assert ref.label == "GCAM@2026-1-6T10:39:43-07:00"
    assert ref.to_dict() == {
        "name": "GCAM",
        "date": "2026-1-6T10:39:43-07:00",
        "version": "ver_9.0",
        "element_count": 10,
        "label": "GCAM@2026-1-6T10:39:43-07:00",
    }
    assert ScenarioRef("a", "b").to_dict()["version"] is None


def test_attribute_change_kind() -> None:
    assert AttributeChange("unit", None, "EJ").change == "added"
    assert AttributeChange("unit", "EJ", None).change == "removed"
    assert AttributeChange("unit", "EJ", "PJ").change == "changed"
    assert AttributeChange("unit", "EJ", "PJ").to_dict() == {
        "attribute": "unit",
        "baseline": "EJ",
        "candidate": "PJ",
        "change": "changed",
    }


def test_node_snapshot_dict() -> None:
    snap = NodeSnapshot(
        count=3,
        metadata=None,
        subtree_size=9,
        variants=(((("unit", "EJ"),), 2), (((("unit", "PJ"),), 1))),
    )
    assert snap.to_dict() == {
        "count": 3,
        "metadata": None,
        "subtree_size": 9,
        "variants": [
            {"metadata": {"unit": "EJ"}, "count": 2},
            {"metadata": {"unit": "PJ"}, "count": 1},
        ],
    }
    assert NodeSnapshot(1, (("a", "1"),), 1).to_dict()["metadata"] == {"a": "1"}


def test_finding_branch_grouping() -> None:
    finding = _finding()
    assert finding.branch() == "region[name=USA]"
    marketplace = _finding(segments=_segments(PathSegment("Marketplace")), path="x")
    assert marketplace.branch() == "Marketplace"
    root_only = _finding(segments=(PathSegment("scenario"),), path="scenario")
    assert root_only.branch() == "scenario"
    assert _finding(segments=(), path="").branch() == ""
    assert finding.branch(skip=()) == "scenario"


def test_finding_dict_is_json_safe() -> None:
    data = _finding().to_dict()
    json.dumps(data)
    assert data["kind"] == "element_added"
    assert data["segments"][2] == {"tag": "region", "identity": [["name", "USA"]]}
    assert data["baseline"] is None
    assert data["candidate"]["count"] == 1
    assert data["descendant_tags"] == [["subsector", 1], ["technology", 3]]


def test_summary_from_findings() -> None:
    findings = [
        _finding(),
        _finding(FindingKind.ELEMENT_REMOVED, descendant_count=2),
        _finding(FindingKind.ATTRIBUTE_CHANGED, descendant_count=0),
    ]
    summary = ComparisonSummary.from_findings(findings)
    assert summary.total == 3
    assert summary.descendants_summarized == 6
    assert summary.counts == {
        "element_added": 1,
        "element_removed": 1,
        "attribute_changed": 1,
        "count_changed": 0,
        "metadata_variants_changed": 0,
    }
    assert ComparisonSummary.from_findings([]).total == 0


def test_pair_comparison_build_and_dict() -> None:
    pair = PairComparison.build(
        _input(1), _input(2, "GCAM_SSP4"), [_finding()], ["dup parent"]
    )
    assert pair.summary.total == 1
    assert pair.warnings == ("dup parent",)
    data = pair.to_dict()
    json.dumps(data)
    assert data["baseline"] == 1
    assert data["candidate"] == 2
    assert len(data["findings"]) == 1


def test_matrix_row_dict() -> None:
    row = MatrixRow(
        "scenario > world", FindingKind.ATTRIBUTE_CHANGED, "unit", ("EJ", "PJ", None)
    )
    assert row.to_dict() == {
        "path": "scenario > world",
        "kind": "attribute_changed",
        "attribute": "unit",
        "cells": ["EJ", "PJ", None],
    }


def _result(with_findings: bool) -> ComparisonResult:
    inputs = (_input(1), _input(2, "GCAM_SSP4"), _input(3, "Other"))
    findings = [_finding()] if with_findings else []
    comparisons = (
        PairComparison.build(inputs[0], inputs[1], findings),
        PairComparison.build(inputs[0], inputs[2], [], ["cmp warning"]),
    )
    matrix = (
        (MatrixRow(findings[0].path, findings[0].kind, None, (None, "present", None)),)
        if with_findings
        else ()
    )
    return ComparisonResult(
        package_version="1.5.0",
        inputs=inputs,
        baseline=1,
        comparisons=comparisons,
        matrix=matrix,
        rules={"identity_attributes": ["name"]},
        warnings=("run warning",),
    )


def test_comparison_result_has_differences() -> None:
    assert _result(True).has_differences
    assert not _result(False).has_differences


def test_comparison_result_warnings_and_dict() -> None:
    result = _result(True)
    assert result.all_warnings == ("run warning", "cmp warning")
    data = result.to_dict()
    text = json.dumps(data, sort_keys=True)
    assert data["has_differences"] is True
    assert data["baseline"] == 1
    assert [item["index"] for item in data["inputs"]] == [1, 2, 3]
    assert data["rules"] == {"identity_attributes": ["name"]}
    assert data["matrix"][0]["cells"] == [None, "present", None]
    # Deterministic: an identical result serializes identically.
    assert json.dumps(_result(True).to_dict(), sort_keys=True) == text


def test_model_objects_are_immutable() -> None:
    with pytest.raises(AttributeError):
        _scenario().name = "x"  # type: ignore[misc]
    with pytest.raises(AttributeError):
        _finding().kind = FindingKind.COUNT_CHANGED  # type: ignore[misc]


@pytest.mark.parametrize(
    ("identities", "expected"),
    [
        ((), ""),
        (((("name", "coal"), ("year", "2020")),), "name=coal, year=2020"),
        (
            ((("year", "2040"),), (("year", "2045"),), (("year", "2100"),)),
            "year=2040..2100 (3 values)",
        ),
        (
            ((("year", "5"),), (("year", "10"),)),
            "year=5..10 (2 values)",
        ),
        (
            (
                (("name", "coal"), ("year", "2020")),
                (("name", "coal"), ("year", "2025")),
            ),
            "name=coal, year=2020..2025 (2 values)",
        ),
        (
            ((("name", "b"),), (("name", "a"),), (("name", "c, d"),)),
            'name: a, b, "c, d" (3 values)',
        ),
        (
            (
                (("name", "e"),),
                (("name", "d"),),
                (("name", "c"),),
                (("name", "b"),),
                (("name", "a"),),
            ),
            "name: a, b, c, +2 more (5 values)",
        ),
    ],
)
def test_summarize_identities(identities, expected: str) -> None:
    assert summarize_identities(identities) == expected


def test_grouped_finding_fields_and_summary_elements() -> None:
    grouped = _finding(
        FindingKind.ELEMENT_REMOVED,
        identity=(),
        identities=((("year", "2040"),), (("year", "2045"),)),
        members=2,
        baseline=NodeSnapshot(count=2, metadata=None, subtree_size=2),
        candidate=None,
    )
    assert grouped.is_grouped
    assert not _finding().is_grouped
    data = grouped.to_dict()
    json.dumps(data)
    assert data["members"] == 2
    assert data["identities"] == [[["year", "2040"]], [["year", "2045"]]]
    summary = ComparisonSummary.from_findings(
        [grouped, _finding(), _finding(FindingKind.COUNT_CHANGED)]
    )
    assert summary.counts["element_removed"] == 1
    assert summary.elements == {"element_added": 1, "element_removed": 2}
    assert summary.to_dict()["elements"] == {"element_added": 1, "element_removed": 2}
