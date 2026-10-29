"""Unit tests for :mod:`gcamreader.structure.compare`.

Each test builds two small indexes from handcrafted XML and compares them.
Test names reference the acceptance criteria of the specification.

@license BSD 2-Clause
"""

from __future__ import annotations

import io
import itertools
import json
from pathlib import Path

import pytest

from gcamreader.structure import compare, extract
from gcamreader.structure.errors import StructureIndexError
from gcamreader.structure.index import StructureIndex
from gcamreader.structure.model import (
    Finding,
    FindingKind,
    InputIdentity,
    PairComparison,
    PathSegment,
    ScenarioRef,
)
from gcamreader.structure.rules import ComparisonRules

_counter = itertools.count()

BASE = """
<scenario name="GCAM" date="2026-1-6">
  <model-version>ver_9.0</model-version>
  <world>
    <region name="USA">
      <sector name="electricity" type="x">
        <subsector name="coal">
          <technology name="coal (conv pul)" year="2020">
            <physical-output unit="EJ" vintage="2020">1.5</physical-output>
            <physical-output unit="EJ" vintage="2025">1.4</physical-output>
            <input name="regional coal">0.1</input>
          </technology>
        </subsector>
        <subsector name="gas">
          <technology name="gas (CC)" year="2020"><input name="gas">0.2</input></technology>
        </subsector>
      </sector>
      <sector name="transport"><subsector name="road"/></sector>
    </region>
    <region name="China">
      <sector name="electricity"><subsector name="coal"/></sector>
    </region>
  </world>
</scenario>
"""

USA_ELEC = "scenario > world > region[name=USA] > sector[name=electricity]"
COAL_TECH = (
    USA_ELEC + " > subsector[name=coal] > technology[name=coal (conv pul), year=2020]"
)


def _input(index: int, name: str = "GCAM") -> InputIdentity:
    return InputIdentity(
        index, f"db{index}", f"db{index}", ScenarioRef(name, "d"), "f" * 8
    )


def build(
    xml: str, tmp_path: Path, rules: ComparisonRules | None = None
) -> StructureIndex:
    path = tmp_path / f"idx{next(_counter)}.sqlite"
    return extract.build_index_from_stream(
        io.BytesIO(xml.encode()),
        str(path),
        rules or ComparisonRules.default(),
        source="t",
    )


def compare_xml(
    a: str, b: str, tmp_path: Path, rules: ComparisonRules | None = None
) -> PairComparison:
    with build(a, tmp_path, rules) as left, build(b, tmp_path, rules) as right:
        return compare.compare_indexes(
            left, right, baseline_input=_input(1), candidate_input=_input(2)
        )


def kinds(result: PairComparison) -> list[tuple[str, str]]:
    return [(f.kind.value, f.path) for f in result.findings]


# --- criterion 1: identical ----------------------------------------------------


def test_identical_documents_have_no_findings(tmp_path: Path, monkeypatch) -> None:
    calls: list[bytes] = []
    original = StructureIndex.children

    def counting(self, parent_hash):
        calls.append(parent_hash)
        return original(self, parent_hash)

    monkeypatch.setattr(StructureIndex, "children", counting)
    result = compare_xml(BASE, BASE, tmp_path)
    assert result.findings == ()
    assert result.summary.total == 0
    assert result.warnings == ()
    # Both roots were loaded once each, then the walk stopped at the root.
    assert len(calls) == 2


# --- criterion 2: values, formatting, order, run metadata ignored ----------------


@pytest.mark.parametrize(
    "variant",
    [
        BASE.replace("1.5", "99.9").replace("0.1", "0"),  # values
        BASE.replace("  ", "").replace("\n", ""),  # formatting
        BASE.replace('name="GCAM" date="2026-1-6"', 'name="Other" date="2027-2-2"'),
        "<!-- c --><?pi x?>" + BASE.replace("<world>", "<world><!-- inside -->"),
        BASE.replace('<region name="China">', '<region name="China" xmlns:p="urn:x">'),
    ],
)
def test_ignored_differences(tmp_path: Path, variant: str) -> None:
    assert compare_xml(BASE, variant, tmp_path).findings == ()


def test_shuffled_siblings_ignored(tmp_path: Path) -> None:
    shuffled = BASE.replace(
        '<physical-output unit="EJ" vintage="2020">1.5</physical-output>\n'
        '            <physical-output unit="EJ" vintage="2025">1.4</physical-output>',
        '<physical-output unit="EJ" vintage="2025">1.4</physical-output>\n'
        '            <physical-output unit="EJ" vintage="2020">1.5</physical-output>',
    )
    assert shuffled != BASE
    assert compare_xml(BASE, shuffled, tmp_path).findings == ()


def test_element_containing_zero_is_still_present(tmp_path: Path) -> None:
    zero = BASE.replace(
        '<input name="regional coal">0.1</input>',
        '<input name="regional coal">0</input>',
    )
    assert compare_xml(BASE, zero, tmp_path).findings == ()
    gone = BASE.replace('<input name="regional coal">0.1</input>', "")
    assert kinds(compare_xml(BASE, gone, tmp_path)) == [
        ("element_removed", COAL_TECH + " > input[name=regional coal]")
    ]


# --- criterion 3: new sector under the right region --------------------------------


def test_added_sector_reported_once_with_descendants(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(compare, "HISTOGRAM_MIN_DESCENDANTS", 1)
    added = BASE.replace(
        '<region name="China">',
        '<region name="China"><sector name="new"><subsector name="s1"><technology name="t" year="2020"/>'
        '<technology name="t" year="2025"/></subsector><subsector name="s2"/></sector>',
    )
    result = compare_xml(BASE, added, tmp_path)
    (finding,) = result.findings
    assert finding.kind is FindingKind.ELEMENT_ADDED
    assert finding.path == "scenario > world > region[name=China] > sector[name=new]"
    assert finding.segments[-2:] == (
        PathSegment("region", (("name", "China"),)),
        PathSegment("sector", (("name", "new"),)),
    )
    assert finding.branch() == "region[name=China]"
    assert finding.baseline is None
    assert finding.candidate is not None and finding.candidate.count == 1
    assert finding.descendant_count == 4
    assert finding.descendant_tags == (("subsector", 2), ("technology", 2))
    assert "4 descendant element(s)" in finding.detail
    assert finding.members == 1 and not finding.is_grouped
    assert finding.identities == ((("name", "new"),),)
    assert result.summary.counts["element_added"] == 1
    assert result.summary.elements["element_added"] == 1
    assert result.summary.descendants_summarized == 4


def test_histogram_threshold_default(tmp_path: Path) -> None:
    added = BASE.replace(
        '<region name="China">',
        '<region name="China"><sector name="new"><subsector name="s1"><technology name="t" year="2020"/>'
        '<technology name="t" year="2025"/></subsector><subsector name="s2"/></sector>',
    )
    (finding,) = compare_xml(BASE, added, tmp_path).findings
    assert compare.HISTOGRAM_MIN_DESCENDANTS == 50
    assert finding.descendant_count == 4  # always available
    assert finding.descendant_tags == ()  # below the threshold: no histogram


# --- criterion 4: removal, move, counts --------------------------------------------


def test_removed_technology_is_one_finding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(compare, "HISTOGRAM_MIN_DESCENDANTS", 1)
    removed = BASE.replace(
        '<subsector name="gas">\n          <technology name="gas (CC)" year="2020"><input name="gas">0.2</input></technology>\n        </subsector>',
        '<subsector name="gas"/>',
    )
    result = compare_xml(BASE, removed, tmp_path)
    assert kinds(result) == [
        (
            "element_removed",
            USA_ELEC + " > subsector[name=gas] > technology[name=gas (CC), year=2020]",
        )
    ]
    (finding,) = result.findings
    assert finding.descendant_count == 1
    assert finding.descendant_tags == (("input", 1),)
    assert finding.baseline is not None and finding.candidate is None


def test_moved_subsector_is_removed_plus_added(tmp_path: Path) -> None:
    moved = BASE.replace(
        '<subsector name="gas">\n          <technology name="gas (CC)" year="2020"><input name="gas">0.2</input></technology>\n        </subsector>',
        "",
    ).replace(
        '<sector name="transport">',
        '<sector name="transport"><subsector name="gas"><technology name="gas (CC)" year="2020"><input name="gas">0.2</input></technology></subsector>',
    )
    result = compare_xml(BASE, moved, tmp_path)
    assert kinds(result) == [
        ("element_removed", USA_ELEC + " > subsector[name=gas]"),
        (
            "element_added",
            "scenario > world > region[name=USA] > sector[name=transport] > subsector[name=gas]",
        ),
    ]


def test_count_changed_for_identical_siblings(tmp_path: Path) -> None:
    two = "<r><x name='a'/><x name='a'/></r>"
    three = "<r><x name='a'/><x name='a'/><x name='a'/></r>"
    result = compare_xml(two, three, tmp_path)
    (finding,) = result.findings
    assert finding.kind is FindingKind.COUNT_CHANGED
    assert finding.path == "r > x[name=a]"
    assert (finding.baseline.count, finding.candidate.count) == (2, 3)
    assert finding.detail == "Count changed: 2 in baseline, 3 in candidate"


def test_count_changed_with_several_variants(tmp_path: Path) -> None:
    a = "<r><x name='a' u='1'/><x name='a' u='2'/></r>"
    b = "<r><x name='a' u='1'/><x name='a' u='2'/><x name='a' u='2'/></r>"
    (finding,) = compare_xml(a, b, tmp_path).findings
    assert finding.kind is FindingKind.COUNT_CHANGED
    assert "u=2 (x1)" in finding.detail and "u=2 (x2)" in finding.detail


# --- criterion 5: years, units, attributes ------------------------------------------


def test_missing_vintage_is_removed_element(tmp_path: Path) -> None:
    fewer = BASE.replace(
        '<physical-output unit="EJ" vintage="2025">1.4</physical-output>', ""
    )
    assert kinds(compare_xml(BASE, fewer, tmp_path)) == [
        ("element_removed", COAL_TECH + " > physical-output[vintage=2025]")
    ]


def test_unit_change_is_attribute_changed(tmp_path: Path) -> None:
    changed = BASE.replace('unit="EJ" vintage="2025"', 'unit="PJ" vintage="2025"')
    (finding,) = compare_xml(BASE, changed, tmp_path).findings
    assert finding.kind is FindingKind.ATTRIBUTE_CHANGED
    assert finding.path == COAL_TECH + " > physical-output[vintage=2025]"
    (change,) = finding.attribute_changes
    assert (change.attribute, change.baseline, change.candidate, change.change) == (
        "unit",
        "EJ",
        "PJ",
        "changed",
    )
    assert finding.detail == "Attributes changed: unit changed ('EJ' -> 'PJ')"
    assert finding.baseline.metadata == (("unit", "EJ"),)
    assert finding.candidate.metadata == (("unit", "PJ"),)


def test_attribute_added_and_removed(tmp_path: Path) -> None:
    changed = BASE.replace(
        '<sector name="electricity" type="x">',
        '<sector name="electricity" fillout="1">',
    )
    (finding,) = compare_xml(BASE, changed, tmp_path).findings
    assert finding.kind is FindingKind.ATTRIBUTE_CHANGED
    assert finding.path == USA_ELEC
    assert [(c.attribute, c.change) for c in finding.attribute_changes] == [
        ("fillout", "added"),
        ("type", "removed"),
    ]
    assert "fillout added (absent -> '1')" in finding.detail
    assert "type removed ('x' -> absent)" in finding.detail


def test_attribute_and_count_change_both_reported(tmp_path: Path) -> None:
    a = "<r><x name='a' u='1'/></r>"
    b = "<r><x name='a' u='2'/><x name='a' u='2'/></r>"
    result = compare_xml(a, b, tmp_path)
    assert [f.kind for f in result.findings] == [
        FindingKind.ATTRIBUTE_CHANGED,
        FindingKind.COUNT_CHANGED,
    ]


def test_metadata_variants_not_collapsed(tmp_path: Path) -> None:
    a = "<r><x name='a' unit='EJ'/><x name='a' unit='EJ'/></r>"
    b = "<r><x name='a' unit='EJ'/><x name='a' unit='PJ'/></r>"
    (finding,) = compare_xml(a, b, tmp_path).findings
    assert finding.kind is FindingKind.METADATA_VARIANTS_CHANGED
    assert finding.baseline.variants == (((("unit", "EJ"),), 2),)
    assert finding.candidate.variants == (
        ((("unit", "EJ"),), 1),
        ((("unit", "PJ"),), 1),
    )
    assert finding.baseline.metadata == (("unit", "EJ"),)
    assert finding.candidate.metadata is None
    assert finding.detail == (
        "Metadata variants differ (not collapsed): baseline unit=EJ (x2); "
        "candidate unit=EJ (x1); unit=PJ (x1)"
    )
    assert finding.attribute_changes == ()


def test_variants_with_no_attributes_formatted(tmp_path: Path) -> None:
    a = "<r><x name='a'/></r>"
    b = "<r><x name='a'/><x name='a' u='1'/></r>"
    (finding,) = compare_xml(a, b, tmp_path).findings
    assert "no attributes (x1); u=1 (x1)" in finding.detail


# --- criterion 7 and 8: scenarios, unfamiliar branches -------------------------------


def test_root_identity_difference_when_name_not_excluded(tmp_path: Path) -> None:
    rules = ComparisonRules(excluded_attributes=["/scenario/@date"])
    other = BASE.replace('name="GCAM"', 'name="Other"')
    result = compare_xml(BASE, other, tmp_path, rules)
    assert kinds(result) == [
        ("element_added", "scenario[name=Other]"),
        ("element_removed", "scenario[name=GCAM]"),
    ]
    assert (
        result.findings[0].descendant_count == result.findings[1].descendant_count == 17
    )


def test_unfamiliar_elements_detected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(compare, "HISTOGRAM_MIN_DESCENDANTS", 1)
    novel = BASE.replace(
        '<region name="China">',
        '<region name="China"><hydrogen-hub name="h2" capacity-unit="GW"><node id="1" year="2030"/></hydrogen-hub>',
    )
    result = compare_xml(BASE, novel, tmp_path)
    (finding,) = result.findings
    assert (
        finding.path == "scenario > world > region[name=China] > hydrogen-hub[name=h2]"
    )
    assert finding.candidate.metadata == (("capacity-unit", "GW"),)
    assert finding.descendant_tags == (("node", 1),)
    # And a change deep inside an unfamiliar element is still localized.
    novel2 = novel.replace('<node id="1" year="2030"/>', '<node id="2" year="2030"/>')
    (finding,) = compare_xml(novel, novel2, tmp_path).findings
    assert finding.kind is FindingKind.ATTRIBUTE_CHANGED
    assert finding.path.endswith("hydrogen-hub[name=h2] > node[year=2030]")
    assert finding.attribute_changes[0].attribute == "id"


# --- structure of the walk ------------------------------------------------------------


def test_leaf_gaining_children(tmp_path: Path) -> None:
    a = "<r><s name='a'/></r>"
    b = "<r><s name='a'><t name='x'/><t name='y'/></s></r>"
    result = compare_xml(a, b, tmp_path)
    # Two added siblings with the same tag are one grouped finding.
    assert kinds(result) == [
        ("element_added", "r > s[name=a] > t[name: x, y (2 values)]"),
    ]
    (finding,) = result.findings
    assert finding.is_grouped and finding.members == 2
    assert finding.identity == ()
    assert finding.identities == ((("name", "x"),), (("name", "y"),))
    assert finding.segments[-1] == PathSegment("t", ())
    assert finding.candidate.count == 2 and finding.candidate.metadata is None
    assert finding.detail == (
        "Added 2 t elements (2 occurrence(s), 0 descendant element(s)): "
        "t[name=x], t[name=y]"
    )
    assert result.summary.counts["element_added"] == 1
    assert result.summary.elements["element_added"] == 2
    reverse = compare_xml(b, a, tmp_path)
    assert [(f.kind.value, f.members) for f in reverse.findings] == [
        ("element_removed", 2)
    ]


def test_findings_in_deterministic_depth_first_order(tmp_path: Path) -> None:
    a = "<r><b name='1'><x/><y name='k' u='1'/></b><a name='2'><z/></a><c/></r>"
    b = "<r><b name='1'><y name='k' u='2'/></b><a name='2'><z/><w/></a><c/><d/></r>"
    result = compare_xml(a, b, tmp_path)
    # Per position: in-place changes first, then added, then removed groups;
    # children are visited depth-first in sorted order.
    assert kinds(result) == [
        ("element_added", "r > d"),
        ("element_added", "r > a[name=2] > w"),
        ("attribute_changed", "r > b[name=1] > y[name=k]"),
        ("element_removed", "r > b[name=1] > x"),
    ]
    again = compare_xml(a, b, tmp_path)
    assert json.dumps(again.to_dict(), sort_keys=True) == json.dumps(
        result.to_dict(), sort_keys=True
    )


def test_duplicate_parent_warnings_carried(tmp_path: Path) -> None:
    dup = "<r><s name='a'><t/></s><s name='a'><t/></s></r>"
    result = compare_xml(dup, "<r><s name='a'><t/></s></r>", tmp_path)
    assert len(result.warnings) == 1
    assert result.warnings[0].startswith("db1: 1 repeated sibling")
    assert kinds(result) == [
        ("count_changed", "r > s[name=a]"),
        ("count_changed", "r > s[name=a] > t"),
    ]


def test_incompatible_indexes_rejected(tmp_path: Path) -> None:
    a = build("<r/>", tmp_path)
    b = build("<r/>", tmp_path, ComparisonRules(ignored_attributes=["x"]))
    with pytest.raises(StructureIndexError, match="different comparison rules"):
        compare.compare_indexes(
            a, b, baseline_input=_input(1), candidate_input=_input(2)
        )
    a.close()
    b.close()


def test_descendant_tag_counts(tmp_path: Path) -> None:
    xml = "<r><s name='a'><t/><t/><u><v/></u></s><s name='a'><t/></s></r>"
    with build(xml, tmp_path) as index:
        (s,) = [row for row in index.children(index.root().path_hash)]
        assert s.count == 2
        assert index.descendant_tag_counts(s.path_hash) == (
            ("t", 3),
            ("u", 1),
            ("v", 1),
        )
        (leaf,) = [r for r in index.children(s.path_hash) if r.tag == "t"]
        assert leaf.path_hash is None


def test_format_variants() -> None:
    assert compare.format_variants(()) == ""
    assert compare.format_variants((((), 2),)) == "no attributes (x2)"
    assert compare.format_variants((((("a", "1"), ("b", "2")), 1),)) == "a=1, b=2 (x1)"


# --- matrix ------------------------------------------------------------------------------


def test_build_matrix_three_inputs(tmp_path: Path) -> None:
    base = "<r><x name='a' u='1'/><y name='b'/><z name='c'/><z name='c'/></r>"
    cand1 = "<r><x name='a' u='2'/><z name='c'/><z name='c'/><n/></r>"  # unit change, y removed, n added
    cand2 = "<r><x name='a' u='3' w='9'/><y name='b'/><z name='c'/></r>"  # unit change + w added, z count
    inputs = (_input(1), _input(2), _input(3))
    with (
        build(base, tmp_path) as b,
        build(cand1, tmp_path) as c1,
        build(cand2, tmp_path) as c2,
    ):
        comparisons = (
            compare.compare_indexes(
                b, c1, baseline_input=inputs[0], candidate_input=inputs[1]
            ),
            compare.compare_indexes(
                b, c2, baseline_input=inputs[0], candidate_input=inputs[2]
            ),
        )
    matrix = compare.build_matrix(inputs, comparisons)
    table = {(row.path, row.kind.value, row.attribute): row.cells for row in matrix}
    assert table == {
        ("r > n", "element_added", None): ("absent", "present", "absent"),
        ("r > x[name=a]", "attribute_changed", "u"): ("1", "2", "3"),
        ("r > x[name=a]", "attribute_changed", "w"): ("absent", "absent", "9"),
        ("r > y[name=b]", "element_removed", None): ("present", "absent", "present"),
        ("r > z[name=c]", "count_changed", None): ("2", "2", "1"),
    }
    assert [row.path for row in matrix] == sorted(row.path for row in matrix)
    json.dumps([row.to_dict() for row in matrix])


def test_build_matrix_variants_and_order(tmp_path: Path) -> None:
    base = "<r><x name='a' u='1'/><x name='a' u='1'/></r>"
    cand = "<r><x name='a' u='1'/><x name='a' u='2'/></r>"
    inputs = (_input(1), _input(2))
    with build(base, tmp_path) as b, build(cand, tmp_path) as c:
        comparison = compare.compare_indexes(
            b, c, baseline_input=inputs[0], candidate_input=inputs[1]
        )
    (row,) = compare.build_matrix(inputs, (comparison,))
    assert row.kind is FindingKind.METADATA_VARIANTS_CHANGED
    assert row.cells == ("u=1 (x2)", "u=1 (x1); u=2 (x1)")


def test_build_matrix_baseline_not_first_column(tmp_path: Path) -> None:
    inputs = (_input(2), _input(1))  # baseline (1) listed second
    with build("<r><a/></r>", tmp_path) as b, build("<r/>", tmp_path) as c:
        comparison = compare.compare_indexes(
            b, c, baseline_input=_input(1), candidate_input=_input(2)
        )
    (row,) = compare.build_matrix(inputs, (comparison,))
    assert row.cells == ("absent", "present")


def test_build_matrix_empty_and_mismatched_baseline() -> None:
    assert compare.build_matrix((_input(1),), ()) == ()
    c1 = PairComparison.build(_input(1), _input(2), [])
    c2 = PairComparison.build(_input(3), _input(2), [])
    with pytest.raises(ValueError, match="same baseline"):
        compare.build_matrix((_input(1), _input(2), _input(3)), (c1, c2))


def test_finding_objects_are_json_safe(tmp_path: Path) -> None:
    changed = BASE.replace('unit="EJ" vintage="2025"', 'unit="PJ" vintage="2025"')
    result = compare_xml(BASE, changed, tmp_path)
    data = result.to_dict()
    json.dumps(data)
    assert isinstance(result.findings[0], Finding)
    assert data["summary"]["counts"]["attribute_changed"] == 1


def test_descendant_tag_counts_many_batches(tmp_path: Path) -> None:
    xml = (
        "<r><a name='1'><t/><t/><u><v/></u></a><a name='2'><t/></a>"
        "<a name='3'/><a name='4'><w/></a></r>"
    )
    with build(xml, tmp_path) as index:
        rows = index.children(index.root().path_hash)
        seeds = [row.path_hash for row in rows if row.path_hash is not None]
        assert len(seeds) == 3  # a[3] is a leaf
        many = index.descendant_tag_counts_many(seeds, batch_size=2)
        by_name = {row.identity[0][1]: row.path_hash for row in rows}
        assert many[by_name["1"]] == (("t", 2), ("u", 1), ("v", 1))
        assert many[by_name["2"]] == (("t", 1),)
        assert many[by_name["4"]] == (("w", 1),)
        assert all(many[h] == index.descendant_tag_counts(h) for h in seeds)
        assert index.descendant_tag_counts_many([]) == {}
        # Duplicate seeds are tolerated.
        assert index.descendant_tag_counts_many([seeds[0], seeds[0]]) == {
            seeds[0]: many[seeds[0]]
        }


def test_one_sided_findings_get_histograms_after_walk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(compare, "HISTOGRAM_MIN_DESCENDANTS", 1)
    a = "<r><s name='x'><t/><t/><u name='k'><v/></u></s><leaf/></r>"
    result = compare_xml(a, "<r/>", tmp_path)
    by_path = {f.path: f for f in result.findings}
    assert by_path["r > s[name=x]"].descendant_tags == (("t", 2), ("u", 1), ("v", 1))
    assert by_path["r > s[name=x]"].descendant_count == 4
    assert by_path["r > leaf"].descendant_tags == ()
    assert by_path["r > leaf"].descendant_count == 0


# --- grouped one-sided findings ----------------------------------------------------------


def _years(tag: str, years: range, children: str = "") -> str:
    return "".join(f"<{tag} year='{y}'>{children}</{tag}>" for y in years)


def test_grouped_leaf_siblings(tmp_path: Path) -> None:
    base = (
        "<r><tech name='coal'>"
        + _years("Non-CO2", range(2020, 2105, 5))
        + "</tech></r>"
    )
    cand = (
        "<r><tech name='coal'>"
        + _years("Non-CO2", range(2020, 2040, 5))
        + "</tech></r>"
    )
    result = compare_xml(base, cand, tmp_path)
    (finding,) = result.findings
    assert finding.kind is FindingKind.ELEMENT_REMOVED
    assert finding.path == "r > tech[name=coal] > Non-CO2[year=2040..2100 (13 values)]"
    assert finding.members == 13
    assert [dict(i)["year"] for i in finding.identities] == [
        str(y) for y in range(2040, 2105, 5)
    ]
    assert finding.baseline.count == 13 and finding.baseline.subtree_size == 13
    assert finding.candidate is None
    assert finding.descendant_count == 0 and finding.descendant_tags == ()
    assert finding.detail.startswith(
        "Removed 13 Non-CO2 elements (13 occurrence(s), 0 descendant element(s)): Non-CO2[year=2040]"
    )
    assert finding.detail.endswith("Non-CO2[year=2100]")
    assert finding.branch() == "r"  # no scenario/world to skip
    assert result.summary.elements == {"element_added": 0, "element_removed": 13}
    assert result.summary.total == 1


def test_grouped_siblings_separate_by_tag_and_kind(tmp_path: Path) -> None:
    base = "<r><a name='1'/><a name='2'/><b name='1'/><c name='1'/></r>"
    cand = "<r><c name='1'/><c name='2'/><d name='1'/><d name='2'/><d name='3'/></r>"
    result = compare_xml(base, cand, tmp_path)
    assert [(f.kind.value, f.tag, f.members, f.path) for f in result.findings] == [
        ("element_added", "c", 1, "r > c[name=2]"),
        ("element_added", "d", 3, "r > d[name=1..3 (3 values)]"),
        ("element_removed", "a", 2, "r > a[name=1..2 (2 values)]"),
        ("element_removed", "b", 1, "r > b[name=1]"),
    ]


def test_grouped_siblings_histogram_covers_qualifying_members(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(compare, "HISTOGRAM_MIN_DESCENDANTS", 3)
    big = "<t name='x'><p/><p/><q/></t>"  # 3 descendants: qualifies
    small = "<t name='y'><p/></t>"  # 1 descendant: counted, not summarised
    base = "<r><s>" + big + small + "<t name='z'/></s></r>"
    result = compare_xml(base, "<r><s/></r>", tmp_path)
    (finding,) = result.findings
    assert finding.members == 3
    assert finding.descendant_count == 4
    assert finding.descendant_tags == (("p", 2), ("q", 1))
    # Everything qualifies when the threshold is lowered.
    monkeypatch.setattr(compare, "HISTOGRAM_MIN_DESCENDANTS", 1)
    (finding,) = compare_xml(base, "<r><s/></r>", tmp_path).findings
    assert finding.descendant_tags == (("p", 3), ("q", 1))


def test_grouped_findings_in_matrix(tmp_path: Path) -> None:
    base = "<r>" + _years("x", range(2020, 2035, 5)) + "</r>"
    inputs = (_input(1), _input(2), _input(3))
    with (
        build(base, tmp_path) as b,
        build("<r/>", tmp_path) as c1,
        build(base, tmp_path) as c2,
    ):
        comparisons = (
            compare.compare_indexes(
                b, c1, baseline_input=inputs[0], candidate_input=inputs[1]
            ),
            compare.compare_indexes(
                b, c2, baseline_input=inputs[0], candidate_input=inputs[2]
            ),
        )
    (row,) = compare.build_matrix(inputs, comparisons)
    assert row.path == "r > x[year=2020..2030 (3 values)]"
    assert row.cells == ("present", "absent", "present")


def test_grouped_findings_are_json_safe_and_deterministic(tmp_path: Path) -> None:
    base = "<r><s name='a'>" + _years("x", range(2020, 2050, 5)) + "</s></r>"
    cand = "<r><s name='a'>" + _years("x", range(2020, 2030, 5)) + "</s></r>"
    first = compare_xml(base, cand, tmp_path).to_dict()
    second = compare_xml(base, cand, tmp_path).to_dict()
    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)
    assert first["findings"][0]["members"] == 4
    assert len(first["findings"][0]["identities"]) == 4
