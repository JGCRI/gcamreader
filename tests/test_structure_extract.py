"""Unit tests for the structure index and the stream-based extractor.

All tests feed in-memory XML through
:func:`gcamreader.structure.extract.build_index_from_stream`; no Java or
database is required.

@license BSD 2-Clause
"""

from __future__ import annotations

import hashlib
import io
import itertools
import sqlite3
from pathlib import Path

import pytest

from gcamreader.structure import extract
from gcamreader.structure.errors import ExtractionError, StructureIndexError
from gcamreader.structure.index import (
    RAW_SUFFIX,
    ROOT_PARENT_HASH,
    SCHEMA_VERSION,
    SEP,
    IndexWriter,
    NodeRow,
    StructureIndex,
    decode_pairs,
    encode_pairs,
)
from gcamreader.structure.rules import ComparisonRules

SAMPLE = """<?xml version="1.0"?>
<scenario name="GCAM" date="2026-1-6T10:39:43-07:00">
  <model-version>ver_9.0</model-version>
  <world>
    <region name="USA">
      <sector name="electricity" type="x">
        <subsector name="coal">
          <technology name="coal (conv pul)" year="2020">
            <physical-output unit="EJ" vintage="2020">1.5</physical-output>
            <physical-output unit="EJ" vintage="2025">1.4</physical-output>
          </technology>
        </subsector>
      </sector>
    </region>
    <region name="China"/>
  </world>
</scenario>
"""
SAMPLE_ELEMENTS = 10

_counter = itertools.count()


def build(
    xml: str, tmp_path: Path, rules: ComparisonRules | None = None, **kwargs
) -> StructureIndex:
    path = tmp_path / f"index{next(_counter)}.sqlite"
    return extract.build_index_from_stream(
        io.BytesIO(xml.encode("utf-8")),
        str(path),
        rules or ComparisonRules.default(),
        source=kwargs.pop("source", "test"),
        **kwargs,
    )


Row = tuple[str, tuple, int, int, tuple, bool, int]


def dump(index: StructureIndex) -> list[Row]:
    """Canonical, order-independent listing of an index."""
    out: list[Row] = []
    seen: set[bytes] = set()

    def walk(parent_hash: bytes, prefix: str) -> None:
        for row in index.children(parent_hash):
            ident = ", ".join(f"{k}={v}" for k, v in row.identity)
            path = f"{prefix}{row.tag}[{ident}]" if ident else f"{prefix}{row.tag}"
            out.append(
                (
                    path,
                    row.metadata,
                    row.count,
                    row.subtree_size,
                    row.fingerprint,
                    row.is_leaf,
                    row.depth,
                )
            )
            if row.path_hash is not None and row.path_hash not in seen:
                seen.add(row.path_hash)
                walk(row.path_hash, path + " > ")

    walk(ROOT_PARENT_HASH, "")
    return out


def paths(index: StructureIndex) -> dict[str, Row]:
    return {row[0]: row for row in dump(index)}


# --- pair encoding ------------------------------------------------------------


def test_encode_decode_pairs() -> None:
    pairs = (("name", "USA"), ("year", "2020"))
    assert encode_pairs(pairs) == f"name{SEP}USA{SEP}year{SEP}2020"
    assert decode_pairs(encode_pairs(pairs)) == pairs
    assert encode_pairs(()) == ""
    assert decode_pairs("") == ()
    assert decode_pairs(f"unit{SEP}") == (("unit", ""),)


# --- basic structure ----------------------------------------------------------


def test_sample_structure(tmp_path: Path) -> None:
    with build(SAMPLE, tmp_path) as index:
        assert index.element_count == SAMPLE_ELEMENTS
        root = index.root()
        assert root.tag == "scenario"
        assert root.depth == 1
        assert root.identity == ()  # name excluded on the root by default
        assert root.metadata == ()  # date excluded too
        assert root.count == 1
        assert root.subtree_size == SAMPLE_ELEMENTS
        assert not root.is_leaf

        rows = paths(index)
        assert set(rows) == {
            "scenario",
            "scenario > model-version",
            "scenario > world",
            "scenario > world > region[name=USA]",
            "scenario > world > region[name=China]",
            "scenario > world > region[name=USA] > sector[name=electricity]",
            "scenario > world > region[name=USA] > sector[name=electricity] > subsector[name=coal]",
            "scenario > world > region[name=USA] > sector[name=electricity] > subsector[name=coal] > technology[name=coal (conv pul), year=2020]",
            "scenario > world > region[name=USA] > sector[name=electricity] > subsector[name=coal] > technology[name=coal (conv pul), year=2020] > physical-output[vintage=2020]",
            "scenario > world > region[name=USA] > sector[name=electricity] > subsector[name=coal] > technology[name=coal (conv pul), year=2020] > physical-output[vintage=2025]",
        }
        sector = rows["scenario > world > region[name=USA] > sector[name=electricity]"]
        assert sector[1] == (("type", "x"),)  # metadata
        assert sector[2] == 1 and sector[3] == 5 and sector[6] == 4
        leaf = rows[
            "scenario > world > region[name=USA] > sector[name=electricity] > subsector[name=coal] > technology[name=coal (conv pul), year=2020] > physical-output[vintage=2020]"
        ]
        assert leaf[1] == (("unit", "EJ"),)
        assert leaf[5] is True and leaf[3] == 1 and leaf[6] == 7
        assert rows["scenario > world > region[name=China]"][5] is True
        assert rows["scenario > world"][5] is False
        assert index.node_count() == SAMPLE_ELEMENTS
        assert sum(1 for _ in index.iter_rows()) == SAMPLE_ELEMENTS


def test_children_sorted_by_tag_then_identity(tmp_path: Path) -> None:
    xml = (
        "<r><b name='2'/><a name='z'/><b name='1'/><a name='a' year='3'/>"
        "<a name='a' year='1'/></r>"
    )
    with build(xml, tmp_path) as index:
        children = index.children(index.root().path_hash)
        assert [(c.tag, c.identity) for c in children] == [
            ("a", (("name", "a"), ("year", "1"))),
            ("a", (("name", "a"), ("year", "3"))),
            ("a", (("name", "z"),)),
            ("b", (("name", "1"),)),
            ("b", (("name", "2"),)),
        ]


def test_repeated_identical_siblings_are_counted(tmp_path: Path) -> None:
    xml = "<r><x name='a'/><x name='a'/><x name='a'/><x name='b'/></r>"
    with build(xml, tmp_path) as index:
        rows = paths(index)
        assert rows["r > x[name=a]"][2] == 3
        assert rows["r > x[name=a]"][3] == 3
        assert rows["r > x[name=b]"][2] == 1
        assert rows["r"][3] == 5


def test_metadata_variants_are_separate_rows(tmp_path: Path) -> None:
    xml = "<r><x name='a' unit='EJ'/><x name='a' unit='PJ'/><x name='a' unit='EJ'/></r>"
    with build(xml, tmp_path) as index:
        variants = [
            (row.metadata, row.count) for row in index.children(index.root().path_hash)
        ]
        assert variants == [((("unit", "EJ"),), 2), ((("unit", "PJ"),), 1)]


def test_attribute_order_does_not_matter(tmp_path: Path) -> None:
    a = "<r><x unit='EJ' type='t' name='a' year='1'/></r>"
    b = "<r><x year='1' name='a' type='t' unit='EJ'/></r>"
    with build(a, tmp_path) as ia, build(b, tmp_path) as ib:
        assert dump(ia) == dump(ib)
        (row,) = ia.children(ia.root().path_hash)
        assert row.identity == (("name", "a"), ("year", "1"))
        assert row.metadata == (("type", "t"), ("unit", "EJ"))


def test_element_text_is_ignored(tmp_path: Path) -> None:
    a = "<r><x name='a'>1.5</x><y/></r>"
    b = "<r><x name='a'>0</x><y>text</y></r>"
    with build(a, tmp_path) as ia, build(b, tmp_path) as ib:
        assert dump(ia) == dump(ib)


# --- duplicate parents --------------------------------------------------------


def test_duplicate_parent_merges_children_and_warns(tmp_path: Path) -> None:
    xml = (
        "<r><s name='a'><t name='x'/><t name='y'/></s>"
        "<s name='a'><t name='x'/></s>"
        "<s name='b'><t name='x'/></s></r>"
    )
    with build(xml, tmp_path) as index:
        rows = paths(index)
        assert rows["r > s[name=a]"][2] == 2  # two occurrences
        assert rows["r > s[name=a]"][3] == 5  # 2 parents + 3 children
        assert rows["r > s[name=a] > t[name=x]"][2] == 2  # merged across parents
        assert rows["r > s[name=a] > t[name=y]"][2] == 1
        assert rows["r > s[name=b] > t[name=x]"][2] == 1
        assert len(index.warnings) == 1
        assert "/r/s" in index.warnings[0]
        assert "merged" in index.warnings[0]


def test_duplicate_leaf_siblings_do_not_warn(tmp_path: Path) -> None:
    with build("<r><x name='a'/><x name='a'/></r>", tmp_path) as index:
        assert index.warnings == []


def test_duplicate_parent_detected_when_first_occurrence_is_leaf(
    tmp_path: Path,
) -> None:
    xml = "<r><s name='a'/><s name='a'><t/></s></r>"
    with build(xml, tmp_path) as index:
        rows = paths(index)
        assert rows["r > s[name=a]"][2] == 2
        assert rows["r > s[name=a]"][5] is False  # has a path hash now
        assert "r > s[name=a] > t" in rows
        assert len(index.warnings) == 1


# --- rules --------------------------------------------------------------------


def test_excluded_and_ignored_attributes(tmp_path: Path) -> None:
    rules = ComparisonRules(
        excluded_attributes=["/scenario/@name", "/scenario/world/@foo"],
        ignored_attributes=["fillout"],
    )
    xml = (
        "<scenario name='n' date='d'><world foo='1' bar='2'>"
        "<region name='USA' fillout='1' unit='u'/></world></scenario>"
    )
    with build(xml, tmp_path, rules) as index:
        rows = paths(index)
        assert rows["scenario"][1] == (("date", "d"),)  # date no longer excluded
        assert rows["scenario > world"][1] == (("bar", "2"),)
        assert rows["scenario > world > region[name=USA]"][1] == (("unit", "u"),)


def test_excluded_element_subtree_skipped_but_counted(tmp_path: Path) -> None:
    rules = ComparisonRules(excluded_elements=["/scenario/world/Marketplace"])
    xml = (
        "<scenario><world><Marketplace><market name='m'><p/><p/></market></Marketplace>"
        "<region name='USA'/></world></scenario>"
    )
    with build(xml, tmp_path, rules, expected_elements=7) as index:
        rows = paths(index)
        assert set(rows) == {
            "scenario",
            "scenario > world",
            "scenario > world > region[name=USA]",
        }
        assert index.element_count == 7
        assert rows["scenario"][3] == 3  # subtree size counts indexed elements only


def test_per_element_identity_override(tmp_path: Path) -> None:
    rules = ComparisonRules(identity_by_element={"technology": ["year"]})
    xml = "<r><technology name='a' year='1'/><technology name='b' year='1'/></r>"
    with build(xml, tmp_path, rules) as index:
        rows = index.children(index.root().path_hash)
        # One identity (year=1) with two metadata variants, kept as two rows.
        assert [(r.identity, r.metadata, r.count) for r in rows] == [
            ((("year", "1"),), (("name", "a"),), 1),
            ((("year", "1"),), (("name", "b"),), 1),
        ]
        assert "r > technology[year=1]" in paths(index)


# --- fingerprints -------------------------------------------------------------


BASE = (
    "<r><a name='x' unit='u'><b year='1'>1.0</b><b year='2'>2.0</b></a>"
    "<a name='y'/><c/></r>"
)
VARIANTS = {
    "shuffled siblings": "<r><c/><a name='y'/><a name='x' unit='u'><b year='2'>2.0</b><b year='1'>1.0</b></a></r>",
    "changed values": "<r><a name='x' unit='u'><b year='1'>9.9</b><b year='2'>0</b></a><a name='y'/><c/></r>",
    "indented": "<r>\n  <a name='x' unit='u'>\n    <b year='1'>1.0</b>\n    <b year='2'>2.0</b>\n  </a>\n  <a name='y'/>\n  <c/>\n</r>\n",
    "comments and PIs": "<?xml version='1.0'?><!-- c --><r><?pi x?><a name='x' unit='u'><!-- c --><b year='1'>1.0</b><b year='2'>2.0</b></a><a name='y'/><c/></r>",
    "attribute order": "<r><a unit='u' name='x'><b year='1'>1.0</b><b year='2'>2.0</b></a><a name='y'/><c/></r>",
    "empty vs self-closing": "<r><a name='x' unit='u'><b year='1'>1.0</b><b year='2'>2.0</b></a><a name='y'></a><c></c></r>",
}


@pytest.mark.parametrize("label", sorted(VARIANTS))
def test_fingerprint_and_rows_invariant(tmp_path: Path, label: str) -> None:
    with build(BASE, tmp_path) as base, build(VARIANTS[label], tmp_path) as other:
        assert dump(base) == dump(other)
        assert base.root().fingerprint == other.root().fingerprint


def test_namespace_prefix_change_is_invariant(tmp_path: Path) -> None:
    a = "<p:r xmlns:p='urn:x'><p:a name='1'/></p:r>"
    b = "<q:r xmlns:q='urn:x'><q:a name='1'/></q:r>"
    with build(a, tmp_path) as ia, build(b, tmp_path) as ib:
        assert dump(ia) == dump(ib)
        assert ia.root().tag == "{urn:x}r"


@pytest.mark.parametrize(
    "changed",
    [
        "<r><a name='x' unit='v'><b year='1'/><b year='2'/></a><a name='y'/><c/></r>",  # metadata value
        "<r><a name='x' unit='u'><b year='1'/><b year='3'/></a><a name='y'/><c/></r>",  # identity value
        "<r><a name='x' unit='u'><b year='1'/><b year='2'/><b year='2'/></a><a name='y'/><c/></r>",  # count
        "<r><a name='x' unit='u'><b year='1'/><b year='2'/></a><a name='y'/><c/><d/></r>",  # added element
        "<r><a name='x' unit='u'><b year='1'/><b year='2'/></a><a name='y'/></r>",  # removed element
        "<r><a name='x'><b year='1'/><b year='2'/></a><a name='y'/><c/></r>",  # removed attribute
        "<r><a name='x' unit='u'><b year='1'/></a><a name='y'><b year='2'/></a><c/></r>",  # moved child
    ],
)
def test_fingerprint_detects_changes(tmp_path: Path, changed: str) -> None:
    original = (
        "<r><a name='x' unit='u'><b year='1'/><b year='2'/></a><a name='y'/><c/></r>"
    )
    with build(original, tmp_path) as a, build(changed, tmp_path) as b:
        assert a.root().fingerprint != b.root().fingerprint


def test_leaf_rows_have_no_path_hash_and_parents_do(tmp_path: Path) -> None:
    with build("<r><a name='x'><b/></a><c/></r>", tmp_path) as index:
        root = index.root()
        assert root.parent_hash == ROOT_PARENT_HASH
        assert (
            root.path_hash
            == hashlib.blake2b(
                ROOT_PARENT_HASH + f"r{SEP}".encode(), digest_size=16
            ).digest()
        )
        a, c = index.children(root.path_hash)
        assert a.tag == "a" and not a.is_leaf
        assert (
            a.path_hash
            == hashlib.blake2b(
                root.path_hash + f"a{SEP}name{SEP}x".encode(), digest_size=16
            ).digest()
        )
        assert c.tag == "c" and c.is_leaf
        (b,) = index.children(a.path_hash)
        assert b.is_leaf and b.parent_hash == a.path_hash
        assert index.children(b.parent_hash) == [b]


def test_subtree_fingerprint_is_sum_of_children(tmp_path: Path) -> None:
    # Fingerprint of a subtree depends only on its own position+metadata and
    # its children's fingerprints, so an identical subtree under an identical
    # path fingerprints identically in two documents.
    a = "<r><s name='1'><t name='a'/></s><s name='2'><t name='a'/><t name='b'/></s></r>"
    b = "<r><s name='2'><t name='b'/><t name='a'/></s><s name='1'><t name='a'/></s></r>"
    with build(a, tmp_path) as ia, build(b, tmp_path) as ib:
        pa = {r.identity: r.fingerprint for r in ia.children(ia.root().path_hash)}
        pb = {r.identity: r.fingerprint for r in ib.children(ib.root().path_hash)}
        assert pa == pb


# --- completeness and errors --------------------------------------------------


def test_truncated_stream_raises_and_leaves_no_file(tmp_path: Path) -> None:
    path = tmp_path / "trunc.sqlite"
    truncated = SAMPLE[: len(SAMPLE) // 2]
    with pytest.raises(ExtractionError, match="not well-formed or was truncated"):
        extract.build_index_from_stream(
            io.BytesIO(truncated.encode()),
            str(path),
            ComparisonRules.default(),
            source="db1",
        )
    assert not path.exists()


def test_empty_stream_raises(tmp_path: Path) -> None:
    with pytest.raises(ExtractionError):
        build("", tmp_path)


def test_two_document_elements_raise(tmp_path: Path) -> None:
    with pytest.raises(ExtractionError):
        build("<r/><r/>", tmp_path)


def test_expected_element_count_mismatch(tmp_path: Path) -> None:
    path = tmp_path / "count.sqlite"
    with pytest.raises(
        ExtractionError, match="read 10 elements but the database reports 11"
    ):
        extract.build_index_from_stream(
            io.BytesIO(SAMPLE.encode()),
            str(path),
            ComparisonRules.default(),
            source="db1",
            expected_elements=SAMPLE_ELEMENTS + 1,
        )
    assert not path.exists()
    with build(SAMPLE, tmp_path, expected_elements=SAMPLE_ELEMENTS) as index:
        assert index.element_count == SAMPLE_ELEMENTS


def test_existing_file_is_replaced(tmp_path: Path) -> None:
    path = tmp_path / "reuse.sqlite"
    path.write_text("garbage")
    index = extract.build_index_from_stream(
        io.BytesIO(SAMPLE.encode()), str(path), ComparisonRules.default(), source="s"
    )
    index.close()
    assert StructureIndex.open(str(path)).element_count == SAMPLE_ELEMENTS


def test_incomplete_index_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "incomplete.sqlite"
    writer = IndexWriter(str(path))
    writer.begin()
    writer.add_row((ROOT_PARENT_HASH, 1, 2, 2, None, 1, 1, 1, 0, 0, 0, 0))
    writer.flush()
    writer._conn.execute("COMMIT")  # leave tables but no meta_info/complete
    writer._conn.close()
    with pytest.raises(StructureIndexError, match="incomplete"):
        StructureIndex.open(str(path))


def test_schema_version_mismatch_refused(tmp_path: Path) -> None:
    index = build(SAMPLE, tmp_path)
    index.close()
    conn = sqlite3.connect(index.path)
    conn.execute(
        "UPDATE meta_info SET value = ? WHERE key = 'schema_version'",
        (str(SCHEMA_VERSION + 1),),
    )
    conn.commit()
    conn.close()
    with pytest.raises(StructureIndexError, match="schema"):
        StructureIndex.open(index.path)


def test_open_errors(tmp_path: Path) -> None:
    with pytest.raises(StructureIndexError, match="not found"):
        StructureIndex.open(str(tmp_path / "missing.sqlite"))
    bogus = tmp_path / "bogus.sqlite"
    bogus.write_bytes(b"not a database at all, definitely longer than a header")
    with pytest.raises(StructureIndexError, match="not a structure index"):
        StructureIndex.open(str(bogus))


def test_abort_removes_file(tmp_path: Path) -> None:
    path = tmp_path / "abort.sqlite"
    writer = IndexWriter(str(path))
    writer.begin()
    assert path.exists()
    writer.abort()
    assert not path.exists()
    writer.abort()  # idempotent


# --- metadata, progress, determinism -----------------------------------------


def test_meta_info(tmp_path: Path) -> None:
    rules = ComparisonRules(ignored_attributes=["fillout"])
    with build(
        SAMPLE,
        tmp_path,
        rules,
        source="data/db1",
        info={"scenario_name": "GCAM", "n": 3},
    ) as index:
        from gcamreader import __version__

        assert index.info["source"] == "data/db1"
        assert index.info["scenario_name"] == "GCAM"
        assert index.info_json("n") == 3
        assert index.info["root_tag"] == "scenario"
        assert index.info["rules_fingerprint"] == rules.fingerprint()
        assert index.info_json("rules") == rules.to_dict()
        assert index.info["gcamreader_version"] == __version__
        assert index.info_json("extractor_version") == extract.EXTRACTOR_VERSION
        assert index.info["schema_version"] == str(SCHEMA_VERSION)
        assert index.info["complete"] == "1"
        assert index.warnings == []
        assert index.info_json("missing", "dflt") == "dflt"


def test_progress_callback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(extract, "PROGRESS_INTERVAL", 4)
    messages: list[str] = []
    with build(
        SAMPLE,
        tmp_path,
        progress=messages.append,
        expected_elements=SAMPLE_ELEMENTS,
        source="db1",
    ):
        pass
    assert messages == [
        "db1: 4 elements (40%)",
        "db1: 8 elements (80%)",
        "db1: 10 elements (100%), done",
    ]
    messages.clear()
    with build(SAMPLE, tmp_path, progress=messages.append):
        pass
    assert messages[-1] == "test: 10 elements, done"


def test_two_builds_are_identical(tmp_path: Path) -> None:
    with build(SAMPLE, tmp_path) as a, build(SAMPLE, tmp_path) as b:
        assert dump(a) == dump(b)
        rows_a = sorted(
            a.iter_rows(), key=lambda r: (r.parent_hash, r.tag, r.identity, r.metadata)
        )
        rows_b = sorted(
            b.iter_rows(), key=lambda r: (r.parent_hash, r.tag, r.identity, r.metadata)
        )
        assert rows_a == rows_b
        assert all(isinstance(r, NodeRow) for r in rows_a)


def test_root_with_several_document_rows_is_an_error(tmp_path: Path) -> None:
    index = build("<r/>", tmp_path)
    index.close()
    conn = sqlite3.connect(index.path)
    conn.execute("INSERT INTO strings(id, value) VALUES (99, 'other')")
    conn.execute(
        "INSERT INTO node VALUES (?, 99, 2, 2, NULL, 1, 1, 1, 0, 0, 0, 0)",
        (ROOT_PARENT_HASH,),
    )
    conn.commit()
    conn.close()
    with StructureIndex.open(index.path) as reopened:
        with pytest.raises(StructureIndexError, match="2 document elements"):
            reopened.root()


def test_scratch_file_is_removed(tmp_path: Path) -> None:
    index = build(SAMPLE, tmp_path)
    index.close()
    assert Path(index.path).exists()
    assert not Path(index.path + RAW_SUFFIX).exists()
    # Abort removes the scratch file as well.
    writer = IndexWriter(str(tmp_path / "aborted.sqlite"))
    writer.begin()
    assert Path(writer.raw_path).exists()
    writer.abort()
    assert not Path(writer.raw_path).exists()
    assert not Path(writer.path).exists()
