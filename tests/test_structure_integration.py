"""End-to-end structural comparison with a real Java runtime.

The bundled sample database is exported to XML with BaseX, mutated with lxml
(an added sector, a removed element, a changed ``unit``, a removed year leaf;
and separately a reshuffled copy with changed values), loaded into new BaseX
databases, and compared through the API and the command line. Mutation
targets are derived from the real tree, so nothing here depends on the
sample's particular content beyond having regions and leaves with ``unit``
and ``year`` attributes.

@license BSD 2-Clause
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import subprocess
from pathlib import Path

import pytest
from lxml import etree
from typer.testing import CliRunner

import gcamreader
from gcamreader import cli
from gcamreader.querymi import _default_miclasspath
from gcamreader.structure import api, markdown
from gcamreader.structure.extract import _basex_command, validate_database_dir
from gcamreader.structure.java_runtime import check_java
from gcamreader.structure.model import FindingKind, PathSegment, format_path
from gcamreader.structure.rules import DEFAULT_IDENTITY_ATTRIBUTES
from tests.conftest import DATA_DIR, SAMPLE_DB_NAME, requires_java

pytestmark = requires_java

SAMPLE_DB = str(DATA_DIR / SAMPLE_DB_NAME)
runner = CliRunner()


# --- helpers -------------------------------------------------------------------


def _identity(element: etree._Element) -> tuple[tuple[str, str], ...]:
    return tuple(
        (name, element.get(name))
        for name in DEFAULT_IDENTITY_ATTRIBUTES
        if element.get(name) is not None
    )


def _key(element: etree._Element) -> tuple[str, tuple[tuple[str, str], ...]]:
    return element.tag, _identity(element)


_SIBLING_KEYS: dict[int, dict[tuple, int]] = {}


def _unique_among_siblings(element: etree._Element) -> bool:
    """Whether no sibling shares the element's (tag, identity).

    Sibling key counts are cached per parent: the sample's Marketplace has
    tens of thousands of children, so a per-element scan would be quadratic.
    """
    parent = element.getparent()
    if parent is None:
        return True
    # lxml hands out a fresh proxy object per access, so id(parent) is not a
    # stable key; the XPath of the parent is.
    cache_key = parent.getroottree().getpath(parent)
    counts = _SIBLING_KEYS.get(cache_key)
    if counts is None:
        counts = {}
        for sibling in parent:
            key = _key(sibling)
            counts[key] = counts.get(key, 0) + 1
        _SIBLING_KEYS[cache_key] = counts
    return counts[_key(element)] == 1


def _path(element: etree._Element) -> str:
    """Path as the comparison formats it with default rules.

    The document element's ``name`` is excluded by the default rules, so the
    root segment is bare ``scenario``.
    """
    segments = []
    node: etree._Element | None = element
    while node is not None:
        identity = _identity(node) if node.getparent() is not None else ()
        segments.append(PathSegment(node.tag, identity))
        node = node.getparent()
    return format_path(reversed(segments))


def _depth(element: etree._Element) -> int:
    depth = 0
    node: etree._Element | None = element
    while node is not None:
        depth += 1
        node = node.getparent()
    return depth


def _db_hashes(path: str) -> dict[str, str]:
    return {
        name: hashlib.sha256((Path(path) / name).read_bytes()).hexdigest()
        for name in sorted(os.listdir(path))
        if name.endswith(".basex")
    }


def _create_db(db_dir: Path, name: str, xml_file: Path) -> str:
    """Load ``xml_file`` into a new BaseX database ``name`` under ``db_dir``."""
    java = check_java().path
    command = f'CREATE DB {name} "{xml_file.as_posix()}"'
    cmd = [
        java,
        "-cp",
        _default_miclasspath,
        "-Xmx2g",
        "-Dfile.encoding=UTF-8",
        "-Djava.awt.headless=true",
        f"-Dorg.basex.DBPATH={db_dir}",
        "org.basex.BaseX",
        "-c",
        command,
    ]
    completed = subprocess.run(cmd, capture_output=True, encoding="utf-8", check=False)
    assert completed.returncode == 0, completed.stderr
    created = str(db_dir / name)
    validate_database_dir(created)
    return created


# --- module fixtures -------------------------------------------------------------


@pytest.fixture(scope="module")
def work(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("integration")


@pytest.fixture(scope="module")
def export(work: Path) -> Path:
    """The sample scenario serialized by BaseX, as the extractor sees it."""
    parent, name = validate_database_dir(SAMPLE_DB)
    target = work / "sample.xml"
    cmd = _basex_command(
        check_java().path,
        None,
        "2g",
        parent,
        name,
        "-smethod=xml",
        "-sindent=no",
        "-somit-xml-declaration=yes",
        "-o",
        str(target),
        query="collection()/scenario",
    )
    completed = subprocess.run(cmd, capture_output=True, encoding="utf-8", check=False)
    assert completed.returncode == 0, completed.stderr
    assert target.stat().st_size > 1_000_000
    return target


class Mutation:
    """What was changed in the modified copy and what the comparison must say."""

    def __init__(self) -> None:
        self.added_path = ""
        self.removed_path = ""
        self.removed_descendants = 0
        self.unit_path = ""
        self.unit_before = ""
        self.unit_after = ""
        self.year_leaf_path = ""
        self.scenario_name = ""
        self.scenario_date = ""


@pytest.fixture(scope="module")
def modified(work: Path, export: Path) -> tuple[str, Mutation]:
    tree = etree.parse(str(export))
    root = tree.getroot()
    mutation = Mutation()
    mutation.scenario_name = root.get("name")
    mutation.scenario_date = root.get("date")

    region = root.find("world/region")
    assert region is not None

    # 1. Add a sector with a small subtree (2 descendants).
    sector = etree.SubElement(
        region, "sector", name="integration-test-sector", type="test"
    )
    subsector = etree.SubElement(sector, "subsector", name="s1")
    etree.SubElement(subsector, "technology", name="t1", year="2020")
    mutation.added_path = _path(sector)

    # 2. Remove a non-leaf element that is unique among its siblings.
    candidates = (
        e
        for e in root.iter()
        if len(e) > 0
        and _identity(e)
        and _depth(e) >= 4
        and _unique_among_siblings(e)
        and e is not sector
        and sector not in e.iterancestors()
    )
    removed = next(candidates)
    mutation.removed_path = _path(removed)
    mutation.removed_descendants = sum(1 for _ in removed.iter()) - 1
    removed.getparent().remove(removed)

    # 3. Change a unit on a unique leaf outside the removed subtree.
    unit_leaf = next(
        e
        for e in root.iter()
        if e.get("unit") and len(e) == 0 and _unique_among_siblings(e)
    )
    mutation.unit_path = _path(unit_leaf)
    mutation.unit_before = unit_leaf.get("unit")
    mutation.unit_after = mutation.unit_before + "-test"
    unit_leaf.set("unit", mutation.unit_after)

    # 4. Remove a year-identified leaf (different tag than the removed element).
    year_leaf = next(
        e
        for e in root.iter()
        if e.get("year")
        and len(e) == 0
        and _unique_among_siblings(e)
        and e.tag != removed.tag
        and e is not unit_leaf
    )
    mutation.year_leaf_path = _path(year_leaf)
    year_leaf.getparent().remove(year_leaf)

    xml_file = work / "modified.xml"
    tree.write(str(xml_file), encoding="utf-8")
    return _create_db(work / "dbs", "modified_db", xml_file), mutation


@pytest.fixture(scope="module")
def reshuffled(work: Path, export: Path) -> str:
    tree = etree.parse(str(export))
    root = tree.getroot()
    rng = random.Random(7)
    region = root.find("world/region")
    children = list(region)
    rng.shuffle(children)
    for child in children:
        region.remove(child)
    for child in children:
        region.append(child)
    leaves = [e for e in root.iter() if len(e) == 0 and e.text and e.text.strip()]
    for leaf in rng.sample(leaves, min(5000, len(leaves))):
        leaf.text = "123.456"
    root.insert(0, etree.Comment("reshuffled copy for the integration test"))
    xml_file = work / "reshuffled.xml"
    tree.write(str(xml_file), encoding="utf-8")
    return _create_db(work / "dbs", "reshuffled_db", xml_file)


@pytest.fixture(scope="module")
def hashes_before(modified, reshuffled) -> dict[str, dict[str, str]]:
    return {path: _db_hashes(path) for path in (SAMPLE_DB, modified[0], reshuffled)}


# --- tests ---------------------------------------------------------------------------


def test_sample_against_itself_cli(work: Path) -> None:
    result = runner.invoke(
        cli.app,
        ["diff", SAMPLE_DB, SAMPLE_DB, "--work-dir", str(work / "idx"), "--quiet"],
    )
    assert result.exit_code == 0, result.output
    assert "no structural differences" in result.stdout


def test_modified_database_findings(work: Path, modified: tuple[str, Mutation]) -> None:
    db, mutation = modified
    (scenario,) = api.list_scenarios(db)
    assert (scenario.name, scenario.date) == (
        mutation.scenario_name,
        mutation.scenario_date,
    )

    result = gcamreader.compare_databases([SAMPLE_DB, db], work_dir=work / "idx")
    assert result.has_differences
    (comparison,) = result.comparisons
    by_path = {f.path: f for f in comparison.findings}
    assert set(by_path) == {
        mutation.added_path,
        mutation.removed_path,
        mutation.unit_path,
        mutation.year_leaf_path,
    }, [f.path for f in comparison.findings]

    added = by_path[mutation.added_path]
    assert added.kind is FindingKind.ELEMENT_ADDED
    assert added.descendant_count == 2
    assert added.candidate is not None and added.candidate.metadata == (
        ("type", "test"),
    )
    assert added.segments[-2].tag == "region"

    removed = by_path[mutation.removed_path]
    assert removed.kind is FindingKind.ELEMENT_REMOVED
    assert removed.descendant_count == mutation.removed_descendants
    assert removed.members == 1

    unit = by_path[mutation.unit_path]
    assert unit.kind is FindingKind.ATTRIBUTE_CHANGED
    (change,) = unit.attribute_changes
    assert (change.attribute, change.baseline, change.candidate) == (
        "unit",
        mutation.unit_before,
        mutation.unit_after,
    )

    year_leaf = by_path[mutation.year_leaf_path]
    assert year_leaf.kind is FindingKind.ELEMENT_REMOVED
    assert year_leaf.descendant_count == 0

    assert comparison.summary.total == 4
    assert result.warnings == () or all("reused" in w for w in result.warnings)


def test_cli_matches_api(work: Path, modified: tuple[str, Mutation]) -> None:
    db, _ = modified
    report = work / "report.md"
    json_file = work / "result.json"
    cli_result = runner.invoke(
        cli.app,
        [
            "diff",
            SAMPLE_DB,
            db,
            "-o",
            str(report),
            "--json",
            str(json_file),
            "--work-dir",
            str(work / "idx"),
            "--quiet",
        ],
    )
    assert cli_result.exit_code == 1, cli_result.output
    api_result = gcamreader.compare_databases([SAMPLE_DB, db], work_dir=work / "idx")
    assert json.loads(json_file.read_text(encoding="utf-8")) == api_result.to_dict()
    assert report.read_text(encoding="utf-8") == markdown.render_markdown(api_result)
    assert not (work / "report.appendix.md").exists()


def test_reshuffled_database_has_no_differences(work: Path, reshuffled: str) -> None:
    result = gcamreader.compare_databases(
        [SAMPLE_DB, reshuffled], work_dir=work / "idx"
    )
    assert not result.has_differences, [f.path for f in result.comparisons[0].findings]


def test_repeated_runs_are_identical(
    work: Path, modified: tuple[str, Mutation]
) -> None:
    db, _ = modified
    first = gcamreader.compare_databases([SAMPLE_DB, db], work_dir=work / "idx")
    second = gcamreader.compare_databases([SAMPLE_DB, db], work_dir=work / "idx")
    assert first.to_dict() == second.to_dict()
    assert markdown.render_markdown(first) == markdown.render_markdown(second)
    # Without a work directory everything is extracted afresh; the body is the same.
    fresh = gcamreader.compare_databases([SAMPLE_DB, db])
    assert [f.to_dict() for f in fresh.comparisons[0].findings] == [
        f.to_dict() for f in first.comparisons[0].findings
    ]


def test_databases_unchanged(hashes_before: dict[str, dict[str, str]]) -> None:
    for path, before in hashes_before.items():
        assert _db_hashes(path) == before, path
