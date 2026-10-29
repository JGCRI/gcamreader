"""Tests for :mod:`gcamreader.structure.api` and the top-level exports.

The Java-dependent steps (scenario listing, extraction, runtime check) are
replaced by fakes that build indexes from fixture XML, so the orchestration is
exercised end to end without BaseX. One test runs the real pipeline on the
bundled sample database when Java is available.

@license BSD 2-Clause
"""

from __future__ import annotations

import io
import json
import os
from pathlib import Path

import pytest

import gcamreader
from gcamreader.structure import api, extract
from gcamreader.structure import java_runtime as jr
from gcamreader.structure.errors import (
    DatabaseAccessError,
    ExtractionError,
    ScenarioSelectionError,
)
from gcamreader.structure.model import ComparisonResult, FindingKind, ScenarioRef
from gcamreader.structure.rules import ComparisonRules
from tests.conftest import DATA_DIR, SAMPLE_DB_NAME, requires_java
from tests.test_structure_compare import BASE

UNIT_CHANGED = BASE.replace('unit="EJ" vintage="2025"', 'unit="PJ" vintage="2025"')
NEW_SECTOR = BASE.replace(
    '<region name="China">',
    '<region name="China"><sector name="new"><subsector name="s1"/></sector>',
)
DATE = "2026-1-6T10:39:43-07:00"


class FakeBackend:
    """Fake scenario listing and extraction driven by XML per database."""

    def __init__(self) -> None:
        self.xml: dict[str, str] = {}
        self.scenarios: dict[str, list[ScenarioRef]] = {}
        self.extractions: list[str] = []
        self.listings: list[str] = []

    def add(self, path: Path, xml: str, *refs: ScenarioRef) -> str:
        path.mkdir(parents=True, exist_ok=True)
        (path / "inf.basex").write_bytes(f"inf-{path.name}".encode())
        (path / "tbl.basex").write_bytes(b"t" * (100 + len(xml)))
        key = os.path.abspath(path)
        self.xml[key] = xml
        self.scenarios[key] = list(refs) or [
            ScenarioRef(path.name, DATE, "ver_9.0", None)
        ]
        return str(path)

    def list_scenarios(self, db_path, *, with_counts=False, **kwargs):
        self.listings.append(db_path)
        return list(self.scenarios[os.path.abspath(db_path)])

    def extract_structure(
        self, db_path, scenario, index_path, rules, *, progress=None, **kwargs
    ):
        self.extractions.append(db_path)
        if progress is not None:
            progress(f"{db_path}: fake extraction")
        return extract.build_index_from_stream(
            io.BytesIO(self.xml[os.path.abspath(db_path)].encode()),
            index_path,
            rules,
            source=db_path,
            info={
                "db_path": os.path.abspath(db_path),
                "db_fingerprint": extract.fingerprint_database(db_path),
                "scenario_name": scenario.name,
                "scenario_date": scenario.date,
                "scenario_version": scenario.version or "",
            },
        )


@pytest.fixture
def backend(monkeypatch: pytest.MonkeyPatch) -> FakeBackend:
    fake = FakeBackend()
    monkeypatch.setattr(api, "list_scenarios", fake.list_scenarios)
    monkeypatch.setattr(api, "extract_structure", fake.extract_structure)
    monkeypatch.setattr(
        api,
        "check_java",
        lambda *a, **k: jr.JavaInfo("java", 17, "17.0.2", "Fake", "PATH"),
    )
    return fake


@pytest.fixture
def dbs(backend: FakeBackend, tmp_path: Path) -> list[str]:
    return [
        backend.add(tmp_path / "dbs" / "ref", BASE),
        backend.add(tmp_path / "dbs" / "units", UNIT_CHANGED),
        backend.add(tmp_path / "dbs" / "sector", NEW_SECTOR),
    ]


# --- orchestration ---------------------------------------------------------------


def test_compare_three_databases(backend: FakeBackend, dbs: list[str]) -> None:
    messages: list[str] = []
    result = api.compare_databases(dbs, progress=messages.append)
    assert isinstance(result, ComparisonResult)
    assert result.package_version == gcamreader.__version__
    assert [i.index for i in result.inputs] == [1, 2, 3]
    assert [i.label for i in result.inputs] == ["ref", "units", "sector"]
    assert [i.path for i in result.inputs] == dbs
    assert result.inputs[0].scenario.label == f"ref@{DATE}"
    assert result.inputs[0].fingerprint == extract.fingerprint_database(dbs[0])
    assert result.baseline == 1
    assert [(c.baseline.index, c.candidate.index) for c in result.comparisons] == [
        (1, 2),
        (1, 3),
    ]
    assert result.has_differences
    assert [f.kind for f in result.comparisons[0].findings] == [
        FindingKind.ATTRIBUTE_CHANGED
    ]
    assert [f.kind for f in result.comparisons[1].findings] == [
        FindingKind.ELEMENT_ADDED
    ]
    assert len(result.matrix) == 2
    assert result.rules == ComparisonRules.default().to_dict()
    assert result.warnings == ()
    assert backend.extractions == dbs
    assert backend.listings == dbs
    assert any("listing scenarios" in m for m in messages)
    assert any("extracting ref@" in m for m in messages)
    assert any("comparing units with baseline ref" in m for m in messages)


def test_baseline_other_than_first(backend: FakeBackend, dbs: list[str]) -> None:
    result = api.compare_databases(dbs, baseline=1)
    assert result.baseline == 2
    assert [(c.baseline.index, c.candidate.index) for c in result.comparisons] == [
        (2, 1),
        (2, 3),
    ]
    assert [i.index for i in result.inputs] == [1, 2, 3]  # order as given
    (unit_row,) = [r for r in result.matrix if r.attribute == "unit"]
    assert unit_row.cells == (
        "EJ",
        "PJ",
        "EJ",
    )  # baseline (input 2) shows PJ in column 2


def test_identical_databases_have_no_differences(
    backend: FakeBackend, tmp_path: Path
) -> None:
    a = backend.add(tmp_path / "a", BASE)
    b = backend.add(tmp_path / "b", BASE)
    result = api.compare_databases([a, b])
    assert not result.has_differences
    assert result.matrix == ()
    assert result.comparisons[0].findings == ()


def test_same_database_twice_extracts_once(
    backend: FakeBackend, tmp_path: Path
) -> None:
    a = backend.add(tmp_path / "a", BASE)
    result = api.compare_databases([a, a], use_cache=False)
    assert not result.has_differences
    assert backend.extractions == [a]


# --- scenario selection ------------------------------------------------------------


def test_scenario_selection(backend: FakeBackend, tmp_path: Path) -> None:
    multi = backend.add(
        tmp_path / "multi",
        BASE,
        ScenarioRef("A", "2026-01-01", "v"),
        ScenarioRef("A", "2026-02-02", "v"),
        ScenarioRef("B", "2026-01-01", "v"),
    )
    single = backend.add(tmp_path / "single", BASE)
    with pytest.raises(ScenarioSelectionError, match="contains 3 scenarios"):
        api.compare_databases([single, multi])
    result = api.compare_databases([single, multi], scenarios={2: "B"})
    assert result.inputs[1].scenario.name == "B"
    result = api.compare_databases([single, multi], scenarios=[None, "A@2026-02-02"])
    assert result.inputs[1].scenario.label == "A@2026-02-02"
    with pytest.raises(ScenarioSelectionError, match="matches 2 runs"):
        api.compare_databases([single, multi], scenarios={2: "A"})
    # A bare selector applies to every database.
    both = backend.add(tmp_path / "both", BASE, ScenarioRef("B", "2026-01-01", "v"))
    result = api.compare_databases([both, multi], scenarios="B")
    assert [i.scenario.name for i in result.inputs] == ["B", "B"]


def test_scenario_selector_validation(backend: FakeBackend, dbs: list[str]) -> None:
    with pytest.raises(ValueError, match="keys must be input positions"):
        api.compare_databases(dbs, scenarios={4: "x"})
    with pytest.raises(ValueError, match="one selector"):
        api.compare_databases(dbs, scenarios=["a", "b"])
    assert backend.extractions == []  # validated before any work


# --- index reuse and work directory ---------------------------------------------------


def test_work_dir_reuse(backend: FakeBackend, dbs: list[str], tmp_path: Path) -> None:
    work = tmp_path / "work"
    first = api.compare_databases(dbs, work_dir=work)
    assert len(backend.extractions) == 3
    files = sorted(p.name for p in work.iterdir())
    assert len(files) == 3 and all(f.endswith(api.INDEX_SUFFIX) for f in files)
    second = api.compare_databases(dbs, work_dir=work)
    assert len(backend.extractions) == 3  # nothing re-extracted
    assert len(second.warnings) == 3
    assert all("reused the structure index" in w for w in second.warnings)
    assert second.to_dict()["comparisons"] == first.to_dict()["comparisons"]
    assert work.exists()


def test_work_dir_reuse_invalidated(
    backend: FakeBackend, dbs: list[str], tmp_path: Path
) -> None:
    work = tmp_path / "work"
    api.compare_databases(dbs, work_dir=work)
    backend.extractions.clear()
    # Changed database content (size) -> new fingerprint -> re-extract that one.
    (Path(dbs[1]) / "tbl.basex").write_bytes(b"changed" * 50)
    api.compare_databases(dbs, work_dir=work)
    assert backend.extractions == [dbs[1]]
    backend.extractions.clear()
    # Different rules -> different index names -> all re-extracted.
    api.compare_databases(
        dbs, work_dir=work, rules=ComparisonRules(ignored_attributes=["type"])
    )
    assert len(backend.extractions) == 3
    backend.extractions.clear()
    # use_cache=False -> always extract.
    api.compare_databases(dbs, work_dir=work, use_cache=False)
    assert len(backend.extractions) == 3


def test_corrupt_index_in_work_dir_is_replaced(
    backend: FakeBackend, dbs: list[str], tmp_path: Path
) -> None:
    work = tmp_path / "work"
    api.compare_databases(dbs, work_dir=work)
    target = next(work.iterdir())
    target.write_bytes(b"garbage, not an index file at all")
    backend.extractions.clear()
    result = api.compare_databases(dbs, work_dir=work)
    assert len(backend.extractions) == 1
    assert len(result.warnings) == 2


def test_temporary_work_dir_is_removed(
    backend: FakeBackend,
    dbs: list[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    created: list[str] = []
    real_mkdtemp = api.tempfile.mkdtemp

    def recording_mkdtemp(**kwargs):
        path = real_mkdtemp(dir=tmp_path, **kwargs)
        created.append(path)
        return path

    monkeypatch.setattr(api.tempfile, "mkdtemp", recording_mkdtemp)
    api.compare_databases(dbs)
    assert len(created) == 1 and not os.path.exists(created[0])

    def failing_extract(*args, **kwargs):
        raise ExtractionError("boom")

    monkeypatch.setattr(api, "extract_structure", failing_extract)
    with pytest.raises(ExtractionError):
        api.compare_databases(dbs)
    assert len(created) == 2 and not os.path.exists(created[1])


def test_index_file_name() -> None:
    rules = ComparisonRules.default()
    scenario = ScenarioRef("GCAM", DATE, "v")
    name = api.index_file_name("database_basexdbGCAM", "ab" * 32, scenario, rules)
    assert name.startswith("database_basexdbGCAM-") and name.endswith(
        ".structure.sqlite"
    )
    assert name == api.index_file_name(
        "database_basexdbGCAM", "ab" * 32, scenario, rules
    )
    assert name != api.index_file_name(
        "database_basexdbGCAM", "cd" * 32, scenario, rules
    )
    assert name != api.index_file_name(
        "database_basexdbGCAM", "ab" * 32, ScenarioRef("GCAM", "other", "v"), rules
    )
    assert name != api.index_file_name(
        "database_basexdbGCAM",
        "ab" * 32,
        scenario,
        ComparisonRules(ignored_attributes=["x"]),
    )
    assert api.index_file_name(
        "weird name/with:chars", "00", scenario, rules
    ).startswith("weird_name_with_chars-")


# --- warnings and errors ------------------------------------------------------------------


def test_java_warning_propagates(
    backend: FakeBackend, dbs: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        api,
        "check_java",
        lambda *a, **k: jr.JavaInfo("/j", 25, "25.0.1", "Fake", "PATH"),
    )
    result = api.compare_databases(dbs)
    assert len(result.warnings) == 1
    assert "Java 25.0.1" in result.warnings[0] and "newer" in result.warnings[0]


def test_argument_errors(backend: FakeBackend, dbs: list[str], tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="at least two"):
        api.compare_databases(dbs[:1])
    with pytest.raises(ValueError, match="baseline must be between 0 and 2"):
        api.compare_databases(dbs, baseline=3)
    with pytest.raises(DatabaseAccessError):
        api.compare_databases([dbs[0], str(tmp_path / "missing")])
    assert backend.extractions == []


# --- JSON ----------------------------------------------------------------------------------


def test_iter_json_matches_to_dict(
    backend: FakeBackend, dbs: list[str], tmp_path: Path
) -> None:
    result = api.compare_databases(dbs, scenarios=None)
    chunks = list(api.iter_json(result))
    assert len(chunks) > 10
    text = "".join(chunks)
    assert json.loads(text) == result.to_dict()
    assert text == json.dumps(result.to_dict(), ensure_ascii=False) + "\n"
    written = api.write_json(result, tmp_path / "result.json")
    assert json.loads(written.read_text(encoding="utf-8")) == result.to_dict()


def test_iter_json_empty_result(backend: FakeBackend, tmp_path: Path) -> None:
    a = backend.add(tmp_path / "a", BASE)
    b = backend.add(tmp_path / "b", BASE)
    result = api.compare_databases([a, b])
    assert json.loads("".join(api.iter_json(result))) == result.to_dict()


# --- exports ----------------------------------------------------------------------------------


def test_top_level_exports() -> None:
    for name in (
        "compare_databases",
        "list_scenarios",
        "render_markdown",
        "ComparisonRules",
        "ComparisonResult",
        "Finding",
        "ScenarioRef",
        "StructureError",
    ):
        assert name in gcamreader.__all__
        assert getattr(gcamreader, name) is not None
    assert gcamreader.compare_databases is api.compare_databases
    assert gcamreader.list_scenarios is extract.list_scenarios
    # Existing exports are untouched.
    for name in (
        "Query",
        "parse_batch_query",
        "LocalDBConn",
        "RemoteDBConn",
        "importdata",
        "sample_data_dir",
    ):
        assert name in gcamreader.__all__


# --- real Java --------------------------------------------------------------------------------


@requires_java
def test_sample_database_against_itself(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []
    real = api.extract_structure

    def counting(db_path, *args, **kwargs):
        calls.append(db_path)
        return real(db_path, *args, **kwargs)

    monkeypatch.setattr(api, "extract_structure", counting)
    sample = str(DATA_DIR / SAMPLE_DB_NAME)
    result = gcamreader.compare_databases([sample, sample], work_dir=tmp_path / "work")
    assert not result.has_differences
    assert calls == [sample]  # second input reused the first index within the run
    assert result.inputs[0].scenario.name == "Reference-filtered"
    assert (
        result.inputs[0].scenario.element_count
        and result.inputs[0].scenario.element_count > 100_000
    )
    text = gcamreader.render_markdown(result)
    assert "no structural differences" in text
