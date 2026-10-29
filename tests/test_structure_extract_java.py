"""Tests for the Java/BaseX side of :mod:`gcamreader.structure.extract`.

``subprocess.run`` and ``subprocess.Popen`` are replaced with fakes so the
command lines and error handling can be checked without Java; a few tests
marked ``requires_java`` run against the bundled sample database.

@license BSD 2-Clause
"""

from __future__ import annotations

import hashlib
import io
import os
from collections import namedtuple
from pathlib import Path

import pytest

from gcamreader.querymi import _default_miclasspath
from gcamreader.structure import extract
from gcamreader.structure import java_runtime as jr
from gcamreader.structure.errors import (
    DatabaseAccessError,
    ExtractionError,
    MissingDependencyError,
    ScenarioSelectionError,
    StructureError,
)
from gcamreader.structure.index import StructureIndex
from gcamreader.structure.model import ScenarioRef
from gcamreader.structure.rules import ComparisonRules
from tests.conftest import DATA_DIR, SAMPLE_DB_NAME, requires_java
from tests.test_structure_extract import SAMPLE, SAMPLE_ELEMENTS

FAKE_JAVA = "fake-java"
RS, FS = "\n", "\t"


@pytest.fixture
def fake_java() -> str:
    """Register a fake, pre-checked Java runtime so check_java() never runs it."""
    jr.clear_java_cache()
    jr._cache[FAKE_JAVA] = jr.JavaInfo(FAKE_JAVA, 17, "17.0.2", "Fake", "explicit")
    yield FAKE_JAVA
    jr.clear_java_cache()


@pytest.fixture
def db_dir(tmp_path: Path) -> Path:
    """A directory that passes validate_database_dir."""
    db = tmp_path / "parent" / "mydb"
    db.mkdir(parents=True)
    (db / "inf.basex").write_bytes(b"inf-content")
    (db / "tbl.basex").write_bytes(b"x" * 100)
    (db / "txt.basex").write_bytes(b"y" * 10)
    return db


class FakeRun:
    def __init__(self, stdout: str = "", returncode: int = 0, stderr: str = ""):
        self.stdout, self.returncode, self.stderr = stdout, returncode, stderr
        self.calls: list[list[str]] = []

    def __call__(self, cmd, **kwargs):
        self.calls.append(list(cmd))
        Completed = namedtuple("Completed", "returncode stdout stderr")
        return Completed(self.returncode, self.stdout, self.stderr)


class FakePopen:
    """Stand-in for subprocess.Popen streaming canned bytes on stdout."""

    instances: list[FakePopen] = []

    def __init__(self, cmd, *, stdout=None, stderr=None, bufsize=0):
        self.cmd = list(cmd)
        self.stdout = io.BytesIO(type(self).payload)
        if type(self).stderr_text:
            stderr.write(type(self).stderr_text.encode())
        self.returncode = None
        self.killed = False
        type(self).instances.append(self)

    payload = b""
    stderr_text = ""
    exit_code = 0

    def poll(self):
        if self.stdout.tell() >= len(type(self).payload) and self.returncode is None:
            self.returncode = type(self).exit_code
        return self.returncode

    def wait(self):
        if self.returncode is None:
            self.returncode = type(self).exit_code
        return self.returncode

    def kill(self):
        self.killed = True
        self.returncode = -9


@pytest.fixture
def fake_popen(monkeypatch: pytest.MonkeyPatch):
    FakePopen.instances = []
    FakePopen.payload = SAMPLE.encode()
    FakePopen.stderr_text = ""
    FakePopen.exit_code = 0
    monkeypatch.setattr(extract.subprocess, "Popen", FakePopen)
    return FakePopen


SCENARIO = ScenarioRef("GCAM", "2026-1-6T10:39:43-07:00", "ver_9.0", SAMPLE_ELEMENTS)


# --- validate_database_dir ------------------------------------------------------


def test_validate_database_dir(db_dir: Path) -> None:
    parent, name = extract.validate_database_dir(str(db_dir))
    assert Path(parent) == db_dir.parent
    assert name == "mydb"
    # Trailing separator is tolerated.
    assert extract.validate_database_dir(str(db_dir) + os.sep)[1] == "mydb"


def test_validate_database_dir_not_a_directory(tmp_path: Path) -> None:
    with pytest.raises(DatabaseAccessError, match="remote databases are not supported"):
        extract.validate_database_dir(str(tmp_path / "missing"))
    with pytest.raises(DatabaseAccessError, match="not a directory"):
        extract.validate_database_dir("http://server:8984/rest/db")


def test_validate_database_dir_missing_files(tmp_path: Path) -> None:
    (tmp_path / "inf.basex").write_bytes(b"")
    with pytest.raises(DatabaseAccessError, match="missing tbl.basex"):
        extract.validate_database_dir(str(tmp_path))


# --- fingerprint_database ----------------------------------------------------------


def test_fingerprint_database(db_dir: Path) -> None:
    first = extract.fingerprint_database(str(db_dir))
    assert len(first) == 64
    assert extract.fingerprint_database(str(db_dir)) == first
    (db_dir / "notes.txt").write_text("ignored")
    assert extract.fingerprint_database(str(db_dir)) == first
    (db_dir / "tbl.basex").write_bytes(b"x" * 101)  # size change
    second = extract.fingerprint_database(str(db_dir))
    assert second != first
    (db_dir / "tbl.basex").write_bytes(b"z" * 101)  # same size: undetected by design
    assert extract.fingerprint_database(str(db_dir)) == second
    (db_dir / "inf.basex").write_bytes(b"inf-changed")  # content change detected
    assert extract.fingerprint_database(str(db_dir)) != second


# --- list_scenarios ---------------------------------------------------------------


def test_list_scenarios_command_and_parsing(
    db_dir: Path, fake_java: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = (
        RS.join(
            [
                FS.join(["GCAM", "2026-1-6T10:39:43-07:00", "ver_9.0", "48527349"]),
                FS.join(["Ref, with comma", "2026-1-7T00:00:00-07:00", "", ""]),
            ]
        )
        + "\n"
    )
    fake = FakeRun(output)
    monkeypatch.setattr(extract.subprocess, "run", fake)
    scenarios = extract.list_scenarios(str(db_dir), with_counts=True, java=fake_java)
    assert scenarios == [
        ScenarioRef("GCAM", "2026-1-6T10:39:43-07:00", "ver_9.0", 48527349),
        ScenarioRef("Ref, with comma", "2026-1-7T00:00:00-07:00", None, None),
    ]
    (cmd,) = fake.calls
    assert cmd[:8] == [
        FAKE_JAVA,
        "-cp",
        _default_miclasspath,
        "-Xmx4g",
        "-Dfile.encoding=UTF-8",
        "-Djava.awt.headless=true",
        f"-Dorg.basex.DBPATH={db_dir.parent}",
        "org.basex.BaseX",
    ]
    assert cmd[8:11] == ["-smethod=text", "-i", "mydb"]
    assert "count($s/descendant-or-self::*)" in cmd[11]
    assert "collection()/scenario" in cmd[11]


def test_list_scenarios_without_counts(
    db_dir: Path, fake_java: str, monkeypatch
) -> None:
    fake = FakeRun(FS.join(["GCAM", "d", "v", ""]))
    monkeypatch.setattr(extract.subprocess, "run", fake)
    scenarios = extract.list_scenarios(
        str(db_dir), java=fake_java, java_max_memory="1g", miclasspath="cp.jar"
    )
    assert scenarios == [ScenarioRef("GCAM", "d", "v", None)]
    (cmd,) = fake.calls
    assert "count(" not in cmd[-1]
    assert cmd[2] == "cp.jar" and cmd[3] == "-Xmx1g"


def test_list_scenarios_empty_database(
    db_dir: Path, fake_java: str, monkeypatch
) -> None:
    monkeypatch.setattr(extract.subprocess, "run", FakeRun("\n"))
    assert extract.list_scenarios(str(db_dir), java=fake_java) == []


def test_list_scenarios_java_failure(db_dir: Path, fake_java: str, monkeypatch) -> None:
    monkeypatch.setattr(
        extract.subprocess,
        "run",
        FakeRun(
            "",
            1,
            "Stopped at ., 1/1:\n[BXDB0001] Database 'mydb' is incompatible with BaseX 9.5.",
        ),
    )
    with pytest.raises(DatabaseAccessError) as excinfo:
        extract.list_scenarios(str(db_dir), java=fake_java)
    message = str(excinfo.value)
    assert message.startswith(
        f"Listing scenarios in {db_dir} failed (exit code 1). The database storage format"
    )
    assert "BXDB0001" in message


def test_list_scenarios_unexpected_output(
    db_dir: Path, fake_java: str, monkeypatch
) -> None:
    monkeypatch.setattr(extract.subprocess, "run", FakeRun("garbage"))
    with pytest.raises(DatabaseAccessError, match="Unexpected scenario listing"):
        extract.list_scenarios(str(db_dir), java=fake_java)


def test_list_scenarios_java_not_startable(
    db_dir: Path, fake_java: str, monkeypatch
) -> None:
    def boom(cmd, **kwargs):
        raise FileNotFoundError(cmd[0])

    monkeypatch.setattr(extract.subprocess, "run", boom)
    with pytest.raises(MissingDependencyError, match="cannot start Java"):
        extract.list_scenarios(str(db_dir), java=fake_java)


def test_list_scenarios_rejects_bad_directory(tmp_path: Path, fake_java: str) -> None:
    with pytest.raises(DatabaseAccessError):
        extract.list_scenarios(str(tmp_path), java=fake_java)


# --- selection ----------------------------------------------------------------------


def test_parse_selector() -> None:
    assert extract.parse_selector("GCAM") == ("GCAM", None)
    assert extract.parse_selector(" GCAM ") == ("GCAM", None)
    assert extract.parse_selector("GCAM@2026-1-6T10:39:43-07:00") == (
        "GCAM",
        "2026-1-6T10:39:43-07:00",
    )
    assert extract.parse_selector("a@b@2026") == ("a@b", "2026")
    for bad in ("", "@date", "name@", "@"):
        with pytest.raises(ScenarioSelectionError):
            extract.parse_selector(bad)


A1 = ScenarioRef("A", "2026-01-01", "v")
A2 = ScenarioRef("A", "2026-02-02", "v")
B1 = ScenarioRef("B", "2026-01-01", "v")


def test_select_scenario_single_without_selector() -> None:
    assert extract.select_scenario([A1]) is A1


def test_select_scenario_ambiguous_without_selector() -> None:
    with pytest.raises(ScenarioSelectionError) as excinfo:
        extract.select_scenario([A1, B1], source="db1")
    message = str(excinfo.value)
    assert "db1 contains 2 scenarios" in message
    assert "A@2026-01-01" in message and "B@2026-01-01" in message


def test_select_scenario_empty() -> None:
    with pytest.raises(ScenarioSelectionError, match="contains no scenarios"):
        extract.select_scenario([], "A")


def test_select_scenario_by_name() -> None:
    assert extract.select_scenario([A1, B1], "B") is B1
    with pytest.raises(ScenarioSelectionError, match="matches 2 runs") as excinfo:
        extract.select_scenario([A1, A2, B1], "A")
    assert "A@2026-01-01, A@2026-02-02" in str(excinfo.value)


def test_select_scenario_by_name_and_date() -> None:
    assert extract.select_scenario([A1, A2, B1], "A@2026-02-02") is A2
    with pytest.raises(ScenarioSelectionError, match="No scenario 'A@2027'") as excinfo:
        extract.select_scenario([A1, A2], "A@2027")
    assert "available: A@2026-01-01, A@2026-02-02" in str(excinfo.value)
    with pytest.raises(ScenarioSelectionError, match="No scenario 'C'"):
        extract.select_scenario([A1], "C")


def test_select_scenario_identical_runs_cannot_be_told_apart() -> None:
    with pytest.raises(ScenarioSelectionError, match="share the name and date"):
        extract.select_scenario(
            [A1, ScenarioRef("A", "2026-01-01", "w")], "A@2026-01-01"
        )


def test_select_scenario_never_picks_latest() -> None:
    with pytest.raises(ScenarioSelectionError):
        extract.select_scenario([A1, A2])


# --- extract_structure ---------------------------------------------------------------


def test_extract_structure_success(
    db_dir: Path, fake_java: str, fake_popen, tmp_path: Path
) -> None:
    index_path = tmp_path / "idx.sqlite"
    messages: list[str] = []
    rules = ComparisonRules.default()
    with extract.extract_structure(
        str(db_dir),
        SCENARIO,
        str(index_path),
        rules,
        java=fake_java,
        progress=messages.append,
    ) as index:
        assert index.element_count == SAMPLE_ELEMENTS
        assert index.root().tag == "scenario"
        assert index.info["db_path"] == str(db_dir.resolve())
        assert index.info["db_fingerprint"] == extract.fingerprint_database(str(db_dir))
        assert index.info["scenario_name"] == "GCAM"
        assert index.info["scenario_date"] == SCENARIO.date
        assert index.info["scenario_version"] == "ver_9.0"
        assert index.info["java_version"] == "17.0.2"
        assert index.info["source"] == str(db_dir)
        assert index.info["rules_fingerprint"] == rules.fingerprint()
    assert messages[-1].endswith("(100%), done")

    (proc,) = fake_popen.instances
    assert proc.cmd[:8] == [
        FAKE_JAVA,
        "-cp",
        _default_miclasspath,
        "-Xmx4g",
        "-Dfile.encoding=UTF-8",
        "-Djava.awt.headless=true",
        f"-Dorg.basex.DBPATH={db_dir.parent}",
        "org.basex.BaseX",
    ]
    assert proc.cmd[8:] == [
        "-smethod=xml",
        "-sindent=no",
        "-somit-xml-declaration=yes",
        "-bn=GCAM",
        f"-bd={SCENARIO.date}",
        "-i",
        "mydb",
        extract.SCENARIO_QUERY,
    ]
    assert not proc.killed
    assert proc.stdout.closed


def test_extract_structure_nonzero_exit_removes_index(
    db_dir: Path, fake_java: str, fake_popen, tmp_path: Path
) -> None:
    fake_popen.exit_code = 1
    fake_popen.stderr_text = (
        'Exception in thread "main" java.lang.OutOfMemoryError: Java heap space'
    )
    index_path = tmp_path / "idx.sqlite"
    with pytest.raises(MissingDependencyError) as excinfo:
        extract.extract_structure(
            str(db_dir),
            SCENARIO,
            str(index_path),
            ComparisonRules.default(),
            java=fake_java,
        )
    message = str(excinfo.value)
    assert "BaseX failed while exporting GCAM@" in message
    assert "exit code 1" in message
    assert "--java-max-memory" in message
    assert "OutOfMemoryError" in message
    assert not index_path.exists()


def test_extract_structure_truncated_stream_includes_java_output(
    db_dir: Path, fake_java: str, fake_popen, tmp_path: Path
) -> None:
    fake_popen.payload = SAMPLE.encode()[: len(SAMPLE) // 2]
    fake_popen.exit_code = 1
    fake_popen.stderr_text = "Database 'mydb' was not found."
    index_path = tmp_path / "idx.sqlite"
    with pytest.raises(StructureError) as excinfo:
        extract.extract_structure(
            str(db_dir),
            SCENARIO,
            str(index_path),
            ComparisonRules.default(),
            java=fake_java,
        )
    message = str(excinfo.value)
    assert "not well-formed or was truncated" in message
    assert "did not find the database" in message
    assert "was not found" in message
    assert not index_path.exists()
    assert not (tmp_path / "idx.sqlite.raw").exists()


def test_extract_structure_truncated_stream_without_java_output(
    db_dir: Path, fake_java: str, fake_popen, tmp_path: Path
) -> None:
    fake_popen.payload = SAMPLE.encode()[: len(SAMPLE) // 2]
    with pytest.raises(ExtractionError, match="not well-formed or was truncated"):
        extract.extract_structure(
            str(db_dir),
            SCENARIO,
            str(tmp_path / "i.sqlite"),
            ComparisonRules.default(),
            java=fake_java,
        )


def test_extract_structure_count_mismatch(
    db_dir: Path, fake_java: str, fake_popen, tmp_path: Path
) -> None:
    wrong = ScenarioRef("GCAM", SCENARIO.date, "v", SAMPLE_ELEMENTS + 5)
    with pytest.raises(ExtractionError, match="extraction is incomplete"):
        extract.extract_structure(
            str(db_dir),
            wrong,
            str(tmp_path / "i.sqlite"),
            ComparisonRules.default(),
            java=fake_java,
        )
    assert not (tmp_path / "i.sqlite").exists()


def test_extract_structure_without_count_skips_completeness_check(
    db_dir: Path, fake_java: str, fake_popen, tmp_path: Path
) -> None:
    unknown = ScenarioRef("GCAM", SCENARIO.date, "v", None)
    with extract.extract_structure(
        str(db_dir),
        unknown,
        str(tmp_path / "i.sqlite"),
        ComparisonRules.default(),
        java=fake_java,
    ) as index:
        assert index.element_count == SAMPLE_ELEMENTS


def test_extract_structure_disk_space_check(
    db_dir: Path, fake_java: str, fake_popen, tmp_path: Path, monkeypatch
) -> None:
    Usage = namedtuple("Usage", "total used free")
    monkeypatch.setattr(extract.shutil, "disk_usage", lambda path: Usage(100, 90, 10))
    big = ScenarioRef("GCAM", SCENARIO.date, "v", 1_000_000)
    with pytest.raises(ExtractionError, match="Not enough free disk space") as excinfo:
        extract.extract_structure(
            str(db_dir),
            big,
            str(tmp_path / "i.sqlite"),
            ComparisonRules.default(),
            java=fake_java,
        )
    assert "--work-dir" in str(excinfo.value)
    assert fake_popen.instances == []  # Java never started
    # Unknown element count: no check possible, extraction proceeds.
    unknown = ScenarioRef("GCAM", SCENARIO.date, "v", None)
    extract.extract_structure(
        str(db_dir),
        unknown,
        str(tmp_path / "i.sqlite"),
        ComparisonRules.default(),
        java=fake_java,
    ).close()


def test_extract_structure_java_not_startable(
    db_dir: Path, fake_java: str, tmp_path: Path, monkeypatch
) -> None:
    def boom(*args, **kwargs):
        raise FileNotFoundError(FAKE_JAVA)

    monkeypatch.setattr(extract.subprocess, "Popen", boom)
    with pytest.raises(MissingDependencyError, match="Cannot start Java"):
        extract.extract_structure(
            str(db_dir),
            SCENARIO,
            str(tmp_path / "i.sqlite"),
            ComparisonRules.default(),
            java=fake_java,
        )
    assert not (tmp_path / "i.sqlite").exists()


def test_extract_structure_rejects_bad_directory(
    tmp_path: Path, fake_java: str
) -> None:
    with pytest.raises(DatabaseAccessError):
        extract.extract_structure(
            str(tmp_path),
            SCENARIO,
            str(tmp_path / "i.sqlite"),
            ComparisonRules.default(),
            java=fake_java,
        )


# --- real Java on the bundled sample database -----------------------------------------

SAMPLE_DB = DATA_DIR / SAMPLE_DB_NAME


def _db_hashes(path: Path) -> dict[str, str]:
    return {
        f.name: hashlib.sha256(f.read_bytes()).hexdigest()
        for f in sorted(path.iterdir())
        if f.suffix == ".basex"
    }


@requires_java
def test_list_scenarios_sample_db() -> None:
    plain = extract.list_scenarios(str(SAMPLE_DB))
    assert len(plain) == 1
    assert plain[0].name == "Reference-filtered"
    assert plain[0].version == "ver_4.3_rgcam-v4.3"
    assert plain[0].element_count is None
    counted = extract.list_scenarios(str(SAMPLE_DB), with_counts=True)
    assert counted[0].label == plain[0].label
    assert counted[0].element_count and counted[0].element_count > 100_000


@requires_java
def test_extract_structure_sample_db(tmp_path: Path) -> None:
    before = _db_hashes(SAMPLE_DB)
    (scenario,) = extract.list_scenarios(str(SAMPLE_DB), with_counts=True)
    index_path = tmp_path / "sample.sqlite"
    with extract.extract_structure(
        str(SAMPLE_DB), scenario, str(index_path), ComparisonRules.default()
    ) as index:
        assert index.element_count == scenario.element_count
        root = index.root()
        assert root.tag == "scenario"
        assert root.subtree_size == scenario.element_count
        assert root.identity == () and root.metadata == ()
        regions = [
            row
            for row in index.children(index.children(root.path_hash)[-1].path_hash)
            if row.tag == "region"
        ]
        assert regions, "expected region elements under world"
        assert index.info["scenario_name"] == "Reference-filtered"
        assert index.info["db_fingerprint"] == extract.fingerprint_database(
            str(SAMPLE_DB)
        )
    assert _db_hashes(SAMPLE_DB) == before
    assert not (tmp_path / "sample.sqlite.raw").exists()
    assert StructureIndex.open(str(index_path)).element_count == scenario.element_count
