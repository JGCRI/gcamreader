"""Unit tests for :mod:`gcamreader.structure.java_runtime`.

Runtime discovery and version checks are exercised with fake executables and
monkeypatched ``subprocess.run`` so no Java is needed; one test runs the real
``java`` when it is available.

@license BSD 2-Clause
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from gcamreader.structure import java_runtime as jr
from gcamreader.structure.errors import DatabaseAccessError, MissingDependencyError
from tests.conftest import requires_java

TEMURIN_17 = (
    'openjdk version "17.0.20.1" 2026-08-18\n'
    "OpenJDK Runtime Environment Temurin-17.0.20.1+1 (build 17.0.20.1+1)\n"
    "OpenJDK 64-Bit Server VM Temurin-17.0.20.1+1 "
    "(build 17.0.20.1+1, mixed mode, sharing)\n"
)
ORACLE_8 = (
    'java version "1.8.0_392"\n'
    "Java(TM) SE Runtime Environment (build 1.8.0_392-b08)\n"
    "Java HotSpot(TM) 64-Bit Server VM (build 25.392-b08, mixed mode)\n"
)
OPENJ9_11 = (
    'openjdk version "11.0.21" 2023-10-17\n'
    "IBM Semeru Runtime Open Edition 11.0.21.0 (build 11.0.21+9)\n"
    "Eclipse OpenJ9 VM 11.0.21.0 (build openj9-0.41.0, JRE 11 Windows 10 "
    "amd64-64-Bit Compressed References 20231017_123 (JIT enabled, AOT enabled)\n"
)
GRAALVM_21 = (
    'openjdk version "21.0.1" 2023-10-17\n'
    "OpenJDK Runtime Environment GraalVM CE 21.0.1+12.1 "
    "(build 21.0.1+12-jvmci-23.1-b19)\n"
    "OpenJDK 64-Bit Server VM GraalVM CE 21.0.1+12.1 "
    "(build 21.0.1+12-jvmci-23.1-b19, mixed mode, sharing)\n"
)
EARLY_ACCESS_22 = (
    'openjdk version "22-ea" 2024-03-19\n'
    "OpenJDK Runtime Environment (build 22-ea+27-2262)\n"
)
JAVA_7 = (
    'java version "1.7.0_80"\n' "Java(TM) SE Runtime Environment (build 1.7.0_80-b15)\n"
)
JAVA_9 = 'java version "9.0.4"\nJava(TM) SE Runtime Environment (build 9.0.4+11)\n'


@pytest.fixture(autouse=True)
def _clear_cache():
    jr.clear_java_cache()
    yield
    jr.clear_java_cache()


@pytest.mark.parametrize(
    ("output", "major", "version", "runtime_prefix"),
    [
        (TEMURIN_17, 17, "17.0.20.1", "OpenJDK Runtime Environment Temurin"),
        (ORACLE_8, 8, "1.8.0_392", "Java(TM) SE Runtime Environment"),
        (OPENJ9_11, 11, "11.0.21", "IBM Semeru Runtime"),
        (GRAALVM_21, 21, "21.0.1", "OpenJDK Runtime Environment GraalVM"),
        (EARLY_ACCESS_22, 22, "22-ea", "OpenJDK Runtime Environment (build"),
        (JAVA_7, 7, "1.7.0_80", "Java(TM) SE Runtime Environment"),
        (JAVA_9, 9, "9.0.4", "Java(TM) SE Runtime Environment"),
    ],
)
def test_parse_java_version(
    output: str, major: int, version: str, runtime_prefix: str
) -> None:
    got_major, got_version, got_runtime = jr.parse_java_version(output)
    assert got_major == major
    assert got_version == version
    assert got_runtime.startswith(runtime_prefix)


def test_parse_java_version_single_line() -> None:
    assert jr.parse_java_version('openjdk version "17.0.2" 2022-01-18') == (
        17,
        "17.0.2",
        "",
    )


@pytest.mark.parametrize("output", ["", "Error occurred during initialization of VM"])
def test_parse_java_version_rejects_garbage(output: str) -> None:
    with pytest.raises(ValueError):
        jr.parse_java_version(output)


# --- find_java -----------------------------------------------------------


def _fake_java(directory: Path) -> Path:
    """Create a fake java launcher inside ``directory`` and return its path."""
    directory.mkdir(parents=True, exist_ok=True)
    exe = directory / jr.java_executable_name()
    exe.write_text("#!/bin/sh\nexit 0\n")
    exe.chmod(0o755)
    return exe


def test_find_java_prefers_env_var_executable(tmp_path: Path) -> None:
    exe = _fake_java(tmp_path / "override" / "bin")
    other = _fake_java(tmp_path / "home" / "bin")
    env = {jr.JAVA_ENV_VAR: str(exe), "JAVA_HOME": str(other.parent.parent)}
    assert jr.find_java(env) == (str(exe), jr.JAVA_ENV_VAR)


def test_find_java_env_var_accepts_home_directory(tmp_path: Path) -> None:
    exe = _fake_java(tmp_path / "jdk" / "bin")
    env = {jr.JAVA_ENV_VAR: str(tmp_path / "jdk")}
    assert jr.find_java(env) == (str(exe), jr.JAVA_ENV_VAR)


def test_find_java_env_var_invalid_is_an_error(tmp_path: Path) -> None:
    _fake_java(tmp_path / "home" / "bin")  # would be found, but override wins
    env = {
        jr.JAVA_ENV_VAR: str(tmp_path / "nowhere"),
        "JAVA_HOME": str(tmp_path / "home"),
    }
    with pytest.raises(MissingDependencyError, match=jr.JAVA_ENV_VAR):
        jr.find_java(env)


def test_find_java_uses_java_home(tmp_path: Path) -> None:
    exe = _fake_java(tmp_path / "home" / "bin")
    env = {"JAVA_HOME": str(tmp_path / "home"), "PATH": ""}
    assert jr.find_java(env) == (str(exe), "JAVA_HOME")


def test_find_java_falls_back_to_path(tmp_path: Path) -> None:
    exe = _fake_java(tmp_path / "bin")
    env = {"JAVA_HOME": str(tmp_path / "empty-home"), "PATH": str(exe.parent)}
    path, source = jr.find_java(env)
    assert source == "PATH"
    assert Path(path).resolve() == exe.resolve()


def test_find_java_nothing_found_lists_searched_locations(tmp_path: Path) -> None:
    env = {"PATH": str(tmp_path / "no-such-dir")}
    with pytest.raises(MissingDependencyError) as excinfo:
        jr.find_java(env)
    message = str(excinfo.value)
    assert jr.JAVA_ENV_VAR in message
    assert "JAVA_HOME" in message
    assert "PATH" in message
    assert "Temurin" in message  # install hint


# --- check_java ----------------------------------------------------------


class _FakeRun:
    """Replacement for ``subprocess.run`` returning canned ``java -version`` output."""

    def __init__(self, stderr: str = "", returncode: int = 0, raise_exc=None):
        self.stderr = stderr
        self.returncode = returncode
        self.raise_exc = raise_exc
        self.calls: list[list[str]] = []

    def __call__(self, cmd, **kwargs):
        self.calls.append(list(cmd))
        if self.raise_exc is not None:
            raise self.raise_exc
        return subprocess.CompletedProcess(cmd, self.returncode, "", self.stderr)


def test_check_java_explicit_path(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeRun(TEMURIN_17)
    monkeypatch.setattr(jr.subprocess, "run", fake)
    info = jr.check_java("/opt/jdk/bin/java")
    assert info == jr.JavaInfo(
        path="/opt/jdk/bin/java",
        major=17,
        version="17.0.20.1",
        runtime="OpenJDK Runtime Environment Temurin-17.0.20.1+1 (build 17.0.20.1+1)",
        source="explicit",
    )
    assert fake.calls == [["/opt/jdk/bin/java", "-version"]]
    assert not info.is_newer_than_tested


def test_check_java_resolves_with_find_java(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    exe = _fake_java(tmp_path / "jdk" / "bin")
    monkeypatch.setattr(jr.subprocess, "run", _FakeRun(ORACLE_8))
    info = jr.check_java(env={jr.JAVA_ENV_VAR: str(exe)})
    assert (info.path, info.source, info.major) == (str(exe), jr.JAVA_ENV_VAR, 8)


def test_check_java_caches_per_executable(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeRun(TEMURIN_17)
    monkeypatch.setattr(jr.subprocess, "run", fake)
    first = jr.check_java("java")
    second = jr.check_java("java")
    assert first is second
    assert len(fake.calls) == 1
    jr.check_java("java", use_cache=False)
    assert len(fake.calls) == 2


def test_check_java_too_old(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(jr.subprocess, "run", _FakeRun(JAVA_7))
    with pytest.raises(MissingDependencyError, match="too old"):
        jr.check_java("java")


def test_check_java_newer_than_tested_warns(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(jr.subprocess, "run", _FakeRun(GRAALVM_21))
    with caplog.at_level("WARNING", logger="gcamreader.structure"):
        info = jr.check_java("java")
    assert info.major == 21
    assert info.is_newer_than_tested
    assert any("newer than" in rec.getMessage() for rec in caplog.records)


def test_check_java_nonzero_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        jr.subprocess, "run", _FakeRun("Error: could not create the JVM", 1)
    )
    with pytest.raises(MissingDependencyError, match="exited with code 1"):
        jr.check_java("java")


def test_check_java_unparseable_output(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(jr.subprocess, "run", _FakeRun("Picked up JAVA_TOOL_OPTIONS"))
    with pytest.raises(MissingDependencyError, match="Could not understand"):
        jr.check_java("java")


def test_check_java_not_executable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        jr.subprocess, "run", _FakeRun(raise_exc=FileNotFoundError("java"))
    )
    with pytest.raises(MissingDependencyError, match="Cannot execute"):
        jr.check_java("/no/such/java")


def test_check_java_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        jr.subprocess,
        "run",
        _FakeRun(raise_exc=subprocess.TimeoutExpired(["java"], 60)),
    )
    with pytest.raises(MissingDependencyError, match="did not finish"):
        jr.check_java("java")


def test_check_java_missing_runtime_propagates(tmp_path: Path) -> None:
    with pytest.raises(MissingDependencyError, match="No Java runtime found"):
        jr.check_java(env={"PATH": str(tmp_path)})


@requires_java
def test_check_java_real_runtime() -> None:
    info = jr.check_java(use_cache=False)
    assert info.major >= jr.MIN_JAVA_MAJOR
    assert Path(info.path).exists()
    assert info.source in {jr.JAVA_ENV_VAR, "JAVA_HOME", "PATH"}


# --- failure classification ---------------------------------------------


@pytest.mark.parametrize(
    ("stderr", "keyword", "error_class"),
    [
        (
            'Exception in thread "main" java.lang.UnsupportedClassVersionError: '
            "org/basex/BaseX has been compiled by a more recent version of the "
            "Java Runtime",
            "too old",
            MissingDependencyError,
        ),
        (
            'Exception in thread "main" java.lang.OutOfMemoryError: Java heap space',
            "--java-max-memory",
            MissingDependencyError,
        ),
        (
            "java.lang.OutOfMemoryError: GC overhead limit exceeded",
            "--java-max-memory",
            MissingDependencyError,
        ),
        (
            "java.lang.reflect.InaccessibleObjectException: Unable to make field "
            "private final byte[] java.lang.String.value accessible: module "
            'java.base does not "opens java.lang" to unnamed module @1b6d3586',
            "module system",
            MissingDependencyError,
        ),
        (
            "Error: Could not find or load main class org.basex.BaseX",
            "installation",
            MissingDependencyError,
        ),
        (
            "Stopped at ., 1/1:\n[BXDB0001] Database 'x' is incompatible with "
            "BaseX 9.5: created with a newer version.",
            "BaseX 9.5.0",
            DatabaseAccessError,
        ),
        (
            "Database 'database_basexdbGCAM' was not found.",
            "did not find the database",
            DatabaseAccessError,
        ),
        (
            "java.nio.file.AccessDeniedException: /data/db/tbl.basex",
            "permission denied",
            DatabaseAccessError,
        ),
    ],
)
def test_classify_java_failure(
    stderr: str, keyword: str, error_class: type[Exception]
) -> None:
    result = jr.classify_java_failure(stderr)
    assert result is not None
    advice, cls = result
    assert keyword.lower() in advice.lower()
    assert cls is error_class


@pytest.mark.parametrize("stderr", ["", "Picked up JAVA_TOOL_OPTIONS: -Dfoo=bar"])
def test_classify_java_failure_unknown(stderr: str) -> None:
    assert jr.classify_java_failure(stderr) is None


def test_java_failure_error_recognized() -> None:
    err = jr.java_failure_error(
        "java.lang.OutOfMemoryError: Java heap space",
        returncode=1,
        context="Structure extraction failed",
    )
    assert isinstance(err, MissingDependencyError)
    text = str(err)
    assert text.startswith("Structure extraction failed (exit code 1). Java ran out")
    assert "Java output:" in text
    assert "OutOfMemoryError" in text


def test_java_failure_error_unrecognized_uses_default() -> None:
    err = jr.java_failure_error("something odd", returncode=2)
    assert type(err) is DatabaseAccessError
    assert "exit code 2" in str(err)
    assert "something odd" in str(err)

    err2 = jr.java_failure_error("", default=MissingDependencyError)
    assert type(err2) is MissingDependencyError
    assert "Java output" not in str(err2)
