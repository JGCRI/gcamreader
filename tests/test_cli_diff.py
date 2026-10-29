"""Tests for the ``gcamreader diff`` and ``gcamreader scenarios`` commands.

The comparison API is replaced by fakes so no Java is needed; argument
parsing, validation, exit codes, output files and error mapping are checked
with :class:`typer.testing.CliRunner`.

@license BSD 2-Clause
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

import pytest
from typer.testing import CliRunner

from gcamreader import cli
from gcamreader.structure import api, markdown
from gcamreader.structure import java_runtime as jr
from gcamreader.structure.errors import MissingDependencyError
from gcamreader.structure.model import ComparisonResult, ScenarioRef
from gcamreader.structure.rules import ComparisonRules
from tests.test_structure_api import BASE, UNIT_CHANGED, FakeBackend

runner = CliRunner()


def make_db(path: Path) -> str:
    path.mkdir(parents=True, exist_ok=True)
    (path / "inf.basex").write_bytes(b"inf")
    (path / "tbl.basex").write_bytes(b"tbl")
    return str(path)


@pytest.fixture
def dbs(tmp_path: Path) -> list[str]:
    return [
        make_db(tmp_path / "ref"),
        make_db(tmp_path / "units"),
        make_db(tmp_path / "extra"),
    ]


@pytest.fixture
def results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> dict[str, ComparisonResult]:
    """Real results (built without Java through the fake backend) to hand to the CLI."""
    backend = FakeBackend()
    monkeypatch.setattr(api, "list_scenarios", backend.list_scenarios)
    monkeypatch.setattr(api, "extract_structure", backend.extract_structure)
    monkeypatch.setattr(
        api,
        "check_java",
        lambda *a, **k: jr.JavaInfo("java", 17, "17.0.2", "Fake", "PATH"),
    )
    a = backend.add(tmp_path / "fixtures" / "a", BASE)
    b = backend.add(tmp_path / "fixtures" / "b", UNIT_CHANGED)
    c = backend.add(tmp_path / "fixtures" / "c", BASE)
    return {
        "same": api.compare_databases([a, c]),
        "diff": api.compare_databases([a, b, c]),
    }


class FakeCompare:
    def __init__(
        self, result: ComparisonResult | None = None, raise_exc: Exception | None = None
    ):
        self.result = result
        self.raise_exc = raise_exc
        self.calls: list[dict] = []

    def __call__(self, paths, **kwargs):
        self.calls.append({"paths": list(paths), **kwargs})
        if self.raise_exc is not None:
            raise self.raise_exc
        return self.result


@pytest.fixture
def fake_compare(monkeypatch: pytest.MonkeyPatch, results):
    fake = FakeCompare(results["diff"])
    monkeypatch.setattr(api, "compare_databases", fake)
    return fake


# --- help and validation ----------------------------------------------------------


def test_help_lists_new_commands() -> None:
    result = runner.invoke(cli.app, ["--help"])
    assert result.exit_code == 0
    assert "diff" in result.stdout and "scenarios" in result.stdout
    assert "local" in result.stdout and "remote" in result.stdout


def test_diff_help_mentions_exit_codes_and_local_only() -> None:
    result = runner.invoke(cli.app, ["diff", "--help"])
    assert result.exit_code == 0
    # Rich wraps help text inside a box: drop box characters, collapse whitespace.
    text = " ".join(re.sub(r"[─-╿]", " ", result.stdout).split())
    assert "0: no structural differences; 1: differences found; 2: any error" in text
    assert "Remote databases are not supported" in text
    for option in (
        "--baseline",
        "--scenario",
        "--rules",
        "--work-dir",
        "--json",
        "--inline-limit",
    ):
        assert option in text


def test_fewer_than_two_databases(dbs: list[str], fake_compare: FakeCompare) -> None:
    result = runner.invoke(cli.app, ["diff", dbs[0]])
    assert result.exit_code == 2
    assert "at least 2 database directories" in result.output
    assert fake_compare.calls == []


def test_directory_without_basex_files(
    dbs: list[str], tmp_path: Path, fake_compare: FakeCompare
) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    result = runner.invoke(cli.app, ["diff", dbs[0], str(empty)])
    assert result.exit_code == 2
    assert "does not look like a BaseX database" in result.output
    assert fake_compare.calls == []


def test_missing_directory(
    dbs: list[str], tmp_path: Path, fake_compare: FakeCompare
) -> None:
    result = runner.invoke(cli.app, ["diff", dbs[0], str(tmp_path / "nope")])
    assert result.exit_code == 2
    assert "not a directory" in result.output
    assert fake_compare.calls == []


@pytest.mark.parametrize("value", ["0", "4", "nope"])
def test_bad_baseline(dbs: list[str], fake_compare: FakeCompare, value: str) -> None:
    result = runner.invoke(cli.app, ["diff", *dbs, "--baseline", value])
    assert result.exit_code == 2
    assert "--baseline" in result.output
    assert fake_compare.calls == []


def test_bad_scenario_prefix(dbs: list[str], fake_compare: FakeCompare) -> None:
    result = runner.invoke(cli.app, ["diff", *dbs, "-s", "7=GCAM"])
    assert result.exit_code == 2
    assert "out of range" in result.output
    result = runner.invoke(cli.app, ["diff", *dbs, "-s", "A", "-s", "B"])
    assert result.exit_code == 2
    assert "only one bare selector" in result.output
    assert fake_compare.calls == []


def test_bad_rules(dbs: list[str], tmp_path: Path, fake_compare: FakeCompare) -> None:
    bad = tmp_path / "rules.toml"
    bad.write_text('identity_attrs = ["name"]\n', encoding="utf-8")
    result = runner.invoke(cli.app, ["diff", *dbs, "--rules", str(bad)])
    assert result.exit_code == 2
    assert "unknown key" in result.output
    result = runner.invoke(
        cli.app, ["diff", *dbs, "--rules", str(tmp_path / "missing.toml")]
    )
    assert result.exit_code == 2
    result = runner.invoke(cli.app, ["diff", *dbs, "--ignore-attr", "name"])
    assert result.exit_code == 2
    assert "identity attributes" in result.output
    assert fake_compare.calls == []


# --- exit codes and output ----------------------------------------------------------


def test_no_differences_exit_zero(
    dbs: list[str], results, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(api, "compare_databases", FakeCompare(results["same"]))
    result = runner.invoke(cli.app, ["diff", dbs[0], dbs[1]])
    assert result.exit_code == 0
    assert result.stdout.startswith(
        "# Structural comparison of GCAM output databases\n"
    )
    assert "no structural differences" in result.stdout
    assert "Generated:" not in result.stdout


def test_differences_exit_one_and_report_on_stdout(
    dbs: list[str], fake_compare: FakeCompare, results
) -> None:
    result = runner.invoke(cli.app, ["diff", *dbs])
    assert result.exit_code == 1
    assert result.stdout == markdown.render_markdown(results["diff"])
    call = fake_compare.calls[0]
    assert call["paths"] == dbs
    assert call["baseline"] == 0
    assert call["scenarios"] is None
    assert call["rules"] == ComparisonRules.default()
    assert call["work_dir"] is None
    assert call["use_cache"] is True
    assert call["java_max_memory"] == "4g"
    assert callable(call["progress"])


def test_stdout_includes_appendix_when_over_limit(
    dbs: list[str], fake_compare: FakeCompare, results
) -> None:
    result = runner.invoke(cli.app, ["diff", *dbs, "--inline-limit", "0"])
    assert result.exit_code == 1
    main = markdown.render_markdown(results["diff"], inline_limit=0)
    appendix = "\n".join(markdown.iter_appendix(results["diff"])) + "\n"
    assert result.stdout == main + "\n" + appendix


def test_output_file_json_and_timestamp(
    dbs: list[str], fake_compare: FakeCompare, results, tmp_path: Path
) -> None:
    report = tmp_path / "out" / "report.md"
    report.parent.mkdir()
    json_file = tmp_path / "out" / "result.json"
    result = runner.invoke(
        cli.app,
        [
            "diff",
            *dbs,
            "-o",
            str(report),
            "--json",
            str(json_file),
            "--timestamp",
            "--inline-limit",
            "0",
        ],
    )
    assert result.exit_code == 1
    assert result.stdout == ""  # report went to the file
    assert f"wrote {report}" in result.stderr and f"wrote {json_file}" in result.stderr
    text = report.read_text(encoding="utf-8")
    assert "Generated: " in text.splitlines()[-1]
    appendix = tmp_path / "out" / "report.appendix.md"
    assert appendix.exists()
    assert "(`report.appendix.md`)" in text
    # API/CLI equivalence: the JSON is exactly the API result.
    assert (
        json.loads(json_file.read_text(encoding="utf-8")) == results["diff"].to_dict()
    )
    # And the report body equals render_markdown with the same settings.
    body = text.rsplit("\nGenerated: ", 1)[0]
    assert body == markdown.render_markdown(
        results["diff"], inline_limit=0, appendix_name="report.appendix.md"
    )


def test_quiet_suppresses_progress(
    dbs: list[str], fake_compare: FakeCompare, tmp_path: Path
) -> None:
    report = tmp_path / "r.md"
    result = runner.invoke(cli.app, ["diff", *dbs, "-o", str(report), "--quiet"])
    assert result.exit_code == 1
    assert result.stderr == ""
    assert report.exists()


# --- option parsing passed to the API --------------------------------------------------


def test_baseline_by_position_and_path(
    dbs: list[str], fake_compare: FakeCompare
) -> None:
    runner.invoke(cli.app, ["diff", *dbs, "--baseline", "2"])
    assert fake_compare.calls[-1]["baseline"] == 1
    runner.invoke(cli.app, ["diff", *dbs, "--baseline", dbs[2]])
    assert fake_compare.calls[-1]["baseline"] == 2


def test_scenario_selectors(dbs: list[str], fake_compare: FakeCompare) -> None:
    runner.invoke(cli.app, ["diff", *dbs, "-s", "GCAM"])
    assert fake_compare.calls[-1]["scenarios"] == "GCAM"
    runner.invoke(cli.app, ["diff", *dbs, "-s", "2=Policy@2026-01-01"])
    assert fake_compare.calls[-1]["scenarios"] == {2: "Policy@2026-01-01"}
    runner.invoke(cli.app, ["diff", *dbs, "-s", "units=Policy", "-s", "3=Other"])
    assert fake_compare.calls[-1]["scenarios"] == {2: "Policy", 3: "Other"}
    runner.invoke(cli.app, ["diff", *dbs, "-s", "2=Policy", "-s", "Ref"])
    assert fake_compare.calls[-1]["scenarios"] == {1: "Ref", 2: "Policy", 3: "Ref"}
    # An '=' that is not a known prefix belongs to the selector itself.
    runner.invoke(cli.app, ["diff", *dbs, "-s", "name=with=equals"])
    assert fake_compare.calls[-1]["scenarios"] == "name=with=equals"


def test_rules_file_and_overrides(
    dbs: list[str], fake_compare: FakeCompare, tmp_path: Path
) -> None:
    toml = tmp_path / "rules.toml"
    toml.write_text(
        'ignored_attributes = ["fillout"]\n[identity_by_element]\ntechnology = ["year"]\n',
        encoding="utf-8",
    )
    result = runner.invoke(
        cli.app,
        [
            "diff",
            *dbs,
            "--rules",
            str(toml),
            "--identity-attr",
            "name",
            "--identity-attr",
            "year",
            "--ignore-attr",
            "type",
        ],
    )
    assert result.exit_code == 1
    rules = fake_compare.calls[-1]["rules"]
    assert rules == ComparisonRules(
        identity_attributes=["name", "year"],
        identity_by_element={"technology": ["year"]},
        ignored_attributes=["fillout", "type"],
    )


def test_work_dir_cache_and_memory_options(
    dbs: list[str], fake_compare: FakeCompare, tmp_path: Path
) -> None:
    work = tmp_path / "work"
    runner.invoke(
        cli.app,
        [
            "diff",
            *dbs,
            "--work-dir",
            str(work),
            "--no-cache",
            "--java-max-memory",
            "8g",
        ],
    )
    call = fake_compare.calls[-1]
    assert call["work_dir"] == work
    assert call["use_cache"] is False
    assert call["java_max_memory"] == "8g"


# --- error mapping --------------------------------------------------------------------------


def test_structure_error_exits_two(
    dbs: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        api,
        "compare_databases",
        FakeCompare(
            raise_exc=MissingDependencyError(
                "No Java runtime found. Install Temurin 17."
            )
        ),
    )
    result = runner.invoke(cli.app, ["diff", *dbs])
    assert result.exit_code == 2
    assert result.stderr.strip() == "error: No Java runtime found. Install Temurin 17."
    assert result.stdout == ""


def test_unexpected_error_exits_two(
    dbs: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        api, "compare_databases", FakeCompare(raise_exc=RuntimeError("boom"))
    )
    result = runner.invoke(cli.app, ["diff", *dbs])
    assert result.exit_code == 2
    assert "error: RuntimeError: boom" in result.stderr


def test_debug_reraises(dbs: list[str], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        api, "compare_databases", FakeCompare(raise_exc=RuntimeError("boom"))
    )
    result = runner.invoke(cli.app, ["diff", *dbs, "--debug"])
    assert result.exit_code != 0
    assert isinstance(result.exception, RuntimeError)


# --- scenarios command --------------------------------------------------------------------


def test_scenarios_command(dbs: list[str], monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []

    def fake_list(path, *, with_counts=False, **kwargs):
        calls.append({"path": path, "with_counts": with_counts, **kwargs})
        refs = [
            ScenarioRef(
                "GCAM",
                "2026-1-6T10:39:43-07:00",
                "ver_9.0",
                48527349 if with_counts else None,
            )
        ]
        if path.endswith("units"):
            refs.append(
                ScenarioRef(
                    "Policy",
                    "2026-1-7T00:00:00-07:00",
                    None,
                    10 if with_counts else None,
                )
            )
        return refs

    monkeypatch.setattr(api, "list_scenarios", fake_list)
    result = runner.invoke(cli.app, ["scenarios", dbs[0], dbs[1]])
    assert result.exit_code == 0
    lines = result.stdout.splitlines()
    assert lines[0].split() == ["#", "database", "scenario", "date", "GCAM", "version"]
    assert len(lines) == 4
    assert "Policy" in lines[3] and "2026-1-7T00:00:00-07:00" in lines[3]
    assert all(not c["with_counts"] for c in calls)
    result = runner.invoke(
        cli.app, ["scenarios", dbs[1], "--counts", "--java-max-memory", "2g"]
    )
    assert result.exit_code == 0
    assert "elements" in result.stdout.splitlines()[0]
    assert "48,527,349" in result.stdout
    assert calls[-1]["with_counts"] is True and calls[-1]["java_max_memory"] == "2g"


def test_scenarios_validation_and_errors(
    dbs: list[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = runner.invoke(cli.app, ["scenarios", str(tmp_path / "missing")])
    assert result.exit_code == 2

    def failing(path, **kwargs):
        raise MissingDependencyError("no java")

    monkeypatch.setattr(api, "list_scenarios", failing)
    result = runner.invoke(cli.app, ["scenarios", dbs[0]])
    assert result.exit_code == 2
    assert "error: no java" in result.stderr


def test_scenarios_empty(dbs: list[str], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api, "list_scenarios", lambda path, **kwargs: [])
    result = runner.invoke(cli.app, ["scenarios", dbs[0]])
    assert result.exit_code == 0
    assert "no scenarios found" in result.stdout


# --- entry point -----------------------------------------------------------------------------


def test_module_entry_point_help() -> None:
    completed = subprocess.run(
        [sys.executable, "-m", "gcamreader", "diff", "--help"],
        capture_output=True,
        encoding="utf-8",
        check=False,
    )
    assert completed.returncode == 0
    assert "Compare the structure" in completed.stdout
