"""Unit tests for the :mod:`gcamreader.cli` command line interface.

The CLI is exercised with :class:`typer.testing.CliRunner` and the database
connection layer is mocked so that no Java runtime or network access is
required. The ``save`` and ``execute`` helpers are tested directly.

@license BSD 2-Clause
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

import gcamreader
from gcamreader import cli

runner = CliRunner()


_UNSET = object()


class _FakeConn:
    """A stand-in connection that returns a fixed result for any query."""

    def __init__(self, result: object = _UNSET) -> None:
        if result is _UNSET:
            result = pd.DataFrame({"region": ["USA", "China"], "value": [1.0, 2.0]})
        self.result = result
        self.calls: list[object] = []

    def runQuery(self, query: object) -> pd.DataFrame | None:
        """Record the query and return the configured result."""
        self.calls.append(query)
        return self.result


class TestVersionOption:
    """Tests for the ``--version`` option."""

    def test_version_prints_and_exits(self) -> None:
        """``--version`` should print the package version and exit cleanly."""
        result = runner.invoke(cli.app, ["--version"])
        assert result.exit_code == 0
        assert gcamreader.__version__ in result.stdout

    def test_no_args_shows_help(self) -> None:
        """Invoking with no arguments should display help text."""
        result = runner.invoke(cli.app, [])
        # ``no_args_is_help`` causes typer to exit with code 2 after printing
        # the help text.
        assert result.exit_code == 2
        assert "Usage" in result.stdout


class TestSave:
    """Tests for the :func:`cli.save` helper."""

    def _make_query(self) -> gcamreader.Query:
        xml = '<aQuery title="My Query"><region name="USA"/></aQuery>'
        return gcamreader.Query(xml)

    def test_writes_csv(self, output_dir: Path) -> None:
        """A successful query should write a pipe-delimited CSV file."""
        save_to = output_dir / "my_query.csv"
        cli.save(
            {
                "conn": _FakeConn(),
                "query": self._make_query(),
                "save_to": save_to,
                "force": False,
            }
        )
        assert save_to.exists()
        content = save_to.read_text()
        assert "|" in content
        assert "value" in content

    def test_skips_existing_without_force(self, output_dir: Path) -> None:
        """An existing output should not be overwritten without ``force``."""
        save_to = output_dir / "my_query.csv"
        save_to.write_text("original")
        cli.save(
            {
                "conn": _FakeConn(),
                "query": self._make_query(),
                "save_to": save_to,
                "force": False,
            }
        )
        assert save_to.read_text() == "original"

    def test_overwrites_existing_with_force(self, output_dir: Path) -> None:
        """An existing output should be overwritten when ``force`` is set."""
        save_to = output_dir / "my_query.csv"
        save_to.write_text("original")
        cli.save(
            {
                "conn": _FakeConn(),
                "query": self._make_query(),
                "save_to": save_to,
                "force": True,
            }
        )
        assert save_to.read_text() != "original"
        assert "value" in save_to.read_text()

    def test_empty_result_writes_nothing(self, output_dir: Path) -> None:
        """A ``None`` query result should not create an output file."""
        save_to = output_dir / "my_query.csv"
        cli.save(
            {
                "conn": _FakeConn(result=None),
                "query": self._make_query(),
                "save_to": save_to,
                "force": False,
            }
        )
        assert not save_to.exists()

    def test_failed_query_writes_nothing(self, output_dir: Path) -> None:
        """A subprocess failure during the query should be handled gracefully."""

        class _FailingConn:
            def runQuery(self, query: object) -> pd.DataFrame:
                raise subprocess.CalledProcessError(1, ["java"])

        save_to = output_dir / "my_query.csv"
        cli.save(
            {
                "conn": _FailingConn(),
                "query": self._make_query(),
                "save_to": save_to,
                "force": False,
            }
        )
        assert not save_to.exists()


class TestExecute:
    """Tests for the :func:`cli.execute` helper."""

    def test_runs_all_queries_and_writes_outputs(
        self, output_dir: Path, land_query_path: Path
    ) -> None:
        """execute should parse the file and write one CSV per query."""
        conn = _FakeConn()
        cli.execute(conn, land_query_path, output_dir, force=False)
        outputs = list(output_dir.glob("*.csv"))
        assert len(outputs) == 1
        assert outputs[0].name == "crop_land_allocation.csv"


class TestLocalCommand:
    """Tests for the ``local`` subcommand."""

    def test_missing_basex_files_errors(
        self, tmp_path: Path, land_query_path: Path, output_dir: Path
    ) -> None:
        """A database directory without ``*.basex`` files should error out."""
        empty_db = tmp_path / "empty_db"
        empty_db.mkdir()
        result = runner.invoke(
            cli.app,
            [
                "local",
                "-d",
                str(empty_db),
                "-q",
                str(land_query_path),
                "-o",
                str(output_dir),
            ],
        )
        assert result.exit_code == 1
        assert "basex files missing" in result.stderr

    def test_local_invokes_execute(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        land_query_path: Path,
        output_dir: Path,
    ) -> None:
        """A valid local invocation should build a connection and run execute."""
        db_dir = tmp_path / "mydb_basexdb"
        db_dir.mkdir()
        (db_dir / "tbl.basex").write_text("")

        monkeypatch.setattr(cli, "LocalDBConn", lambda parent, name: _FakeConn())
        called: dict[str, object] = {}

        def _fake_execute(
            conn: object, query_path: Path, out: Path, force: bool
        ) -> None:
            called["query_path"] = query_path
            called["force"] = force

        monkeypatch.setattr(cli, "execute", _fake_execute)
        result = runner.invoke(
            cli.app,
            [
                "local",
                "-d",
                str(db_dir),
                "-q",
                str(land_query_path),
                "-o",
                str(output_dir),
            ],
        )
        assert result.exit_code == 0
        assert called["query_path"] == land_query_path
        assert called["force"] is False


class TestRemoteCommand:
    """Tests for the ``remote`` subcommand."""

    def test_remote_invokes_execute(
        self,
        monkeypatch: pytest.MonkeyPatch,
        land_query_path: Path,
        output_dir: Path,
    ) -> None:
        """A valid remote invocation should build a connection and run execute."""
        created: dict[str, object] = {}

        def _fake_remote(**kwargs: object) -> _FakeConn:
            created.update(kwargs)
            return _FakeConn()

        monkeypatch.setattr(cli, "RemoteDBConn", _fake_remote)
        monkeypatch.setattr(
            cli, "execute", lambda conn, q, o, f: created.setdefault("ran", True)
        )
        result = runner.invoke(
            cli.app,
            [
                "remote",
                "-u",
                "user",
                "-d",
                "mydb",
                "-q",
                str(land_query_path),
                "-o",
                str(output_dir),
                "-w",
                "secret",
                "-n",
                "example.com",
                "-p",
                "1234",
            ],
        )
        assert result.exit_code == 0
        assert created["username"] == "user"
        assert created["dbfile"] == "mydb"
        assert created["address"] == "example.com"
        assert created["port"] == 1234
        assert created.get("ran") is True
