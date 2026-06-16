"""Unit tests for the database-connection logic in :mod:`gcamreader.querymi`.

These tests use mocking to exercise the subprocess- and network-backed code
paths without requiring a real Java runtime, BaseX server, or database. The
goal is to verify the orchestration logic (command construction, result
parsing, error handling, and validation) in isolation.

@license BSD 2-Clause
"""

from __future__ import annotations

import subprocess as sp

import pandas as pd
import pytest

import gcamreader
from gcamreader import querymi


class TestParserslt:
    """Tests for the internal :func:`querymi._parserslt` result parser."""

    def test_aggregates_value_rows(self, sample_result_csv: str) -> None:
        """Rows sharing all non-value columns should be summed."""
        df = querymi._parserslt(sample_result_csv, warn_empty=True, title="t")
        assert df is not None
        forest = df[(df["region"] == "USA") & (df["land-allocation"] == "Forest")]
        assert forest["value"].iloc[0] == pytest.approx(15.0)

    def test_no_value_column_is_passed_through(
        self, sample_scenario_csv: str
    ) -> None:
        """When there is no value column, rows should not be aggregated."""
        df = querymi._parserslt(sample_scenario_csv, warn_empty=True, title="t")
        assert df is not None
        assert list(df["name"]) == ["Reference"]
        assert "value" not in df.columns

    def test_empty_input_returns_none(self) -> None:
        """An empty result string should return ``None``."""
        assert querymi._parserslt("", warn_empty=False, title="t") is None

    def test_empty_input_warns(self, capsys: pytest.CaptureFixture[str]) -> None:
        """An empty result should warn to stderr when requested."""
        result = querymi._parserslt(
            "", warn_empty=True, title="My Query", stderr="boom"
        )
        captured = capsys.readouterr()
        assert result is None
        assert "empty string" in captured.err
        assert "My Query" in captured.err
        assert "boom" in captured.err


class TestRunmi:
    """Tests for the internal :func:`querymi._runmi` subprocess wrapper."""

    def test_returns_stdout_and_stderr(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A successful run should return its stdout and stderr."""

        class _FakeReturn:
            stdout = "out"
            stderr = "err"

        monkeypatch.setattr(querymi.sp, "run", lambda *a, **k: _FakeReturn())
        out, err = querymi._runmi(["java"], "querystr")
        assert out == "out"
        assert err == "err"

    def test_propagates_called_process_error(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A non-zero exit should re-raise and log diagnostics to stderr."""

        def _raise(*args: object, **kwargs: object) -> None:
            raise sp.CalledProcessError(1, ["java"], stderr="java failed")

        monkeypatch.setattr(querymi.sp, "run", _raise)
        with pytest.raises(sp.CalledProcessError):
            querymi._runmi(["java", "-cp"], "the-query")
        captured = capsys.readouterr()
        assert "Model interface run failed" in captured.err
        assert "the-query" in captured.err
        assert "java failed" in captured.err


class TestLocalDBConn:
    """Tests for :class:`gcamreader.LocalDBConn` orchestration logic."""

    def test_default_classpath_is_used(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Skipping validation should still set the default class path."""
        monkeypatch.setattr(
            querymi.LocalDBConn, "listScenariosInDB", lambda self: None
        )
        conn = gcamreader.LocalDBConn(
            "/some/path", "db", validatedb=False
        )
        assert conn.miclasspath == querymi._default_miclasspath
        assert conn.dbfile == "db"
        assert conn.maxMemory == "4g"

    def test_validation_failure_raises_oserror(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failed validation query should raise ``OSError``."""
        monkeypatch.setattr(
            querymi.LocalDBConn, "listScenariosInDB", lambda self: None
        )
        with pytest.raises(OSError):
            gcamreader.LocalDBConn("/some/path", "db", validatedb=True)

    def test_validation_success_prints_scenarios(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """A successful validation should print the available scenarios."""
        scen = pd.DataFrame({"name": ["Reference", "Policy"]})
        monkeypatch.setattr(
            querymi.LocalDBConn, "listScenariosInDB", lambda self: scen
        )
        gcamreader.LocalDBConn("/some/path", "db", validatedb=True)
        captured = capsys.readouterr()
        assert "Reference" in captured.out
        assert "Policy" in captured.out

    def test_runquery_builds_command_and_parses(
        self, monkeypatch: pytest.MonkeyPatch, simple_query: gcamreader.Query
    ) -> None:
        """runQuery should call the model interface and parse the result."""
        monkeypatch.setattr(
            querymi.LocalDBConn, "listScenariosInDB", lambda self: None
        )
        conn = gcamreader.LocalDBConn("/some/path", "db", validatedb=False)

        captured_cmd: dict[str, list[str]] = {}

        def _fake_runmi(cmd: list[str], querystr: str) -> tuple[str, str]:
            captured_cmd["cmd"] = cmd
            return "region,value\nUSA,1.0\n", ""

        monkeypatch.setattr(querymi, "_runmi", _fake_runmi)
        df = conn.runQuery(simple_query)
        assert df is not None
        assert "java" in captured_cmd["cmd"]
        assert "RUN" in captured_cmd["cmd"]

    def test_runquery_regions_override(
        self, monkeypatch: pytest.MonkeyPatch, simple_query: gcamreader.Query
    ) -> None:
        """An explicit regions argument should reach the query file content."""
        monkeypatch.setattr(
            querymi.LocalDBConn, "listScenariosInDB", lambda self: None
        )
        conn = gcamreader.LocalDBConn("/some/path", "db", validatedb=False)

        written: dict[str, str] = {}
        real_runmi_called = {"value": False}

        def _fake_runmi(cmd: list[str], querystr: str) -> tuple[str, str]:
            real_runmi_called["value"] = True
            # The query temp file is the last element of the command.
            with open(cmd[-1]) as fh:
                written["content"] = fh.read()
            return "region,value\nChina,2.0\n", ""

        monkeypatch.setattr(querymi, "_runmi", _fake_runmi)
        conn.runQuery(simple_query, regions=["China"])
        assert real_runmi_called["value"]
        assert "('China')" in written["content"]

    def test_listscenarios_adds_fqname(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """listScenariosInDB should append a fully qualified name column."""
        monkeypatch.setattr(
            querymi,
            "_runmi",
            lambda cmd, q: ("name,date\nReference,2020-1-1\n", ""),
        )
        conn = gcamreader.LocalDBConn.__new__(gcamreader.LocalDBConn)
        conn.dbpath = "/some/path"
        conn.dbfile = "db"
        conn.maxMemory = "4g"
        conn.miclasspath = querymi._default_miclasspath
        scen = conn.listScenariosInDB()
        assert scen is not None
        assert scen["fqName"].iloc[0] == "Reference 2020-1-1"


class _FakeResponse:
    """A minimal stand-in for a :mod:`requests` response object."""

    def __init__(self, text: str, status_ok: bool = True) -> None:
        self.text = text
        self._status_ok = status_ok

    def raise_for_status(self) -> None:
        """Raise when the simulated status is not OK."""
        if not self._status_ok:
            raise RuntimeError("HTTP error")


class TestRemoteDBConn:
    """Tests for :class:`gcamreader.RemoteDBConn` orchestration logic."""

    def test_validation_failure_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failed validation should raise an exception."""
        monkeypatch.setattr(
            querymi.RemoteDBConn, "listScenariosInDB", lambda self: None
        )
        with pytest.raises(Exception, match="Failed to validate"):
            gcamreader.RemoteDBConn("db", "user", "pw", validatedb=True)

    def test_attributes_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Connection attributes should be stored when validation is skipped."""
        conn = gcamreader.RemoteDBConn(
            "db", "user", "pw", address="example.com", port=9999, validatedb=False
        )
        assert conn.dbfile == "db"
        assert conn.username == "user"
        assert conn.address == "example.com"
        assert conn.port == 9999

    def test_runquery_posts_and_parses(
        self, monkeypatch: pytest.MonkeyPatch, simple_query: gcamreader.Query
    ) -> None:
        """runQuery should POST to the REST endpoint and parse the response."""
        conn = gcamreader.RemoteDBConn("db", "user", "pw", validatedb=False)

        captured: dict[str, object] = {}

        def _fake_post(url: str, auth: object, data: str) -> _FakeResponse:
            captured["url"] = url
            captured["data"] = data
            return _FakeResponse("region,value\nUSA,1.0\n")

        monkeypatch.setattr("requests.post", _fake_post)
        df = conn.runQuery(simple_query)
        assert df is not None
        assert "/rest/db" in captured["url"]
        assert "runMIQuery" in captured["data"]

    def test_runquery_raises_on_http_error(
        self, monkeypatch: pytest.MonkeyPatch, simple_query: gcamreader.Query
    ) -> None:
        """A non-OK HTTP status should propagate as an exception."""
        conn = gcamreader.RemoteDBConn("db", "user", "pw", validatedb=False)
        monkeypatch.setattr(
            "requests.post",
            lambda url, auth, data: _FakeResponse("", status_ok=False),
        )
        with pytest.raises(RuntimeError):
            conn.runQuery(simple_query)

    def test_listscenarios_adds_fqname(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """listScenariosInDB should append a fully qualified name column."""
        conn = gcamreader.RemoteDBConn("db", "user", "pw", validatedb=False)
        monkeypatch.setattr(
            "requests.post",
            lambda url, auth, data: _FakeResponse(
                "name,date\nReference,2020-1-1\n"
            ),
        )
        scen = conn.listScenariosInDB()
        assert scen is not None
        assert scen["fqName"].iloc[0] == "Reference 2020-1-1"


class TestImportdata:
    """Tests for the :func:`gcamreader.importdata` convenience function."""

    def test_uses_existing_connection(
        self, simple_query: gcamreader.Query
    ) -> None:
        """A provided connection should be used to run each query."""

        class _FakeConn:
            def __init__(self) -> None:
                self.calls: list[gcamreader.Query] = []

            def runQuery(
                self,
                query: gcamreader.Query,
                scenarios: object = None,
                regions: object = None,
                warn_empty: bool = False,
            ) -> pd.DataFrame:
                self.calls.append(query)
                return pd.DataFrame({"value": [1.0]})

        conn = _FakeConn()
        results = gcamreader.importdata(conn, [simple_query])
        assert set(results.keys()) == {"CO2 emissions"}
        assert len(conn.calls) == 1

    def test_string_dbspec_builds_local_connection(
        self, monkeypatch: pytest.MonkeyPatch, simple_query: gcamreader.Query
    ) -> None:
        """A string ``dbspec`` should construct a :class:`LocalDBConn`."""
        created: dict[str, tuple[str, str]] = {}

        class _FakeLocal:
            def __init__(
                self, dbdir: str, dbname: str, *args: object, **kwargs: object
            ) -> None:
                created["args"] = (dbdir, dbname)

            def runQuery(self, *args: object, **kwargs: object) -> pd.DataFrame:
                return pd.DataFrame({"value": [1.0]})

        monkeypatch.setattr(querymi, "LocalDBConn", _FakeLocal)
        results = gcamreader.importdata("/db/path/mydb", [simple_query])
        assert created["args"] == ("/db/path", "mydb")
        assert "CO2 emissions" in results

    def test_string_queries_are_parsed(
        self, monkeypatch: pytest.MonkeyPatch, land_query_path: object
    ) -> None:
        """A string ``queries`` path should be parsed into Query objects."""

        class _FakeConn:
            def runQuery(self, *args: object, **kwargs: object) -> pd.DataFrame:
                return pd.DataFrame({"value": [1.0]})

        results = gcamreader.importdata(_FakeConn(), str(land_query_path))
        assert "Crop Land Allocation" in results

    def test_empty_results_are_stored_as_none(
        self, simple_query: gcamreader.Query
    ) -> None:
        """A ``None`` query result should be preserved in the output dict."""

        class _FakeConn:
            def runQuery(self, *args: object, **kwargs: object) -> None:
                return None

        results = gcamreader.importdata(_FakeConn(), [simple_query])
        assert results["CO2 emissions"] is None
