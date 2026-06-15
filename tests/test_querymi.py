"""High-level functionality and output-consistency tests for gcamreader.

These tests ensure that the public API behaves as expected and that query
outputs remain consistent with the bundled reference data. The database-backed
tests require a Java runtime and are skipped when Java is unavailable.

@license BSD 2-Clause
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
import pytest

import gcamreader

requires_java = pytest.mark.skipif(
    shutil.which("java") is None, reason="Java runtime not available"
)

SORT_COLUMNS = ["region", "land-allocation", "Year"]


def test_version_is_exposed() -> None:
    """The package should expose a version string."""
    assert isinstance(gcamreader.__version__, str)
    assert gcamreader.__version__


def test_public_api_is_importable() -> None:
    """The documented public API names should be importable."""
    for name in (
        "Query",
        "parse_batch_query",
        "LocalDBConn",
        "RemoteDBConn",
        "importdata",
        "sample_data_dir",
    ):
        assert hasattr(gcamreader, name)


def test_parse_batch_query(land_query_path: Path) -> None:
    """parse_batch_query should return Query objects with parsed metadata."""
    queries = gcamreader.parse_batch_query(str(land_query_path))
    assert len(queries) == 1
    query = queries[0]
    assert query.title == "Crop Land Allocation"
    assert isinstance(query.querystr, str)


@requires_java
def test_connection(connection: "gcamreader.LocalDBConn") -> None:
    """The connection object should be a LocalDBConn instance."""
    assert isinstance(connection, gcamreader.LocalDBConn)


@requires_java
def test_land_query(
    connection: "gcamreader.LocalDBConn",
    land_query_path: Path,
    comp_land_output_path: Path,
) -> None:
    """The land-allocation query output should match the reference data."""
    query = gcamreader.parse_batch_query(str(land_query_path))[0]

    df = connection.runQuery(query).sort_values(by=SORT_COLUMNS, ignore_index=True)

    comp = pd.read_csv(comp_land_output_path).sort_values(
        by=SORT_COLUMNS, ignore_index=True
    )

    pd.testing.assert_frame_equal(df, comp)
