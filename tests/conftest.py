"""Shared pytest fixtures and markers for the gcamreader test suite."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

import gcamreader

DATA_DIR = Path(gcamreader.sample_data_dir())
SAMPLE_DB_NAME = "sample_basexdb"
LAND_QUERY = DATA_DIR / "queries" / "query_land_reg32_basin235_gcam5p0.xml"
COMP_LAND_OUTPUT = DATA_DIR / "comp_outputs" / "land_query.csv"

java_available = shutil.which("java") is not None
requires_java = pytest.mark.skipif(
    not java_available, reason="Java runtime not available"
)


@pytest.fixture(scope="session")
def data_dir() -> Path:
    """Return the path to the bundled sample data directory.

    Returns:
        The sample data directory path.
    """
    return DATA_DIR


@pytest.fixture(scope="session")
def land_query_path() -> Path:
    """Return the path to the bundled land-allocation query XML file.

    Returns:
        The land query XML path.
    """
    return LAND_QUERY


@pytest.fixture(scope="session")
def comp_land_output_path() -> Path:
    """Return the path to the reference land-allocation output CSV file.

    Returns:
        The reference output CSV path.
    """
    return COMP_LAND_OUTPUT


@pytest.fixture
def connection() -> gcamreader.LocalDBConn:
    """Create a local database connection to the bundled sample database.

    Returns:
        An initialized :class:`gcamreader.LocalDBConn`.
    """
    return gcamreader.LocalDBConn(str(DATA_DIR), SAMPLE_DB_NAME, suppress_gabble=True)
