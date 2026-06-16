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


# A minimal CSV payload that mimics the output of the model interface for a
# query that contains a "value" column (and therefore triggers aggregation).
SAMPLE_RESULT_CSV = (
    "scenario,region,land-allocation,Year,value\n"
    "Reference,USA,Forest,2010,10.0\n"
    "Reference,USA,Forest,2010,5.0\n"
    "Reference,USA,Crops,2010,3.0\n"
)

# A CSV payload that lists scenarios (no "value" column, so no aggregation).
SAMPLE_SCENARIO_CSV = "name,date,version\nReference,2020-1-1,gcam-v7.0\n"


@pytest.fixture
def sample_result_csv() -> str:
    """Return a CSV string mimicking an aggregatable query result.

    Returns:
        A CSV payload containing a ``value`` column with duplicate rows.
    """
    return SAMPLE_RESULT_CSV


@pytest.fixture
def sample_scenario_csv() -> str:
    """Return a CSV string mimicking a scenario listing result.

    Returns:
        A CSV payload without a ``value`` column.
    """
    return SAMPLE_SCENARIO_CSV


@pytest.fixture
def land_query(land_query_path: Path) -> gcamreader.Query:
    """Return the parsed bundled land-allocation query.

    Returns:
        The first :class:`gcamreader.Query` parsed from the bundled file.
    """
    return gcamreader.parse_batch_query(str(land_query_path))[0]


@pytest.fixture
def simple_query() -> gcamreader.Query:
    """Return a small :class:`gcamreader.Query` built from an XML string.

    Returns:
        A query with a title and a single region.
    """
    xml = (
        '<supplyDemandQuery title="CO2 emissions">'
        '<region name="USA"/>'
        "</supplyDemandQuery>"
    )
    return gcamreader.Query(xml)


@pytest.fixture
def output_dir(tmp_path: Path) -> Path:
    """Return a temporary directory for CLI/output tests.

    Returns:
        A writable temporary directory path.
    """
    out = tmp_path / "outputs"
    out.mkdir()
    return out
