"""Tools for handling GCAM output databases.

``gcamreader`` provides functions and classes for reading data from the XML
output databases produced by GCAM, returning results as pandas DataFrames,
and for comparing the structure of two or more output databases
(:func:`compare_databases`).
"""

from gcamreader.querymi import (
    LocalDBConn,
    Query,
    RemoteDBConn,
    importdata,
    parse_batch_query,
    sample_data_dir,
)
from gcamreader.structure import (
    ComparisonResult,
    ComparisonRules,
    Finding,
    ScenarioRef,
    StructureError,
    compare_databases,
    list_scenarios,
    render_markdown,
)

__version__ = "1.5.0"

__all__ = [
    "Query",
    "parse_batch_query",
    "LocalDBConn",
    "RemoteDBConn",
    "importdata",
    "sample_data_dir",
    "compare_databases",
    "list_scenarios",
    "render_markdown",
    "ComparisonRules",
    "ComparisonResult",
    "Finding",
    "ScenarioRef",
    "StructureError",
    "__version__",
]
