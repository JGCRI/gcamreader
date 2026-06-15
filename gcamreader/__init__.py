"""Tools for handling GCAM output databases.

``gcamreader`` provides functions and classes for reading data from the XML
output databases produced by GCAM, returning results as pandas DataFrames.
"""

from gcamreader.querymi import (
    LocalDBConn,
    Query,
    RemoteDBConn,
    importdata,
    parse_batch_query,
)

__version__ = "1.5.0"

__all__ = [
    "Query",
    "parse_batch_query",
    "LocalDBConn",
    "RemoteDBConn",
    "importdata",
    "__version__",
]
