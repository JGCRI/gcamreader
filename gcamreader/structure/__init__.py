"""Structural comparison of GCAM output databases.

This subpackage compares the *structure* of two or more local GCAM BaseX
output databases (regions, sectors, technologies, inputs, years and other
components, together with their metadata attributes) while ignoring numeric
values. It backs the ``gcamreader diff`` and ``gcamreader scenarios`` commands
and the matching public Python API::

    import gcamreader

    result = gcamreader.compare_databases(["db_ref", "db_policy"])
    print(gcamreader.render_markdown(result))

Modules import from :mod:`gcamreader.querymi` rather than :mod:`gcamreader` to
avoid circular imports.
"""

from __future__ import annotations

import logging

from gcamreader.structure.api import (
    compare_databases,
    iter_json,
    list_scenarios,
    render_markdown,
    write_json,
    write_report,
)
from gcamreader.structure.errors import (
    DatabaseAccessError,
    ExtractionError,
    MissingDependencyError,
    RulesError,
    ScenarioSelectionError,
    StructureError,
    StructureIndexError,
)
from gcamreader.structure.model import (
    AttributeChange,
    ComparisonResult,
    ComparisonSummary,
    Finding,
    FindingKind,
    InputIdentity,
    MatrixRow,
    NodeSnapshot,
    PairComparison,
    PathSegment,
    ScenarioRef,
)
from gcamreader.structure.rules import ComparisonRules

# Library code must not configure logging; the CLI attaches handlers.
logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    "compare_databases",
    "list_scenarios",
    "render_markdown",
    "write_report",
    "write_json",
    "iter_json",
    "ComparisonRules",
    "ComparisonResult",
    "ComparisonSummary",
    "PairComparison",
    "Finding",
    "FindingKind",
    "AttributeChange",
    "NodeSnapshot",
    "MatrixRow",
    "PathSegment",
    "InputIdentity",
    "ScenarioRef",
    "StructureError",
    "MissingDependencyError",
    "DatabaseAccessError",
    "ScenarioSelectionError",
    "ExtractionError",
    "RulesError",
    "StructureIndexError",
]
