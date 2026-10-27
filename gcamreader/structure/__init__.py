"""Structural comparison of GCAM output databases.

This subpackage compares the *structure* of two or more local GCAM BaseX
output databases (regions, sectors, technologies, inputs, years and other
components, together with their metadata attributes) while ignoring numeric
values. It backs the ``gcamreader diff`` and ``gcamreader scenarios`` commands
and the matching public Python API.

Modules import from :mod:`gcamreader.querymi` rather than :mod:`gcamreader` to
avoid circular imports.
"""

from __future__ import annotations

import logging

from gcamreader.structure.errors import (
    DatabaseAccessError,
    ExtractionError,
    MissingDependencyError,
    RulesError,
    ScenarioSelectionError,
    StructureError,
    StructureIndexError,
)

# Library code must not configure logging; the CLI attaches handlers.
logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    "StructureError",
    "MissingDependencyError",
    "DatabaseAccessError",
    "ScenarioSelectionError",
    "ExtractionError",
    "RulesError",
    "StructureIndexError",
]
