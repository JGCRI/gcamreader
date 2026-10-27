"""Exception hierarchy for :mod:`gcamreader.structure`.

All errors raised by the structural comparison code derive from
:class:`StructureError`, so callers can catch one type at the API or CLI
boundary. Messages are written for end users and name the remedy where one
exists.
"""

from __future__ import annotations


class StructureError(Exception):
    """Base class for all structural-comparison errors."""


class MissingDependencyError(StructureError):
    """A required external component (normally a Java runtime) is unusable."""


class DatabaseAccessError(StructureError):
    """A BaseX database could not be opened or queried."""


class ScenarioSelectionError(StructureError):
    """A scenario could not be selected unambiguously from a database."""


class ExtractionError(StructureError):
    """Structure extraction did not complete; no index was produced."""


class RulesError(StructureError):
    """A comparison-rules file or override is invalid."""


class StructureIndexError(StructureError):
    """A structure index file is missing, incomplete, or incompatible."""
