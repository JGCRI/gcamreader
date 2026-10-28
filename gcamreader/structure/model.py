"""Data model for structural comparison results.

Every class is an immutable dataclass with a ``to_dict`` method returning
JSON-safe values (strings, integers, booleans, lists, dictionaries and
``None``), so a :class:`ComparisonResult` can be written with ``json.dumps``
unchanged. Paths are carried both as formatted strings (for reports) and as
:class:`PathSegment` tuples (for API users, who should never parse strings).
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

Pairs = tuple[tuple[str, str], ...]


class FindingKind(StrEnum):
    """The kinds of structural difference a comparison can report."""

    ELEMENT_ADDED = "element_added"
    ELEMENT_REMOVED = "element_removed"
    ATTRIBUTE_CHANGED = "attribute_changed"
    COUNT_CHANGED = "count_changed"
    METADATA_VARIANTS_CHANGED = "metadata_variants_changed"


# -- paths -------------------------------------------------------------------

_NEEDS_QUOTE = re.compile(r'[\],="]|^\s|\s$')


def format_value(value: str) -> str:
    """Format an identity value for a path.

    Values are written bare unless they contain ``]``, ``,``, ``=``, a double
    quote, or leading/trailing whitespace, or are empty; then they are
    double-quoted with backslash escapes for ``"`` and ``\\``.
    """
    if value == "" or _NEEDS_QUOTE.search(value):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    return value


@dataclass(frozen=True)
class PathSegment:
    """One element on a path: its tag and identity attributes.

    Attributes:
        tag: Element tag. Namespaced tags use Clark notation, ``{uri}local``,
            as produced by lxml.
        identity: Identity attribute pairs in the configured order.
    """

    tag: str
    identity: Pairs = ()

    def format(self) -> str:
        """Return ``tag`` or ``tag[attr=value, ...]``."""
        if not self.identity:
            return self.tag
        inner = ", ".join(f"{k}={format_value(v)}" for k, v in self.identity)
        return f"{self.tag}[{inner}]"

    def to_dict(self) -> dict[str, Any]:
        return {"tag": self.tag, "identity": [list(pair) for pair in self.identity]}


def format_path(segments: Iterable[PathSegment]) -> str:
    """Join segments as ``scenario > world > region[name=USA]``."""
    return " > ".join(segment.format() for segment in segments)


# -- inputs ------------------------------------------------------------------


@dataclass(frozen=True)
class ScenarioRef:
    """A scenario run stored in a database.

    Attributes:
        name: The ``scenario/@name`` value.
        date: The ``scenario/@date`` value exactly as stored.
        version: GCAM version tag (``model-version`` text), or ``None``.
        element_count: Number of elements in the scenario document when
            known, used as a completeness check during extraction.
    """

    name: str
    date: str
    version: str | None = None
    element_count: int | None = None

    @property
    def label(self) -> str:
        """``name@date``, the selector form accepted on the command line."""
        return f"{self.name}@{self.date}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "date": self.date,
            "version": self.version,
            "element_count": self.element_count,
            "label": self.label,
        }


@dataclass(frozen=True)
class InputIdentity:
    """One compared database and the scenario selected from it.

    Attributes:
        index: 1-based position among the inputs, as shown in reports.
        path: Database directory as given by the user.
        label: Short label for tables, normally the directory name.
        scenario: The selected scenario.
        fingerprint: Digest of the database files proving the input was
            unchanged (see ``extract.fingerprint_database``).
    """

    index: int
    path: str
    label: str
    scenario: ScenarioRef
    fingerprint: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "path": self.path,
            "label": self.label,
            "scenario": self.scenario.to_dict(),
            "fingerprint": self.fingerprint,
        }


# -- findings ----------------------------------------------------------------


@dataclass(frozen=True)
class AttributeChange:
    """A metadata attribute that differs between baseline and candidate.

    ``None`` on one side means the attribute is absent there.
    """

    attribute: str
    baseline: str | None
    candidate: str | None

    @property
    def change(self) -> str:
        """``added``, ``removed`` or ``changed``."""
        if self.baseline is None:
            return "added"
        if self.candidate is None:
            return "removed"
        return "changed"

    def to_dict(self) -> dict[str, Any]:
        return {
            "attribute": self.attribute,
            "baseline": self.baseline,
            "candidate": self.candidate,
            "change": self.change,
        }


@dataclass(frozen=True)
class NodeSnapshot:
    """What one side knows about a path.

    Attributes:
        count: Number of occurrences of the path (duplicate siblings add up).
        metadata: Metadata pairs when the path has a single metadata variant,
            otherwise ``None``.
        subtree_size: Elements in the subtree(s), including the node(s).
        variants: ``(metadata, count)`` per distinct metadata variant. Has one
            entry when ``metadata`` is set.
    """

    count: int
    metadata: Pairs | None
    subtree_size: int
    variants: tuple[tuple[Pairs, int], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "metadata": None if self.metadata is None else dict(self.metadata),
            "subtree_size": self.subtree_size,
            "variants": [
                {"metadata": dict(meta), "count": count}
                for meta, count in self.variants
            ],
        }


@dataclass(frozen=True)
class Finding:
    """One structural difference at one path.

    Attributes:
        kind: The kind of difference.
        path: Formatted path (see :func:`format_path`).
        segments: The same path as data.
        tag: Tag of the differing element.
        identity: Identity pairs of the differing element.
        baseline: Snapshot on the baseline side, ``None`` if absent there.
        candidate: Snapshot on the candidate side, ``None`` if absent there.
        attribute_changes: Per-attribute detail for ``attribute_changed``.
        descendant_count: Elements below an added or removed element.
        descendant_tags: ``(tag, count)`` histogram of those descendants,
            sorted by tag.
        detail: Free-text explanation shown in reports.
    """

    kind: FindingKind
    path: str
    segments: tuple[PathSegment, ...]
    tag: str
    identity: Pairs = ()
    baseline: NodeSnapshot | None = None
    candidate: NodeSnapshot | None = None
    attribute_changes: tuple[AttributeChange, ...] = ()
    descendant_count: int = 0
    descendant_tags: tuple[tuple[str, int], ...] = ()
    detail: str = ""

    def branch(self, skip: Sequence[str] = ("scenario", "world")) -> str:
        """Return the formatted segment used to group findings in reports.

        The first segment whose tag is not in ``skip`` (by default the first
        below ``scenario > world``, e.g. ``region[name=USA]`` or
        ``Marketplace``); the root segment if there is none.
        """
        for segment in self.segments:
            if segment.tag not in skip:
                return segment.format()
        return self.segments[0].format() if self.segments else ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind.value,
            "path": self.path,
            "segments": [segment.to_dict() for segment in self.segments],
            "tag": self.tag,
            "identity": [list(pair) for pair in self.identity],
            "baseline": None if self.baseline is None else self.baseline.to_dict(),
            "candidate": None if self.candidate is None else self.candidate.to_dict(),
            "attribute_changes": [
                change.to_dict() for change in self.attribute_changes
            ],
            "descendant_count": self.descendant_count,
            "descendant_tags": [list(pair) for pair in self.descendant_tags],
            "detail": self.detail,
        }


@dataclass(frozen=True)
class ComparisonSummary:
    """Finding counts for one pairwise comparison.

    Attributes:
        counts: Findings per kind, keyed by :attr:`FindingKind.value`; every
            kind is present, possibly with zero.
        total: All findings.
        descendants_summarized: Elements below added or removed elements that
            are reported only as a summary.
    """

    counts: Mapping[str, int]
    total: int
    descendants_summarized: int

    @classmethod
    def from_findings(cls, findings: Iterable[Finding]) -> ComparisonSummary:
        counts = {kind.value: 0 for kind in FindingKind}
        total = 0
        descendants = 0
        for finding in findings:
            counts[finding.kind.value] += 1
            total += 1
            descendants += finding.descendant_count
        return cls(counts=counts, total=total, descendants_summarized=descendants)

    def to_dict(self) -> dict[str, Any]:
        return {
            "counts": dict(self.counts),
            "total": self.total,
            "descendants_summarized": self.descendants_summarized,
        }


@dataclass(frozen=True)
class PairComparison:
    """Findings of one candidate against the baseline."""

    baseline: InputIdentity
    candidate: InputIdentity
    findings: tuple[Finding, ...]
    summary: ComparisonSummary
    warnings: tuple[str, ...] = ()

    @classmethod
    def build(
        cls,
        baseline: InputIdentity,
        candidate: InputIdentity,
        findings: Iterable[Finding],
        warnings: Iterable[str] = (),
    ) -> PairComparison:
        """Create a comparison, deriving the summary from the findings."""
        findings = tuple(findings)
        return cls(
            baseline=baseline,
            candidate=candidate,
            findings=findings,
            summary=ComparisonSummary.from_findings(findings),
            warnings=tuple(warnings),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "baseline": self.baseline.index,
            "candidate": self.candidate.index,
            "findings": [finding.to_dict() for finding in self.findings],
            "summary": self.summary.to_dict(),
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True)
class MatrixRow:
    """One row of the input-by-difference matrix.

    Attributes:
        path: Formatted path of the differing element.
        kind: Kind of difference.
        attribute: Attribute name for attribute-level rows, else ``None``.
        cells: One entry per input (baseline first): the value or marker
            shown for that input, ``None`` when the path is absent there.
    """

    path: str
    kind: FindingKind
    attribute: str | None
    cells: tuple[str | None, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "kind": self.kind.value,
            "attribute": self.attribute,
            "cells": list(self.cells),
        }


@dataclass(frozen=True)
class ComparisonResult:
    """Complete result of comparing two or more databases.

    Attributes:
        package_version: ``gcamreader.__version__`` that produced the result.
        inputs: All inputs in report order; ``inputs[0]`` is the baseline.
        baseline: Index (1-based) of the baseline input.
        comparisons: One entry per candidate, in input order.
        matrix: Union of differences across all comparisons.
        rules: ``ComparisonRules.to_dict()`` of the rules used.
        warnings: Run-level warnings (not tied to one comparison).
    """

    package_version: str
    inputs: tuple[InputIdentity, ...]
    baseline: int
    comparisons: tuple[PairComparison, ...]
    matrix: tuple[MatrixRow, ...]
    rules: Mapping[str, Any] = field(default_factory=dict)
    warnings: tuple[str, ...] = ()

    @property
    def has_differences(self) -> bool:
        """Whether any comparison produced at least one finding."""
        return any(comparison.findings for comparison in self.comparisons)

    @property
    def all_warnings(self) -> tuple[str, ...]:
        """Run-level warnings followed by each comparison's warnings."""
        collected = list(self.warnings)
        for comparison in self.comparisons:
            collected.extend(comparison.warnings)
        return tuple(collected)

    def to_dict(self) -> dict[str, Any]:
        return {
            "package_version": self.package_version,
            "inputs": [item.to_dict() for item in self.inputs],
            "baseline": self.baseline,
            "comparisons": [comparison.to_dict() for comparison in self.comparisons],
            "matrix": [row.to_dict() for row in self.matrix],
            "rules": dict(self.rules),
            "warnings": list(self.warnings),
            "has_differences": self.has_differences,
        }
