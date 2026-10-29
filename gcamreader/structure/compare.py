"""Compare two structure indexes and build the difference matrix.

:func:`compare_indexes` walks two :class:`~gcamreader.structure.index.StructureIndex`
files top-down from the document element. At each position the children of
both sides are grouped by ``(tag, identity)`` and classified:

* present on one side only: ``element_removed`` / ``element_added``, with a
  descendant summary instead of one finding per descendant;
* present on both sides with identical metadata variants and an identical
  subtree fingerprint: skipped, together with the whole subtree;
* otherwise ``count_changed``, ``attribute_changed`` or
  ``metadata_variants_changed`` as appropriate, then the walk descends while
  fingerprints differ.

Children come from the index sorted by tag and identity and the walk is
depth-first, so the finding order is deterministic. Moves and renames appear
as a removal plus an addition.

:func:`build_matrix` merges the findings of several comparisons against one
baseline into rows with one cell per input.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, replace

from gcamreader.structure.errors import StructureIndexError
from gcamreader.structure.index import ROOT_PARENT_HASH, NodeRow, StructureIndex
from gcamreader.structure.model import (
    AttributeChange,
    Finding,
    FindingKind,
    InputIdentity,
    MatrixRow,
    NodeSnapshot,
    PairComparison,
    PathSegment,
    format_path,
    summarize_identities,
)

logger = logging.getLogger(__name__)

Pairs = tuple[tuple[str, str], ...]
Variants = tuple[tuple[Pairs, int], ...]
_MASK = 0xFFFFFFFF

#: Added/removed elements with fewer descendants than this get a descendant
#: count but no tag histogram. Measured on the dev databases: 534 k one-sided
#: non-leaf elements, mostly technologies with ~5 descendants, cost 12 min of
#: histogram queries; the threshold keeps the summaries for removed sectors and
#: branches while making the phase negligible.
HISTOGRAM_MIN_DESCENDANTS = 50


@dataclass(frozen=True)
class _Node:
    """All rows of one ``(tag, identity)`` under one parent, merged."""

    tag: str
    identity: Pairs
    path_hash: bytes | None
    variants: Variants
    count: int
    subtree_size: int
    fingerprint: tuple[int, int, int, int]

    @property
    def metadata(self) -> Pairs | None:
        return self.variants[0][0] if len(self.variants) == 1 else None

    @property
    def metadata_set(self) -> frozenset[Pairs]:
        return frozenset(meta for meta, _ in self.variants)

    def snapshot(self) -> NodeSnapshot:
        return NodeSnapshot(
            count=self.count,
            metadata=self.metadata,
            subtree_size=self.subtree_size,
            variants=self.variants,
        )


def _group(rows: list[NodeRow]) -> dict[tuple[str, Pairs], _Node]:
    """Merge metadata-variant rows into one node per ``(tag, identity)``.

    ``rows`` must be sorted by tag, identity and metadata, as
    :meth:`StructureIndex.children` returns them.
    """
    nodes: dict[tuple[str, Pairs], _Node] = {}
    index = 0
    total = len(rows)
    while index < total:
        first = rows[index]
        key = (first.tag, first.identity)
        variants: list[tuple[Pairs, int]] = []
        count = size = 0
        f0 = f1 = f2 = f3 = 0
        path_hash = None
        while (
            index < total
            and rows[index].tag == key[0]
            and rows[index].identity == key[1]
        ):
            row = rows[index]
            variants.append((row.metadata, row.count))
            count += row.count
            size += row.subtree_size
            f0 += row.fingerprint[0]
            f1 += row.fingerprint[1]
            f2 += row.fingerprint[2]
            f3 += row.fingerprint[3]
            if path_hash is None:
                path_hash = row.path_hash
            index += 1
        nodes[key] = _Node(
            tag=key[0],
            identity=key[1],
            path_hash=path_hash,
            variants=tuple(variants),
            count=count,
            subtree_size=size,
            fingerprint=(f0 & _MASK, f1 & _MASK, f2 & _MASK, f3 & _MASK),
        )
    return nodes


def format_variants(variants: Variants) -> str:
    """``unit=EJ (x2); unit=PJ (x1)`` for report text and matrix cells."""
    parts = []
    for metadata, count in variants:
        attrs = ", ".join(f"{k}={v}" for k, v in metadata) or "no attributes"
        parts.append(f"{attrs} (x{count})")
    return "; ".join(parts)


def _attribute_changes(before: Pairs, after: Pairs) -> tuple[AttributeChange, ...]:
    left = dict(before)
    right = dict(after)
    changes = []
    for name in sorted(set(left) | set(right)):
        if left.get(name) != right.get(name):
            changes.append(AttributeChange(name, left.get(name), right.get(name)))
    return tuple(changes)


def check_compatible(baseline: StructureIndex, candidate: StructureIndex) -> None:
    """Refuse to compare indexes built with different rules or extractor.

    Raises:
        StructureIndexError: On a mismatch.
    """
    for key, what in (
        ("rules_fingerprint", "comparison rules"),
        ("extractor_version", "extractor version"),
        ("schema_version", "index schema"),
    ):
        if baseline.info.get(key) != candidate.info.get(key):
            raise StructureIndexError(
                f"Cannot compare {baseline.path} with {candidate.path}: they were "
                f"built with different {what}. Re-extract both with the same "
                "gcamreader version and rules."
            )


class _Walker:
    def __init__(self, baseline: StructureIndex, candidate: StructureIndex) -> None:
        self.baseline = baseline
        self.candidate = candidate
        self.findings: list[Finding] = []
        # (finding position, member position hashes needing a histogram)
        self.pending_removed: list[tuple[int, list[bytes]]] = []
        self.pending_added: list[tuple[int, list[bytes]]] = []
        self.positions = 0
        self.pruned = 0

    def run(self) -> None:
        stack: list[tuple[bytes, tuple[PathSegment, ...]]] = [(ROOT_PARENT_HASH, ())]
        while stack:
            parent_hash, ancestors = stack.pop()
            self.positions += 1
            left = _group(self.baseline.children(parent_hash))
            right = _group(self.candidate.children(parent_hash))
            descend: list[tuple[bytes, tuple[PathSegment, ...]]] = []
            one_sided: dict[tuple[FindingKind, str], list[_Node]] = {}
            for key in sorted(set(left) | set(right)):
                node_l = left.get(key)
                node_r = right.get(key)
                if node_r is None:
                    assert node_l is not None
                    one_sided.setdefault(
                        (FindingKind.ELEMENT_REMOVED, key[0]), []
                    ).append(node_l)
                    continue
                if node_l is None:
                    one_sided.setdefault(
                        (FindingKind.ELEMENT_ADDED, key[0]), []
                    ).append(node_r)
                    continue
                if (
                    node_l.variants == node_r.variants
                    and node_l.fingerprint == node_r.fingerprint
                ):
                    self.pruned += 1
                    continue
                segments = (*ancestors, PathSegment(key[0], key[1]))
                self._compare_node(node_l, node_r, segments)
                if node_l.fingerprint != node_r.fingerprint:
                    path_hash = node_l.path_hash or node_r.path_hash
                    if path_hash is not None:
                        descend.append((path_hash, segments))
            for kind, _tag in sorted(one_sided):
                self._one_sided(kind, one_sided[(kind, _tag)], ancestors)
            stack.extend(reversed(descend))

    def _one_sided(
        self, kind: FindingKind, nodes: list[_Node], ancestors: tuple[PathSegment, ...]
    ) -> None:
        """Report siblings present on one side only; several share one finding."""
        removed = kind is FindingKind.ELEMENT_REMOVED
        tag = nodes[0].tag
        count = sum(node.count for node in nodes)
        subtree_size = sum(node.subtree_size for node in nodes)
        descendants = subtree_size - count
        seeds = [
            node.path_hash
            for node in nodes
            if node.path_hash is not None
            and node.subtree_size - node.count >= HISTOGRAM_MIN_DESCENDANTS
        ]
        if seeds:
            # Histograms are filled in afterwards, in batches (see fill_histograms).
            pending = self.pending_removed if removed else self.pending_added
            pending.append((len(self.findings), seeds))
        verb = "Removed" if removed else "Added"
        if len(nodes) == 1:
            node = nodes[0]
            segments = (*ancestors, PathSegment(tag, node.identity))
            path = format_path(segments)
            identity = node.identity
            snapshot = node.snapshot()
            occurrences = f"{count} occurrence(s), " if count != 1 else ""
            detail = f"{verb}: {occurrences}{descendants} descendant element(s)"
        else:
            identities = tuple(node.identity for node in nodes)
            segments = (*ancestors, PathSegment(tag, ()))
            summary = f"{tag}[{summarize_identities(identities)}]"
            path = f"{format_path(ancestors)} > {summary}" if ancestors else summary
            identity = ()
            snapshot = NodeSnapshot(
                count=count, metadata=None, subtree_size=subtree_size, variants=()
            )
            listed = ", ".join(
                PathSegment(tag, node.identity).format() for node in nodes
            )
            detail = (
                f"{verb} {len(nodes)} {tag} elements ({count} occurrence(s), "
                f"{descendants} descendant element(s)): {listed}"
            )
        self.findings.append(
            Finding(
                kind=kind,
                path=path,
                segments=segments,
                tag=tag,
                identity=identity,
                identities=tuple(node.identity for node in nodes),
                members=len(nodes),
                baseline=snapshot if removed else None,
                candidate=None if removed else snapshot,
                descendant_count=descendants,
                detail=detail,
            )
        )

    def fill_histograms(self) -> None:
        """Attach descendant tag histograms to one-sided findings, batched.

        Only members with at least :data:`HISTOGRAM_MIN_DESCENDANTS`
        descendants are summarised; a grouped finding's histogram is the sum
        over its qualifying members.
        """
        for index, pending in (
            (self.baseline, self.pending_removed),
            (self.candidate, self.pending_added),
        ):
            if not pending:
                continue
            histograms = index.descendant_tag_counts_many(
                [h for _, seeds in pending for h in seeds]
            )
            for position, seeds in pending:
                merged: dict[str, int] = {}
                for seed in seeds:
                    for tag, count in histograms[seed]:
                        merged[tag] = merged.get(tag, 0) + count
                self.findings[position] = replace(
                    self.findings[position],
                    descendant_tags=tuple(sorted(merged.items())),
                )
            pending.clear()

    def _compare_node(
        self, node_l: _Node, node_r: _Node, segments: tuple[PathSegment, ...]
    ) -> None:
        common = dict(
            path=format_path(segments),
            segments=segments,
            tag=node_l.tag,
            identity=node_l.identity,
            baseline=node_l.snapshot(),
            candidate=node_r.snapshot(),
        )
        if node_l.metadata_set == node_r.metadata_set:
            if node_l.variants != node_r.variants:
                self.findings.append(
                    Finding(
                        kind=FindingKind.COUNT_CHANGED,
                        detail=(
                            f"Count changed: {node_l.count} in baseline, "
                            f"{node_r.count} in candidate"
                            + (
                                f" (baseline {format_variants(node_l.variants)}; "
                                f"candidate {format_variants(node_r.variants)})"
                                if len(node_l.variants) > 1
                                else ""
                            )
                        ),
                        **common,
                    )
                )
            return
        if len(node_l.variants) == 1 and len(node_r.variants) == 1:
            changes = _attribute_changes(node_l.variants[0][0], node_r.variants[0][0])
            self.findings.append(
                Finding(
                    kind=FindingKind.ATTRIBUTE_CHANGED,
                    attribute_changes=changes,
                    detail="Attributes changed: "
                    + ", ".join(
                        f"{c.attribute} {c.change} "
                        f"({_show(c.baseline)} -> {_show(c.candidate)})"
                        for c in changes
                    ),
                    **common,
                )
            )
            if node_l.count != node_r.count:
                self.findings.append(
                    Finding(
                        kind=FindingKind.COUNT_CHANGED,
                        detail=(
                            f"Count changed: {node_l.count} in baseline, "
                            f"{node_r.count} in candidate"
                        ),
                        **common,
                    )
                )
            return
        self.findings.append(
            Finding(
                kind=FindingKind.METADATA_VARIANTS_CHANGED,
                detail=(
                    "Metadata variants differ (not collapsed): baseline "
                    f"{format_variants(node_l.variants)}; candidate "
                    f"{format_variants(node_r.variants)}"
                ),
                **common,
            )
        )


def _show(value: str | None) -> str:
    return "absent" if value is None else f"'{value}'"


def compare_indexes(
    baseline: StructureIndex,
    candidate: StructureIndex,
    *,
    baseline_input: InputIdentity,
    candidate_input: InputIdentity,
) -> PairComparison:
    """Compare two structure indexes.

    Args:
        baseline: Index of the baseline database.
        candidate: Index of the database compared against it.
        baseline_input: Identity of the baseline, carried into the result.
        candidate_input: Identity of the candidate.

    Returns:
        The findings in deterministic order with a summary; extraction
        warnings of both indexes are included, prefixed with the input label.

    Raises:
        StructureIndexError: If the indexes were built with different rules,
            extractor or schema versions.
    """
    check_compatible(baseline, candidate)
    walker = _Walker(baseline, candidate)
    walker.run()
    walker.fill_histograms()
    logger.debug(
        "Compared %s with %s: %d positions visited, %d identical subtrees "
        "skipped, %d findings",
        baseline_input.label,
        candidate_input.label,
        walker.positions,
        walker.pruned,
        len(walker.findings),
    )
    warnings = [f"{baseline_input.label}: {w}" for w in baseline.warnings]
    warnings += [f"{candidate_input.label}: {w}" for w in candidate.warnings]
    return PairComparison.build(
        baseline_input, candidate_input, walker.findings, warnings
    )


# -- matrix ------------------------------------------------------------------

MatrixKey = tuple[str, FindingKind, str | None]


def _matrix_cells(finding: Finding) -> Iterator[tuple[MatrixKey, str, str]]:
    """Yield ``(key, baseline_cell, candidate_cell)`` for a finding."""
    kind = finding.kind
    if kind is FindingKind.ELEMENT_REMOVED:
        yield (finding.path, kind, None), "present", "absent"
    elif kind is FindingKind.ELEMENT_ADDED:
        yield (finding.path, kind, None), "absent", "present"
    elif kind is FindingKind.COUNT_CHANGED:
        assert finding.baseline is not None and finding.candidate is not None
        yield (
            (finding.path, kind, None),
            str(finding.baseline.count),
            str(finding.candidate.count),
        )
    elif kind is FindingKind.ATTRIBUTE_CHANGED:
        for change in finding.attribute_changes:
            yield (
                (finding.path, kind, change.attribute),
                "absent" if change.baseline is None else change.baseline,
                "absent" if change.candidate is None else change.candidate,
            )
    else:
        assert finding.baseline is not None and finding.candidate is not None
        yield (
            (finding.path, kind, None),
            format_variants(finding.baseline.variants),
            format_variants(finding.candidate.variants),
        )


def build_matrix(
    inputs: Sequence[InputIdentity], comparisons: Sequence[PairComparison]
) -> tuple[MatrixRow, ...]:
    """Merge findings of several comparisons into one row per difference.

    Rows are keyed by path, kind and (for attribute changes) attribute name,
    and sorted the same way. Each row has one cell per input in ``inputs``
    order. The baseline cell shows the baseline-side value; a candidate cell
    shows the candidate-side value when that comparison reported the
    difference, otherwise the baseline value, because the walk found nothing
    different at that exact path. A difference in an ancestor (for example a
    removed region) is its own row and is not propagated to descendants.

    Args:
        inputs: All inputs, in report order.
        comparisons: Comparisons that all share the same baseline.
    """
    if not comparisons:
        return ()
    baseline_index = comparisons[0].baseline.index
    for comparison in comparisons:
        if comparison.baseline.index != baseline_index:
            raise ValueError("all comparisons must share the same baseline")
    candidate_cells: dict[MatrixKey, dict[int, str]] = {}
    baseline_cells: dict[MatrixKey, str] = {}
    for comparison in comparisons:
        column = comparison.candidate.index
        for finding in comparison.findings:
            for key, base_cell, cand_cell in _matrix_cells(finding):
                baseline_cells.setdefault(key, base_cell)
                candidate_cells.setdefault(key, {})[column] = cand_cell
    rows = []
    for key in sorted(baseline_cells, key=lambda k: (k[0], k[1].value, k[2] or "")):
        base = baseline_cells[key]
        cells = tuple(
            (
                base
                if item.index == baseline_index
                else candidate_cells[key].get(item.index, base)
            )
            for item in inputs
        )
        rows.append(MatrixRow(path=key[0], kind=key[1], attribute=key[2], cells=cells))
    return tuple(rows)
