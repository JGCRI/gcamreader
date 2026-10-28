"""Build a :class:`~gcamreader.structure.index.StructureIndex` from scenario XML.

:func:`build_index_from_stream` consumes any binary file-like object holding
one serialized ``<scenario>`` document and writes the structure index without
ever holding the document in memory. The BaseX subprocess that produces the
stream for a real database is added by ``extract_structure`` (Java path).

The traversal keeps an explicit frame stack alongside ``lxml.iterparse``:

* On ``start`` the element's attributes are split into identity and metadata
  using the rules' precomputed tables. If the parent has no position hash yet
  it just became a non-leaf, so its hash is computed now
  (``blake2b(grandparent_hash + parent_key)``). Leaves never get one.
* On ``end`` the element is folded into its parent's children accumulator
  (count, metadata variants, subtree size, fingerprint). When a non-leaf
  element closes, its children are complete and are written as rows.

Per-element cost is what bounds runtime on 50 M element documents (measured
at 2.4 µs for parsing alone), so the loop body is deliberately flat: no
helper calls per element, interning and row buffering are inlined, and the
metadata sort is skipped for the common single-attribute case.
"""

from __future__ import annotations

import hashlib
import logging
import struct
from collections.abc import Callable, Mapping
from typing import Any, BinaryIO

from lxml import etree

from gcamreader.structure.errors import ExtractionError
from gcamreader.structure.index import (
    ROOT_PARENT_HASH,
    SEP,
    IndexWriter,
    StructureIndex,
)
from gcamreader.structure.rules import ComparisonRules

logger = logging.getLogger(__name__)

#: Bump when the traversal changes what rows or fingerprints mean.
EXTRACTOR_VERSION = 1

#: Elements between progress callbacks.
PROGRESS_INTERVAL = 1_000_000

#: Separates identity from metadata in leaf fingerprint input; like SEP it
#: cannot appear in well-formed XML 1.0.
_FIELD_SEP = "\x1e"

_MASK = 0xFFFFFFFF
_unpack_lanes = struct.Struct("<4I").unpack

ProgressCallback = Callable[[str], None]


def _package_version() -> str:
    # Imported lazily: gcamreader/__init__.py imports this package.
    from gcamreader import __version__

    return __version__


def build_index_from_stream(
    stream: BinaryIO,
    index_path: str,
    rules: ComparisonRules,
    *,
    source: str,
    expected_elements: int | None = None,
    progress: ProgressCallback | None = None,
    info: Mapping[str, Any] | None = None,
) -> StructureIndex:
    """Stream one scenario document into a new structure index.

    Args:
        stream: Binary stream of exactly one XML document.
        index_path: Where to write the SQLite index. Replaced if it exists.
        rules: Identity, metadata and exclusion rules.
        source: Label of the stream for messages (database path or file).
        expected_elements: Element count the document must have; when given,
            a mismatch is an error. Elements inside excluded subtrees count.
        progress: Called with a short message every
            :data:`PROGRESS_INTERVAL` elements.
        info: Extra ``meta_info`` entries (scenario, database fingerprint).

    Returns:
        The completed index, opened read-only.

    Raises:
        ExtractionError: If the XML is not well-formed or truncated, has no
            or several document elements, or its element count differs from
            ``expected_elements``. The partial index file is deleted first.
    """
    writer = IndexWriter(index_path)
    writer.begin()
    try:
        stats = _traverse(stream, writer, rules, source, expected_elements, progress)
        meta: dict[str, Any] = dict(info or {})
        meta.update(
            source=source,
            element_count=stats["element_count"],
            root_tag=stats["root_tag"],
            rules=rules.to_dict(),
            rules_fingerprint=rules.fingerprint(),
            gcamreader_version=_package_version(),
            extractor_version=EXTRACTOR_VERSION,
            warnings=stats["warnings"],
        )
        writer.finish(meta)
    except BaseException:
        writer.abort()
        raise
    return StructureIndex.open(index_path)


def _traverse(
    stream: BinaryIO,
    writer: IndexWriter,
    rules: ComparisonRules,
    source: str,
    expected_elements: int | None,
    progress: ProgressCallback | None,
) -> dict[str, Any]:
    classifier = rules.classifier()
    default_identity = classifier.default_identity
    identity_by_tag = dict(classifier.identity_by_tag)
    ignored = classifier.ignored
    excluded_by_path = dict(classifier.excluded_by_path)
    excluded_elements = classifier.excluded_elements

    strings = writer.strings
    pending = writer.pending
    pending_append = pending.append
    batch_size = writer.batch_size
    flush = writer.flush
    blake = hashlib.blake2b
    unpack = _unpack_lanes
    sep = SEP
    field_sep = _FIELD_SEP
    mask = _MASK

    # Frame: [path_hash | None, children, key_s, meta_s, tag, ident_s, tag_path]
    # children: key_s -> [path_hash | None, tag, ident_s, variants, occurrences]
    # variants: meta_s -> [count, subtree_size, f0, f1, f2, f3]
    root_frame: list[Any] = [ROOT_PARENT_HASH, {}, "", "", "", "", ""]
    stack: list[list[Any]] = [root_frame]
    depth = 0  # of the innermost open element; the document element is 1
    skip_depth = 0
    elements_seen = 0
    roots_seen = 0
    root_tag = ""
    next_progress = PROGRESS_INTERVAL if progress is not None else None
    duplicates: dict[str, int] = {}

    def emit(
        parent_hash: bytes, children: dict[str, Any], child_depth: int
    ) -> tuple[int, int, int, int, int]:
        """Write rows for a closed parent's children; return size and lane sums."""
        size = 0
        f0 = f1 = f2 = f3 = 0
        for child_hash, tag, ident_s, variants, _ in children.values():
            tag_id = strings.get(tag)
            if tag_id is None:
                tag_id = strings[tag] = len(strings) + 1
            ident_id = strings.get(ident_s)
            if ident_id is None:
                ident_id = strings[ident_s] = len(strings) + 1
            for meta_s, (count, vsize, v0, v1, v2, v3) in variants.items():
                meta_id = strings.get(meta_s)
                if meta_id is None:
                    meta_id = strings[meta_s] = len(strings) + 1
                pending_append(
                    (
                        parent_hash,
                        tag_id,
                        ident_id,
                        meta_id,
                        child_hash,
                        child_depth,
                        count,
                        vsize,
                        v0,
                        v1,
                        v2,
                        v3,
                    )
                )
                size += vsize
                f0 += v0
                f1 += v1
                f2 += v2
                f3 += v3
        if len(pending) >= batch_size:
            flush()
        return size, f0 & mask, f1 & mask, f2 & mask, f3 & mask

    parser = etree.iterparse(
        stream,
        events=("start", "end"),
        remove_comments=True,
        remove_pis=True,
        resolve_entities=False,
        no_network=True,
        huge_tree=True,
    )
    try:
        for event, el in parser:
            if event == "start":
                elements_seen += 1
                if skip_depth:
                    skip_depth += 1
                    continue
                tag = el.tag
                parent = stack[-1]
                tag_path = parent[6] + "/" + tag
                if excluded_elements and tag_path in excluded_elements:
                    skip_depth = 1
                    continue
                if depth == 0:
                    roots_seen += 1
                    if roots_seen > 1:
                        raise ExtractionError(
                            f"{source}: more than one document element "
                            f"(second is <{tag}>)"
                        )
                    root_tag = tag
                attrib = el.attrib
                if attrib:
                    order = identity_by_tag.get(tag, default_identity)
                    dropped = ignored
                    excluded = excluded_by_path.get(tag_path)
                    if excluded:
                        dropped = dropped | excluded
                    ident_parts = []
                    meta_pairs = []
                    for k, v in attrib.items():
                        if k in dropped:
                            continue
                        if k in order:
                            continue
                        meta_pairs.append((k, v))
                    for k in order:
                        if k in attrib and k not in dropped:
                            ident_parts.append(k)
                            ident_parts.append(attrib[k])
                    ident_s = sep.join(ident_parts)
                    if len(meta_pairs) > 1:
                        meta_pairs.sort()
                    meta_parts = []
                    for k, v in meta_pairs:
                        meta_parts.append(k)
                        meta_parts.append(v)
                    meta_s = sep.join(meta_parts)
                else:
                    ident_s = ""
                    meta_s = ""
                if parent[0] is None:
                    # The parent just became a non-leaf: give it a position hash.
                    parent[0] = blake(
                        stack[-2][0] + parent[2].encode(), digest_size=16
                    ).digest()
                stack.append(
                    [None, {}, tag + sep + ident_s, meta_s, tag, ident_s, tag_path]
                )
                depth += 1
                if next_progress is not None and elements_seen >= next_progress:
                    next_progress += PROGRESS_INTERVAL
                    _report(progress, source, elements_seen, expected_elements)
            else:
                if skip_depth:
                    skip_depth -= 1
                    el.clear()
                    continue
                path_hash, children, key_s, meta_s, tag, ident_s, tag_path = stack.pop()
                if path_hash is None:
                    f0, f1, f2, f3 = unpack(
                        blake(
                            (key_s + field_sep + meta_s).encode(), digest_size=16
                        ).digest()
                    )
                    size = 1
                else:
                    f0, f1, f2, f3 = unpack(
                        blake(path_hash + meta_s.encode(), digest_size=16).digest()
                    )
                    csize, c0, c1, c2, c3 = emit(path_hash, children, depth + 1)
                    size = 1 + csize
                    f0 = (f0 + c0) & mask
                    f1 = (f1 + c1) & mask
                    f2 = (f2 + c2) & mask
                    f3 = (f3 + c3) & mask
                depth -= 1
                pchildren = stack[-1][1]
                acc = pchildren.get(key_s)
                if acc is None:
                    pchildren[key_s] = [
                        path_hash,
                        tag,
                        ident_s,
                        {meta_s: [1, size, f0, f1, f2, f3]},
                        1,
                    ]
                else:
                    if path_hash is not None or acc[0] is not None:
                        # Repeated sibling with children: their subtrees merge.
                        duplicates[tag_path] = duplicates.get(tag_path, 0) + 1
                        if acc[0] is None:
                            acc[0] = path_hash
                    acc[4] += 1
                    variant = acc[3].get(meta_s)
                    if variant is None:
                        acc[3][meta_s] = [1, size, f0, f1, f2, f3]
                    else:
                        variant[0] += 1
                        variant[1] += size
                        variant[2] = (variant[2] + f0) & mask
                        variant[3] = (variant[3] + f1) & mask
                        variant[4] = (variant[4] + f2) & mask
                        variant[5] = (variant[5] + f3) & mask
                el.clear()
    except etree.XMLSyntaxError as exc:
        raise ExtractionError(
            f"{source}: XML is not well-formed or was truncated after "
            f"{elements_seen:,} elements: {exc}"
        ) from exc

    if roots_seen == 0 or len(stack) != 1:
        raise ExtractionError(f"{source}: no complete document element was read")
    emit(ROOT_PARENT_HASH, root_frame[1], 1)
    if expected_elements is not None and elements_seen != expected_elements:
        raise ExtractionError(
            f"{source}: read {elements_seen:,} elements but the database reports "
            f"{expected_elements:,}; the extraction is incomplete"
        )
    if progress is not None:
        _report(progress, source, elements_seen, expected_elements, final=True)

    warnings = [
        f"{count} repeated sibling element(s) with children at {tag_path}: their "
        "subtrees are merged, so counts below are totals across the repeats"
        for tag_path, count in sorted(duplicates.items())
    ]
    for warning in warnings:
        logger.warning("%s: %s", source, warning)
    return {"element_count": elements_seen, "root_tag": root_tag, "warnings": warnings}


def _report(
    progress: ProgressCallback | None,
    source: str,
    seen: int,
    expected: int | None,
    *,
    final: bool = False,
) -> None:
    if progress is None:
        return
    message = f"{source}: {seen:,} elements"
    if expected:
        message += f" ({100.0 * seen / expected:.0f}%)"
    if final:
        message += ", done"
    progress(message)
