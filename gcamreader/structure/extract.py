"""Build a :class:`~gcamreader.structure.index.StructureIndex` from scenario XML.

:func:`build_index_from_stream` consumes any binary file-like object holding
one serialized ``<scenario>`` document and writes the structure index without
ever holding the document in memory. :func:`extract_structure` drives the
BaseX subprocess that produces that stream for a real database;
:func:`list_scenarios`, :func:`select_scenario` and
:func:`fingerprint_database` cover discovery.

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
import os
import shutil
import struct
import subprocess
import tempfile
from collections.abc import Callable, Mapping, Sequence
from typing import Any, BinaryIO

from lxml import etree

from gcamreader.querymi import _default_miclasspath
from gcamreader.structure.errors import (
    DatabaseAccessError,
    ExtractionError,
    MissingDependencyError,
    ScenarioSelectionError,
)
from gcamreader.structure.index import (
    ROOT_PARENT_HASH,
    SEP,
    IndexWriter,
    StructureIndex,
)
from gcamreader.structure.java_runtime import check_java, java_failure_error
from gcamreader.structure.model import ScenarioRef
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


# ---------------------------------------------------------------------------
# Java path: database validation, discovery, selection, extraction
# ---------------------------------------------------------------------------

#: Default JVM heap. Step 1 showed 512 MB suffices for streaming a 2.7 GiB
#: scenario; 4g keeps parity with ``LocalDBConn``.
DEFAULT_JAVA_MAX_MEMORY = "4g"

#: Estimated peak disk per element during a build (index plus scratch file),
#: from the step 3 full run: ~7 GB for 48.5 M elements.
BYTES_PER_ELEMENT_ESTIMATE = 150

#: Files every BaseX database directory contains.
REQUIRED_DB_FILES = ("inf.basex", "tbl.basex")

#: Separators in the scenario listing output. XQuery string literals cannot
#: hold characters that are invalid in XML, so control characters are out.
#: Tab and newline are safe: XML attribute-value normalization turns them
#: into spaces, so no stored @name or @date can contain them.
_LIST_RECORD_SEP = "\n"
_LIST_FIELD_SEP = "\t"

# One string: records separated by newline, fields (name, date, version,
# count) by tab. ``{count}`` is substituted with a count expression or ``""``.
_LIST_QUERY_TEMPLATE = (
    "string-join(for $s in collection()/scenario return string-join(("
    "string($s/@name), string($s/@date), string($s/model-version[1]), {count}"
    "), codepoints-to-string(9)), codepoints-to-string(10))"
)
_COUNT_EXPRESSION = "string(count($s/descendant-or-self::*))"

#: Streams exactly one scenario document; name and date are bound externally.
SCENARIO_QUERY = (
    "declare variable $n external; declare variable $d external; "
    "collection()/scenario[@name = $n and @date = $d]"
)


def validate_database_dir(db_path: str) -> tuple[str, str]:
    """Check that ``db_path`` is a local BaseX database directory.

    Returns:
        ``(parent, name)``: the directory BaseX must use as ``DBPATH`` and
        the database name to open.

    Raises:
        DatabaseAccessError: If the path is not a directory or lacks the
            BaseX storage files.
    """
    path = os.path.abspath(db_path)
    if not os.path.isdir(path):
        raise DatabaseAccessError(
            f"{db_path} is not a directory. Structural comparison works on local "
            "BaseX databases (directories containing *.basex files); remote "
            "databases are not supported."
        )
    missing = [
        name
        for name in REQUIRED_DB_FILES
        if not os.path.isfile(os.path.join(path, name))
    ]
    if missing:
        raise DatabaseAccessError(
            f"{db_path} does not look like a BaseX database: missing "
            + ", ".join(missing)
        )
    parent, name = os.path.split(path)
    if not name:
        raise DatabaseAccessError(f"{db_path} has no database name component")
    return parent, name


def fingerprint_database(db_path: str) -> str:
    """Return a sha256 hex digest identifying the database contents cheaply.

    Covers the full content of ``inf.basex`` (BaseX metadata, including the
    modification timestamp) plus the names and sizes of all ``*.basex``
    files. It does not hash the multi-gigabyte table files, so an in-place
    modification that keeps sizes unchanged is not detected; the CLI offers
    ``--no-cache`` for that case.
    """
    path = os.path.abspath(db_path)
    digest = hashlib.sha256()
    with open(os.path.join(path, "inf.basex"), "rb") as handle:
        digest.update(handle.read())
    for name in sorted(os.listdir(path)):
        if name.endswith(".basex"):
            size = os.path.getsize(os.path.join(path, name))
            digest.update(f"{name}\0{size}\0".encode())
    return digest.hexdigest()


def _basex_command(
    java: str,
    miclasspath: str | None,
    java_max_memory: str,
    parent: str,
    db_name: str,
    *serializer_options: str,
    binds: Sequence[tuple[str, str]] = (),
    query: str,
) -> list[str]:
    """Assemble a ``java ... org.basex.BaseX`` command line."""
    return [
        java,
        "-cp",
        miclasspath or _default_miclasspath,
        f"-Xmx{java_max_memory}",
        "-Dfile.encoding=UTF-8",
        "-Djava.awt.headless=true",
        f"-Dorg.basex.DBPATH={parent}",
        "org.basex.BaseX",
        *serializer_options,
        *(f"-b{name}={value}" for name, value in binds),
        "-i",
        db_name,
        query,
    ]


def _run_basex(cmd: list[str], *, context: str) -> str:
    """Run a short BaseX command to completion and return its stdout."""
    try:
        proc = subprocess.run(
            cmd, capture_output=True, encoding="utf-8", errors="replace", check=False
        )
    except OSError as exc:
        raise MissingDependencyError(f"{context}: cannot start Java: {exc}") from exc
    if proc.returncode != 0:
        raise java_failure_error(
            proc.stderr, returncode=proc.returncode, context=context
        )
    return proc.stdout


def list_scenarios(
    db_path: str,
    *,
    with_counts: bool = False,
    java_max_memory: str = DEFAULT_JAVA_MAX_MEMORY,
    miclasspath: str | None = None,
    java: str | None = None,
) -> list[ScenarioRef]:
    """List the scenario runs stored in a local database.

    Args:
        db_path: Database directory.
        with_counts: Also count the elements of each scenario (a full
            in-database traversal, ~5 s for a 3 GB database). Needed before
            :func:`extract_structure` can verify completeness.
        java_max_memory: JVM ``-Xmx`` value.
        miclasspath: Java class path; defaults to the bundled jars.
        java: Explicit ``java`` executable; otherwise resolved by
            :func:`~gcamreader.structure.java_runtime.check_java`.

    Returns:
        Scenarios in database order, possibly empty.

    Raises:
        DatabaseAccessError: If the directory is not a database or BaseX
            fails to read it.
        MissingDependencyError: If no usable Java runtime is found.
    """
    parent, name = validate_database_dir(db_path)
    java_info = check_java(java)
    query = _LIST_QUERY_TEMPLATE.format(
        count=_COUNT_EXPRESSION if with_counts else '""'
    )
    cmd = _basex_command(
        java_info.path,
        miclasspath,
        java_max_memory,
        parent,
        name,
        "-smethod=text",
        query=query,
    )
    output = _run_basex(cmd, context=f"Listing scenarios in {db_path} failed")
    output = output.rstrip("\r\n")
    scenarios: list[ScenarioRef] = []
    if not output:
        return scenarios
    for record in output.split(_LIST_RECORD_SEP):
        fields = record.split(_LIST_FIELD_SEP)
        if len(fields) != 4:
            raise DatabaseAccessError(
                f"Unexpected scenario listing from BaseX for {db_path}: {record!r}"
            )
        scenario_name, date, version, count = fields
        scenarios.append(
            ScenarioRef(
                name=scenario_name,
                date=date,
                version=version or None,
                element_count=int(count) if count else None,
            )
        )
    return scenarios


def parse_selector(selector: str) -> tuple[str, str | None]:
    """Split a ``NAME`` or ``NAME@DATE`` selector.

    The split is on the last ``@`` so names containing ``@`` work as long as
    the date is given.

    Raises:
        ScenarioSelectionError: If either part is empty.
    """
    text = selector.strip()
    if "@" not in text:
        if not text:
            raise ScenarioSelectionError("Scenario selector must be NAME or NAME@DATE")
        return text, None
    name, date = text.rsplit("@", 1)
    if not name or not date:
        raise ScenarioSelectionError(
            f"Scenario selector {selector!r} must be NAME or NAME@DATE"
        )
    return name, date


def select_scenario(
    scenarios: Sequence[ScenarioRef],
    selector: str | None = None,
    *,
    source: str = "the database",
) -> ScenarioRef:
    """Pick exactly one scenario, never guessing.

    Args:
        scenarios: Candidates, normally from :func:`list_scenarios`.
        selector: ``None`` (only valid when there is exactly one scenario),
            ``NAME`` (valid when one run has that name) or ``NAME@DATE``.
        source: Database label for messages.

    Raises:
        ScenarioSelectionError: If zero or several scenarios match. The
            message lists the available ``NAME@DATE`` labels.
    """
    available = ", ".join(s.label for s in scenarios)
    if not scenarios:
        raise ScenarioSelectionError(f"{source} contains no scenarios")
    if selector is None:
        if len(scenarios) == 1:
            return scenarios[0]
        raise ScenarioSelectionError(
            f"{source} contains {len(scenarios)} scenarios; choose one with "
            f"--scenario NAME@DATE: {available}"
        )
    name, date = parse_selector(selector)
    matches = [
        s for s in scenarios if s.name == name and (date is None or s.date == date)
    ]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise ScenarioSelectionError(
            f"No scenario {selector!r} in {source}; available: {available}"
        )
    labels = ", ".join(m.label for m in matches)
    if date is None:
        raise ScenarioSelectionError(
            f"Scenario name {name!r} matches {len(matches)} runs in {source}; "
            f"select one with NAME@DATE: {labels}"
        )
    raise ScenarioSelectionError(
        f"{len(matches)} runs in {source} share the name and date {selector!r}; "
        "they cannot be told apart, so this database cannot be compared"
    )


def _check_disk_space(index_path: str, element_count: int | None) -> None:
    if not element_count:
        return
    directory = os.path.dirname(os.path.abspath(index_path))
    needed = element_count * BYTES_PER_ELEMENT_ESTIMATE
    free = shutil.disk_usage(directory).free
    if free < needed:
        raise ExtractionError(
            f"Not enough free disk space in {directory}: building the structure "
            f"index of {element_count:,} elements needs about {needed / 2**30:.1f} "
            f"GiB, {free / 2**30:.1f} GiB available. Use --work-dir to choose "
            "another location."
        )


def _read_spool(spool: Any) -> str:
    spool.seek(0)
    return spool.read().decode("utf-8", "replace")


def extract_structure(
    db_path: str,
    scenario: ScenarioRef,
    index_path: str,
    rules: ComparisonRules,
    *,
    java_max_memory: str = DEFAULT_JAVA_MAX_MEMORY,
    miclasspath: str | None = None,
    progress: ProgressCallback | None = None,
    java: str | None = None,
) -> StructureIndex:
    """Stream one scenario out of a database into a new structure index.

    BaseX serializes the scenario document to stdout, which is parsed
    incrementally by :func:`build_index_from_stream`; no temporary XML is
    written. The database is opened read-only and never modified.

    Args:
        db_path: Database directory.
        scenario: The run to extract; its ``element_count`` (from
            :func:`list_scenarios` with ``with_counts=True``) enables the
            completeness check.
        index_path: Where to write the index. Replaced if present.
        rules: Comparison rules.
        java_max_memory: JVM ``-Xmx`` value.
        miclasspath: Java class path; defaults to the bundled jars.
        progress: Progress callback, see :func:`build_index_from_stream`.
        java: Explicit ``java`` executable.

    Returns:
        The completed index.

    Raises:
        ExtractionError: If BaseX exits with an error, the stream is cut
            short or malformed, the element count does not match, or disk
            space is insufficient. No index file is left behind.
        DatabaseAccessError: If the directory is not a database.
        MissingDependencyError: If Java is unusable or runs out of memory.
    """
    parent, name = validate_database_dir(db_path)
    java_info = check_java(java)
    _check_disk_space(index_path, scenario.element_count)
    cmd = _basex_command(
        java_info.path,
        miclasspath,
        java_max_memory,
        parent,
        name,
        "-smethod=xml",
        "-sindent=no",
        "-somit-xml-declaration=yes",
        binds=(("n", scenario.name), ("d", scenario.date)),
        query=SCENARIO_QUERY,
    )
    info = {
        "db_path": os.path.abspath(db_path),
        "db_fingerprint": fingerprint_database(db_path),
        "scenario_name": scenario.name,
        "scenario_date": scenario.date,
        "scenario_version": scenario.version or "",
        "java_version": java_info.version,
        "java_path": java_info.path,
    }
    logger.info("Extracting %s from %s", scenario.label, db_path)
    logger.debug("BaseX command: %s", " ".join(cmd))
    stderr = tempfile.SpooledTemporaryFile(max_size=1 << 20)
    try:
        try:
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=stderr, bufsize=1 << 20
            )
        except OSError as exc:
            raise MissingDependencyError(
                f"Cannot start Java at {java_info.path!r}: {exc}"
            ) from exc
        try:
            assert proc.stdout is not None
            try:
                index = build_index_from_stream(
                    proc.stdout,
                    index_path,
                    rules,
                    source=db_path,
                    expected_elements=scenario.element_count,
                    progress=progress,
                    info=info,
                )
            except ExtractionError as exc:
                # The stream ended early or was malformed; BaseX may say why.
                returncode = proc.poll()
                if returncode is None:
                    proc.kill()
                    proc.wait()
                java_output = _read_spool(stderr)
                if returncode is None and not java_output.strip():
                    raise
                raise java_failure_error(
                    java_output,
                    returncode=returncode,
                    context=str(exc),
                    default=ExtractionError,
                ) from exc
            returncode = proc.wait()
            if returncode != 0:
                index.close()
                try:
                    os.remove(index_path)
                except FileNotFoundError:
                    pass
                raise java_failure_error(
                    _read_spool(stderr),
                    returncode=returncode,
                    context=f"BaseX failed while exporting {scenario.label} from {db_path}",
                    default=ExtractionError,
                )
            noise = _read_spool(stderr).strip()
            if noise:
                logger.debug("BaseX stderr for %s: %s", db_path, noise[-800:])
            return index
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            if proc.stdout is not None:
                proc.stdout.close()
    finally:
        stderr.close()
