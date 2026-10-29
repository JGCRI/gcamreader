"""SQLite-backed structure index of one scenario document.

A :class:`StructureIndex` stores the *shape* of a GCAM scenario: one row per
``(parent, tag, identity, metadata variant)`` with occurrence counts, subtree
sizes and an order-insensitive subtree fingerprint. Element text is never
stored. Two indexes built from two databases can be compared top-down without
re-reading either database.

Layout (schema version 1)::

    strings(id INTEGER PRIMARY KEY, value TEXT UNIQUE)
    node(parent_hash BLOB NOT NULL, tag_id INT NOT NULL, ident_id INT NOT NULL,
         meta_id INT NOT NULL, path_hash BLOB, depth INT, count INT,
         subtree_size INT, f0 INT, f1 INT, f2 INT, f3 INT,
         PRIMARY KEY(parent_hash, tag_id, ident_id, meta_id)) WITHOUT ROWID
    meta_info(key TEXT PRIMARY KEY, value TEXT)

``path_hash`` identifies an element position (``blake2b(parent_hash + key)``)
and is set only for elements that have children; leaves store ``NULL`` because
no row ever refers to a leaf as its parent. The document element's parent hash
is :data:`ROOT_PARENT_HASH`. Identity and metadata attribute pairs are encoded
as strings with the ASCII unit separator (``\\x1f``), which cannot occur in
well-formed XML 1.0 attribute values.

Loading is two-phase: rows are appended to an unindexed ``node_raw`` table in
an attached scratch file (``<index>.raw``) while the document streams, then
aggregated once with ``GROUP BY`` into ``node`` and the scratch file is
deleted. This keeps inserts sequential at tens of millions of rows, merges the
children of duplicate parents (siblings with identical tag and identity), and
leaves no free pages in the index file. ``meta_info['complete'] = '1'`` is
written last; readers refuse an index without it.
"""

from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from gcamreader.structure.errors import StructureIndexError

#: Bump when the on-disk layout or the row semantics change.
SCHEMA_VERSION = 1

#: ``parent_hash`` of the document element.
ROOT_PARENT_HASH = b"\0" * 16

#: Separator between attribute names and values in identity/metadata strings.
SEP = "\x1f"

#: Suffix of the scratch file holding ``node_raw`` during a build.
RAW_SUFFIX = ".raw"

Pairs = tuple[tuple[str, str], ...]

_PRAGMAS_WRITE = (
    "PRAGMA journal_mode=OFF",
    "PRAGMA raw.journal_mode=OFF",
    "PRAGMA synchronous=OFF",
    "PRAGMA raw.synchronous=OFF",
    "PRAGMA temp_store=FILE",
    "PRAGMA cache_size=-524288",
)

_PRAGMAS_READ = (
    "PRAGMA query_only=ON",
    "PRAGMA cache_size=-262144",
)

_CREATE = """
CREATE TABLE main.strings(id INTEGER PRIMARY KEY, value TEXT NOT NULL UNIQUE);
CREATE TABLE main.meta_info(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE raw.node_raw(
    parent_hash BLOB NOT NULL, tag_id INT NOT NULL, ident_id INT NOT NULL,
    meta_id INT NOT NULL, path_hash BLOB, depth INT NOT NULL, count INT NOT NULL,
    subtree_size INT NOT NULL, f0 INT NOT NULL, f1 INT NOT NULL,
    f2 INT NOT NULL, f3 INT NOT NULL);
"""

_AGGREGATE = """
CREATE TABLE main.node(
    parent_hash BLOB NOT NULL, tag_id INT NOT NULL, ident_id INT NOT NULL,
    meta_id INT NOT NULL, path_hash BLOB, depth INT NOT NULL, count INT NOT NULL,
    subtree_size INT NOT NULL, f0 INT NOT NULL, f1 INT NOT NULL,
    f2 INT NOT NULL, f3 INT NOT NULL,
    PRIMARY KEY(parent_hash, tag_id, ident_id, meta_id)) WITHOUT ROWID;
INSERT INTO main.node
    SELECT parent_hash, tag_id, ident_id, meta_id, MAX(path_hash), MIN(depth),
           SUM(count), SUM(subtree_size),
           SUM(f0) & 4294967295, SUM(f1) & 4294967295,
           SUM(f2) & 4294967295, SUM(f3) & 4294967295
    FROM raw.node_raw
    GROUP BY parent_hash, tag_id, ident_id, meta_id;
"""

_DESCENDANT_TAGS = """
WITH RECURSIVE sub(h) AS (
    SELECT ?
    UNION
    SELECT n.path_hash FROM node AS n JOIN sub ON n.parent_hash = sub.h
    WHERE n.path_hash IS NOT NULL
)
SELECT t.value, SUM(n.count)
FROM node AS n
JOIN sub ON n.parent_hash = sub.h
JOIN strings AS t ON t.id = n.tag_id
GROUP BY t.value
ORDER BY t.value
"""

_SELECT_CHILDREN = """
SELECT n.parent_hash, n.path_hash, t.value, i.value, m.value, n.depth, n.count,
       n.subtree_size, n.f0, n.f1, n.f2, n.f3
FROM node AS n
JOIN strings AS t ON t.id = n.tag_id
JOIN strings AS i ON i.id = n.ident_id
JOIN strings AS m ON m.id = n.meta_id
WHERE n.parent_hash = ?
"""


def encode_pairs(pairs: Pairs) -> str:
    """Encode attribute pairs as ``k1<SEP>v1<SEP>k2<SEP>v2``."""
    return SEP.join(part for pair in pairs for part in pair)


def decode_pairs(text: str) -> Pairs:
    """Inverse of :func:`encode_pairs`."""
    if not text:
        return ()
    parts = text.split(SEP)
    return tuple(zip(parts[0::2], parts[1::2], strict=True))


@dataclass(frozen=True)
class NodeRow:
    """One ``(parent, tag, identity, metadata variant)`` entry of an index.

    Attributes:
        parent_hash: Position hash of the parent element.
        path_hash: Position hash of this element, ``None`` for leaves.
        tag: Element tag (Clark notation when namespaced).
        identity: Identity attribute pairs in rule order.
        metadata: Metadata attribute pairs sorted by name.
        depth: 1 for the document element.
        count: Occurrences of this tag, identity and metadata under the
            parent position (summed across duplicate parents).
        subtree_size: Elements in those occurrences' subtrees, including the
            occurrences themselves.
        fingerprint: Four 32-bit lanes summarising the subtrees; equal
            fingerprints mean (with overwhelming probability) identical
            structure and metadata below this node.
    """

    parent_hash: bytes
    path_hash: bytes | None
    tag: str
    identity: Pairs
    metadata: Pairs
    depth: int
    count: int
    subtree_size: int
    fingerprint: tuple[int, int, int, int]

    @property
    def key(self) -> tuple[str, Pairs]:
        """``(tag, identity)``: what identifies the element among siblings."""
        return self.tag, self.identity

    @property
    def is_leaf(self) -> bool:
        return self.path_hash is None


def _remove_quietly(path: str) -> None:
    for suffix in ("", "-journal", "-wal", "-shm"):
        try:
            os.remove(path + suffix)
        except FileNotFoundError:
            pass


class IndexWriter:
    """Append rows during extraction and finalize the index.

    The writer owns the index file and its scratch file: :meth:`finish` makes
    the index valid and removes the scratch file, :meth:`abort` deletes both,
    so a partially written file is never left behind looking like a result.

    The extraction hot loop uses :attr:`strings` and :attr:`pending` directly
    instead of :meth:`intern` and :meth:`add_row`, which exist for clarity and
    tests.

    Attributes:
        strings: Interned text -> id, written to the ``strings`` table on
            :meth:`finish`.
        pending: Buffered ``node_raw`` rows awaiting :meth:`flush`.
    """

    def __init__(self, path: str, *, batch_size: int = 20_000) -> None:
        self.path = path
        self.raw_path = path + RAW_SUFFIX
        self.batch_size = batch_size
        self._conn: sqlite3.Connection | None = None
        self.strings: dict[str, int] = {}
        self.pending: list[tuple[Any, ...]] = []
        self.row_count = 0

    def begin(self) -> None:
        """Create fresh index and scratch files (replacing existing ones)."""
        _remove_quietly(self.path)
        _remove_quietly(self.raw_path)
        conn = sqlite3.connect(self.path, isolation_level=None)
        conn.execute("PRAGMA page_size=8192")
        conn.execute("ATTACH DATABASE ? AS raw", (self.raw_path,))
        conn.execute("PRAGMA raw.page_size=8192")
        for pragma in _PRAGMAS_WRITE:
            conn.execute(pragma)
        conn.executescript(_CREATE)
        conn.execute("BEGIN")
        self._conn = conn

    def intern(self, value: str) -> int:
        """Return the id of ``value`` in the strings table, assigning one if new."""
        ident = self.strings.get(value)
        if ident is None:
            ident = len(self.strings) + 1
            self.strings[value] = ident
        return ident

    def add_row(self, row: tuple[Any, ...]) -> None:
        """Queue one ``node_raw`` row.

        ``row`` is ``(parent_hash, tag_id, ident_id, meta_id, path_hash, depth,
        count, subtree_size, f0, f1, f2, f3)``.
        """
        self.pending.append(row)
        if len(self.pending) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        """Write buffered rows. The buffer list object is reused."""
        if self.pending:
            assert self._conn is not None
            self._conn.executemany(
                "INSERT INTO raw.node_raw VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                self.pending,
            )
            self.row_count += len(self.pending)
            self.pending.clear()

    def finish(self, info: Mapping[str, Any]) -> None:
        """Aggregate rows, write metadata, mark complete, drop the scratch file.

        Args:
            info: ``meta_info`` entries. Non-string values are JSON-encoded.
                ``schema_version`` and ``complete`` are set by the writer.
        """
        conn = self._conn
        assert conn is not None
        self.flush()
        conn.executemany(
            "INSERT INTO main.strings(id, value) VALUES (?, ?)",
            ((ident, value) for value, ident in self.strings.items()),
        )
        conn.execute("COMMIT")
        conn.executescript(_AGGREGATE)
        conn.execute("DETACH DATABASE raw")
        _remove_quietly(self.raw_path)
        meta = {str(key): _meta_text(value) for key, value in info.items()}
        meta["schema_version"] = str(SCHEMA_VERSION)
        conn.executemany(
            "INSERT OR REPLACE INTO meta_info(key, value) VALUES (?, ?)", meta.items()
        )
        conn.execute("INSERT OR REPLACE INTO meta_info VALUES ('complete', '1')")
        conn.commit()
        conn.close()
        self._conn = None

    def abort(self) -> None:
        """Close and delete the index and scratch files."""
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None
        _remove_quietly(self.path)
        _remove_quietly(self.raw_path)


def _meta_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True)


class StructureIndex:
    """Read-only view of a completed index file.

    Use as a context manager or call :meth:`close`.
    """

    def __init__(self, path: str, conn: sqlite3.Connection, info: dict[str, str]):
        self.path = path
        self._conn = conn
        self.info = info

    @classmethod
    def open(cls, path: str) -> StructureIndex:
        """Open an index, refusing incomplete or incompatible files.

        Raises:
            StructureIndexError: If the file is missing, not an index, was
                not completed, or has another schema version.
        """
        if not os.path.isfile(path):
            raise StructureIndexError(f"structure index not found: {path}")
        try:
            conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            for pragma in _PRAGMAS_READ:
                conn.execute(pragma)
            rows = conn.execute("SELECT key, value FROM meta_info").fetchall()
        except sqlite3.DatabaseError as exc:
            raise StructureIndexError(
                f"{path} is not a structure index ({exc})"
            ) from exc
        info = dict(rows)
        if info.get("complete") != "1":
            conn.close()
            raise StructureIndexError(
                f"{path} is an incomplete structure index (extraction did not "
                "finish); delete it and extract again"
            )
        version = info.get("schema_version")
        if version != str(SCHEMA_VERSION):
            conn.close()
            raise StructureIndexError(
                f"{path} has structure index schema {version!r}; this version of "
                f"gcamreader needs {SCHEMA_VERSION}. Delete it and extract again"
            )
        return cls(path, conn, info)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> StructureIndex:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- metadata ------------------------------------------------------------

    def info_json(self, key: str, default: Any = None) -> Any:
        """Return a JSON-encoded ``meta_info`` entry, decoded."""
        raw = self.info.get(key)
        return default if raw is None else json.loads(raw)

    @property
    def warnings(self) -> list[str]:
        """Warnings recorded during extraction."""
        return list(self.info_json("warnings", []))

    @property
    def element_count(self) -> int:
        """Elements seen in the document, including excluded subtrees."""
        return int(self.info["element_count"])

    # -- rows ----------------------------------------------------------------

    def node_count(self) -> int:
        return int(self._conn.execute("SELECT COUNT(*) FROM node").fetchone()[0])

    def root(self) -> NodeRow:
        """Return the document element's row.

        Raises:
            StructureIndexError: If the index has no or several roots.
        """
        roots = self.children(ROOT_PARENT_HASH)
        if len(roots) != 1:
            raise StructureIndexError(
                f"{self.path} has {len(roots)} document elements; expected 1"
            )
        return roots[0]

    def children(self, parent_hash: bytes) -> list[NodeRow]:
        """Return the rows under ``parent_hash``, sorted by tag then identity."""
        rows = [
            _row(record)
            for record in self._conn.execute(_SELECT_CHILDREN, (parent_hash,))
        ]
        rows.sort(key=lambda row: (row.tag, row.identity, row.metadata))
        return rows

    def descendant_tag_counts(self, path_hash: bytes) -> tuple[tuple[str, int], ...]:
        """Return ``(tag, occurrences)`` for every element below a position.

        One recursive query; positions reached through several metadata
        variants of the same element are visited once.
        """
        return tuple(
            (tag, int(count))
            for tag, count in self._conn.execute(_DESCENDANT_TAGS, (path_hash,))
        )

    def iter_rows(self) -> Iterator[NodeRow]:
        """Iterate over every row (order unspecified)."""
        query = _SELECT_CHILDREN.replace("WHERE n.parent_hash = ?", "")
        for record in self._conn.execute(query):
            yield _row(record)


def _row(record: tuple[Any, ...]) -> NodeRow:
    parent, path, tag, ident, meta, depth, count, size, f0, f1, f2, f3 = record
    return NodeRow(
        parent_hash=bytes(parent),
        path_hash=None if path is None else bytes(path),
        tag=tag,
        identity=decode_pairs(ident),
        metadata=decode_pairs(meta),
        depth=depth,
        count=count,
        subtree_size=size,
        fingerprint=(f0, f1, f2, f3),
    )
