"""Public entry points for structural comparison of GCAM output databases.

:func:`compare_databases` runs the whole pipeline for two or more local
databases: fingerprint, list scenarios, select one run per database, extract
(or reuse) a structure index per input, compare every candidate with the
baseline, and build the matrix. It prints nothing and returns a
:class:`~gcamreader.structure.model.ComparisonResult`; the command line layer
turns that into exit codes and files.

:func:`write_json` and :func:`iter_json` serialise a result finding by finding
so very large results stream to disk without materialising
``result.to_dict()``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import tempfile
from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

from gcamreader.structure.compare import build_matrix, compare_indexes
from gcamreader.structure.errors import StructureIndexError
from gcamreader.structure.extract import (
    DEFAULT_JAVA_MAX_MEMORY,
    EXTRACTOR_VERSION,
    extract_structure,
    fingerprint_database,
    list_scenarios,
    select_scenario,
    validate_database_dir,
)
from gcamreader.structure.index import SCHEMA_VERSION, StructureIndex
from gcamreader.structure.java_runtime import NEWEST_TESTED_JAVA_MAJOR, check_java
from gcamreader.structure.markdown import render_markdown, write_report
from gcamreader.structure.model import ComparisonResult, InputIdentity, ScenarioRef
from gcamreader.structure.rules import ComparisonRules

__all__ = [
    "compare_databases",
    "list_scenarios",
    "render_markdown",
    "write_report",
    "write_json",
    "iter_json",
    "index_file_name",
]

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[str], None]
ScenarioSelectors = str | Mapping[int, str] | Sequence[str | None] | None

#: Suffix of structure index files in the work directory.
INDEX_SUFFIX = ".structure.sqlite"


def _package_version() -> str:
    from gcamreader import __version__

    return __version__


def _report(progress: ProgressCallback | None, message: str) -> None:
    logger.info("%s", message)
    if progress is not None:
        progress(message)


def _selector_for(
    selectors: ScenarioSelectors, position: int, count: int
) -> str | None:
    """Resolve the scenario selector for the 1-based input ``position``."""
    if selectors is None:
        return None
    if isinstance(selectors, str):
        return selectors
    if isinstance(selectors, Mapping):
        bad = sorted(
            k for k in selectors if not (isinstance(k, int) and 1 <= k <= count)
        )
        if bad:
            raise ValueError(
                f"scenarios: keys must be input positions 1..{count}, got {bad}"
            )
        return selectors.get(position)
    items = list(selectors)
    if len(items) != count:
        raise ValueError(
            f"scenarios: expected one selector (or None) per database, "
            f"{count} databases but {len(items)} selectors"
        )
    return items[position - 1]


def index_file_name(
    db_name: str, db_fingerprint: str, scenario: ScenarioRef, rules: ComparisonRules
) -> str:
    """Name of the index file for one input in the work directory.

    The name encodes everything a reusable index must agree on: database
    fingerprint, scenario name and date, rules, extractor and schema version.
    """
    key = hashlib.sha256(
        "\0".join(
            [
                db_fingerprint,
                scenario.name,
                scenario.date,
                rules.fingerprint(),
                str(EXTRACTOR_VERSION),
                str(SCHEMA_VERSION),
            ]
        ).encode("utf-8")
    ).hexdigest()[:16]
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", db_name)[:60] or "db"
    return f"{safe}-{key}{INDEX_SUFFIX}"


def _reusable_index(
    path: str, db_fingerprint: str, scenario: ScenarioRef, rules: ComparisonRules
) -> StructureIndex | None:
    """Open ``path`` if it is a complete index matching the input, else ``None``."""
    if not os.path.isfile(path):
        return None
    try:
        index = StructureIndex.open(path)
    except StructureIndexError as exc:
        logger.info("Not reusing %s: %s", path, exc)
        return None
    info = index.info
    expected = {
        "db_fingerprint": db_fingerprint,
        "scenario_name": scenario.name,
        "scenario_date": scenario.date,
        "rules_fingerprint": rules.fingerprint(),
        "extractor_version": str(EXTRACTOR_VERSION),
    }
    if all(info.get(key) == value for key, value in expected.items()):
        return index
    index.close()
    logger.info("Not reusing %s: metadata does not match the input", path)
    return None


def compare_databases(
    paths: Sequence[str | os.PathLike[str]],
    *,
    baseline: int = 0,
    scenarios: ScenarioSelectors = None,
    rules: ComparisonRules | None = None,
    work_dir: str | os.PathLike[str] | None = None,
    use_cache: bool = True,
    java_max_memory: str = DEFAULT_JAVA_MAX_MEMORY,
    miclasspath: str | None = None,
    progress: ProgressCallback | None = None,
) -> ComparisonResult:
    """Compare the structure of two or more local GCAM output databases.

    Args:
        paths: Database directories, at least two. Inputs keep this order and
            are numbered from 1 in the result.
        baseline: 0-based position of the baseline database in ``paths``.
        scenarios: Which run to use when a database holds several: a single
            ``NAME`` or ``NAME@DATE`` applied to every database, a mapping from
            1-based input position to selector, or a sequence aligned with
            ``paths`` (``None`` entries select automatically). A database
            with exactly one run needs no selector.
        rules: Comparison rules; defaults to :meth:`ComparisonRules.default`.
        work_dir: Directory holding the structure indexes (about the size of
            each database). Indexes found there that match an input are
            reused; the directory is kept. Without it a temporary directory is
            used and removed afterwards.
        use_cache: Set ``False`` to re-extract even when a matching index
            exists in ``work_dir``.
        java_max_memory: JVM ``-Xmx`` for BaseX.
        miclasspath: Java class path; defaults to the bundled jars.
        progress: Called with short status messages (phases and extraction
            progress).

    Returns:
        The comparison result. ``result.has_differences`` tells whether any
        candidate differs from the baseline.

    Raises:
        ValueError: On fewer than two paths, an invalid baseline, or malformed
            ``scenarios``.
        StructureError: For database, Java, selection and extraction problems
            (see :mod:`gcamreader.structure.errors`).
    """
    path_list = [os.fspath(p) for p in paths]
    if len(path_list) < 2:
        raise ValueError("compare_databases needs at least two database paths")
    if not 0 <= baseline < len(path_list):
        raise ValueError(
            f"baseline must be between 0 and {len(path_list) - 1}, got {baseline}"
        )
    rules = rules or ComparisonRules.default()
    for path in path_list:
        validate_database_dir(path)
    for position in range(1, len(path_list) + 1):
        _selector_for(scenarios, position, len(path_list))

    java = check_java()
    warnings: list[str] = []
    if java.is_newer_than_tested:
        warnings.append(
            f"Java {java.version} at {java.path} is newer than the versions "
            f"gcamreader is tested with (up to Java {NEWEST_TESTED_JAVA_MAJOR})."
        )

    temp_dir: str | None = None
    if work_dir is None:
        temp_dir = tempfile.mkdtemp(prefix="gcamreader-structure-")
        work = temp_dir
    else:
        work = os.fspath(work_dir)
        os.makedirs(work, exist_ok=True)

    indexes: list[StructureIndex] = []
    produced: dict[str, StructureIndex] = {}
    try:
        inputs: list[InputIdentity] = []
        total = len(path_list)
        for position, path in enumerate(path_list, start=1):
            label = os.path.basename(os.path.normpath(path))
            _report(progress, f"[{position}/{total}] {path}: listing scenarios")
            fingerprint = fingerprint_database(path)
            refs = list_scenarios(
                path,
                with_counts=True,
                java_max_memory=java_max_memory,
                miclasspath=miclasspath,
                java=java.path,
            )
            scenario = select_scenario(
                refs, _selector_for(scenarios, position, total), source=path
            )
            inputs.append(InputIdentity(position, path, label, scenario, fingerprint))
            _, db_name = validate_database_dir(path)
            index_path = os.path.join(
                work, index_file_name(db_name, fingerprint, scenario, rules)
            )
            index = produced.get(index_path)
            if index is not None:
                # Same database and run given twice in this call.
                index = StructureIndex.open(index_path)
            elif use_cache:
                index = _reusable_index(index_path, fingerprint, scenario, rules)
                if index is not None:
                    # Operational notice, not a result warning: a warm run must
                    # produce the same report as a cold run.
                    _report(
                        progress,
                        f"[{position}/{total}] {path}: reusing the structure index "
                        f"{index_path} (database not re-read)",
                    )
            if index is None:
                count = scenario.element_count or 0
                _report(
                    progress,
                    f"[{position}/{total}] {path}: extracting {scenario.label} "
                    f"({count:,} elements)",
                )
                index = extract_structure(
                    path,
                    scenario,
                    index_path,
                    rules,
                    java_max_memory=java_max_memory,
                    miclasspath=miclasspath,
                    progress=progress,
                    java=java.path,
                )
                produced[index_path] = index
            indexes.append(index)

        base_input = inputs[baseline]
        base_index = indexes[baseline]
        comparisons = []
        for position, (candidate_input, candidate_index) in enumerate(
            zip(inputs, indexes, strict=True)
        ):
            if position == baseline:
                continue
            _report(
                progress,
                f"comparing {candidate_input.label} with baseline {base_input.label}",
            )
            comparisons.append(
                compare_indexes(
                    base_index,
                    candidate_index,
                    baseline_input=base_input,
                    candidate_input=candidate_input,
                )
            )
        matrix = build_matrix(inputs, comparisons)
        return ComparisonResult(
            package_version=_package_version(),
            inputs=tuple(inputs),
            baseline=base_input.index,
            comparisons=tuple(comparisons),
            matrix=matrix,
            rules=rules.to_dict(),
            warnings=tuple(warnings),
        )
    finally:
        for index in indexes:
            index.close()
        if temp_dir is not None:
            shutil.rmtree(temp_dir, ignore_errors=True)


# -- JSON --------------------------------------------------------------------


def _dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)


def iter_json(result: ComparisonResult) -> Iterator[str]:
    """Yield the JSON text of ``result.to_dict()`` in chunks.

    Findings and matrix rows are emitted one at a time, so the whole
    dictionary never exists in memory. Parsing the concatenated chunks gives
    exactly ``result.to_dict()``.
    """
    yield '{"package_version": ' + _dumps(result.package_version)
    yield ', "inputs": ' + _dumps([item.to_dict() for item in result.inputs])
    yield ', "baseline": ' + _dumps(result.baseline)
    yield ', "comparisons": ['
    for number, comparison in enumerate(result.comparisons):
        yield ("" if number == 0 else ", ") + (
            '{"baseline": '
            + _dumps(comparison.baseline.index)
            + ', "candidate": '
            + _dumps(comparison.candidate.index)
            + ', "findings": ['
        )
        for index, finding in enumerate(comparison.findings):
            yield ("" if index == 0 else ", ") + _dumps(finding.to_dict())
        yield (
            '], "summary": '
            + _dumps(comparison.summary.to_dict())
            + ', "warnings": '
            + _dumps(list(comparison.warnings))
            + "}"
        )
    yield "]"
    yield ', "matrix": ['
    for index, row in enumerate(result.matrix):
        yield ("" if index == 0 else ", ") + _dumps(row.to_dict())
    yield "]"
    yield ', "rules": ' + _dumps(dict(result.rules))
    yield ', "warnings": ' + _dumps(list(result.warnings))
    yield ', "has_differences": ' + _dumps(result.has_differences)
    yield "}\n"


def write_json(result: ComparisonResult, path: str | os.PathLike[str]) -> Path:
    """Write ``result`` as JSON (see :func:`iter_json`) and return the path."""
    target = Path(path)
    with open(target, "w", encoding="utf-8", newline="\n") as handle:
        for chunk in iter_json(result):
            handle.write(chunk)
    return target
