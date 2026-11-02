"""Locate and validate the Java runtime used for structural comparison.

``gcamreader`` runs the bundled BaseX and ModelInterface jars (Java 8 class
files) in a ``java`` subprocess. The existing ``local``/``remote`` commands call
a bare ``java`` from ``PATH``; the structural comparison runs a longer,
higher-memory Java process, so it resolves the runtime explicitly, checks the
version once up front, and translates common JVM and BaseX failures into
actionable messages.

Resolution order for :func:`find_java`:

1. ``GCAMREADER_JAVA`` environment variable: a ``java`` executable or a JDK/JRE
   home directory. Set this on machines where Java cannot be installed
   system-wide (a user-space JRE, or ``module load java`` on HPC).
2. ``JAVA_HOME/bin/java``.
3. ``java`` on ``PATH``.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass

from gcamreader.structure.errors import DatabaseAccessError, MissingDependencyError

logger = logging.getLogger(__name__)

#: Environment variable naming a ``java`` executable or a Java home directory.
JAVA_ENV_VAR = "GCAMREADER_JAVA"

#: Oldest Java major version able to load the bundled jars (Java 8 bytecode).
MIN_JAVA_MAJOR = 8

#: Newest Java major version exercised by the project's CI. Newer runtimes are
#: accepted with a warning; see the Java runtime section of the documentation.
NEWEST_TESTED_JAVA_MAJOR = 17

#: Seconds allowed for ``java -version`` before the runtime is deemed unusable.
VERSION_CHECK_TIMEOUT = 60.0

_INSTALL_HINT = (
    f"Install a Java runtime (version {MIN_JAVA_MAJOR} or newer, for example "
    "Eclipse Temurin 17) and make sure 'java' is on PATH, or point "
    f"{JAVA_ENV_VAR} at a java executable or Java home directory. On HPC "
    "systems a 'module load java' before running usually suffices; a "
    "user-space JRE needs no administrator rights."
)


@dataclass(frozen=True)
class JavaInfo:
    """Facts about a resolved Java runtime.

    Attributes:
        path: Path to the ``java`` executable that was checked.
        major: Java major version (``8`` for ``1.8.0_xxx``, ``17`` for
            ``17.0.20``).
        version: The version string exactly as printed by ``java -version``.
        runtime: The runtime description line (vendor and build), or an empty
            string when the output had no second line.
        source: Where the executable came from: ``GCAMREADER_JAVA``,
            ``JAVA_HOME``, ``PATH`` or ``explicit``.
    """

    path: str
    major: int
    version: str
    runtime: str
    source: str

    @property
    def is_newer_than_tested(self) -> bool:
        """Whether this runtime is newer than the CI-tested range."""
        return self.major > NEWEST_TESTED_JAVA_MAJOR


def java_executable_name() -> str:
    """Return the platform-specific file name of the ``java`` launcher."""
    return "java.exe" if sys.platform.startswith("win") else "java"


def _java_from_home(home: str) -> str | None:
    """Return ``<home>/bin/java`` if it exists, else ``None``."""
    candidate = os.path.join(home, "bin", java_executable_name())
    if os.path.isfile(candidate):
        return candidate
    return None


def find_java(env: Mapping[str, str] | None = None) -> tuple[str, str]:
    """Locate a ``java`` executable.

    Args:
        env: Environment mapping to consult; defaults to :data:`os.environ`.

    Returns:
        A ``(path, source)`` tuple where ``source`` is one of
        ``GCAMREADER_JAVA``, ``JAVA_HOME`` or ``PATH``.

    Raises:
        MissingDependencyError: If ``GCAMREADER_JAVA`` is set but does not
            point at a usable executable or home directory, or if no runtime
            was found in any of the searched locations.
    """
    if env is None:
        env = os.environ
    searched: list[str] = []

    override = env.get(JAVA_ENV_VAR)
    if override:
        override = os.path.expanduser(override)
        if os.path.isfile(override):
            return override, JAVA_ENV_VAR
        if os.path.isdir(override):
            from_home = _java_from_home(override)
            if from_home is not None:
                return from_home, JAVA_ENV_VAR
        raise MissingDependencyError(
            f"{JAVA_ENV_VAR}={override!r} is not a java executable or a Java "
            "home directory containing bin/java. Fix or unset the variable."
        )
    searched.append(f"{JAVA_ENV_VAR} (not set)")

    java_home = env.get("JAVA_HOME")
    if java_home:
        from_home = _java_from_home(os.path.expanduser(java_home))
        if from_home is not None:
            return from_home, "JAVA_HOME"
        searched.append(f"JAVA_HOME={java_home!r} (no bin/java there)")
    else:
        searched.append("JAVA_HOME (not set)")

    on_path = shutil.which("java", path=env.get("PATH"))
    if on_path is not None:
        return on_path, "PATH"
    searched.append("PATH (no java found)")

    raise MissingDependencyError(
        "No Java runtime found. Searched: " + "; ".join(searched) + ". " + _INSTALL_HINT
    )


_VERSION_RE = re.compile(r'version\s+"([^"]+)"')


def parse_java_version(output: str) -> tuple[int, str, str]:
    """Parse the output of ``java -version``.

    Handles the formats printed by HotSpot (Oracle, OpenJDK, Temurin), OpenJ9
    (IBM Semeru), GraalVM and early-access builds, for example
    ``java version "1.8.0_392"`` or ``openjdk version "22-ea" 2024-03-19``.

    Args:
        output: Combined ``java -version`` output (it is written to stderr).

    Returns:
        ``(major, version, runtime)`` where ``runtime`` is the second
        non-empty line, or an empty string when missing.

    Raises:
        ValueError: If no version string could be found.
    """
    match = _VERSION_RE.search(output)
    if match is None:
        raise ValueError("no 'version \"...\"' token in java -version output")
    version = match.group(1)
    parts = re.findall(r"\d+", version)
    if not parts:
        raise ValueError(f"unparseable Java version {version!r}")
    if version.startswith("1.") and len(parts) >= 2:
        # Legacy scheme: 1.8.0_392 is Java 8.
        major = int(parts[1])
    else:
        major = int(parts[0])
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    runtime = lines[1] if len(lines) > 1 else ""
    return major, version, runtime


_cache: dict[str, JavaInfo] = {}


def clear_java_cache() -> None:
    """Forget previously checked runtimes (used by tests)."""
    _cache.clear()


def check_java(
    path: str | None = None,
    *,
    env: Mapping[str, str] | None = None,
    use_cache: bool = True,
) -> JavaInfo:
    """Resolve and validate a Java runtime, once per process by default.

    Args:
        path: Explicit ``java`` executable. When ``None`` the runtime is
            located with :func:`find_java`.
        env: Environment mapping passed to :func:`find_java`.
        use_cache: Reuse the result of an earlier check of the same
            executable.

    Returns:
        The validated runtime. A runtime newer than the CI-tested range is
        accepted; a warning is logged.

    Raises:
        MissingDependencyError: If no runtime is found, ``java -version``
            cannot be run or fails, its output cannot be parsed, or the
            runtime is older than Java 8.
    """
    if path is None:
        path, source = find_java(env)
    else:
        source = "explicit"

    if use_cache and path in _cache:
        return _cache[path]

    try:
        proc = subprocess.run(
            [path, "-version"],
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            timeout=VERSION_CHECK_TIMEOUT,
            check=False,
        )
    except OSError as exc:
        raise MissingDependencyError(
            f"Cannot execute Java at {path!r} (from {source}): {exc}. {_INSTALL_HINT}"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise MissingDependencyError(
            f"'java -version' at {path!r} (from {source}) did not finish within "
            f"{VERSION_CHECK_TIMEOUT:.0f} s."
        ) from exc

    output = (proc.stderr or "") + (proc.stdout or "")
    if proc.returncode != 0:
        raise MissingDependencyError(
            f"'java -version' at {path!r} (from {source}) exited with code "
            f"{proc.returncode}: {output.strip()[-500:]}"
        )
    try:
        major, version, runtime = parse_java_version(output)
    except ValueError as exc:
        raise MissingDependencyError(
            f"Could not understand the version reported by {path!r} (from "
            f"{source}): {output.strip()[-300:]!r}"
        ) from exc

    if major < MIN_JAVA_MAJOR:
        raise MissingDependencyError(
            f"Java {version} at {path!r} (from {source}) is too old; the bundled "
            f"BaseX and ModelInterface jars need Java {MIN_JAVA_MAJOR} or newer. "
            + _INSTALL_HINT
        )

    info = JavaInfo(
        path=path, major=major, version=version, runtime=runtime, source=source
    )
    if info.is_newer_than_tested:
        logger.warning(
            "Java %s at %s is newer than the versions gcamreader is tested with "
            "(up to Java %d). It will probably work; if BaseX fails, try a "
            "Java %d runtime via %s.",
            version,
            path,
            NEWEST_TESTED_JAVA_MAJOR,
            NEWEST_TESTED_JAVA_MAJOR,
            JAVA_ENV_VAR,
        )
    if use_cache:
        _cache[path] = info
    return info


# Patterns in Java/BaseX stderr mapped to one-line advice. Checked in order;
# the first match wins, so more specific patterns come first.
_FAILURE_PATTERNS: tuple[tuple[re.Pattern[str], str, type[Exception]], ...] = (
    (
        re.compile(r"UnsupportedClassVersionError"),
        "The Java runtime is too old for the bundled jars; use Java "
        f"{MIN_JAVA_MAJOR} or newer (set {JAVA_ENV_VAR} to pick a runtime).",
        MissingDependencyError,
    ),
    (
        re.compile(r"java\.lang\.OutOfMemoryError|GC overhead limit exceeded"),
        "Java ran out of heap memory; raise the limit with --java-max-memory "
        "(for example 8g).",
        MissingDependencyError,
    ),
    (
        re.compile(
            r"InaccessibleObjectException|IllegalAccessError|"
            r"IllegalAccessException|does not (?:\"?opens\"?|export) .* to unnamed module"
        ),
        "The Java module system blocked an operation BaseX needs; use Java "
        f"{NEWEST_TESTED_JAVA_MAJOR} (set {JAVA_ENV_VAR} to pick a runtime).",
        MissingDependencyError,
    ),
    (
        re.compile(r"Could not find or load main class org\.basex"),
        "The bundled BaseX jar could not be loaded; the gcamreader installation "
        "appears incomplete. Reinstall the package.",
        MissingDependencyError,
    ),
    (
        re.compile(
            r"(?:incompatible|not compatible|newer version|older version)"
            r".{0,120}(?:database|BaseX|version)|BXDB\d{4}",
            re.IGNORECASE | re.DOTALL,
        ),
        "The database storage format is not compatible with the BaseX 9.5.0 "
        "bundled in gcamreader (it was probably written by a newer BaseX). "
        "This limitation is shared with 'gcamreader local'.",
        DatabaseAccessError,
    ),
    (
        re.compile(r"Database '[^']*' was not found|Database .* does not exist"),
        "BaseX did not find the database at the given path; check that the "
        "directory contains the *.basex files of a GCAM output database.",
        DatabaseAccessError,
    ),
    (
        re.compile(
            r"Permission denied|AccessDeniedException|is locked by another process"
        ),
        "BaseX could not read the database files (permission denied or locked "
        "by another process, such as a running ModelInterface).",
        DatabaseAccessError,
    ),
)


def classify_java_failure(stderr: str) -> tuple[str, type[Exception]] | None:
    """Translate Java/BaseX stderr into advice and an error class.

    Args:
        stderr: Standard error captured from the Java subprocess.

    Returns:
        ``(advice, error_class)`` for a recognized failure, or ``None`` when
        the output matched no known pattern. ``error_class`` is
        :class:`~gcamreader.structure.errors.MissingDependencyError` for JVM
        problems and :class:`~gcamreader.structure.errors.DatabaseAccessError`
        for BaseX database problems.
    """
    if not stderr:
        return None
    for pattern, advice, error_class in _FAILURE_PATTERNS:
        if pattern.search(stderr):
            return advice, error_class
    return None


def java_failure_error(
    stderr: str,
    *,
    returncode: int | None = None,
    context: str = "Java subprocess failed",
    default: type[Exception] = DatabaseAccessError,
) -> Exception:
    """Build the exception to raise for a failed Java subprocess.

    Args:
        stderr: Standard error captured from the subprocess.
        returncode: Exit status, included in the message when given.
        context: Short description of what was being attempted.
        default: Error class used when the output is not recognized.

    Returns:
        An exception instance (not raised) whose message combines the context,
        advice from :func:`classify_java_failure` when available, and the tail
        of the captured stderr.
    """
    headline = context
    if returncode is not None:
        headline += f" (exit code {returncode})"
    error_class = default
    classified = classify_java_failure(stderr)
    if classified is not None:
        advice, error_class = classified
        headline += ". " + advice
    tail = stderr.strip()[-800:]
    if tail:
        headline += "\nJava output:\n" + tail
    return error_class(headline)
