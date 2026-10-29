"""Command line interface for running queries against GCAM databases.

This module exposes a :mod:`typer` application with ``local`` and ``remote``
subcommands that run the queries contained in a GCAM queries XML file and save
the results as pipe-delimited CSV files, plus ``scenarios`` and ``diff``
subcommands that list the runs in local output databases and compare their
structure (see :mod:`gcamreader.structure`).

Examples:
    Compare the structure of two output databases::

        $ gcamreader diff database_basexdb_ref database_basexdb_policy -o report.md

    Query a local database from the shell::

        $ python -m gcamreader local \\
            -d /path/to/database_basexdb \\
            -q Main_queries.xml \\
            -o ./outputs

    Query a remote BaseX server::

        $ python -m gcamreader remote \\
            -u username \\
            -d database_name \\
            -q Main_queries.xml \\
            -o ./outputs \\
            -n localhost \\
            -p 8984
"""

from __future__ import annotations

import logging
import multiprocessing
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from . import __version__
from .querymi import LocalDBConn, RemoteDBConn, parse_batch_query

app = typer.Typer(
    help=(
        "Run queries against a GCAM scenario database and save outputs as CSV, "
        "or compare the structure of GCAM output databases."
    ),
    add_completion=False,
    no_args_is_help=True,
)


def _version_callback(value: bool) -> None:
    """Print the package version and exit when ``--version`` is given.

    Args:
        value: Whether the ``--version`` flag was supplied.

    Raises:
        typer.Exit: Always raised after printing when ``value`` is ``True``.
    """
    if value:
        typer.echo(f"gcamreader {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            help="Show the gcamreader version and exit.",
            callback=_version_callback,
            is_eager=True,
        ),
    ] = False,
) -> None:
    """Run queries against a GCAM scenario database.

    Args:
        version: When ``True``, print the version and exit.
    """


@app.command()
def local(
    database_path: Annotated[
        Path,
        typer.Option(
            "-d",
            "--database_path",
            exists=True,
            file_okay=False,
            readable=True,
            help="path to database file (i.e. parent of *.basex dir)",
        ),
    ],
    query_path: Annotated[
        Path,
        typer.Option(
            "-q",
            "--query_path",
            exists=True,
            dir_okay=False,
            readable=True,
            help="path to xml with queries to run (i.e: Main_queries.xml)",
        ),
    ],
    output_path: Annotated[
        Path,
        typer.Option(
            "-o",
            "--output_path",
            file_okay=False,
            writable=True,
            help="path to output (i.e. where .csv files should be created)",
        ),
    ],
    force: Annotated[
        bool,
        typer.Option(
            "-f",
            "--force",
            help="overwrite existing .csv in output path",
        ),
    ] = False,
) -> None:
    """Query a local GCAM scenario database.

    Args:
        database_path: Path to the database directory (parent of the ``*.basex``
            directory).
        query_path: Path to the queries XML file to run.
        output_path: Directory in which the result CSV files are created.
        force: Whether to overwrite existing CSV files in the output path.

    Examples:
        Invoke from the shell::

            $ python -m gcamreader local \\
                -d /path/to/database_basexdb \\
                -q Main_queries.xml \\
                -o ./outputs
    """
    typer.echo(f"opening: {database_path.absolute()}", err=True)
    if not list(database_path.glob("*.basex")):
        typer.echo(f"basex files missing: {database_path}", err=True)
        raise typer.Exit(code=1)
    parent = str(database_path.parent)
    name = database_path.name

    # Establish database connection - uses ModelInterface.jar.
    conn = LocalDBConn(parent, name)

    execute(conn, query_path, output_path, force)


@app.command()
def remote(
    username: Annotated[
        str,
        typer.Option(
            "-u",
            "--username",
            help="username of remote server authentication",
        ),
    ],
    database_name: Annotated[
        str,
        typer.Option(
            "-d",
            "--database_name",
            help="name of database to query (i.e. parent of *.basex dir)",
        ),
    ],
    query_path: Annotated[
        Path,
        typer.Option(
            "-q",
            "--query_path",
            exists=True,
            dir_okay=False,
            readable=True,
            help="path to xml with queries to run (i.e: Main_queries.xml)",
        ),
    ],
    output_path: Annotated[
        Path,
        typer.Option(
            "-o",
            "--output_path",
            file_okay=False,
            writable=True,
            help="path to output (i.e. where .csv files should be created)",
        ),
    ],
    password: Annotated[
        str,
        typer.Option(
            "-w",
            "--password",
            prompt=True,
            hide_input=True,
            help="password of remote server authentication",
        ),
    ],
    hostname: Annotated[
        str,
        typer.Option(
            "-n",
            "--hostname",
            help="hostname of remote server",
        ),
    ] = "localhost",
    port: Annotated[
        int,
        typer.Option(
            "-p",
            "--port",
            help="port on remote server",
        ),
    ] = 8984,
    force: Annotated[
        bool,
        typer.Option(
            "-f",
            "--force",
            help="overwrite existing .csv in output path",
        ),
    ] = False,
) -> None:
    """Query a remote server containing GCAM scenario databases.

    Args:
        username: Username for remote server authentication.
        database_name: Name of the database to query.
        query_path: Path to the queries XML file to run.
        output_path: Directory in which the result CSV files are created.
        password: Password for remote server authentication.
        hostname: Hostname of the remote server.
        port: Port on the remote server.
        force: Whether to overwrite existing CSV files in the output path.

    Examples:
        Invoke from the shell::

            $ python -m gcamreader remote \\
                -u username \\
                -d database_name \\
                -q Main_queries.xml \\
                -o ./outputs \\
                -n localhost \\
                -p 8984
    """
    # Establish database connection - uses ModelInterface.jar.
    conn = RemoteDBConn(
        username=username,
        password=password,
        address=hostname,
        port=port,
        dbfile=database_name,
    )

    execute(conn, query_path, output_path, force)


def save(data: dict) -> None:
    """Run a single query and save its result to CSV.

    Args:
        data: A dictionary with keys ``conn``, ``query``, ``save_to``, and
            ``force`` describing the query to run and where to write it.
    """
    conn, query, save_to, force = (
        data["conn"],
        data["query"],
        data["save_to"],
        data["force"],
    )
    if save_to.exists():
        typer.echo(f"output exists: {save_to.name}", err=True)
        if not force:
            typer.echo(f"skipping: {save_to.name}", err=True)
            return
    typer.echo(f"running: {query.title}", err=True)
    try:
        df = conn.runQuery(query)
    except subprocess.CalledProcessError:
        typer.echo(f"failed: {query.title}", err=True)
        return
    if df is None:
        typer.echo(f"empty: {query.title}", err=True)
        return
    df.to_csv(save_to, index=False, sep="|")
    typer.echo(f"saved: {save_to.absolute()}", err=True)


def execute(
    conn: LocalDBConn | RemoteDBConn,
    query_path: Path,
    output_path: Path,
    force: bool,
) -> None:
    """Parse a queries file and run all queries, saving each result to CSV.

    Args:
        conn: The database connection to run the queries against.
        query_path: Path to the queries XML file.
        output_path: Directory in which the result CSV files are created.
        force: Whether to overwrite existing CSV files in the output path.
    """
    # Parse query xml.
    typer.echo(f"parsing: {query_path.name}", err=True)
    queries = []
    for query in parse_batch_query(str(query_path)):
        data = {
            "conn": conn,
            "query": query,
            "save_to": output_path
            / f"{str(query.title).replace(' ', '_').lower()}.csv",
            "force": force,
        }
        queries.append(data)
    with multiprocessing.Pool() as pool:
        pool.map(save, queries)
    typer.echo("extract complete", err=True)


# ---------------------------------------------------------------------------
# Structural comparison: ``scenarios`` and ``diff``
# ---------------------------------------------------------------------------

#: Exit code of ``diff`` when the databases differ structurally.
EXIT_DIFFERENCES = 1
#: Exit code of ``diff`` and ``scenarios`` for any error (usage, Java, database).
EXIT_ERROR = 2


def _configure_structure_logging(quiet: bool, verbose: bool, debug: bool) -> None:
    """Send ``gcamreader.structure`` log records to stderr at the chosen level."""
    level = logging.WARNING
    if verbose:
        level = logging.INFO
    if debug:
        level = logging.DEBUG
    if quiet and not debug:
        level = logging.ERROR
    logger = logging.getLogger("gcamreader.structure")
    for handler in list(logger.handlers):
        if getattr(handler, "_gcamreader_cli", False):
            logger.removeHandler(handler)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    handler._gcamreader_cli = True  # type: ignore[attr-defined]
    logger.addHandler(handler)
    logger.setLevel(level)


def _check_databases(databases: list[Path], *, minimum: int) -> list[str]:
    """Validate DB arguments before any Java starts; usage errors exit 2."""
    from .structure.errors import DatabaseAccessError
    from .structure.extract import validate_database_dir

    if len(databases) < minimum:
        noun = "directory is" if minimum == 1 else "directories are"
        raise typer.BadParameter(
            f"at least {minimum} database {noun} required", param_hint="DB"
        )
    for database in databases:
        try:
            validate_database_dir(str(database))
        except DatabaseAccessError as exc:
            raise typer.BadParameter(str(exc), param_hint="DB") from exc
    return [str(database) for database in databases]


def _resolve_baseline(baseline: str, databases: list[str]) -> int:
    """Turn ``--baseline IDX|PATH`` into a 0-based position."""
    text = baseline.strip()
    count = len(databases)
    if text.isdigit():
        position = int(text)
        if 1 <= position <= count:
            return position - 1
        raise typer.BadParameter(
            f"must be between 1 and {count}, got {position}", param_hint="--baseline"
        )
    normalized = [os.path.normcase(os.path.abspath(d)) for d in databases]
    target = os.path.normcase(os.path.abspath(text))
    if target in normalized:
        return normalized.index(target)
    raise typer.BadParameter(
        f"{baseline!r} is neither an input number (1..{count}) nor one of the "
        "database paths",
        param_hint="--baseline",
    )


def _parse_scenario_options(
    options: list[str], databases: list[str]
) -> str | dict[int, str] | None:
    """Parse repeatable ``--scenario [IDX=|BASENAME=]SELECTOR`` values.

    A prefix is recognised only when it is an input number in range or the
    basename of exactly one database; otherwise the whole text is a selector
    that applies to every database (so names containing ``=`` still work).
    """
    if not options:
        return None
    count = len(databases)
    basenames: dict[str, int | None] = {}
    for position, database in enumerate(databases, start=1):
        name = os.path.basename(os.path.normpath(database))
        basenames[name] = None if name in basenames else position
    targeted: dict[int, str] = {}
    bare: str | None = None
    for option in options:
        prefix, separator, rest = option.partition("=")
        position: int | None = None
        if separator and rest:
            if prefix.strip().isdigit():
                position = int(prefix)
                if not 1 <= position <= count:
                    raise typer.BadParameter(
                        f"input number {position} is out of range 1..{count} in "
                        f"{option!r}",
                        param_hint="--scenario",
                    )
            elif basenames.get(prefix) is not None:
                position = basenames[prefix]
        if position is None:
            if bare is not None and bare != option:
                raise typer.BadParameter(
                    "only one bare selector (applying to all databases) can be "
                    f"given; got {bare!r} and {option!r}",
                    param_hint="--scenario",
                )
            bare = option
        else:
            targeted[position] = rest
    if targeted and bare is not None:
        return {i: targeted.get(i, bare) for i in range(1, count + 1)}
    if targeted:
        return targeted
    return bare


def _load_rules(rules_file: Path | None, identity: list[str], ignore: list[str]):
    """Build :class:`ComparisonRules` from a TOML file and command-line overrides."""
    from .structure.errors import RulesError
    from .structure.rules import ComparisonRules

    try:
        rules = (
            ComparisonRules.from_toml(str(rules_file))
            if rules_file is not None
            else ComparisonRules.default()
        )
        if identity or ignore:
            rules = rules.with_overrides(
                identity_attributes=identity or None,
                ignored_attributes=ignore or None,
            )
    except RulesError as exc:
        hint = "--rules" if rules_file is not None else "--identity-attr/--ignore-attr"
        raise typer.BadParameter(str(exc), param_hint=hint) from exc
    return rules


def _fail(exc: BaseException, debug: bool) -> None:
    """Report an error on stderr and exit 2 (or re-raise with ``--debug``)."""
    if debug:
        raise exc
    from .structure.errors import StructureError

    if isinstance(exc, StructureError):
        typer.echo(f"error: {exc}", err=True)
    else:
        typer.echo(f"error: {type(exc).__name__}: {exc}", err=True)
    raise typer.Exit(code=EXIT_ERROR)


@app.command()
def scenarios(
    databases: Annotated[
        list[Path],
        typer.Argument(
            metavar="DB...",
            help="One or more local database directories (containing *.basex files).",
        ),
    ],
    counts: Annotated[
        bool,
        typer.Option(
            "--counts",
            help="Also count the elements of each scenario (a full traversal).",
        ),
    ] = False,
    java_max_memory: Annotated[
        str, typer.Option("--java-max-memory", help="JVM heap limit (-Xmx).")
    ] = "4g",
    debug: Annotated[
        bool, typer.Option("--debug", help="Show tracebacks instead of messages.")
    ] = False,
) -> None:
    """List the scenario runs stored in local GCAM output databases.

    Use the ``NAME@DATE`` shown here with ``diff --scenario`` when a database
    holds several runs. Exit code 0 on success, 2 on any error. Remote
    databases are not supported.
    """
    db_paths = _check_databases(databases, minimum=1)
    _configure_structure_logging(False, False, debug)
    from .structure import api

    rows: list[list[str]] = []
    try:
        for position, path in enumerate(db_paths, start=1):
            for ref in api.list_scenarios(
                path, with_counts=counts, java_max_memory=java_max_memory
            ):
                row = [str(position), path, ref.name, ref.date, ref.version or ""]
                if counts:
                    row.append(f"{ref.element_count:,}" if ref.element_count else "")
                rows.append(row)
    except Exception as exc:
        _fail(exc, debug)
    headers = ["#", "database", "scenario", "date", "GCAM version"]
    if counts:
        headers.append("elements")
    if not rows:
        typer.echo("no scenarios found")
        return
    widths = [
        max(len(headers[i]), *(len(row[i]) for row in rows))
        for i in range(len(headers))
    ]
    typer.echo("  ".join(h.ljust(w) for h, w in zip(headers, widths, strict=True)))
    for row in rows:
        typer.echo("  ".join(c.ljust(w) for c, w in zip(row, widths, strict=True)))


@app.command()
def diff(
    databases: Annotated[
        list[Path],
        typer.Argument(
            metavar="DB DB...",
            help=(
                "Two or more local database directories (each containing *.basex "
                "files). The first is the baseline unless --baseline is given."
            ),
        ),
    ],
    output: Annotated[
        Path | None,
        typer.Option(
            "-o",
            "--output",
            dir_okay=False,
            help=(
                "Write the Markdown report here; when lists exceed --inline-limit an "
                "appendix <stem>.appendix.md is written beside it. Default: stdout "
                "(report followed by the appendix)."
            ),
        ),
    ] = None,
    baseline: Annotated[
        str,
        typer.Option(
            "--baseline",
            help="Baseline database: its 1-based position among DB arguments, or its path.",
        ),
    ] = "1",
    scenario: Annotated[
        list[str] | None,
        typer.Option(
            "-s",
            "--scenario",
            help=(
                "Scenario run to compare: NAME or NAME@DATE, optionally prefixed with "
                "IDX= or BASENAME= to target one database. Repeatable; a bare selector "
                "applies to every database. Needed only when a database holds several runs."
            ),
        ),
    ] = None,
    rules: Annotated[
        Path | None,
        typer.Option(
            "--rules",
            exists=True,
            dir_okay=False,
            help="TOML file with comparison rules (identity, excluded and ignored attributes).",
        ),
    ] = None,
    identity_attr: Annotated[
        list[str] | None,
        typer.Option(
            "--identity-attr",
            help="Attribute that identifies elements (repeatable; replaces the default list).",
        ),
    ] = None,
    ignore_attr: Annotated[
        list[str] | None,
        typer.Option(
            "--ignore-attr",
            help="Metadata attribute to ignore everywhere (repeatable).",
        ),
    ] = None,
    java_max_memory: Annotated[
        str, typer.Option("--java-max-memory", help="JVM heap limit (-Xmx).")
    ] = "4g",
    work_dir: Annotated[
        Path | None,
        typer.Option(
            "--work-dir",
            file_okay=False,
            help=(
                "Directory for the structure indexes (about the size of each database). "
                "Kept and reused across runs; default: a temporary directory."
            ),
        ),
    ] = None,
    no_cache: Annotated[
        bool,
        typer.Option(
            "--no-cache",
            help="Re-extract even when a matching index exists in --work-dir.",
        ),
    ] = False,
    json_path: Annotated[
        Path | None,
        typer.Option(
            "--json",
            dir_okay=False,
            help="Also write the machine-readable result as JSON.",
        ),
    ] = None,
    inline_limit: Annotated[
        int,
        typer.Option(
            "--inline-limit",
            min=0,
            help=(
                "Findings per comparison (and matrix rows) listed in the report itself; "
                "above this they go to the appendix and the report shows aggregates."
            ),
        ),
    ] = 2000,
    timestamp: Annotated[
        bool,
        typer.Option(
            "--timestamp",
            help="Append a 'Generated:' line with the current time (off: byte-stable output).",
        ),
    ] = False,
    quiet: Annotated[
        bool, typer.Option("--quiet", help="No progress messages on stderr.")
    ] = False,
    verbose: Annotated[
        bool, typer.Option("--verbose", help="Informational log messages on stderr.")
    ] = False,
    debug: Annotated[
        bool, typer.Option("--debug", help="Debug log messages and full tracebacks.")
    ] = False,
) -> None:
    """Compare the structure of two or more local GCAM output databases.

    Reports regions, sectors, technologies, inputs, years and other elements
    (and their metadata attributes) present in one database but not another,
    as a Markdown report; numeric values are ignored. Exit code 0: no
    structural differences; 1: differences found; 2: any error. Remote
    databases are not supported.
    """
    db_paths = _check_databases(databases, minimum=2)
    baseline_position = _resolve_baseline(baseline, db_paths)
    selectors = _parse_scenario_options(scenario or [], db_paths)
    rules_obj = _load_rules(rules, identity_attr or [], ignore_attr or [])
    _configure_structure_logging(quiet, verbose, debug)
    from .structure import api, markdown

    def progress(message: str) -> None:
        if not quiet:
            typer.echo(message, err=True)

    try:
        result = api.compare_databases(
            db_paths,
            baseline=baseline_position,
            scenarios=selectors,
            rules=rules_obj,
            work_dir=work_dir,
            use_cache=not no_cache,
            java_max_memory=java_max_memory,
            progress=progress,
        )
        generated = (
            datetime.now().astimezone().isoformat(timespec="seconds")
            if timestamp
            else None
        )
        if output is not None:
            for written in markdown.write_report(
                result, output, inline_limit=inline_limit, generated_at=generated
            ):
                progress(f"wrote {written}")
        else:
            sys.stdout.write(
                markdown.render_markdown(
                    result, generated_at=generated, inline_limit=inline_limit
                )
            )
            if markdown.needs_appendix(result, inline_limit):
                sys.stdout.write("\n")
                for line in markdown.iter_appendix(result):
                    sys.stdout.write(line)
                    sys.stdout.write("\n")
            sys.stdout.flush()
        if json_path is not None:
            api.write_json(result, json_path)
            progress(f"wrote {json_path}")
    except typer.Exit:
        raise
    except Exception as exc:
        _fail(exc, debug)
    raise typer.Exit(code=EXIT_DIFFERENCES if result.has_differences else 0)


if __name__ == "__main__":
    app()
