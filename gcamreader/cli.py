"""Command line interface for running queries against GCAM databases.

This module exposes a :mod:`typer` application with ``local`` and ``remote``
subcommands that run the queries contained in a GCAM queries XML file and save
the results as pipe-delimited CSV files.
"""

from __future__ import annotations

import multiprocessing
import subprocess
from pathlib import Path
from typing import Annotated

import typer

from . import __version__
from .querymi import LocalDBConn, RemoteDBConn, parse_batch_query

app = typer.Typer(
    help="Run queries against a GCAM scenario database and save outputs as CSV.",
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


if __name__ == "__main__":
    app()
