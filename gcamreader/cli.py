from pathlib import Path
from .querymi import LocalDBConn, RemoteDBConn, parse_batch_query
import click
from click_default_group import DefaultGroup
import sys
import subprocess
import multiprocessing


@click.group(
    cls=DefaultGroup,
    default="local",
    default_if_no_args=True,
)
@click.version_option()
def cli():
    """
    Run queries against a gcam scenario database.

        Saves outputs as .csv

    Documentation: https://github.com/JGCRI/gcamreader/
    """


@cli.command(name="local")
@click.option(
    "-d",
    "--database_path",
    type=click.Path(exists=True, file_okay=False, readable=True, path_type=Path),
    required=True,
    help="path to database file (i.e. parent of *.basex dir)",
)
@click.option(
    "-q",
    "--query_path",
    type=click.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
    required=True,
    help="path to xml with queries to run (i.e: Main_queries.xml)",
)
@click.option(
    "-o",
    "--output_path",
    type=click.Path(exists=True, file_okay=False, writable=True, path_type=Path),
    help="path to output (i.e. where .csv files should be created)",
)
@click.option("-f", "--force", type=bool, default=False, help="overwrite existing .csv in output path")
def local(database_path: Path, query_path: Path, output_path: Path, force: bool):
    """
    query gcam scenario databases
    """

    click.echo(f"opening: {database_path.absolute()}", err=True)
    if not list(database_path.glob("*.basex")):
        click.echo(f"basex files missing: {database_path}", err=True)
        return False
    parent = str(database_path.parent)
    name = database_path.name

    # establish database connection - uses ModelInterface.jar
    conn = LocalDBConn(parent, name)

    execute(conn, query_path, output_path, force)


@cli.command(name="remote")
@click.option(
    "-u",
    "--username",
    type=str,
    required=True,
    help="username of remote server authentication",
)
@click.option(
    "-w",
    "--password",
    type=str,
    prompt=True,
    hide_input=True,
    help="password of remote server authentication",
)
@click.option(
    "-n",
    "--hostname",
    type=str,
    default="localhost",
    required=True,
    help="hostname of remote server",
)
@click.option(
    "-p",
    "--port",
    type=int,
    default=8984,
    required=True,
    help="port on remote server",
)
@click.option(
    "-d",
    "--database_name",
    type=str,
    required=True,
    help="name of database to query (i.e. parent of *.basex dir)",
)
@click.option(
    "-q",
    "--query_path",
    type=click.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
    required=True,
    help="path to xml with queries to run (i.e: Main_queries.xml)",
)
@click.option(
    "-o",
    "--output_path",
    type=click.Path(exists=True, file_okay=False, writable=True, path_type=Path),
    help="path to output (i.e. where .csv files should be created)",
)
@click.option("-f", "--force", type=bool, default=False, help="overwrite existing .csv in output path")
def remote(
    username: str,
    password: str,
    hostname: str,
    port: int,
    database_name: str,
    query_path: Path,
    output_path: Path,
    force: bool
):
    """
    query a remote server containing gcam scenario databases
    """

    # establish database connection - uses ModelInterface.jar
    conn = RemoteDBConn(
        username=username,
        password=password,
        address=hostname,
        port=port,
        dbfile=database_name,
    )

    execute(conn, query_path, output_path, force)


def save(data):
    conn, query, save_to, force = map(data.get, ["conn", "query", "save_to", "force"])
    if save_to.exists():
        click.echo(f"output exists: {save_to.name}", err=True)
        if not force:
            click.echo(f"skipping: {save_to.name}", err=True)
            return
    click.echo(f"running: {query.title}", err=True)
    try:
        df = conn.runQuery(query)
    except subprocess.CalledProcessError as e:
        click.echo(f"failed: {query.title}", err=True)
        return
    if df is None:
        click.echo(f"empty: {query.title}", err=True)
        return
    df.to_csv(save_to, index=False, sep="|")
    click.echo(f"saved: {save_to.absolute()}", err=True)


def execute(conn, query_path: Path, output_path: Path, force: bool):
    # parse query xml
    click.echo(f"parsing: {query_path.name}", err=True)
    queries = []
    for query in parse_batch_query(str(query_path)):
        data = {}
        data["conn"] = conn
        data["query"] = query
        data["save_to"] = (
            output_path / f"{str(query.title).replace(' ', '_').lower()}.csv"
        )
        data["force"] = force
        queries.append(data)
    with multiprocessing.Pool() as pool:
        pool.map(save, queries)
    click.echo(f"extract complete", err=True)
