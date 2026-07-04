"""Functions and classes for running GCAM output database queries.

This module provides the public API for ``gcamreader``: a :class:`Query`
container, a :func:`parse_batch_query` helper, local and remote database
connection classes (:class:`LocalDBConn` and :class:`RemoteDBConn`), and the
:func:`importdata` convenience function. Query results are returned as pandas
DataFrames.
"""

from __future__ import annotations

import os
import os.path as path
import re
import subprocess as sp
import sys
import tempfile
from importlib import resources
from io import StringIO

import lxml.etree as ET
import pandas as pd

# Structure to hold a query (i.e., the stuff we send to the model interface).


class Query:
    """A parsed GCAM query definition.

    Attributes:
        querystr: String representation of the query XML.
        regions: List of region names parsed from the query, or ``None`` when
            the query does not specify regions.
        title: The query title.

    Examples:
        Build a query directly from an XML string::

            >>> import gcamreader
            >>> xml = (
            ...     '<supplyDemandQuery title="CO2 emissions">'
            ...     '<region name="USA"/>'
            ...     '</supplyDemandQuery>'
            ... )
            >>> query = gcamreader.Query(xml)
            >>> query.title
            'CO2 emissions'
            >>> query.regions
            ['USA']
    """

    def __init__(self, xmlin: str | ET._Element) -> None:
        """Initialize a query structure from an XML definition.

        Args:
            xmlin: XML query definition. This can be either a string containing
                the query XML or an already parsed lxml element.

        Examples:
            >>> import gcamreader
            >>> query = gcamreader.Query(
            ...     '<aQuery title="Population"><region name="China"/></aQuery>'
            ... )
            >>> query.title
            'Population'
        """
        if isinstance(xmlin, str):
            parser = ET.XMLParser(strip_cdata=False)
            query = ET.XML(xmlin, parser)
        else:
            query = xmlin

        self.querystr: str = ET.tostring(query, encoding="unicode")

        regions = query.findall("region")
        if len(regions) == 0:
            self.regions: list[str] | None = None
        else:
            self.regions = [e.get("name") for e in regions]

        self.title: str | None = query.get("title")


def parse_batch_query(filename: str) -> list[Query]:
    """Parse a GCAM query file into a list of :class:`Query` objects.

    Args:
        filename: Path to a GCAM batch query XML file.

    Returns:
        A list of :class:`Query` objects, one per ``title`` element found.

    Examples:
        Parse the sample land-allocation query bundled with the package::

            >>> import os
            >>> import gcamreader
            >>> query_file = os.path.join(
            ...     gcamreader.sample_data_dir(),
            ...     "queries",
            ...     "query_land_reg32_basin235_gcam5p0.xml",
            ... )
            >>> queries = gcamreader.parse_batch_query(query_file)
            >>> queries[0].title
            'Crop Land Allocation'
    """
    parser = ET.XMLParser(strip_cdata=False)
    root = ET.parse(filename, parser)

    queries = root.xpath("//*[@title]")

    return [Query(q) for q in queries]


def _modelinterface_dir() -> str:
    """Return the filesystem path to the bundled ModelInterface directory.

    Returns:
        The absolute path to the ``ModelInterface`` package data directory.
    """
    return str(resources.files("gcamreader") / "ModelInterface")


def sample_data_dir() -> str:
    """Return the filesystem path to the bundled sample data directory.

    The sample data directory contains a small example BaseX database, an
    example query, and a reference output, all used by the test suite and
    examples.

    Returns:
        The absolute path to the ``data`` package data directory.

    Examples:
        >>> import os
        >>> import gcamreader
        >>> data_dir = gcamreader.sample_data_dir()
        >>> os.path.isdir(data_dir)
        True
    """
    return str(resources.files("gcamreader") / "data")


# Default class path for the GCAM model interface. On unix this produces
# something like:
#     /foo/bar/baz/jars/*:/foo/bar/baz/ModelInterface.jar
_mifiles_dir = _modelinterface_dir()


_default_miclasspath = (
    f"{_mifiles_dir}{path.sep}jars{path.sep}*"
    f"{path.pathsep}{_mifiles_dir}{path.sep}ModelInterface.jar"
)


# Helper functions for formatting and parsing queries.
def _querylist(items: str | list[str] | None) -> str:
    """Format a region or scenario list as an XQuery sequence literal.

    Args:
        items: A list of item names, a single item string, or ``None``.

    Returns:
        A string of the form ``('item1','item2',...,'itemN')``, or ``()`` when
        ``items`` is empty or ``None``.
    """
    if items is None or len(items) == 0:
        return "()"
    if isinstance(items, str):
        # items was supposed to be a list, so convert a string into a
        # single-element list.
        items = [items]
    return "('" + "','".join(items) + "')"


def _parserslt(
    txt: str,
    warn_empty: bool,
    title: str | None,
    stderr: str = "",
) -> pd.DataFrame | None:
    """Parse a model interface CSV result string into a pandas DataFrame.

    When a ``value`` column is present, rows sharing all non-value columns are
    summed. Aggregation uses ``dropna=False`` so that rows containing missing
    values in one or more grouping columns are retained rather than silently
    dropped.

    Args:
        txt: The text returned by the query.
        warn_empty: Whether to emit a warning when the result is empty.
        title: Query title used in warning messages.
        stderr: Standard error output from the model interface, used in
            warning messages.

    Returns:
        A DataFrame of the parsed results, or ``None`` if the result was empty.
    """
    from pandas.errors import EmptyDataError

    buf = StringIO(txt)

    try:
        rslt = pd.read_csv(buf)
        # The value column name is always assumed to be "value".
        value_col_name = "value"
        cols = rslt.columns
        # If we have a value column then aggregate, otherwise it may just be
        # listing the scenario information and no aggregation is necessary.
        if cols.str.contains(value_col_name).any():
            rslt = rslt.groupby(
                cols.drop(value_col_name).to_list(), as_index=False, dropna=False
            ).sum()
    except EmptyDataError:
        if warn_empty:
            sys.stderr.write("Model interface returned empty string.\n")
            sys.stderr.write(f"Query title: \n\t{title}\n")
            sys.stderr.write(f"Model interface stderr output:\n\t{stderr}\n")
        return None
    else:
        return rslt


def _runmi(cmd: list[str], querystr: str) -> tuple[str, str]:
    """Run a model interface subprocess and return its stdout and stderr.

    Args:
        cmd: The command and arguments to execute.
        querystr: The query string, used in error messages.

    Returns:
        A tuple of ``(stdout, stderr)`` produced by the command.

    Raises:
        subprocess.CalledProcessError: If the command exits with a non-zero
            return code.
    """
    try:
        mireturn = sp.run(cmd, capture_output=True, check=True, encoding="UTF-8")
        return mireturn.stdout, mireturn.stderr
    except sp.CalledProcessError as e:
        sys.stderr.write("Model interface run failed.\n")
        sys.stderr.write("Command line: \n\t{}\n".format(" ".join(cmd)))
        sys.stderr.write(f"Query string: \n\t{querystr}\n")
        sys.stderr.write("Model interface stderr output:\n")
        sys.stderr.write(e.stderr)
        raise


class LocalDBConn:
    """Connection to a local GCAM database.

    A local database connection comprises a name and location for the database,
    along with a class path for the Java program used to extract data and some
    options passed to the functions that run the queries.

    Attributes:
        dbpath: Absolute path to the directory containing the GCAM database.
        dbfile: Name of the GCAM database.
        suppress_gabble: Whether to suppress model interface console output.
        maxMemory: Maximum memory passed to the Java runtime.
        miclasspath: Java class path for the GCAM model interface.

    Examples:
        Open the bundled sample database and run a query (requires a Java
        runtime)::

            >>> import os
            >>> import gcamreader
            >>> data_dir = gcamreader.sample_data_dir()
            >>> conn = gcamreader.LocalDBConn(  # doctest: +SKIP
            ...     data_dir, "sample_basexdb"
            ... )
            >>> query = gcamreader.parse_batch_query(  # doctest: +SKIP
            ...     os.path.join(
            ...         data_dir,
            ...         "queries",
            ...         "query_land_reg32_basin235_gcam5p0.xml",
            ...     )
            ... )[0]
            >>> df = conn.runQuery(query)  # doctest: +SKIP
    """

    def __init__(
        self,
        dbpath: str,
        dbfile: str,
        suppress_gabble: bool = True,
        miclasspath: str | None = None,
        validatedb: bool = True,
        maxMemory: str = "4g",
    ) -> None:
        """Initialize a local database connection.

        Args:
            dbpath: Directory containing the GCAM database.
            dbfile: Name of the GCAM database.
            suppress_gabble: If ``True``, suppress the console output normally
                produced by the model interface; otherwise display it.
            miclasspath: Java class path for the GCAM model interface. The
                default points to the copy installed with this package.
            validatedb: If ``True``, check that a simple query works on the
                connection; otherwise skip the check.
            maxMemory: Maximum memory for the Java runtime used to run queries.
                The default is ``'4g'``. The numeric value may be suffixed with
                ``"g"`` for gigabytes or ``"m"`` for megabytes.

        Raises:
            IOError: If ``validatedb`` is ``True`` and the database cannot be
                validated.
        """
        self.dbpath = path.abspath(dbpath)
        self.dbfile = dbfile
        self.suppress_gabble = suppress_gabble
        self.maxMemory = maxMemory

        if miclasspath is None:
            self.miclasspath = _default_miclasspath
        else:
            self.miclasspath = path.abspath(miclasspath)

        if validatedb:
            # Print the scenarios in the database. This also checks whether the
            # database is working.
            dbscen = self.listScenariosInDB()

            if dbscen is None:
                errmsg = "Failed to validate database: " + os.path.join(
                    self.dbpath, self.dbfile
                )
                sys.stderr.write(errmsg + "\n")
                raise OSError(errmsg)

            sys.stdout.write(
                "Database scenarios: {}\n".format(", ".join(dbscen["name"]))
            )

    def runQuery(
        self,
        query: Query,
        scenarios: str | list[str] | None = None,
        regions: str | list[str] | None = None,
        warn_empty: bool = True,
    ) -> pd.DataFrame | None:
        """Run a query on this connection.

        Run the supplied query and return the result as a pandas DataFrame.
        This query will generally have been parsed from a GCAM queries XML
        file. The query can contain a list of regions parsed from the XML file.
        If present, query results will be filtered to this list of regions;
        otherwise, all regions will be included. The ``regions`` argument, if
        present, overrides the region filters parsed from the XML. Passing an
        empty list removes the region filter entirely.

        Args:
            query: A :class:`Query` object.
            scenarios: A list of scenarios to include in query results. If
                ``None``, the last scenario in the database is used.
            regions: A list of regions to filter query results to. See the
                description for the behavior when ``regions`` is ``None``.
            warn_empty: Whether to issue a warning to stderr if the result is
                empty.

        Returns:
            A DataFrame of the query results, or ``None`` if the result was
            empty.

        Examples:
            >>> import gcamreader  # doctest: +SKIP
            >>> conn = gcamreader.LocalDBConn(dbpath, dbfile)  # doctest: +SKIP
            >>> query = gcamreader.parse_batch_query(  # doctest: +SKIP
            ...     "queries.xml"
            ... )[0]
            >>> df = conn.runQuery(  # doctest: +SKIP
            ...     query, scenarios=["Reference"], regions=["USA"]
            ... )
        """
        # Convert region and scenario lists to strings of the form
        # ('item1', 'item2', ..., 'itemN').
        xqscen = _querylist(scenarios)
        if regions is None:
            regions = query.regions
        xqrgn = _querylist(regions)

        # Convert the suppress_gabble flag to a string.
        sg = str(self.suppress_gabble)

        # Strip newlines from the query string.
        querystr = re.sub("\n", "", query.querystr)

        # Write the query to a temporary file which the command then references
        # to work around command-length limits on Windows. The temporary file
        # may not be visible to the rest of the system until it is closed, so
        # we set delete=False and remove it ourselves.
        queryTempFile = tempfile.NamedTemporaryFile(mode="w", delete=False)

        try:
            cmd = [
                "java",
                "-cp",
                self.miclasspath,
                "-Xmx" + self.maxMemory,
                "-Dorg.basex.DBPATH=" + self.dbpath,
                "-DModelInterface.SUPPRESS_OUTPUT=" + sg,
                "org.basex.BaseX",
                "-smethod=csv",
                "-scsv=header=yes,format=xquery",
                "-i",
                self.dbfile,
                "RUN",
                queryTempFile.name,
            ]

            queryTempFile.write(
                "import module namespace mi = "
                "'ModelInterface.ModelGUI2.xmldb.RunMIQuery';"
                "mi:runMIQuery(" + querystr + "," + xqscen + "," + xqrgn + ")"
            )
            queryTempFile.close()

            miout, mierr = _runmi(cmd, query.querystr)

            return _parserslt(miout, warn_empty, query.title, mierr)

        finally:
            # Clean up the query temp file now that the query has finished.
            os.remove(queryTempFile.name)

    def listScenariosInDB(self) -> pd.DataFrame | None:
        """List the scenarios contained in the GCAM database.

        To run a query users typically need to know the names of the scenarios
        in the database. The result is a table with columns ``name``, ``date``,
        ``version``, and ``fqName``. The ``name`` and ``date`` are exactly as
        specified in the database. The ``fqName`` is the fully qualified
        scenario name which can be used in the ``scenarios`` argument of
        :meth:`runQuery` to disambiguate scenario names. The ``version`` is the
        GCAM version tag used to generate the scenario.

        Returns:
            A DataFrame of scenarios, or ``None`` if the query returned no
            results.

        Examples:
            >>> import gcamreader  # doctest: +SKIP
            >>> conn = gcamreader.LocalDBConn(dbpath, dbfile)  # doctest: +SKIP
            >>> scenarios = conn.listScenariosInDB()  # doctest: +SKIP
            >>> list(scenarios["name"])  # doctest: +SKIP
            ['Reference']
        """
        querystr = (
            "let $scns := collection()/scenario return document{ element csv { "
            "for $scn in $scns return element record { element name  { text { "
            "$scn/@name } }, element date { text { $scn/@date } }, element "
            "version { text{ $scn/model-version/text() } } } } }"
        )
        cmd = [
            "java",
            "-cp",
            self.miclasspath,
            "-Xmx" + self.maxMemory,
            "-Dorg.basex.DBPATH=" + self.dbpath,
            "org.basex.BaseX",
            "-smethod=csv",
            "-scsv=header=yes",
            "-i",
            self.dbfile,
            querystr,
        ]

        miout, mierr = _runmi(cmd, querystr)

        scen_df = _parserslt(miout, False, "List Scenarios", mierr)
        if scen_df is not None:
            scen_df["fqName"] = scen_df["name"] + " " + scen_df["date"]

        return scen_df


# Remote connection.
class RemoteDBConn:
    """Connection to a remote GCAM database.

    A remote database connection communicates with a webserver using the BaseX
    REST API. The connection requires a server address, server port, username
    and password (configured in the server setup), and the database name on the
    remote server.

    Attributes:
        dbfile: The database file to query.
        username: The username configured for the BaseX server.
        password: The password configured for the BaseX server.
        address: The server address (URL).
        port: The port the server is running on.

    Examples:
        Connect to a BaseX server and run a query (requires a running server)::

            >>> import gcamreader
            >>> conn = gcamreader.RemoteDBConn(  # doctest: +SKIP
            ...     dbfile="my_database",
            ...     username="user",
            ...     password="secret",
            ...     address="localhost",
            ...     port=8984,
            ... )
            >>> query = gcamreader.parse_batch_query(  # doctest: +SKIP
            ...     "queries.xml"
            ... )[0]
            >>> df = conn.runQuery(query)  # doctest: +SKIP
    """

    def __init__(
        self,
        dbfile: str,
        username: str,
        password: str,
        address: str = "localhost",
        port: int = 8984,
        validatedb: bool = True,
    ) -> None:
        """Initialize a remote database connection.

        Args:
            dbfile: The database file to query.
            username: The username configured for the BaseX server.
            password: The password configured for the BaseX server.
            address: The server address (URL). The default is ``"localhost"``.
            port: The port the server is running on. The default is ``8984``.
            validatedb: If ``True``, check that a simple query works on the
                connection; otherwise skip the check.

        Raises:
            Exception: If ``validatedb`` is ``True`` and the database cannot be
                validated.
        """
        self.dbfile = dbfile
        self.username = username
        self.password = password
        self.address = address
        self.port = port

        if validatedb:
            # Print the scenarios in the database. This also checks whether the
            # database is working.
            dbscen = self.listScenariosInDB()
            if dbscen is None:
                sys.stderr.write("Failed to validate database.\n")
                raise Exception(
                    "Failed to validate database: "
                    + address
                    + ":"
                    + str(port)
                    + " user= "
                    + username
                    + " file= "
                    + dbfile
                )
            sys.stdout.write(
                "Database scenarios: {}\n".format(", ".join(dbscen["name"]))
            )

    def runQuery(
        self,
        query: Query,
        scenarios: str | list[str] | None = None,
        regions: str | list[str] | None = None,
        warn_empty: bool = True,
    ) -> pd.DataFrame | None:
        """Run a query on this connection.

        Run the supplied query and return the result as a pandas DataFrame.
        This query will generally have been parsed from a GCAM queries XML
        file. The query can contain a list of regions parsed from the XML file.
        If present, query results will be filtered to this list of regions;
        otherwise, all regions will be included. The ``regions`` argument, if
        present, overrides the region filters parsed from the XML. Passing an
        empty list removes the region filter entirely.

        Args:
            query: A :class:`Query` object.
            scenarios: A list of scenarios to include in query results. If
                ``None``, the last scenario in the database is used.
            regions: A list of regions to filter query results to. See the
                description for the behavior when ``regions`` is ``None``.
            warn_empty: Whether to issue a warning to stderr if the result is
                empty.

        Returns:
            A DataFrame of the query results, or ``None`` if the result was
            empty.

        Examples:
            >>> import gcamreader  # doctest: +SKIP
            >>> conn = gcamreader.RemoteDBConn(  # doctest: +SKIP
            ...     "my_database", "user", "secret"
            ... )
            >>> query = gcamreader.parse_batch_query(  # doctest: +SKIP
            ...     "queries.xml"
            ... )[0]
            >>> df = conn.runQuery(query, regions=["USA"])  # doctest: +SKIP
        """
        from requests import post

        xqscen = _querylist(scenarios)
        if regions is None:
            regions = query.regions
        xqrgn = _querylist(regions)

        javastr = "".join(
            [
                "import module namespace mi = "
                "'ModelInterface.ModelGUI2.xmldb.RunMIQuery';",
                f"mi:runMIQuery({query.querystr}, {xqscen}, {xqrgn})",
            ]
        )
        # Handle nested CDATA tags.
        javastr = javastr.replace("]]>", "]]]]><![CDATA[>")

        restquery = " ".join(
            [
                '<rest:query xmlns:rest="http://basex.org/rest">',
                "<rest:text><![CDATA[",
                javastr,
                "]]></rest:text>",
                '<rest:parameter name="method" value="csv"/>',
                '<rest:parameter name="media-type" value="text/csv"/>',
                '<rest:parameter name="csv" value="header=yes,format=xquery"/>',
                "</rest:query>",
            ]
        )

        url = "".join(
            ["http://", self.address, ":", str(self.port), "/rest/", self.dbfile]
        )

        r = post(url, auth=(self.username, self.password), data=restquery)
        r.raise_for_status()  # Falls through if status is OK.

        return _parserslt(r.text, warn_empty, query.title)

    def listScenariosInDB(self) -> pd.DataFrame | None:
        """List the scenarios contained in the GCAM database.

        To run a query users typically need to know the names of the scenarios
        in the database. The result is a table with columns ``name``, ``date``,
        ``version``, and ``fqName``. The ``name`` and ``date`` are exactly as
        specified in the database. The ``fqName`` is the fully qualified
        scenario name which can be used in the ``scenarios`` argument of
        :meth:`runQuery` to disambiguate scenario names. The ``version`` is the
        GCAM version tag used to generate the scenario.

        Returns:
            A DataFrame of scenarios, or ``None`` if the query returned no
            results.

        Examples:
            >>> import gcamreader  # doctest: +SKIP
            >>> conn = gcamreader.RemoteDBConn(  # doctest: +SKIP
            ...     "my_database", "user", "secret"
            ... )
            >>> scenarios = conn.listScenariosInDB()  # doctest: +SKIP
            >>> list(scenarios["name"])  # doctest: +SKIP
            ['Reference']
        """
        from requests import post

        restquery = str.join(
            "\n",
            [
                '<rest:query xmlns:rest="http://basex.org/rest">',
                "<rest:text><![CDATA[",
                "let $scns := collection()/scenario return document{ element "
                "csv { for $scn in $scns return element record { element name  "
                "{ text { $scn/@name } }, element date { text { $scn/@date } }, "
                "element version { text{ $scn/model-version/text() } } } } }",
                "]]></rest:text>",
                '<rest:parameter name="method" value="csv"/>',
                '<rest:parameter name="media-type" value="text/csv"/>',
                '<rest:parameter name="csv" value="header=yes"/>',
                "</rest:query>",
            ],
        )

        url = "".join(
            ["http://", self.address, ":", str(self.port), "/rest/", self.dbfile]
        )

        r = post(url, auth=(self.username, self.password), data=restquery)
        r.raise_for_status()  # Falls through if status is OK.

        scen_df = _parserslt(r.text, False, "List Scenarios")
        if scen_df is not None:
            scen_df["fqName"] = scen_df["name"] + " " + scen_df["date"]

        return scen_df


def importdata(
    dbspec: str | LocalDBConn | RemoteDBConn,
    queries: str | list[Query],
    scenarios: str | list[str] | None = None,
    regions: str | list[str] | None = None,
    warn_empty: bool = False,
    suppress_gabble: bool = True,
    miclasspath: str | None = None,
) -> dict[str | None, pd.DataFrame | None]:
    """Run a selection of queries against a database connection.

    Run all of the queries in a GCAM queries file against a database connection
    and return a dictionary of query results indexed by query title.

    Args:
        dbspec: A database connection, or a string with the filename for a GCAM
            database.
        queries: Filename of a GCAM queries XML file, or the output of
            :func:`parse_batch_query` run on such a file.
        scenarios: List of scenario names to include in the queries.
        regions: List of regions to include in the queries.
        warn_empty: Whether to print a warning if a query returns empty. The
            default is ``False``.
        suppress_gabble: Whether to suppress model interface console output
            when a connection is created from a filename.
        miclasspath: Java class path for the GCAM model interface when a
            connection is created from a filename.

    Returns:
        A dictionary mapping each query title to its result DataFrame (or
        ``None`` for empty results).

    Examples:
        Run every query in a batch file against a local database and access a
        result by its title (requires a Java runtime)::

            >>> import gcamreader  # doctest: +SKIP
            >>> results = gcamreader.importdata(  # doctest: +SKIP
            ...     "/path/to/database_basexdb",
            ...     "queries.xml",
            ...     scenarios=["Reference"],
            ... )
            >>> results["Crop Land Allocation"].head()  # doctest: +SKIP

        A previously created connection may be passed instead of a filename::

            >>> conn = gcamreader.LocalDBConn(dbpath, dbfile)  # doctest: +SKIP
            >>> results = gcamreader.importdata(  # doctest: +SKIP
            ...     conn, "queries.xml"
            ... )
    """
    if isinstance(dbspec, str):
        dbdir = path.dirname(dbspec)
        dbname = path.basename(dbspec)
        dbcon: LocalDBConn | RemoteDBConn = LocalDBConn(
            dbdir, dbname, suppress_gabble, miclasspath
        )
    else:
        dbcon = dbspec

    if isinstance(queries, str):
        queries = parse_batch_query(queries)

    queryrslts: dict[str | None, pd.DataFrame | None] = {}
    for query in queries:
        qr = dbcon.runQuery(query, scenarios, regions, warn_empty)
        queryrslts[query.title] = qr

    return queryrslts
