"""Functions and classes for running GCAM output DB queries"""

import sys

if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO

import os
import os.path as path
import pkg_resources
import tempfile
import re
import subprocess as sp
import pandas as pd
import lxml.etree as ET

### Structure to hold a query (i.e., the stuff we send to the model
### interface)


class Query:
    def __init__(self, xmlin):
        """Initialize a query structure from an XML definition.
        Arguments:
          * xmlq: XML query definition. Can be either a string or a parsed XML
            element.

        Attributes:
          * querystr: string representation of the query
          * regions: region list parsed from the query XML
          * title: query title
        """

        if xmlin.__class__ is str:
            parser = ET.XMLParser(strip_cdata=False)
            xmlq = ET.XML(xmlin, parser)
        else:
            xmlq = xmlin

        query = xmlq.find('./*[@title]')
        self.querystr = ET.tounicode(query)

        regions = xmlq.findall('region')
        if len(regions) == 0:
            self.regions = None
        else:
            self.regions = [e.get('name') for e in regions]

        self.title = query.get('title')


def parse_batch_query(filename):
    """Parse a GCAM query file into a list of Query class objects."""

    parser = ET.XMLParser(strip_cdata=False)
    root = ET.parse(filename, parser)

    queries = root.findall('aQuery')

    return [Query(q) for q in queries]



### Default class path for the GCAM model interface
### On unix this should produce something like:
###    /foo/bar/baz/jars/*:/foo/bar/baz/ModelInterface.jar
_mifiles_dir = pkg_resources.resource_filename('gcamreader', 'ModelInterface')


_default_miclasspath = (
    "{dir}{dsep}jars{dsep}*{psep}{dir}{dsep}ModelInterface.jar".format(
        dir=_mifiles_dir, 
        dsep=path.sep,          # directory separator
        psep=path.pathsep)      # path separator
)


### Helper functions for formatting and parsing queries
def _querylist(items):
    ## convert region and scenario lists to strings.  The format is
    ## ('item1', 'item2', ..., 'itemN')
    if items is None or len(items) == 0:
        return("()")
    else:
        if items.__class__ is str:
            ## items was supposed to be a list, so convert a string
            ## into a single-element list.
            items = [items]
        return("('" + "','".join(items) + "')")


def _parserslt(txt, warn_empty, title, stderr=""):
    from pandas.errors import EmptyDataError
    ## Parse the string returned by a model interface into a pandas
    ## data frame.
    ## Arguments: 
    ##     txt: the text returned by the query
    ##     warn_empty: flag for issuing warnings on empty results
    ##     title: query title (used in warning messages)
    ##     stderr: std error output from model interface (used in messages)
    buf = StringIO(txt)

    try:
        rslt = pd.read_csv(buf)
        # The value column name is always assumed to be "value"
        value_col_name = "value"
        cols = rslt.columns
        # if we have a value column then aggregate, otherwise it may
        # just be listing the scenario information and no aggregation
        # is necessary
        if cols.str.contains(value_col_name).any():
            rslt = rslt.groupby(cols.drop(value_col_name).to_list(), as_index=False).sum()
    except EmptyDataError:
        if warn_empty:
            sys.stderr.write("Model interface returned empty string.\n")
            sys.stderr.write("Query string: \n\t{}\n".format(query))
            sys.stderr.write("Model interface stderr output:\n\t{}\n".format(stderr))
        return None
    else:
        return rslt


def _runmi(cmd, querystr):

    v3_5 = 0x03050000
    try:
        if sys.hexversion >= v3_5:
            # python 3.5 or greater has the new interface
            mireturn = sp.run(cmd, stdout=sp.PIPE, stderr=sp.PIPE, check=True, encoding="UTF-8")
            miout = mireturn.stdout
            mierr = mireturn.stderr
        else:
            # Annoyingly, sp.check_output isn't safe to use with pipes for stdout and stderr, and there is no
            # way to get popen to check return codes and raise a CalledProcessError if appropriate. So, we have to
            # emulate this behavior ourselves.
            if sys.version_info[0] < 3:
                # Python 2 doesn't have the encoding parameter
                miproc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
            else:
                miproc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE,
                                  encoding="UTF-8")
            miout, mierr = miproc.communicate()
            if miproc.returncode != 0:
                raise sp.CalledProcessError(returncode=1, cmd=cmd, output=mierr)

        return miout, mierr
    
    except sp.CalledProcessError as e:
        sys.stderr.write("Model interface run failed.\n")
        sys.stderr.write("Command line: \n\t{}\n".format(' '.join(cmd)))
        sys.stderr.write("Query string: \n\t{}\n".format(querystr))
        sys.stderr.write("Model interface stderr output:\n")
        if sys.hexversion >= v3_5:
            sys.stderr.write(e.stderr)
        else:
            sys.stderr.write(e.output)
        raise
    

class LocalDBConn:
    """Connection to a local GCAM database
    
    A local database connection comprises a name and location for the
    database, along with a class path for the java program used to
    extract data and some options to be passed to the functions that
    run the queries.

    """

    def __init__(self, dbpath, dbfile, suppress_gabble=True, miclasspath = None, validatedb=True, maxMemory='4g'):
        """Initialize a local db connection.

        params:
          * dbpath:  directory containing the GCAM database
          * dbfile:  name of the GCAM database
          * suppress_gabble: Flag, if True, suppress the console output normally
                produced by the model interface.  Otherwise, display the console
                output.
          * miclasspath: Java class path for the GCAM model interface.  The default
                value points to a copy that was installed with this package.
          * validatedb: If True, check that a simple db query works on the
                connection; otherwise, don't run the check.
          * maxMemory: Sets the maximum memory for Java which will be used to run
                the queries.  The default value is '4g'.  Users may need to reduce this
                value if they are using a 32-bit Java or increase it if they suspect they are
                running out of memory (by using migabble to check the log).  Note the
                numeric value can be suffixed with "g" for Gigabyte or "m" for Megabyte.
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
            # Print the scenarios in the database.  This will also allow us to check whether the database is working
            dbscen = self.listScenariosInDB()

            if dbscen is None:
                errmsg = "Failed to validate database: " + os.path.join(self.dbpath, self.dbfile) 
                sys.stderr.write(errmsg+"\n")
                raise IOError(errmsg)

            else:
                sys.stdout.write("Database scenarios: {}\n".format(', '.join(dbscen['name'])))

    def runQuery(self, query, scenarios = None, regions = None, warn_empty = True):
        """Run a query on this connection
        
        Run the supplied query and return the result a a Pandas data
        frame.  This query will generally have been parsed from a GCAM
        queries.xml file.  The query can contain a list of regions
        parsed from the XML file. If present, query results will be
        filtered to this list of regions; otherwise, all regions will
        be included.  The regions argument, if present, will override
        the region filters parsed from the XML.  Passing an empty list
        in this argument will remove the region filter entirely.

        Arguments: 
          * query: a Query object.
          * scenarios: A list of scenarios to include in query results.  If None,
            then use the last scenario in the database.
          * regions: A list of regions to filter query results to. See description
            for what happens if regions is None
          * warn_empty: issue a warning to stderr if the query result is empty.

        """
        ## convert region and scenario lists to strings.  The format is
        ## ('item1', 'item2', ..., 'itemN')
        xqscen = _querylist(scenarios)
        if regions is None:
            regions = query.regions
        xqrgn = _querylist(regions)

        ## convert suppress_gabble flag to a string.  I believe the model
        sg = str(self.suppress_gabble)

        ## strip newlines from query string
        querystr = re.sub("\n", "", query.querystr)

        ## write the query to a temporary file which the command will then
        ## reference to work around limits on windows
        ## Note from the docs: the temporary file may not be visible to
        ## the rest of the system until it is closed.  Therefore we must
        ## set the flag not to delete on close and instead handle deleting
        ## this temporary file ourselves
        queryTempFile = tempfile.NamedTemporaryFile(mode='w', delete=False)

        try: 
            cmd =  [
                "java",
                "-cp", self.miclasspath,
                "-Xmx" + self.maxMemory,
                "-Dorg.basex.DBPATH=" + self.dbpath,
                "-DModelInterface.SUPPRESS_OUTPUT=" + sg,
                "org.basex.BaseX",
                "-smethod=csv",
                "-scsv=header=yes,format=xquery",
                "-i", self.dbfile,
                "RUN", queryTempFile.name
            ]

            queryTempFile.write("import module namespace mi = 'ModelInterface.ModelGUI2.xmldb.RunMIQuery';" + "mi:runMIQuery(" + querystr + "," + xqscen + "," + xqrgn + ")")
            queryTempFile.close()

            miout, mierr = _runmi(cmd, query.querystr)

            return _parserslt(miout, warn_empty, query.title, mierr)

        finally:
            ## clean up the query temp file now that the query has finished running
            os.remove(queryTempFile.name)

    def listScenariosInDB(self):
        """Lists the Scenarios contained in a GCAM Database

        To run a query users typically need to know the names of the scenarios in the
        database.  If they are the ones to generate the data in the first place they
        may already know this information.  Otherwise they could use this method to find
        out.  The result of this call will be a table with columns name, date, version, and fqName
        The name and date are exactly as specified in the datbase. The fqName is the fully
        qualified scenario name which a user could use in the scenarios argument of runQuery
        if they need to disambiguate scenario names. We also include the GCAM version tag that was
        used to generate the scenario in the format: ver_<major>.<minor>_r<git describe value>
        """

        querystr = "let $scns := collection()/scenario return document{ element csv { for $scn in $scns return element record { element name  { text { $scn/@name } }, element date { text { $scn/@date } }, element version { text{ $scn/model-version/text() } } } } }"
        cmd =  [
            "java",
            "-cp", self.miclasspath,
            "-Xmx" + self.maxMemory,
            "-Dorg.basex.DBPATH=" + self.dbpath,
            "org.basex.BaseX",
            "-smethod=csv",
            "-scsv=header=yes",
            "-i", self.dbfile,
            querystr
        ]

        miout, mierr = _runmi(cmd, querystr)

        scen_df = _parserslt(miout, False, "List Scenarios", mierr)
        if not scen_df is None:
            scen_df['fqName'] = scen_df['name'] + " " + scen_df['date']

        return scen_df

        

### Remote connection
class RemoteDBConn:
    """Connection to a remote GCAM database

    A remote database connection communicates with a webserver using
    the BaseX REST API.  The connection requires:
    
    * server address
    * server port
    * username and password (configured in the server setup)
    * database name on the remote server

    Instructions for setting up a BaseX server are given in the
    supplemental documentation.
    """

    def __init__(self, dbfile, username, password, address="localhost", port=8984, validatedb=True):
        """Initialize a remote database connection

        Arguments:

        * dbfile: The database file to query
        * username: The username configured for the BaseX server
        * password: The password configured for the BaseX server
        * address: The server address (URL).  The default is "localhost"
        * port: The port the server is running on.  The default is 8984.
        * validatedb: If True, check that a simple db query works on the
              connection; otherwise, don't run the check.
        """

        self.dbfile = dbfile
        self.username = username
        self.password = password
        self.address = address
        self.port = port

        if validatedb:
            ## Print the scenarios in the database.  This will also allow us to check
            ## whether the database is working
            dbscen = self.listScenariosInDB()
            if dbscen is None:
                sys.stderr.write("Failed to validate database.\n")
                raise Exception("Failed to validate database: "+address+":"+port+" user= "+username+" file= "+dbfile)
            else:
                sys.stdout.write("Database scenarios: {}\n".format(', '.join(dbscen['name'])))


    def runQuery(self, query, scenarios = None, regions = None, warn_empty = True):
        """Run a query on this connection
        
        Run the supplied query and return the result a a Pandas data
        frame.  This query will generally have been parsed from a GCAM
        queries.xml file.  The query can contain a list of regions
        parsed from the XML file. If present, query results will be
        filtered to this list of regions; otherwise, all regions will
        be included.  The regions argument, if present, will override
        the region filters parsed from the XML.  Passing an empty list
        in this argument will remove the region filter entirely.

        Arguments: 
          * query: a Query object.
          * scenarios: A list of scenarios to include in query results.  If None,
            then use the last scenario in the database.
          * regions: A list of regions to filter query results to. See description
            for what happens if regions is None
          * warn_empty: issue a warning to stderr if the query result is empty.

        """
        from requests import post
        
        xqscen = _querylist(scenarios)
        if regions is None:
            regions = query.regions
        xqrgn = _querylist(regions)

        javastr = "".join([
            "import module namespace mi = 'ModelInterface.ModelGUI2.xmldb.RunMIQuery';",
            "mi:runMIQuery({}, {}, {})".format(query.querystr, xqscen, xqrgn)
        ])
        # handle nested CDATA tags
        javastr = javastr.replace("]]>", "]]]]><![CDATA[>")
        
        restquery = " ".join([
            '<rest:query xmlns:rest="http://basex.org/rest">',
            '<rest:text><![CDATA[',
            javastr,
            ']]></rest:text>',
            '<rest:parameter name="method" value="csv"/>',
            '<rest:parameter name="media-type" value="text/csv"/>',
            '<rest:parameter name="csv" value="header=yes,format=xquery"/>',
            '</rest:query>'
        ])
        
        url = "".join([
            "http://",
            self.address,
            ":",
            str(self.port),
            "/rest/",
            self.dbfile
        ])


        r = post(url, auth=(self.username, self.password), data=restquery)
        r.raise_for_status()    # falls through if status is OK.

        return _parserslt(r.text, warn_empty, query.title)

    def listScenariosInDB(self):
        """Lists the Scenarios contained in a GCAM Database

        To run a query users typically need to know the names of the scenarios in the
        database.  If they are the ones to generate the data in the first place they
        may already know this information.  Otherwise they could use this method to find
        out.  The result of this call will be a table with columns name, date, version, and fqName
        The name and date are exactly as specified in the datbase. The fqName is the fully
        qualified scenario name which a user could use in the scenarios argument of runQuery
        if they need to disambiguate scenario names.  We also include the GCAM version tag that
        was used to generate the scenario in the format: ver_<major>.<minor>_r<git describe value>
        """
        from requests import post

        restquery = str.join("\n", [
                '<rest:query xmlns:rest="http://basex.org/rest">',
                '<rest:text><![CDATA[',
                'let $scns := collection()/scenario return document{ element csv { for $scn in $scns return element record { element name  { text { $scn/@name } }, element date { text { $scn/@date } }, element version { text{ $scn/model-version/text() } } } } }',
                ']]></rest:text>',
                '<rest:parameter name="method" value="csv"/>',
                '<rest:parameter name="media-type" value="text/csv"/>',
                '<rest:parameter name="csv" value="header=yes"/>',
                '</rest:query>'
                ])
        
        url = "".join([
            "http://",
            self.address,
            ":",
            str(self.port),
            "/rest/",
            self.dbfile
        ])


        r = post(url, auth=(self.username, self.password), data=restquery)
        r.raise_for_status()    # falls through if status is OK.

        scen_df = _parserslt(r.text, False, "List Scenarios")
        if not scen_df is None:
            scen_df['fqName'] = scen_df['name'] + " " + scen_df['date']

        return scen_df


def importdata(dbspec, queries, scenarios=None, regions=None, warn_empty=False,
               suppress_gabble=True, miclasspath=None):
    """Run a selection of queries against a database connection

    Run all of the queries in a GCAM queries file against a database
    connection and return a dictionary of query structures indeed by
    query title.

    Arguments:
      * dbspec: a database connection, or a string with the filename 
          for a GCAM database
      * queries: filename of a GCAM queries XML file, or the output 
          of parse_batch_query run on such a file.
      * scenarios: list of scenario names to include in the queries
      * regions: list of regions to include in the queries
      * warn_empty: Flag: print a warning if a query returns empty. 
          Default is false.
    """

    if dbspec.__class__ is str:
        dbdir = path.dirname(dbspec)
        dbname = path.basename(dbspec)
        dbcon = LocalDBConn(dbdir, dbname, suppress_gabble, miclasspath)
    else:
        dbcon = dbspec

    if queries.__class__ is str:
        queries = parse_batch_query(queries)

    queryrslts = {} 
    for query in queries:
        qr = dbcon.runQuery(query, scenarios, regions, warn_empty)
        queryrslts[query.title] = qr

    return queryrslts

        
