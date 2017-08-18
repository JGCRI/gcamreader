"""Classes for representing GCAM database connections. 
   Required methods for these classes:
      * runQuery
"""

import sys
if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO

import os.path as path
import re
import subprocess as sp
import pandas as pd

### sibling modules in this package
import query


### Default class path for the GCAM model interface
mifiles_dir = path.abspath(path.join(path.dirname(__file__), 'ModelInterface'))
default_miclasspath = "{}/jars/*:{}/ModelInterface.jar".format(mifiles_dir, mifiles_dir)


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
    except EmptyDataError:
        if warn_empty:
            sys.stderr.write("Model interface returned empty string.\n")
            sys.stderr.write("Query string: \n\t{}\n".format(query))
            sys.stderr.write("Model interface stderr output:\n\t{}\n".format(stderr))
        return None
    else:
        return rslt

    
### Local DB connection
class LocalDBConn:
    """Connection to a local GCAM database
    
    A local database connection comprises a name and location for the
    database, along with a class path for the java program used to
    extract data and some options to be passed to the functions that
    run the queries.
    """

    def __init__(self, dbpath, dbfile, suppress_gabble=True, miclasspath = None):
        """Initialize a local db connection.

        params:
          * dbpath:  directory containing the GCAM database
          * dbfile:  name of the GCAM database
          * suppress_gabble: Flag, if True, suppress the console output normally
                produced by the model interface.  Otherwise, display the console
                output.
          * miclasspath: Java class path for the GCAM model interface.  The default
                value points to a copy that was installed with this package.
        """
        self.dbpath = path.abspath(dbpath)
        self.dbfile = dbfile
        self.suppress_gabble = suppress_gabble

        if miclasspath is None:
            self.miclasspath = default_miclasspath
        else:
            self.miclasspath = path.abspath(miclasspath)


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
        ## interface wants all caps, so we can't just use str()
        if self.suppress_gabble:
            sg = "TRUE"
        else:
            sg = "FALSE"

        ## strip newlines from query string
        querystr = re.sub("\n", "", query.querystr)

        cmd =  [
            "java",
            "-cp", self.miclasspath,
            "-Xmx2g",               # TODO: add explicit memory size limits?
            "-Dorg.basex.DBPATH=" + self.dbpath,
            "-DModelInterface.SUPPRESS_OUTPUT=" + sg,
            "org.basex.BaseX",
            "-smethod=csv",
            "-scsv=header=yes",
            "-i", self.dbfile,
            "import module namespace mi = 'ModelInterface.ModelGUI2.xmldb.RunMIQuery';" + "mi:runMIQuery(" + querystr + "," + xqscen + "," + xqrgn + ")",
        ]

        try:
            mireturn = sp.run(cmd, stdout=sp.PIPE, stderr=sp.PIPE, check = True, encoding="UTF-8") 
        except CalledProcessError(e):
            sys.stderr.write("Model interface run failed.\n")
            sys.stderr.write("Command line: \n\t{}\n".format(' '.join(cmd)))
            sys.stderr.write("Query string: \n\t{}\n".format(query.querystr))
            sys.stderr.write("Model interface stderr output:\n\t{}\n".format(e.stderr))
            raise

        return _parserslt(mireturn.stdout, warn_empty, query.title, mireturn.stderr)
        

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

    def __init__(self, dbfile, username, password, address="localhost", port=8984):
        """Initialize a remote database connection

        Arguments:

        * dbfile: The database file to query
        * username: The username configured for the BaseX server
        * password: The password configured for the BaseX server
        * address: The server address (URL).  The default is "localhost"
        * port: The port the server is running on.  The default is 8984.
        """

        self.dbfile = dbfile
        self.username = username
        self.password = password
        self.address = address
        self.port = port


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
        
        restquery = " ".join([
            '<rest:query xmlns:rest="http://basex.org/rest">',
            '<rest:text><![CDATA[',
            javastr,
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

        return _parserslt(r.text, warn_empty, query.title)

