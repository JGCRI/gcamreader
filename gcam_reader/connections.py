"""Classes for representing GCAM database connections. 
   Required methods for these classes:
      * runQuery
"""

import sys
if sys.version_info[0] < 3:
    ## Not 100% sure we plan to support python2, but we might as well
    ## keep the option open.
    from StringIO import StringIO
else:
    from io import StringIO

import os.path as path
import re
import subprocess as sp
import pandas as pd
from pandas.errors import EmptyDataError

### Default class path for the GCAM model interface
mifiles_dir = path.abspath(path.join(path.dirname(__file__), 'ModelInterface'))
default_miclasspath = "{}/jars/*:{}/ModelInterface.jar".format(mifiles_dir, mifiles_dir)


### Local DB connection
class LocalDBConn:
    """Connection to a local GCAM database
    
       A local database connection comprises a name and location for
       the database, along with a class path for the java program used
       to extract data and some options to be passed to the functions
       that run the queries.
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

        ## convert region and scenario lists to strings.  The format is
        ## ('item1', 'item2', ..., 'itemN')
        if scenarios is None or len(scenarios) == 0:
            xqscen = "()"
        else:
            xqscen = "('" + "','".join(scenarios) + "')"

        if regions is None or len(regions) == 0:
            xqrgn = "()"
        else:
            xqrgn = "('" + "','".join(regions) + "')"

        ## convert suppress_gabble flag to a string.  I believe the model
        ## interface wants all caps, so we can't just use str()
        if self.suppress_gabble:
            sg = "TRUE"
        else:
            sg = "FALSE"
            
        ## strip newlines from query string
        query = re.sub("\n", "", query)

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
            "import module namespace mi = 'ModelInterface.ModelGUI2.xmldb.RunMIQuery';" + "mi:runMIQuery(" + query + "," + xqscen + "," + xqrgn + ")",
        ]

        try:
            mireturn = sp.run(cmd, stdout=sp.PIPE, stderr=sp.PIPE, check = True, encoding="UTF-8") 
        except CalledProcessError(e):
            sys.stderr.write("Model interface run failed.\n")
            sys.stderr.write("Command line: \n\t{}\n".format(' '.join(cmd)))
            sys.stderr.write("Query string: \n\t{}\n".format(query))
            sys.stderr.write("Model interface stderr output:\n\t{}\n".format(e.stderr))
            raise


        buf = StringIO(mireturn.stdout)
        try:
            rslt = pd.read_csv(buf)
        except EmptyDataError:
            if warn_empty:
                sys.stderr.write("Model interface returned empty string.\n")
                sys.stderr.write("Query string: \n\t{}\n".format(query))
                sys.stderr.write("Model interface stderr output:\n\t{}\n".format(mireturn.stderr))
            return None
        else:
            return rslt
