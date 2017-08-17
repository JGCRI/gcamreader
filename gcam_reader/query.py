"""Functions and classes for handling GCAM output DB queries."""

import xml.etree.ElementTree as ET
import os.path

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

        if xmlin.__class__ == str:
            xmlq = ET.fromstring(xmlin)
        else:
            xmlq = xmlin

        query = xmlq.find('./*[@title]')
        self.querystr = ET.tostring(query, encoding='unicode')

        regions = xmlq.findall('region')
        if len(regions) == 0:
            self.regions = None
        else:
            self.regions = [e.get('name') for e in regions]

        self.title = query.get('title')

            
        
def parse_batch_query(filename):
    """Parse a GCAM query file into a list of Query class objects."""

    tree = ET.parse(filename)
    root = tree.getroot()

    queries = root.findall('aQuery')

    return [Query(q) for q in queries]

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

    if dbspec.__class__ == str:
        dbdir = os.path.dirname(dbspec)
        dbname = os.path.dirname(dbspec)
        dbcon = LocalDBConn(dbdir, dbname, suppress_gabble, miclasspath)
    else:
        dbcon = dbspec

    if queries.__class__ == str:
        queries = parse_batch_query(queries)

    queryrslts = {} 
    for query in queries:
        qr = dbcon.runQuery(query, scenarios, regions, warn_empty)
        queryrslts[query.title] = qr

    return queryrslts

        
