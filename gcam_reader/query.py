"""Functions and classes for handling GCAM output DB queries."""

import xml.etree.ElementTree as ET


class Query:
    def __init__(self, xmlin):
        """Initialize a query structure from an XML definition.
        Arguments:
          * xmlq: XML query definition. Can be either a string or a parsed XML
            element.
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
