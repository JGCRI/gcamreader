"""test_outputs.py

Tests to ensure high-level functionality and outputs remain consistent.

@author Chris R. Vernon (chris.vernon@pnnl.gov)
@license BSD 2-Clause

"""

import pkg_resources
import unittest

import gcamreader

import pandas as pd


class QueryTests(unittest.TestCase):
    """Test configuration integrity."""

    TEST_GCAM_DB = 'sample_basexdb'
    TEST_GCAM_DB_PATH = pkg_resources.resource_filename('gcamreader', 'tests/data')
    TEST_LAND_QUERY = pkg_resources.resource_filename('gcamreader', 'tests/data/queries/query_land_reg32_basin235_gcam5p0.xml')

    # CSV file from existing land allocation query output
    COMP_LAND_OUTPUT = pd.read_csv(pkg_resources.resource_filename('gcamreader', 'tests/data/comp_outputs/land_query.csv'))

    def __init__(self, obj):

        # get unittest.TestCase init
        super().__init__(obj)

        # create connection object
        self.conn = self.create_connection()

    @classmethod
    def create_connection(cls):
        """Create database connection."""
        return gcamreader.LocalDBConn(cls.TEST_GCAM_DB_PATH, cls.TEST_GCAM_DB)

    def test_connection(self):
        """Check the type output of the gcamreader DB object."""

        self.assertEqual(str(type(self.conn)), "<class 'gcamreader.querymi.LocalDBConn'>")

    def test_land_query(self):
        """Test for land output data frame equality from query."""

        # get land allocation query
        query = gcamreader.parse_batch_query(QueryTests.TEST_LAND_QUERY)[0]

        sort_columns = ['region', 'land-allocation', 'Year']

        # get run output for land
        df = self.conn.runQuery(query).sort_values(by=sort_columns, ignore_index=True)

        # test full equality with comparison output data
        pd.testing.assert_frame_equal(df, QueryTests.COMP_LAND_OUTPUT.sort_values(by=sort_columns, ignore_index=True))


if __name__ == '__main__':

    unittest.main()
