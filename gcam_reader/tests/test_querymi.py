"""test_outputs.py

Tests to ensure high-level functionality and outputs remain consistent.

@author Chris R. Vernon (chris.vernon@pnnl.gov)
@license BSD 2-Clause

"""

import pkg_resources
import unittest

import gcam_reader

import pandas as pd


class QueryTests(unittest.TestCase):
    """Test configuration integrity."""

    TEST_GCAM_DB = 'sample_basexdb'
    TEST_GCAM_DB_PATH = pkg_resources.resource_filename('gcam_reader', 'tests/data')
    TEST_LAND_QUERY = pkg_resources.resource_filename('gcam_reader', 'tests/data/queries/query_land_reg32_basin235_gcam5p0.xml')

    # CSV file from existing land allocation query output
    COMP_LAND_OUTPUT = pd.read_csv(pkg_resources.resource_filename('gcam_reader', 'tests/data/comp_outputs/land_query.csv'))

    def __init__(self, obj):

        # get unittest.TestCase init
        super().__init__(obj)

        # create connection object
        self.conn = self.create_connection()

    @classmethod
    def create_connection(cls):
        """Create database connection."""
        return gcam_reader.LocalDBConn(cls.TEST_GCAM_DB_PATH, cls.TEST_GCAM_DB)

    def test_connection(self):
        """Check the type output of the gcam_reader DB object."""

        self.assertEqual(str(type(self.conn)), "<class 'gcam_reader.querymi.LocalDBConn'>")

    def test_land_query(self):
        """Test for land output data frame equality from query."""

        # get land allocation query
        query = gcam_reader.parse_batch_query(QueryTests.TEST_LAND_QUERY)[0]

        # get run output for land
        df = self.conn.runQuery(query)

        # test full equality with comparison output data
        pd.testing.assert_frame_equal(df, QueryTests.COMP_LAND_OUTPUT)


if __name__ == '__main__':

    unittest.main()
