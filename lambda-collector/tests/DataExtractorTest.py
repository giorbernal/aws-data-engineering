import sys
import os
import unittest

import pandas as pd

sys.path.insert(0, os.getcwd())

from definitions import ROOT_DIR
from lambda_collector.data.DataExtractor import DataExtractor

TEST_FILE = '/'.join([ROOT_DIR,'tests/datasets/sample.csv'])

class DataExtractorTest(unittest.TestCase):

    @unittest.skip
    def test_base(self):
        de = DataExtractor()
        de.process()
        self.assertEqual(0, 0)

    def test_adapt_s3_data(self):
        de = DataExtractor()
        s3_df = de.__adapt_s3_data__(pd.read_csv(TEST_FILE, sep=';'))
        # TODO assert

    def test_adapt_dynamo_data(self):
        de = DataExtractor()
        dynamo_df = de.__adapt_dynamo_data__(pd.read_csv(TEST_FILE, sep=';'))
        # TODO assert


if __name__ == '__main__':
    unittest.main()
