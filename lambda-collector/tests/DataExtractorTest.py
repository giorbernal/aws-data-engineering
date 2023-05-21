import unittest

from lambda_collector.data.Connector import Connector
from lambda_collector.data.DataExtractor import DataExtractor
from tests.ConnectorMock import ConnectorMock


class DataExtractorTest(unittest.TestCase):

    @unittest.skip
    def test_live(self):
        ci = Connector()
        de = DataExtractor(ci)
        de.process()
        self.assertEqual(0, 0)

    def test_mock(self):
        ci = ConnectorMock()
        de = DataExtractor(ci)
        de.process()
        self.assertEqual(0, 0)


if __name__ == '__main__':
    unittest.main()
