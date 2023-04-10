import sys
import os
import pandas as pd

sys.path.insert(0, os.getcwd())

from definitions import ROOT_DIR
from lambda_collector.data.ConnectorInterface import ConnectorInterface

TEST_RAW_FILE = '/'.join([ROOT_DIR,'tests/datasets/sample.csv'])


class ConnectorMock(ConnectorInterface):

    def __init__(self):
        super().__init__()

    def get_raw_data(self):
        df = pd.read_csv(TEST_RAW_FILE, sep=';')
        return df
