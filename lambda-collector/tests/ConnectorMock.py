import sys
import os
import pandas as pd

sys.path.insert(0, os.getcwd())

from definitions import ROOT_DIR
from lambda_collector.data.ConnectorInterface import ConnectorInterface

TEST_RAW_FILE = '/'.join([ROOT_DIR,'tests/datasets/sample.csv'])
TEST_REF_STATIONS_FILE = '/'.join([ROOT_DIR,'../refs/stations.csv'])
TEST_REF_PARAMS_FILE = '/'.join([ROOT_DIR,'../refs/params.csv'])


class ConnectorMock(ConnectorInterface):

    def __init__(self):
        super().__init__()

    def get_raw_data(self):
        df = pd.read_csv(TEST_RAW_FILE, sep=';')
        return df

    def get_ref_stations(self):
        df = pd.read_csv(TEST_REF_STATIONS_FILE, sep=';', header=None)
        return df

    def get_ref_parameters(self):
        df = pd.read_csv(TEST_REF_PARAMS_FILE, sep=';', header=None)
        return df

