import requests
import pandas as pd
import io
from lambda_collector.data.ConnectorInterface import ConnectorInterface


class Connector(ConnectorInterface):

    def __init__(self):
        super().__init__()

        # Raw Data
        self.url = 'https://datos.madrid.es/egob/catalogo/300392-11041819-meteorologia-tiempo-real.csv'
        self.raw_df = None

    def get_raw_data(self):
        s = requests.get(self.url).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')), sep=';')
        return df

