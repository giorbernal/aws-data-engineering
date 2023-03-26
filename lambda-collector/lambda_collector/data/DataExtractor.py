import io
import pandas as pd
import requests


class DataExtractor:

    def __init__(self):
        self.url = 'https://datos.madrid.es/egob/catalogo/300392-11041819-meteorologia-tiempo-real.csv'
        self.raw_df = None

    def process(self):
        try:
            s = requests.get(self.url).content
            self.raw_df = pd.read_csv(io.StringIO(s.decode('utf-8')), sep=';')

            #TODO Do the rest of process: save in S3 and in Dynamo in the proper format
            s3_df = self.__adapt_s3_data__(self.raw_df)
            dynamo_df = self.__adapt_dynamo_data__(self.raw_df)
            # ...
        except:
            print('Error while downloading data')

    def __adapt_s3_data__(self, df):
        # TODO
        return df

    def __adapt_dynamo_data__(self, df):
        # TODO
        return df

