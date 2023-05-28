import requests
import pandas as pd
import io
import os

from datetime import datetime
from lambda_collector.data.ConnectorInterface import ConnectorInterface
from lambda_collector.Utils import get_session


class Connector(ConnectorInterface):

    def __init__(self, s3_client=None, dynamodb_client=None, raw_data=None):
        super().__init__()

        # Raw Data (if it comes in the params, it will always be retrieved)
        self.raw_data = raw_data
        self.url = os.environ.get('RAW_DATA_URL', 'https://datos.madrid.es/egob/catalogo/300392-11041819-meteorologia-tiempo-real.csv')

        # AWS Context info
        self.bucket = os.environ.get('AWS_S3_BUCKET_NAME', 'jbernal-weather-madrid')
        self.refs_key = os.environ.get('AWS_S3_REFS_KEY', 'refs')
        self.refs_stations = os.environ.get('AWS_S3_REF_STATIONS', 'stations.csv')
        self.refs_parameters = os.environ.get('AWS_S3_REF_PARAMETERS', 'params.csv')
        self.history_data_key = os.environ.get('AWS_S3_HISTORY_DATA_KEY', 'history-data')
        self.table_name = os.environ.get('AWS_DYNAMO_TABLE_NAME', 'weather-datapoints')

        if s3_client is None and dynamodb_client is None:
            session = get_session()
            self.s3 = session.client('s3')
            self.dynamodb = session.client('dynamodb')
        else:
            self.s3 = s3_client
            self.dynamodb = dynamodb_client

    def get_raw_data(self):
        if self.raw_data is None:
            s = requests.get(self.url).content
            df = pd.read_csv(io.StringIO(s.decode('utf-8')), sep=';')
            return df
        else:
            return self.raw_data

    def get_ref_stations(self):
        df = self.__read_s3_data__('/'.join([self.refs_key, self.refs_stations]))
        return df

    def get_ref_parameters(self):
        df = self.__read_s3_data__('/'.join([self.refs_key, self.refs_parameters]))
        return df

    def save_s3_data(self, df):
        date_series = df['FECHA'].unique()
        if date_series.size != 1:
            raise ValueError('More than a single date in the s3 data')
        local_key = date_series[0]
        df_to_store = df.drop(columns=['FECHA'])
        self.__write_s3_data__(df_to_store, '/'.join([self.history_data_key, local_key, 'data.csv']))

    def save_dynamo_data(self, df):
        df['ESTACION_MAGNITUD'] = df[['N_ESTACION', 'N_MAGNITUD']].apply(lambda x: '-'.join([x[0], x[1]]), axis=1)
        df_to_store = df.drop(columns=['N_ESTACION', 'N_MAGNITUD'])
        self.__write_dynamo_data_(df_to_store)

    def __read_s3_data__(self, key):
        # Read file from S3 into DataFrame
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        df = pd.read_csv(obj['Body'], sep=';')

        return df

    def __write_s3_data__(self, df, key):
        # create a bytes buffer to hold the data
        csv_buffer = io.StringIO()

        # write the dataframe to the buffer
        df.to_csv(csv_buffer, index=False, sep=';')

        # write the buffer contents to S3
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=csv_buffer.getvalue().encode('utf-8'))

    def __write_dynamo_data_(self, df):
        df['VALOR'] = df['VALOR'].astype(str)
        df['FECHA'] = df[['FECHA']].apply(lambda x: datetime.strptime(x[0], '%Y-%m-%d'), axis=1)
        items_to_put = []
        for i in range(len(df)):
            row = df.iloc[i]
            items_to_put.append({
                'PutRequest': {
                    'Item': {
                        'ESTACION_MAGNITUD': {'S': row['ESTACION_MAGNITUD']},
                        'FECHA': {'N': str(row['FECHA'].timestamp())},
                        'VALOR': {'S': row['VALOR']}
                    }
                }
            })
        self.dynamodb.batch_write_item(
            RequestItems={
                self.table_name: items_to_put,
            }
        )

