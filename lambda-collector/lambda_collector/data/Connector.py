import requests
import boto3
import pandas as pd
import io
import os
from lambda_collector.data.ConnectorInterface import ConnectorInterface


class Connector(ConnectorInterface):

    def __init__(self):
        super().__init__()

        # Raw Data
        self.url = os.environ.get('RAW_DATA_URL', 'https://datos.madrid.es/egob/catalogo/300392-11041819-meteorologia-tiempo-real.csv')

        # Set AWS credentials
        self.aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        self.aws_region = os.environ.get('AWS_REGION')

        # AWS Context info
        self.bucket = os.environ.get('AWS_S3_BUCKET_NAME', 'jbernal-weather-madrid')
        self.refs_key = os.environ.get('AWS_S3_REFS_KEY', 'refs')
        self.refs_stations = os.environ.get('AWS_S3_REF_STATIONS', 'stations.csv')
        self.refs_parameters = os.environ.get('AWS_S3_REF_PARAMETERS', 'params.csv')
        self.history_data_key = os.environ.get('AWS_S3_HISTORY_DATA_KEY', 'history-data')
        self.table_name = os.environ.get('AWS_DYNAMO_TABLE_NAME', 'weather-datapoints')

        # Initiate session
        # create session with AWS credentials
        self.session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region
        )

    def get_raw_data(self):
        s = requests.get(self.url).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')), sep=';')
        return df

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
        # create S3 client
        s3 = self.session.client('s3')

        # Read file from S3 into DataFrame
        obj = s3.get_object(Bucket=self.bucket, Key=key)
        df = pd.read_csv(obj['Body'], sep=';')

        s3.close()
        return df

    def __write_s3_data__(self, df, key):
        # connect to S3
        s3 = self.session.client('s3')

        # create a bytes buffer to hold the data
        csv_buffer = io.StringIO()

        # write the dataframe to the buffer
        df.to_csv(csv_buffer, index=False, sep=';')

        # write the buffer contents to S3
        s3.put_object(Bucket=self.bucket, Key=key, Body=csv_buffer.getvalue().encode('utf-8'))

        s3.close()

    def __write_dynamo_data_(self, df):
        # Create a DynamoDB client
        dynamodb = self.session.resource('dynamodb')

        # Reference the DynamoDB table
        table = dynamodb.Table(self.table_name)

        # Convert pandas dataframe to list of dictionaries
        df['VALOR'] = df['VALOR'].astype(str)
        data = df.to_dict('records')

        # Write data to DynamoDB table
        with table.batch_writer() as batch:
            for item in data:
                batch.put_item(Item=item)

        dynamodb.close()


