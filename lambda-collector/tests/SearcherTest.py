import unittest
import boto3
import os
import pandas as pd

from definitions import ROOT_DIR
from datetime import datetime
from typing import Any
from testcontainers.localstack import LocalStackContainer
from lambda_collector.search.Searcher import Searcher

TEST_DYNAMO_SEARCH_DATA = '/'.join([ROOT_DIR, 'tests/datasets/dynamo_search.csv'])


def get_client(url, name, **kwargs) -> Any:
#    kwargs_ = {
#        "endpoint_url": url,
#        "region_name": "us-west-1",
#        "aws_access_key_id": "testcontainers-localstack",
#        "aws_secret_access_key": "testcontainers-localstack",
#    }
#    kwargs_.update(kwargs)
#    return boto3.client(name, **kwargs_)
    return boto3.client(name, endpoint_url=url)


def create_table(dynamodb_client, table_name):
    # specify the table schema
    table_schema = {
        'AttributeDefinitions': [
            {
                'AttributeName': 'ESTACION_MAGNITUD',
                'AttributeType': 'S'  # S for String
            },
            {
                'AttributeName': 'FECHA',
                'AttributeType': 'N'
            }
        ],
        'KeySchema': [
            {
                'AttributeName': 'ESTACION_MAGNITUD',
                'KeyType': 'HASH'  # HASH key
            },
            {
                'AttributeName': 'FECHA',
                'KeyType': 'RANGE'  # RANGE key
            }
        ],
        'TableName': table_name,
        'BillingMode': 'PAY_PER_REQUEST'  # no provisioned throughput needed
    }

    # create the table
    response = dynamodb_client.create_table(**table_schema)
    return response


def delete_table(dynamodb_client, table_name):
    response = dynamodb_client.delete_table(TableName=table_name)
    return response


def insert_test_data(dynamodb_client, table_name):
    df = pd.read_csv(TEST_DYNAMO_SEARCH_DATA, sep=';')
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
    response = dynamodb_client.batch_write_item(
         RequestItems={
             table_name: items_to_put,
         }
    )
    return response


class SearcherTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(SearcherTest, self).__init__(*args, **kwargs)
        self.run_local_stack_container = os.getenv("RUN_LOCAL_STACK_CONTAINER", default='True') == 'True'
        self.localstack = None

    def __initialize__(self):
        if self.run_local_stack_container:
            self.localstack = LocalStackContainer(image="localstack/localstack:2.0.1")
            self.localstack.start()
            url = self.localstack.get_url()
        else:
            # Execute previously in console: docker run --rm -p 49811:4566 -it localstack/localstack:2.0.1
            url = 'http://localhost:49811'
        return url

    def __destroy__(self):
        if self.run_local_stack_container:
            self.localstack.stop()

    def testSearch(self):
        try:
            url = self.__initialize__()
            dynamodb_client = get_client(url, "dynamodb")
            searcher = Searcher(dynamodb_client)
            table_name = searcher.get_table_name()

            # Creating table
            create_table(dynamodb_client, table_name)
            tables = dynamodb_client.list_tables()
            assert tables["TableNames"] == [table_name]

            # Insert test data in dynamo db
            insert_test_data(dynamodb_client, table_name)

            # Search object
            response = searcher.get_data('VELOCIDAD VIENTO-J.M.D. Moratalaz', '2023-3-26', '2023-3-28')

            self.assertEqual(2, response['Count'])

        finally:
            delete_table(dynamodb_client, table_name)
            self.__destroy__()


if __name__ == '__main__':
    unittest.main()
