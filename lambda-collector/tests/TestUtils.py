import os
import boto3

from typing import Any
from testcontainers.localstack import LocalStackContainer


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


class LocalStackWrapper:

    def __init__(self):
        self.run_local_stack_container = os.getenv("RUN_LOCAL_STACK_CONTAINER", default='True') == 'True'
        self.localstack = None

    def initialize(self):
        if self.run_local_stack_container:
            self.localstack = LocalStackContainer(image="localstack/localstack:2.0.1")
            self.localstack.start()
            url = self.localstack.get_url()
        else:
            # Execute previously in console: docker run --rm -p 49811:4566 -it localstack/localstack:2.0.1
            url = 'http://localhost:49811'
        return url

    def destroy(self):
        if self.run_local_stack_container:
            self.localstack.stop()


