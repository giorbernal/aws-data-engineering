import unittest

from lambda_collector.data.Connector import Connector
from lambda_collector.data.DataExtractor import DataExtractor
from lambda_collector.Constants import AWS_DEFAULT_TABLE, AWS_DEFAULT_BUCKET
from tests.ConnectorMock import ConnectorMock, TEST_REF_STATIONS_FILE, TEST_REF_PARAMS_FILE
from tests.TestUtils import get_client, create_table, delete_table, LocalStackWrapper


def initialize_aws_structure(s3_client, dynamo_client):
    s3_client.create_bucket(
        Bucket=AWS_DEFAULT_BUCKET,
        CreateBucketConfiguration={
            'LocationConstraint': 'eu-west-1'  # Or specify a region of your choice
        }
    )
    with open(TEST_REF_STATIONS_FILE, 'rb') as f:
        s3_client.put_object(Bucket=AWS_DEFAULT_BUCKET, Key=f'refs/stations.csv', Body=f)
    with open(TEST_REF_PARAMS_FILE, 'rb') as f:
        s3_client.put_object(Bucket=AWS_DEFAULT_BUCKET, Key=f'refs/params.csv', Body=f)
    create_table(dynamo_client, AWS_DEFAULT_TABLE)


def destroy_aws_structure(s3_client, dynamo_client):
    objects = s3_client.list_objects(Bucket=AWS_DEFAULT_BUCKET)
    if 'Contents' in objects:
        object_list = [{'Key': obj['Key']} for obj in objects['Contents']]
        s3_client.delete_objects(Bucket=AWS_DEFAULT_BUCKET, Delete={'Objects': object_list})
    s3_client.delete_bucket(
        Bucket=AWS_DEFAULT_BUCKET
    )
    delete_table(dynamo_client, AWS_DEFAULT_TABLE)


def assert_data():
    # TODO
    pass


class DataExtractorTest(unittest.TestCase):

    @unittest.skip
    def test_live(self):
        ci = Connector()
        de = DataExtractor(ci)
        de.process()

    def test_localstack(self):
        local_stack_wrapper = LocalStackWrapper()
        url = local_stack_wrapper.initialize()

        try:
            s3_client = get_client(url, 's3')
            dynamodb_client = get_client(url, 'dynamodb')
            initialize_aws_structure(s3_client, dynamodb_client)
            ci_mock = ConnectorMock()
            ci = Connector(
                s3_client,
                dynamodb_client,
                ci_mock.get_raw_data() # TODO we need to bring less data for this test
            )
            de = DataExtractor(ci)
            de.process()
            assert_data()
        finally:
            destroy_aws_structure(s3_client, dynamodb_client)
            local_stack_wrapper.destroy()

    def test_mock(self):
        ci = ConnectorMock()
        de = DataExtractor(ci)
        de.process()


if __name__ == '__main__':
    unittest.main()
