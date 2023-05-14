import unittest
import boto3
import os

from typing import Any
from testcontainers.localstack import LocalStackContainer
from lambda_operational_search.Searcher import Searcher


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


class SearcherTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(SearcherTest, self).__init__(*args, **kwargs)
        self.run_local_stack_container = os.getenv("RUN_LOCAL_STACK_CONTAINER", default=True) == 'True'
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

    def test(self):
        url = self.__initialize__()
        dynamo_client = get_client(url, "dynamodb")

        # TODO insert test data in dynamo db
        tables = dynamo_client.list_tables()
        #tables = []
        assert tables["TableNames"] == []

        # Search object
        # TODO Do the test case
        searcher = Searcher(dynamo_client)
        searcher.get_data("my key")

        self.assertEqual(0, 0)

        self.__destroy__()


if __name__ == '__main__':
    unittest.main()
