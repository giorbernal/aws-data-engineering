import os
from lambda_collector.Utils import get_session
from boto3.dynamodb.conditions import Attr


class Searcher:

    def __init__(self, client=None):
        self.table_name = os.environ.get('AWS_DYNAMO_TABLE_NAME', 'weather-datapoints')
        if client is None:
            self.client = get_session().client('dynamodb')
        else:
            self.client = client
        pass

    def get_table_name(self):
        return self.table_name

    def get_data(self, main_search_key):
        response = self.client.query(
            TableName=self.table_name,
            KeyConditionExpression='#hk = :hk and #rk between :start and :end',
            ExpressionAttributeNames={'#hk': 'ESTACION_MAGNITUD', '#rk': 'FECHA'},
            ExpressionAttributeValues={
                ':hk': {'S': main_search_key},
                ':start': {'S': '2023-3-26'},
                ':end': {'S': '2023-3-27'}
            }
        )

        return response
