import os
from datetime import datetime
from lambda_collector.Utils import get_session


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

    def get_data(self, main_search_key, start, end):
        start_ts = datetime.strptime(start, '%Y-%m-%d').timestamp()-1
        end_ts = datetime.strptime(end, '%Y-%m-%d').timestamp()+1
        response = self.client.query(
            TableName=self.table_name,
            KeyConditionExpression='#hk = :hk and #rk between :start and :end',
            ExpressionAttributeNames={'#hk': 'ESTACION_MAGNITUD', '#rk': 'FECHA'},
            ExpressionAttributeValues={
                ':hk': {'S': main_search_key},
                ':start': {'N': str(start_ts)},
                ':end': {'N': str(end_ts)}
            }
        )

        return response
