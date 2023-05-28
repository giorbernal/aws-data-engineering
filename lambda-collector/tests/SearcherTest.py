import unittest
import pandas as pd

from definitions import ROOT_DIR
from datetime import datetime
from tests.TestUtils import get_client, create_table, delete_table, LocalStackWrapper
from lambda_collector.search.Searcher import Searcher

TEST_DYNAMO_SEARCH_DATA = '/'.join([ROOT_DIR, 'tests/datasets/dynamo_search.csv'])


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
        self.local_stack_wrapper = LocalStackWrapper()

    def testSearch(self):
        try:
            url = self.local_stack_wrapper.initialize()
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
            self.local_stack_wrapper.destroy()


if __name__ == '__main__':
    unittest.main()
