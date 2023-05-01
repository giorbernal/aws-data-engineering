import json
from lambda_collector.data import Connector, DataExtractor


def __get_response__(code, message):
    response = {
        "statusCode": code,
        "body": json.dumps({
            "message": f"{message}"
        })
    }
    print(message)
    return response


def lambda_handler(event, context):

    try:
        ci = Connector()
        de = DataExtractor(ci)
        de.process()
        return __get_response__(200, 'Process successfully committed!')
    except Exception as e:
        return __get_response__(400, f"Error ({str(e)}) while processing!")
