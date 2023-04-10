# This clas is just to define common methods for the false-interface
class ConnectorInterface:

    def __init__(self):
        pass

    def get_raw_data(self):
        pass

    def get_ref_stations(self):
        pass

    def get_ref_parameters(self):
        pass

    def save_s3_data(self, df):
        pass

    def save_dynamo_data(self, df):
        pass
