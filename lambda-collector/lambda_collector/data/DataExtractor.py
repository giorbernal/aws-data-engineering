class DataExtractor:

    def __init__(self, ci):
        self.ci = ci

    def process(self):
        try:
            raw_df = self.ci.get_raw_data()
            ref_stations = self.ci.get_ref_stations()
            ref_parameters = self.ci.get_ref_parameters()

            # TODO Join raw and reference info
            joined_df = raw_df

            self.__adapt_s3_data__(joined_df)
            self.__adapt_dynamo_data__(joined_df)
        except Exception as e:
            print(str.join(': ', ['Error while processing data', e]))

    def __adapt_s3_data__(self, df):
        # TODO
        agg_df = df
        self.ci.save_s3_data(agg_df)

    def __adapt_dynamo_data__(self, df):
        # TODO
        op_df = df
        self.ci.save_dynamo_data(op_df)

