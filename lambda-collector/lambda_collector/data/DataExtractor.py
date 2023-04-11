class DataExtractor:

    def __init__(self, ci):
        self.ci = ci

    def process(self):
        try:
            raw_df = self.ci.get_raw_data()
            ref_stations = self.__fit_stations__(self.ci.get_ref_stations())
            ref_parameters = self.__fit_parameters__(self.ci.get_ref_parameters())

            # Join raw and reference info
            joined_df = raw_df\
                .merge(ref_stations, on='ESTACION', how='inner')\
                .merge(ref_parameters, on='MAGNITUD', how='inner')
            joined_df.drop(columns=['PROVINCIA', 'MUNICIPIO', 'PUNTO_MUESTREO', 'ESTACION', 'MAGNITUD', 'UNIT'], inplace=True)

            self.__adapt_s3_data__(joined_df)
            self.__adapt_dynamo_data__(joined_df)
        except Exception as e:
            print(str.join(': ', ['Error while processing data', e]))

    def __fit_stations__(self, df):
        df.columns = ['ESTACION', 'N_ESTACION']
        df['ESTACION'] = df[['ESTACION']].apply(lambda x: int(str(x[0])[-3:]), axis=1)
        return df

    def __fit_parameters__(self, df):
        df.columns = ['MAGNITUD', 'N_MAGNITUD', 'UNIT']
        return df

    def temp(self, x):
        print(x)


    def __adapt_s3_data__(self, df):
        # TODO
        agg_df = df
        self.ci.save_s3_data(agg_df)

    def __adapt_dynamo_data__(self, df):
        # TODO
        op_df = df
        self.ci.save_dynamo_data(op_df)

