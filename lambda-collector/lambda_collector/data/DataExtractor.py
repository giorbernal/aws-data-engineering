from typing import Callable, Any, Generator
import pandas as pd
import numpy as np


def fit_stations(df):
    df.columns = ['ESTACION', 'N_ESTACION']
    df['ESTACION'] = df[['ESTACION']].apply(lambda x: int(str(x[0])[-3:]), axis=1)
    return df


def fit_parameters(df):
    df.columns = ['MAGNITUD', 'N_MAGNITUD', 'UNIT']
    return df


def get_value_columns():
    flat_map: Callable[[Any, Any], Generator[Any, Any, None]] = lambda f, xs: (y for ys in xs for y in f(ys))
    columns = []
    for i in flat_map(lambda x: x, [[''.join(['H', str(x).zfill(2)]), ''.join(['V', str(x).zfill(2)])] for x in list(range(1, 25))]):
        columns.append(i)
    return columns


def avg_calculation(x):
    data = [np.NaN if x[i+1] == 'N' else x[i] for i in pd.Series(list(range(0, 48, 2)))]
    return pd.Series(data).mean()


class DataExtractor:

    def __init__(self, ci):
        self.ci = ci

    def process(self):
        try:
            # Read Data
            raw_df = self.ci.get_raw_data()
            ref_stations = fit_stations(self.ci.get_ref_stations())
            ref_parameters = fit_parameters(self.ci.get_ref_parameters())

            # Join raw and reference info
            joined_df = raw_df\
                .merge(ref_stations, on='ESTACION', how='inner')\
                .merge(ref_parameters, on='MAGNITUD', how='inner')
            joined_df['FECHA']=joined_df[['ANO', 'MES', 'DIA']].apply(lambda x: str.join('-', [str(x['ANO']), str(x['MES']), str(x['DIA'])]), axis=1)
            joined_df.drop(columns=['ANO', 'MES', 'DIA', 'PROVINCIA', 'MUNICIPIO', 'PUNTO_MUESTREO', 'ESTACION', 'MAGNITUD', 'UNIT'], inplace=True)

            # Transform and store data
            self.__adapt_s3_data__(joined_df)
            self.__adapt_dynamo_data__(joined_df)
        except Exception as e:
            print(str.join(': ', ['Error while processing data', e]))

    def __adapt_s3_data__(self, df):
        value_columns = get_value_columns()
        df['AVG'] = df[value_columns].apply(avg_calculation, axis=1)
        self.ci.save_s3_data(df.drop(columns=value_columns))

    def __adapt_dynamo_data__(self, df):
        # TODO
        pivot_df = df
        self.ci.save_dynamo_data(pivot_df)

