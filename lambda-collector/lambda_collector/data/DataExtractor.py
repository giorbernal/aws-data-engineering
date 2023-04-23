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
    data = [np.NaN if x[i+1] == 'N' else x[i] for i in list(range(0, 48, 2))]
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
            joined_df['FECHA']=joined_df[['ANO', 'MES', 'DIA']].apply(lambda x: str.join('-', [str(x['ANO']), str(x['MES']).zfill(2), str(x['DIA']).zfill(2)]), axis=1)
            joined_df.drop(columns=['ANO', 'MES', 'DIA', 'PROVINCIA', 'MUNICIPIO', 'PUNTO_MUESTREO', 'ESTACION', 'MAGNITUD', 'UNIT'], inplace=True)

            # Transform and store data
            value_columns = get_value_columns()
            self.__adapt_s3_data__(joined_df, value_columns)
            self.__adapt_dynamo_data__(joined_df, value_columns)
        except Exception as e:
            print(': '.join(['Error while processing data', str(e)]))

    def __adapt_s3_data__(self, df, value_columns):
        df['AVG'] = df[value_columns].apply(avg_calculation, axis=1)
        self.ci.save_s3_data(df.drop(columns=value_columns))
        df.drop(columns=['AVG'], inplace=True)

    def __adapt_dynamo_data__(self, df, value_columns):
        melt_df = pd.melt(df, id_vars=['N_MAGNITUD', 'N_ESTACION', 'FECHA'], value_vars=value_columns,\
                          var_name='HORA_VALIDEZ', value_name='VALOR')
        melt_df['FECHA'] = melt_df[['FECHA', 'HORA_VALIDEZ']].apply(lambda x: '-'.join([str(x[0]), str(int(str(x[1])[-2:])-1).zfill(2)]), axis=1)
        group = melt_df.groupby(by=['N_MAGNITUD', 'N_ESTACION', 'FECHA'])
        melt_df['VALIDEZ'] = group['VALOR'].transform(lambda x: x.iloc[1])
        # TODO Maybe we could see here a trace whit the elements to remove, as an evidence of mistakes in the data
        melt_df = melt_df[((melt_df['VALOR'] != 'V') & (melt_df['VALOR'] != 'N')) & (melt_df['VALIDEZ'] == 'V')]
        melt_df.drop(columns=['HORA_VALIDEZ', 'VALIDEZ'], inplace=True)
        melt_df.reset_index(inplace=True, drop=True)
        melt_df['VALOR'] = pd.to_numeric(melt_df['VALOR'], errors='coerce')
        self.ci.save_dynamo_data(melt_df)


