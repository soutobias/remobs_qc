import os
import sys
import pandas as pd
import numpy as np

home_path = os.environ['HOME']
bd_path = home_path + '/remobs_qc/smm/'
sys.path.append(home_path)
sys.path.append(bd_path)

import bd_function as bd

# JUST SYNIPTIC DATA WILL BE PUBLIC FOR NOW
def get_synoptic_data(df):

    zulu_hours = [0, 3, 6, 9, 12, 15, 18, 21]
    df.set_index('Datetime', inplace=True)
    idx_zulu = df.index.hour.isin(zulu_hours)



    df = df[idx_zulu]

    df.reset_index(inplace=True)

    return df


conn = bd.conn_qc_db('PRI')

df_bmo_qc = bd.get_data_bmo(conn, 2, '','data_buoys', 'ALL')
df_bmo_qc.sort_values(by = 'date_time', inplace = True)

# removing flags:

df_bmo = df_bmo_qc[
    [
        'date_time',
        'lat',
        'lon',
        'battery',
        'compass',
        'wspd',
        'gust',
        'wdir',
        'atmp',
        'dewpt',
        'rh',
        'pres',
        'arad',
        'sst',
        'cspd1',
        'cdir1',
        'cspd2',
        'cdir2',
        'cspd3',
        'cdir3',
        'swvht1',
        'tp1',
        'mxwvht1',
        'wvdir1',
        'wvspread1',
        'swvht2',
        'tp2',
        'wvdir2'
    ]
]

pd.options.mode.chained_assignment = None # default='warn'
df_bmo['tp2'].loc[df_bmo['tp2'].eq(256)]= np.nan

# date_time to Datetime:
df_bmo = df_bmo.rename({'date_time':'Datetime'}, axis = 'columns')
df_bmo = df_bmo.rename({'battery':'Battery'}, axis = 'columns')






df_bmo = get_synoptic_data(df_bmo)



bd.bmo_txt(df_bmo)
