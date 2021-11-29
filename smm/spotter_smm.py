import os
import sys

home_path = os.environ['HOME']
sys.path.append(home_path)

import bd_function as bd
import pandas as pd

from user_config import EMAIL_SPOTTER_ABROLHOS_FILE, EMAIL_SPOTTER_MEXILHAO_FILE


def get_synoptic_data(df):

    zulu_hours = [0, 3, 6, 9, 12, 15, 18, 21]
    df.set_index('Datetime', inplace=True)
    idx_zulu = df.index.hour.isin(zulu_hours)



    df = df[idx_zulu]

    df.reset_index(inplace=True)

    return df

def get_full_hour(df):

	df['Datetime'] = df['Datetime'].dt.round('30min')
	df.set_index('Datetime', inplace=True)

	idx_zulu = df.index.minute.isin([00])

	df = df[idx_zulu]
	df.reset_index(inplace=True)

	return df


# spotter

# Trindade Buoy
#conn = bd.conn_qc_db('PRI')

#df_spotter_qc_trindade = bd.get_data_spotter(conn=conn, buoy_id=19, table='data_buoys', start_date="2021-04-18",last_date=None, interval_hour='ALL')
#df_spotter_qc_trindade.sort_values(by = 'date_time', inplace = True)




#df_spotter_trindade = df_spotter_qc_trindade[['date_time', 'lat', 'lon', 'wspd', 'wdir','sst',
#							'swvht1','tp1', 'wvdir1','wvspread1', 'pk_dir',
#							'pk_wvspread','mean_tp']].copy()




#df_spotter_trindade.rename(columns = {'date_time':'Datetime',
#							 'sst':'wtmp',
#							 'swvht1': 'wvht',
#							 'tp1': 'dpd',
#							 'mean_tp':'mean_dpd',
#							 'wvdir1':'mwd',
#							 'pk_dir':'peak_mwd',
#							 'wvspread1':'spred',
#							 'pk_wvspread':'peak_spred'}, inplace = True)


#df_spotter_trindade = get_full_hour(df_spotter_trindade)
#df_spotter_trindade = get_synoptic_data(df_spotter_trindade)



#bd.spotter_txt(df_spotter_trindade, EMAIL_SPOTTER_TRINDADE_FILE)


# Abrolhos Buoy
conn = bd.conn_qc_db('PRI')

df_spotter_qc_abrolhos = bd.get_data_spotter(conn=conn, buoy_id=20, table='data_buoys', start_date="2021-06-21",last_date=None, interval_hour='ALL')
df_spotter_qc_abrolhos.sort_values(by = 'date_time', inplace = True)




df_spotter_abrolhos = df_spotter_qc_abrolhos[['date_time', 'lat', 'lon', 'wspd', 'wdir','sst',
							'swvht1','tp1', 'wvdir1','wvspread1', 'pk_dir',
							'pk_wvspread','mean_tp']].copy()




df_spotter_abrolhos.rename(columns = {'date_time':'Datetime',
							 'sst':'wtmp',
							 'swvht1': 'wvht',
							 'tp1': 'dpd',
							 'mean_tp':'mean_dpd',
							 'wvdir1':'mwd',
							 'pk_dir':'peak_mwd',
							 'wvspread1':'spred',
							 'pk_wvspread':'peak_spred'}, inplace = True)

df_spotter_abrolhos = get_full_hour(df_spotter_abrolhos)
df_spotter_abrolhos = get_synoptic_data(df_spotter_abrolhos)



bd.spotter_txt(df_spotter_abrolhos, EMAIL_SPOTTER_ABROLHOS_FILE)


# # Mexilao Buoy
# conn = bd.conn_qc_db('PRI')

# df_spotter_qc_mexilhao = bd.get_data_spotter(conn=conn, buoy_id=23, table='data_buoys', start_date="2021-11-24",last_date=None, interval_hour='ALL')
# df_spotter_qc_mexilhao.sort_values(by = 'date_time', inplace = True)




# df_spotter_mexilhao = df_spotter_qc_mexilhao[['date_time', 'lat', 'lon', 'wspd', 'wdir','sst',
# 							'swvht1','tp1', 'wvdir1','wvspread1', 'pk_dir',
# 							'pk_wvspread','mean_tp']].copy()




# df_spotter_mexilhao.rename(columns = {'date_time':'Datetime',
# 							 'sst':'wtmp',
# 							 'swvht1': 'wvht',
# 							 'tp1': 'dpd',
# 							 'mean_tp':'mean_dpd',
# 							 'wvdir1':'mwd',
# 							 'pk_dir':'peak_mwd',
# 							 'wvspread1':'spred',
# 							 'pk_wvspread':'peak_spred'}, inplace = True)

# df_spotter_mexilhao = get_full_hour(df_spotter_mexilhao)
# df_spotter_mexilhao = get_synoptic_data(df_spotter_mexilhao)



# bd.spotter_txt(df_spotter_mexilhao, EMAIL_SPOTTER_MEXILHAO_FILE)