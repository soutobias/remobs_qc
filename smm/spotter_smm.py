import os
import sys

home_path = os.environ['HOME']
sys.path.append(home_path)

import bd_function as bd
import pandas as pd


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
print("Connecting to database...")
conn = bd.conn_qc_db('PRI')

print("Abrolhos buoy...")
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



bd.spotter_txt(df_spotter_abrolhos, 'dados_spotter_abrolhos.txt')
print("Abrolhos buoy file ready.")

# Potter Buoy

# print("Potter buoy ...")
# df_spotter_qc_potter = bd.get_data_spotter(conn=conn, buoy_id=25, table='data_buoys', start_date="2021-12-08",last_date=None, interval_hour='ALL')
# df_spotter_qc_potter.sort_values(by = 'date_time', inplace = True)




# df_spotter_potter = df_spotter_qc_potter[['date_time', 'lat', 'lon', 'wspd', 'wdir','sst',
# 							'swvht1','tp1', 'wvdir1','wvspread1', 'pk_dir',
# 							'pk_wvspread','mean_tp']].copy()




# df_spotter_potter.rename(columns = {'date_time':'Datetime',
# 							 'sst':'wtmp',
# 							 'swvht1': 'wvht',
# 							 'tp1': 'dpd',
# 							 'mean_tp':'mean_dpd',
# 							 'wvdir1':'mwd',
# 							 'pk_dir':'peak_mwd',
# 							 'wvspread1':'spred',
# 							 'pk_wvspread':'peak_spred'}, inplace = True)

# df_spotter_potter = get_full_hour(df_spotter_potter)
# df_spotter_potter = get_synoptic_data(df_spotter_potter)



# bd.spotter_txt(df_spotter_potter, 'dados_spotter_potter.txt')
# print("Potter buoy file ready.")


# Pinguim Buoy

print("Pinguim buoy...")
df_spotter_qc_pinguim = bd.get_data_spotter(conn=conn, buoy_id=26, table='data_buoys', start_date="2021-12-04",last_date=None, interval_hour='ALL')
df_spotter_qc_pinguim.sort_values(by = 'date_time', inplace = True)




df_spotter_pinguim = df_spotter_qc_pinguim[['date_time', 'lat', 'lon', 'wspd', 'wdir','sst',
							'swvht1','tp1', 'wvdir1','wvspread1', 'pk_dir',
							'pk_wvspread','mean_tp']].copy()




df_spotter_pinguim.rename(columns = {'date_time':'Datetime',
							 'sst':'wtmp',
							 'swvht1': 'wvht',
							 'tp1': 'dpd',
							 'mean_tp':'mean_dpd',
							 'wvdir1':'mwd',
							 'pk_dir':'peak_mwd',
							 'wvspread1':'spred',
							 'pk_wvspread':'peak_spred'}, inplace = True)

df_spotter_pinguim = get_full_hour(df_spotter_pinguim)
df_spotter_pinguim = get_synoptic_data(df_spotter_pinguim)



bd.spotter_txt(df_spotter_pinguim, 'dados_spotter_pinguim.txt')
print("Pinguim buoy file ready.")


# Keller Buoy

print("Keller buoy...")
df_spotter_qc_keller = bd.get_data_spotter(conn=conn, buoy_id=24, table='data_buoys', start_date="2021-12-04",last_date=None, interval_hour='ALL')
df_spotter_qc_keller.sort_values(by = 'date_time', inplace = True)




df_spotter_keller = df_spotter_qc_keller[['date_time', 'lat', 'lon', 'wspd', 'wdir','sst',
							'swvht1','tp1', 'wvdir1','wvspread1', 'pk_dir',
							'pk_wvspread','mean_tp']].copy()




df_spotter_keller.rename(columns = {'date_time':'Datetime',
							 'sst':'wtmp',
							 'swvht1': 'wvht',
							 'tp1': 'dpd',
							 'mean_tp':'mean_dpd',
							 'wvdir1':'mwd',
							 'pk_dir':'peak_mwd',
							 'wvspread1':'spred',
							 'pk_wvspread':'peak_spred'}, inplace = True)

df_spotter_keller = get_full_hour(df_spotter_keller)
df_spotter_keller = get_synoptic_data(df_spotter_keller)



bd.spotter_txt(df_spotter_keller, 'dados_spotter_keller.txt')
print("Keller buoy file ready.")

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