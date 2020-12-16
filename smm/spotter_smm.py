import os 
import sys 

home_path = os.environ['HOME']
sys.path.append(home_path)

import bd_function as bd
import pandas as pd




# spotter 

conn = bd.conn_qc_db('PRI')


df_spotter_qc = bd.get_data_spotter(conn, 3, 'data_buoys', '', 'ALL')
df_spotter_qc.sort_values(by = 'date_time', inplace = True)


df_spotter = df_spotter_qc[['date_time', 'lat', 'lon', 'wspd', 'wdir','sst',
							'swvht1','tp1', 'wvdir1','wvspread1', 'pk_dir',
							'pk_wvspread','mean_tp']].copy()



df_spotter.rename(columns = {'date_time':'Datetime',
							 'sst':'wtmp',
							 'swvht1': 'wvht',
							 'tp1': 'dpd',
							 'mean_tp':'mean_dpd',
							 'wvdir1':'mwd',
							 'pk_dir':'peak_mwd',
							 'wvspread1':'spred',
							 'pk_wvspread':'peak_spred'}, inplace = True)




bd.spotter_txt(df_spotter)