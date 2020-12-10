import os 
import sys 

home_path = os.environ['HOME']
sys.path.append(home_path)

import pandas as pd




# spotter 

conn = conn_qc_db('PRI')

def get_data_table_db(conn, id_buoy, last_date, table, interval_hour):


df_spotter_qc = get_data_table_db(conn, 3, '2020-12-04', 'data_buoys', 'ALL')
df_bmo_qc = 