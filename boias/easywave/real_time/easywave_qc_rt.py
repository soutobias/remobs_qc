from datetime import datetime
import pandas as pd 
import numpy as np

import os, sys
sys.path.append(os.environ['HOME'])
home_path = os.environ['HOME']
from user_config import *
cwd_path = home_path + '/remobs_qc/boias/easywave/real_time/'
bd_path = home_path + '/remobs_qc/boias/easywave/bd'

sys.path.append(cwd_path)
sys.path.append(bd_path)

import db_functions as db
from easywave_adjust_data import * 
import easywave_quality_control as eqc



conn = db.remobs_db(host=HOST_RAW, db=DATABASE_RAW, usr=USER_RAW, pwd=PASSWORD_RAW)



ons = conn.ew_on()

for id in ons['buoy_id']:


	last_data = conn.get_last_time(id)
	raw_easywave = conn.get_data_table_db(id, last_data, 168)
	raw_easywave.set_index('date_time', inplace = True)

	print("Qualifying General data...")
    flag, ew_qualified = eqc.qualitycontrol(raw_easywave, id)

    print("Rotating....")
    ew_qualified = rotate_data(conn, ew_qualified, flag, id)
    print("Done!")

    flag = rename_flag_data(flag)

    ew_merged = pd.merge(ew_qualified, flag, how = 'outer', on = 'date_time',
                          validate = 'one_to_one')


    print("EasyWave Data ready.")
    print('\n'*2)

    print("Closing connection with Raw Database\n")
    conn._db.close() # Closing connection with raw database
    print("Connection closed.\n")

    # Adjusting data...
    ew_qc_data = adjust_ew_qc(ew_merged)

    # IDs Key values to delete "old" qualified data...
    ids_pk = ew_qc_data[['id', 'buoy_id']]

    ew_qc_data = zulu_time(ew_qc_data)
    ew_qc_data = check_size_values(ew_qc_data)

    # Connecting to the qc database 

    conn_qc = db.remobs_qc_db(host=HOST_QC, db=DATABASE_QC, usr=USER_QC, pwd=PASSWORD_QC)

    # Deleting old data before insert ...


    conn_qc.delete_ew_qc_data(ids_pk)

    # inserting new data...

    conn_qc.insert_ew_qc_data(ew_qc_data)






