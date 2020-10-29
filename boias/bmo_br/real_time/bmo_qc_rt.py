######################################################
######################################################
########   QUALIFICATION of RAW DATA  ################
##############   BMO BUOY   ##########################
######################################################

import sys
import os
cwd = os.getcwd()
sys.path.insert(0, cwd)
sys.path.insert(0, cwd + '/../bd/')


from bmo_database import *
from bmo_adjust_data import *
import bmo_quality_control as bqc

import pandas as pd

conn = connect_database_remo()

id_buoy = bmo_on(conn)


for id in id_buoy['id_buoy']:

    last_date_raw = check_last_date(conn, 'bmo_br', id)

    last_date_raw = last_date_raw[0][0]


    time_interval = 24 # hours interval of data to be qualified
    bmo_general = get_data_table_db(conn, id, last_date_raw, time_interval, 'bmo_br')
    bmo_general.set_index('date_time', inplace = True)


    bmo_general = bmo_general[~bmo_general.index.duplicated(keep = 'first')]

    print("Qualifying General data...")
    flag, bmo_qualified = bqc.qualitycontrol(bmo_general, id)

    print("Rotating....")
    bmo_qualified = rotate_data(conn, bmo_qualified, flag, id)

    flag = rename_flag_data(flag)

    bmo_merged = pd.merge(bmo_qualified, flag, how = 'outer', on = 'date_time',
                          validate = 'one_to_one')

    print("General Data ready.")
    print('\n'*2)

    print("Closing connection with Raw Database")
    conn.close() # Closing connection with raw database
    print("Connection closed.")
    ###########################################################################
    print("Connecting with Qualified Database")
    conn_qc = conn_qc_db() # Open connection with Qualified Database

    bmo_qc_data = adjust_bmo_qc(bmo_merged)

    insert_bmo_qc_data(conn_qc, bmo_qc_data)
    conn_qc.close()


   # bmo_current = bmo_current[~bmo_current.index.duplicated(keep = 'first')]

   # print("Qualifying General data...")
   # flag, bmo_qualified = bqc.qualitycontrol(bmo_general, id)







    # qc_data = rotate_data(qc_data, flag_data, buoy)
    #
    # qc_data = rename_merge(qc_data, flag_data)