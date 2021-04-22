######################################################
######################################################
########   QUALIFICATION of RAW DATA  ################
##############   BMO BUOY   ##########################
######################################################

import sys
import os

home_path = os.environ['HOME']
abs_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))
cwd_path = os.path.join(abs_dir, 'boias', 'bmo_br', 'real_time')
bd_path = os.path.join(abs_dir, 'boias', 'bmo_br', 'bd')

print(bd_path)

sys.path.append(cwd_path)
sys.path.append(bd_path)

from bmo_database import *
from bmo_adjust_data import *
import bmo_quality_control as bqc

import pandas as pd

conn = connect_database_remo('PRI')

buoy_id = bmo_on(conn)

for id in buoy_id['buoy_id']:

    last_date_raw = check_last_date(conn, 'bmo_br_current', id)

    last_date_raw = last_date_raw[0][0]

    time_interval = 24 # hours interval of data to be qualified
    bmo_general = get_data_table_db(conn, id, last_date_raw, 'bmo_br_current','ALL')
    bmo_general.set_index('date_time', inplace = True)

    print("Qualifying General data...")
    flag, bmo_qualified = bqc.qualitycontrol_adcp(bmo_general, id)

    print("Rotating....")
    parameters = []
    for i in range(20):
        parameters.append(f"cdir{i+1}")
    bmo_qualified = rotate_data(conn, bmo_qualified, flag, id, parameters)

    flag = rename_flag_data(flag)

    print("General Data ready.")
    print('\n'*2)

    print("Closing connection with Raw Database\n")
    conn.close() # Closing connection with raw database
    print("Connection closed.\n")


    ###########################################################################
    print("Connecting with Qualified Database...\n")
    conn_qc = conn_qc_db('PRI') # Open connection with Qualified Database

    bmo_qc_data = adjust_bmo_current(bmo_qualified)

    bmo_qc_data = pd.merge(bmo_qc_data, flag, how = 'outer', on = 'date_time',
                          validate = 'one_to_one')

    # IDs Key values to delete "old" qualified data...
    ids_pk = bmo_qc_data[['id', 'buoy_id']]

    # TRANSFORMING TO ZULU TIME ( LOCAL + 3H )
    bmo_qc_data = zulu_time(bmo_qc_data)

    # Deleting data to replace...
    delete_adcp_qc_data(conn_qc, ids_pk)

    # Inserting new qualified data...
    insert_bmo_adcp_qc_data(bmo_qc_data)

    print("Closing database connection...")
    conn_qc.close()

    print("Connection closed!\n")
    print("Script finished!")

