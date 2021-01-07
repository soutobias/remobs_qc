

import sys
import os


home_path = os.environ['HOME']
cwd_path = home_path + '/remobs_qc/boias/spotter/real_time/'
bd_path = home_path + '/remobs_qc/boias/spotter/bd'

sys.path.append(cwd_path)
sys.path.append(bd_path)


import pandas as pd

from spotter_quality_control import *
from spotter_database import *
from spotter_adjust_data import *

#os.chdir(os.getcwd() + '/real_time')


conn = connect_database_remo("PRI")
spotters_on_ids = spotter_on(conn)

for buoy_id in spotters_on_ids['buoy_id']:

    print(f"Starting Data Qualification for Spotter Buoy {buoy_id}")
    print("\n"*2)

    last_date = check_last_date(conn, 'spotter_general', buoy_id)
    last_date = last_date[0][0]

    raw_data = get_data_table_db(conn, buoy_id, last_date, 'spotter_general', 'ALL')

    # Treating values and index
    raw_data.set_index("date_time", inplace = True)
    raw_data.fillna(value = -9999, inplace = True)

    print("Qualifying Spotter Data...")
    print("\n")
    flag_data = qualitycontrol(raw_data)
    print("Spotter Data Qualified!")
    print("\n")

    print("Rotating Data...")
    spotter_qc_data = rotate_data(conn, raw_data, flag_data, buoy_id)
    print("\n")
    print("Done!")
    print("\n")

    flag_data = rename_flag_data(flag_data)

    spotter_qc_data = pd.merge(spotter_qc_data, flag_data, how = 'outer',
                               on = 'date_time', validate = 'one_to_one' )



    print("Closing connection with raw database...")
    conn.close()

    print("Connecting with Qualified Database...")
    conn_qc = conn_qc_db("PRI")
    print("Connected!")

    # IDs Key values to delete "old" qualified data...
    ids_pk = spotter_qc_data[['id', 'buoy_id']]
    spotter_qc_data = spotter_qc_data.replace(-9999, np.nan)

    # Removing non valid values (none) from directions degrees field
    non_valid_values = spotter_qc_data < -9000
    spotter_qc_data[non_valid_values] = np.nan


    spotter_qc_data = spotter_qc_data.replace({np.nan:None})

    print("Deleting data...")
    delete_qc_data(conn_qc, ids_pk)
    print("Data deleted!\n")

    status_transaction = insert_spotter_qc_data(conn_qc, spotter_qc_data)

    if status_transaction == 1:
        print("New Qualified Data Inserted\n")


    elif status_transaction == 0:
        print("Error! Closing connection with Qualified Database...")
        conn_qc.close()
        raise Exception("No data inserted, something wrong...\n")

    print("Closing connection with Qualified Database...")
    conn_qc.close()

    print("Spotter Qualification Data Script Finished")
