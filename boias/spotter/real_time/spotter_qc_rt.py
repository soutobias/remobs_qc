

import sys
import os

cwd = os.getcwd()
sys.path.insert(0, cwd)
sys.path.insert(0, cwd + '/../bd/')

from spotter_quality_control import *
from spotter_database import *
from spotter_adjust_data import *

#os.chdir(os.getcwd() + '/real_time')


conn = connect_database_remo()
spotters_on_ids = spotter_on(conn)

for id_buoy in spotters_on_ids['id_buoy']:

    print(f"Starting Data Qualification for Spotter Buoy {id_buoy}")
    print("\n"*2)

    last_date = check_last_date(conn, 'spotter_general', id_buoy)
    last_date = last_date[0][0]

    raw_data = get_data_spotter(conn, id_buoy, last_date, 24, 'spotter_general')

    # Treating values and index
    raw_data.set_index("date_time", inplace = True)
    raw_data.fillna(value = -999, inplace = True)

    print("Qualifying Spotter Data...")
    print("\n")
    flag_data = qualitycontrol(raw_data)
    print("Spotter Data Qualified!")
    print("\n")

    print("Rotating Data...")
    spotter_qc_data = rotate_data(conn, raw_data, flag_data, id_buoy)
    print("\n")
    print("Done!")
    print("\n")

    flag_data = rename_flag_data(flag_data)

    spotter_qc_data = pd.merge(spotter_qc_data, flag_data, how = 'outer',
                               on = 'date_time', validate = 'one_to_one' )



    print("Closing connection with raw database...")
    conn.close()

    print("Connecting with Qualified Database...")
    conn_qc = conn_qc_db()
    print("Connected!")

    min_data = min(spotter_qc_data.index)
    print("Deleting data...")
    delete_data(conn_qc, 'data_buoys', min_data, id_buoy)
    print("Data deleted!\n")

    insert_spotter_qc_data(conn_qc, spotter_qc_data)
    print("New Qualified Data Inserted \n")
    print("Closing connection with Qualified Database...")
    conn_qc.close()

    print("Spotter Qualification Data Script Finished")
