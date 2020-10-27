
import numpy as np
import sys
import os

cwd = os.getcwd()
sys.path.insert(0, cwd)
sys.path.insert(0, cwd + '/../bd/')

from spotter_quality_control import *
from spotter_database import *
from spotter_adjust_data import *




conn = connect_database_remo()
spotters_on_ids = spotter_on(conn)

for id_buoy in spotters_on_ids['id_buoy']:

    print(f"Starting Data Qualification for Spotter Buoy {id_buoy}")
    print("\n"*2)

    raw_data = raw_data_spotter(id_buoy, conn)

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


    print("Finished Spotter Data Qualification.")

