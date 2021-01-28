######################################################
######################################################
########   QUALIFICATION of RAW DATA  ################
##############   BMO BUOY   ##########################
######################################################

import sys
import os
home_path = os.environ['HOME']
cwd_path = home_path + '/remobs_qc/boias/bmo_br/real_time/'
print(cwd_path)
bd_path = home_path + '/remobs_qc/boias/bmo_br/bd'



sys.path.append(cwd_path)
sys.path.append(bd_path)


from bmo_database import *
from bmo_adjust_data import *
import bmo_quality_control as bqc

import pandas as pd

conn = connect_database_remo('PRI')

buoy_id = bmo_on(conn)

for id in buoy_id['buoy_id']:

    last_date_raw = check_last_date(conn, 'bmo_br', id)

    last_date_raw = last_date_raw[0][0]

    time_interval = 24 # hours interval of data to be qualified
    bmo_general = get_data_table_db(conn, id, last_date_raw, 'bmo_triaxys_message', time_interval)
    bmo_general[['begin', 'date', 'time', 'serial', 'buoyid', 'lat', 'lon', 'number_of_bands', 'initial_frequency',\
     'frequency_spacing', 'mean_average_direction', 'spread_direction', 'values']] = bmo_general.triaxys_message.str.split(',',12, expand=True)

    bmo_general['date_time_2'] = bmo_general['date'].astype(str) + bmo_general['time']

    bmo_general['date_time_2'] = pd.to_datetime(bmo_general['date_time_2'], format='%Y%m%d%H%M%S')


    del bmo_general['begin']
    del bmo_general['serial']
    del bmo_general['buoyid']
    del bmo_general['lat']
    del bmo_general['lon']
    del bmo_general['date']
    del bmo_general['time']
    del bmo_general['triaxys_message']

    # Deleting data to replace...
    delete_triaxys_old_data(min(bmo_general.date_time), id, conn)

    insert_triaxys_data(bmo_general)

    print("Connection closed!\n")
    print("Script finished!")


# $TSPMA, Date, Time, Serial, BuoyID, Latitude, Longitude, Number of Bands,
# Initial Frequency, Frequency Spacing, Mean Average Direction, Spread Direction,
# Energy 1, Mean Direction 1, Direction Spread1, Energy N, Mean Direction N,
# Direction Spread N, *cs<cr><lf>
