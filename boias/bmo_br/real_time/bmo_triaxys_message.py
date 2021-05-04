######################################################
######################################################
########   QUALIFICATION of RAW DATA  ################
##############   BMO BUOY   ##########################
######################################################

import sys
import os

home_path = os.environ['HOME']
abs_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
cwd_path = os.path.join(abs_dir, 'boias', 'bmo_br', 'real_time')
bd_path = os.path.join(abs_dir, 'boias', 'bmo_br', 'bd')
sys.path.append(cwd_path)
sys.path.append(bd_path)

import warnings
warnings.filterwarnings('ignore')

from bmo_database import *
from bmo_adjust_data import *
import bmo_quality_control as bqc

import pandas as pd
import numpy as np
conn = connect_database_remo('PRI')

buoy_id = bmo_on(conn)

for id in buoy_id['buoy_id']:

    last_date_raw = check_last_date(conn, 'bmo_br', id)

    last_date_raw = last_date_raw[0][0]

    time_interval = 24*10 # hours interval of data to be qualified
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

    bmo_general = bmo_general.dropna()

    bmo_general['values'] = bmo_general['values'].str.replace('\*\d*.*', '').str.strip().str.split(',')
    bmo_general['number_of_bands'] = pd.to_numeric(bmo_general['number_of_bands'])
    bmo_general['initial_frequency'] = pd.to_numeric(bmo_general['initial_frequency'])
    bmo_general['frequency_spacing'] = pd.to_numeric(bmo_general['frequency_spacing'])
    bmo_general['mean_average_direction'] = pd.to_numeric(bmo_general['mean_average_direction'])
    bmo_general['spread_direction'] = pd.to_numeric(bmo_general['spread_direction'])

    spacing = []
    for index, row in bmo_general.iterrows():
        spacing.append(np.round(np.linspace(row['initial_frequency'], \
            row['frequency_spacing']*(row['number_of_bands'] - 1)+row['initial_frequency'], \
            row['number_of_bands']), 3))

    energy = []
    wvdir = []
    spread = []
    for index, row in bmo_general.iterrows():
        e = []
        w = []
        s = []
        choice = 'e'
        for i in row['values']:
            if choice == 'e':
                try:
                    x = float(i)
                except:
                    break
                e.append(float(i))
                choice = 'w'
            elif choice == 'w':
                try:
                    x = float(i)
                except:
                    break
                w.append(float(i))
                choice = 's'
            else:
                try:
                    x = float(i)
                except:
                    break
                s.append(float(i))
                choice = 'e'
        energy.append(e)
        wvdir.append(w)
        spread.append(s)

    date_time = []
    spacing_1 = []
    energy_1 = []
    wvdir_1 = []
    spread_1 = []
    mean_average_direction = []
    spread_direction = []
    buoy_id = []
    data_id = []
    for idx, value in enumerate(spacing):
        if len(value) == len(energy[idx]) and len(value) == len(wvdir[idx]) and len(value) == len(spread[idx]):
            for i in range(len(value)):
                buoy_id.append(bmo_general.iloc[idx]['buoy_id'])
                data_id.append(bmo_general.iloc[idx]['id'])
                date_time.append(bmo_general.iloc[idx]['date_time'])
                mean_average_direction.append(bmo_general.iloc[idx]['mean_average_direction'])
                spread_direction.append(bmo_general.iloc[idx]['spread_direction'])
                spacing_1.append(value[i])
                energy_1.append(energy[idx][i])
                wvdir_1.append(wvdir[idx][i])
                spread_1.append(spread[idx][i])

    total_df = pd.DataFrame(np.array([buoy_id, data_id, date_time, mean_average_direction, spread_direction, spacing_1, energy_1, wvdir_1, spread_1]).T, \
                            columns=['buoy_id', 'data_id', 'date_time', 'mean_average_direction', 'spread_direction', 'period', 'energy', 'wvdir', 'spread'])
    total_df.sort_values('date_time', inplace=True)
    total_df['period'] = np.round((1/total_df['period'].astype(float)), 2)
    total_df['wvdir'] = total_df['wvdir'].astype(float)
    total_df['spread'] = total_df['spread'].astype(float)
    total_df['mean_average_direction'] = total_df['mean_average_direction'].astype(float)
    total_df['spread_direction'] = total_df['spread_direction'].astype(float)
    total_df['energy'] = total_df['energy'].astype(float)
    total_df['buoy_id'] = total_df['buoy_id'].astype(int)
    total_df['data_id'] = total_df['data_id'].astype(int)

    # Deleting data to replace...
    delete_triaxys_old_data(min(total_df.date_time), id, conn)

    insert_triaxys_data(total_df)

    print("Connection closed!\n")
    print("Script finished!")


# $TSPMA, Date, Time, Serial, BuoyID, Latitude, Longitude, Number of Bands,
# Initial Frequency, Frequency Spacing, Mean Average Direction, Spread Direction,
# Energy 1, Mean Direction 1, Direction Spread1, Energy N, Mean Direction N,
# Direction Spread N, *cs<cr><lf>
