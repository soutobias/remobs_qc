# Script to collect spotter data from Sofar API

import pandas as pd
from datetime import datetime

from pysofar.sofar import SofarApi

import sys
import os
cwd = os.getcwd()
sys.path.insert(0, cwd)
sys.path.insert(0, cwd + '/../bd/')
sys.path.insert(0, cwd + '/../limits/')
sys.path.insert(0, cwd + '/../qc_checks/')


from spotter_database import *

# init the api
api = SofarApi()
# get the devices belonging to you

devices = api.device_ids

spotter_grid = api.get_spotters()

# Connecting to database
conn = connect_database_remo('PRI')

for n_buoy in range(len(devices)):

    # spotter buoy data
    spot = spotter_grid[n_buoy]
    spotter_id = spot.id



    id_buoy = check_buoy_id(conn, spotter_id)
    id_buoy = id_buoy[0][0]

    ########################################
    # Status Data
    # Last data available

    columns_status = ['spotter_id', 'timestamp', 'latitude',
                      'longitude', 'battery_power', 'battery_voltage',
                      'humidity', 'solar_voltage']

    id = id_buoy
    lon = (spot.lon + 180) % 360 - 180
    lat = spot.lat
    date = spot.timestamp
    solar_v = float(spot.solar_voltage)
    battery_p = float(spot.battery_power)
    humidity = float(spot.humidity)
    battery_v = float(spot.battery_voltage)

    # date string to datetime
    date_spotter = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.000Z')

    # check last data of database
    last_date_status = check_last_date(conn, 'spotter_status', id)
    last_date_status = last_date_status[0][0]

    if last_date_status == None or date_spotter > last_date_status:
        status_values = {'spotter_id': [id],
                         'timestamp': [date],
                         'latitude': [lat],
                         'longitude': [lon],
                         'battery_power': [battery_p],
                         'battery_voltage': [battery_v],
                         'humidity': [humidity],
                         'solar_voltage': [solar_v]}

        status_spotter = pd.DataFrame(status_values)

        print("Last status:")
        print(status_values)

        print("Inserting on database...")
        insert_spotter_status(conn, status_spotter, id)

        last_date_status = date_spotter


    else:
        print("No new status for spotter %s." % id)





    ####################
    # Environmental Data

    last_date_general = check_last_date(conn, 'spotter_general', id)
    last_date_general = last_date_general[0][0]


    if last_date_general == None or last_date_status == None or last_date_general < last_date_status:
        # getting the deploy date
        if last_date_general == None:
            print("No data on database for buoy %s, start date from deploy date." % id)
            date = check_last_date(conn, 'buoys', id)
            start_date = date[0][0]
            time_to_start = datetime.min.time()
            start_date = datetime.combine(start_date, time_to_start)
            print(start_date)

        elif last_date_general != None and last_date_general < last_date_status:
            start_date = last_date_general


        end_date = datetime.utcnow()

        spt_data = spot.grab_data(
            limit=100,
            start_date=start_date,
            end_date=end_date,
            include_track=True,
            include_wind=True,
            include_surface_temp_data=True,
            include_frequency_data=False,
            include_directional_moments=True
        )

        wind = pd.json_normalize(spt_data, record_path=['wind'], meta=['spotterId'])
        waves = pd.json_normalize(spt_data, record_path=['waves'], meta=['spotterId'])

        # wind dataframe

        wind_spotter = wind[['speed', 'direction', 'seasurfaceId',
                             'latitude', 'longitude', 'timestamp']]

        # getting just new data :
        format_date = '%Y-%m-%dT%H:%M:%S.000Z'

        wind_spotter.loc[:,'timestamp'] = pd.to_datetime(wind.loc[:,'timestamp'], format = format_date)
        wind_spotter = wind_spotter.loc[wind_spotter['timestamp']>start_date]



        waves_spotter = waves[['significantWaveHeight', 'peakPeriod', 'meanPeriod',
                               'peakDirection', 'peakDirectionalSpread',
                               'meanDirection', 'meanDirectionalSpread', 'timestamp']]

        waves_spotter.loc[:,'timestamp'] = pd.to_datetime(waves_spotter.loc[:,'timestamp'], format=format_date)

        waves_spotter = waves_spotter.loc[waves_spotter['timestamp'] > start_date]


        spotter_general = wind_spotter.merge(waves_spotter,
                                             on='timestamp',
                                             how='left')

        spotter_general.rename(columns = {'speed': 'wspd',
                                          'direction': 'wdir',
                                          'significantWaveHeight' : 'swvht'
                                        }, inplace = True)

        insert_spotter_general(conn, spotter_general, id)


    elif last_date_general != None and last_date_general == last_date_status:
        print("No new data for spotter buoy %s." % id)


print("Closing DataBase Connection...")
conn.close()
print("Spotter Script Finished!")
