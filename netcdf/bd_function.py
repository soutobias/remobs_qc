
import os
import sys

home = os.environ['HOME']
sys.path.append(home)

from user_config import *
import psycopg2 as pg

import pandas as pd
from datetime import timedelta


def conn_qc_db(server):
    """Short summary.

    Returns
    -------
    type
        psycopg2 connection
        connection to REMO db

    """
    import psycopg2 as pg

    if server == 'SMM':
        try:
            conn = pg.connect(user=USER_QC,
                              password=PASSWORD_QC,
                              host=HOST_QC,
                              database=DATABASE_QC)

        except Exception as err:
            print("Error: ", err)

        return conn

    elif server == "PRI":
        try:
            conn = pg.connect(user=USER_QC,
                              password=PASSWORD_QC,
                              host=HOST_QC,
                              database=DATABASE_QC)

        except Exception as err:
            print("Error: ", err)

        return conn

    else:
        raise ValueError("Wrong database specification!")
        return


def working_buoys(conn):

    sql = "SELECT * FROM buoys WHERE status = 'Ativa'"
    buoys = pd.read_sql_query(sql, conn)

    return buoys

def cf_names(conn):

    sql = "SELECT * FROM names_dictionaries"
    cf = pd.read_sql_query(sql, conn)

    cf.set_index('id', inplace = True)

    return cf


def get_netcdf_data(buoy, last_date, conn):

    sql = "SELECT * FROM data_buoys WHERE data_buoys.id_buoy = %s \
        AND data_buoys.date_time > '%s' ORDER BY date_time DESC" % (buoy, last_date)

    general_data = pd.read_sql_query(sql, conn)

    general_data = general_data.loc[:,~general_data.columns.duplicated()]

    return general_data


def feed_buoys(conn):

    short_names = ['lat','lon','battery','wspd','gust','wdir','atmp','rh','dewpt','pres','sst','compass','arad','cspd1','cdir1','cspd2','cdir2','cspd3','cdir3','cspd4','cdir4','cspd5','cdir5','cspd6','cdir6','cspd7','cdir7','cspd8','cdir8','cspd9','cdir9','cspd10','cdir10','cspd11','cdir11','cspd12','cdir12','cspd13','cdir13','cspd14','cdir14','cspd15','cdir15','cspd16','cdir16','cspd17','cdir17','cspd18','cdir18','cspd19','cdir19','cspd20','cdir20','swvht1','mxwvht1','tp1','wvdir1','wvspread1','swvht2','mxwvht2','tp2','wvdir2','wvspread2','pk_dir','pk_wvspred','mean_tp']
    long_names = ['latitude','longitude','battery_voltage','wind_speed','wind_speed_of_gust','wind_to_direction','air_temperature','relative_humidity','dew_point_temperature','air_pressure','sea_surface_temperature','buoy_compass','solar_irradiance','sea_water_speed','sea_water_velocity_to_direction','sea_water_speed','sea_water_velocity_to_direction','sea_water_speed','sea_water_velocity_to_direction','sea_water_speed','sea_water_velocity_to_direction','sea_water_speed','sea_water_velocity_to_direction','sea_water_speed','sea_water_velocity_to_direction','sea_water_speed','sea_water_velocity_to_direction','sea_water_speed','sea_water_velocity_to_direction','sea_water_speed','sea_water_velocity_to_direction','sea_water_speed','sea_water_velocity_to_direction','sea_water_speed','sea_water_velocity_to_direction','sea_water_speed','sea_water_velocity_to_direction','sea_water_speed','sea_water_velocity_to_direction','sea_water_speed','sea_water_velocity_to_direction','sea_water_speed','sea_water_velocity_to_direction','sea_water_speed','sea_water_velocity_to_direction','sea_water_speed','sea_water_velocity_to_direction','sea_water_speed','sea_water_velocity_to_direction','sea_water_speed','sea_water_velocity_to_direction','sea_water_speed','sea_water_velocity_to_direction','sea_surface_wave_significant_height','sea_surface_wave_maximum_height','sea_surface_wave_maximum_period','sea_surface_wave_from_direction','sea_surface_wave_directional_spread','sea_surface_wave_significant_height','sea_surface_wave_maximum_height','sea_surface_wave_maximum_period','sea_surface_wave_from_direction','sea_surface_wave_directional_spread','sea_surface_wave_from_direction_maximum','sea_surface_wave_directional_spread_maximum','sea_surface_wave_mean_period']
    units = ['degree_north','degree_east','V','m s-1','m s-1','degree','K','1','K','Pa','K','degree','W m-2','m s-1','degree','m s-1','degree','m s-1','degree','m s-1','degree','m s-1','degree','m s-1','degree','m s-1','degree','m s-1','degree','m s-1','degree','m s-1','degree','m s-1','degree','m s-1','degree','m s-1','degree','m s-1','degree','m s-1','degree','m s-1','degree','m s-1','degree','m s-1','degree','m s-1','degree','m s-1','degree','m','m','s','degree','degree','m','m','s','degree','degree','degree','degree','s']

    cur = conn.cursor()
    for i in range(len(short_names)):
        sql = "INSERT INTO names_dictionaries (short_names, long_names, units) VALUES ('%s', '%s', '%s')" % (short_names[i], long_names[i], units[i])
        cur.execute(sql)
        conn.commit()

    return
