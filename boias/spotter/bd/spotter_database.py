import os
import sys
sys.path.append(os.environ['HOME'])
from user_config import *


###
# Connecting to database
################################################################################
def connect_database_remo(server):
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
            conn = pg.connect(user=USER_RAW,
                              password=PASSWORD_RAW,
                              host=HOST_RAW,
                              database=DATABASE_RAW)

        except Exception as err:
            print("Error: ", err)

        return conn

    elif server == "PRI":
        try:
            conn = pg.connect(user=USER_RAW,
                              password=PASSWORD_RAW,
                              host=HOST_RAW,
                              database=DATABASE_RAW)

        except Exception as err:
            print("Error: ", err)

        return conn

    else:
        raise ValueError("Wrong database specification!")
        return



################################################################################


def spotter_on(conn):
    import pandas as pd

    query = "SELECT buoy_id FROM buoys WHERE model = 'Spotter' AND" \
            " status = 1;"

    buoy_ids = pd.read_sql_query(query, conn)

    return buoy_ids


def check_buoy_id(conn, spotter_id):
    """Short summary.

    Parameters
    ----------
    conn : type
        Description of parameter `conn`.
    spotter_id : type
        Description of parameter `spotter_id`.

    Returns
    -------
    type
        Description of returned object.

    """

    cursor = conn.cursor()

    cursor.execute(f"SELECT buoy_id FROM buoys WHERE sat_number = '{spotter_id}'")

    buoy_id = cursor.fetchall()

    return buoy_id


################################################################################


def insert_spotter_status(conn, status_df, buoy_id):
    id = buoy_id
    date_time = status_df['timestamp'][0]
    lat = status_df['latitude'][0].round(6)
    lon = status_df['longitude'][0].round(6)
    bat_pow = status_df['battery_power'][0].round(2)
    bat_vol = status_df['battery_voltage'][0].round(2)
    sol_vol = status_df['solar_voltage'][0].round(2)
    humi = status_df['humidity'][0].round(2)

    cursor = conn.cursor()

    try:
        cursor.execute("""INSERT INTO spotter_status (buoy_id, date_time, latitude,\
                                                    longitude, battery_power,\
                                                    battery_voltage, solar_voltage,\
                                                    humidity) VALUES \
                        (%(id)s, %(date)s, %(lat)s, %(lon)s, %(bat_p)s, %(bat_v)s,\
                         %(sol_v)s, %(humi)s);""", {'id': id,
                                                    'date': date_time,
                                                    'lat': lat,
                                                    'lon': lon,
                                                    'bat_p': bat_pow,
                                                    'bat_v': bat_vol,
                                                    'sol_v': sol_vol,
                                                    'humi': humi})

        conn.commit()
        cursor.close()

        return "Status Spotter Data Inserted Successfully"

    except Exception as err:

        print("Error: ", err)
        print("Rollback Transaction - Cancelled Operation")
        conn.rollback()


        return



################################################################################

def insert_spotter_general(conn, spotter_df, buoy_id):
    cols = spotter_df.columns.tolist()

    cursor = conn.cursor()
    for i, row in spotter_df[cols].iterrows():
        buoy_id = buoy_id
        date_time = row['timestamp']
        lat = row['latitude']
        lon = row['longitude']
        sst = row['sst']
        wspd = row['wspd']
        wdir = row['wdir']
        seaId = row['seasurfaceId']
        swvht = row['swvht']
        peak_tp = row['peakPeriod']
        mean_tp = row['meanPeriod']
        peak_dir = row['peakDirection']
        wvdir = row['meanDirection']
        pk_wvspread = row['peakDirectionalSpread']
        wvspread = row['meanDirectionalSpread']

        spotter_data = {'buoy_id': buoy_id, 'date': date_time, 'lat': lat, 'lon': lon,
                        'sst': sst,
                        'wspd': wspd, 'wdir': wdir, 'seaId': seaId,
                        'swvht': swvht, 'tp': peak_tp, 'mean_tp': mean_tp,
                        'pk_dir': peak_dir, 'wvdir': wvdir,
                        'pk_wvspread': pk_wvspread,
                        'wvspread': wvspread
                        }


        cursor.execute("""INSERT INTO spotter_general (buoy_id, date_time, lat, lon,
                    sst, swvht, tp, mean_tp, pk_dir, wvdir, pk_wvspread,
                    wvspread, wspd, wdir, sea_surface_id) VALUES
                    (%(buoy_id)s, %(date)s, %(lat)s, %(lon)s, %(sst)s,%(swvht)s,
                    %(tp)s, %(mean_tp)s, %(pk_dir)s, %(wvdir)s, 
                    %(pk_wvspread)s, %(wvspread)s,%(wspd)s, %(wdir)s,
                     %(seaId)s);""", spotter_data)

    conn.commit()
    print("Data entered succesfully! %s rows entered" % i)

    return


################################################################################


def check_last_date(conn, table, buoy_id):
    """Short summary.

    Parameters
    ----------
    conn : type
        Description of parameter `conn`.
    table : type
        Description of parameter `table`.

    Returns
    -------
    type
        Description of returned object.

    """

    cursor = conn.cursor()

    if table == 'buoys':
        cursor.execute(f"SELECT deploy_date FROM {table} WHERE buoy_id = {buoy_id}")
        date = cursor.fetchall()

    else:
        cursor.execute(f"SELECT max(date_time) FROM {table} WHERE buoy_id = {buoy_id}")
        date = cursor.fetchall()

    return date

################################################################################


def raw_data_spotter(buoy_id, conn, interval):
    import pandas as pd

    cursor = conn.cursor()

    query = "SELECT * FROM spotter_general" \
            " WHERE date_time > date_trunc('day', NOW() - interval '30 day') ORDER BY date_time"

    raw_spotter = pd.read_sql_query(query, conn)


    return raw_spotter



def get_declination(conn, buoy_id):
    import pandas as pd

    query = (f"SELECT mag_dec, var_mag_dec FROM buoys WHERE buoy_id = {buoy_id};")

    df = pd.read_sql_query(query, conn)

    return df



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


def get_data_table_db(conn, buoy_id, last_date, table, interval_hour):

    import pandas as pd
    from datetime import timedelta

    # Getting data from the last x hours
    if interval_hour == "ALL":

        query = f"SELECT * FROM {table} WHERE buoy_id = {buoy_id};"

        raw_data = pd.read_sql_query(query, conn)

        return raw_data

    else:
        date_period = last_date - timedelta(hours = interval_hour)

        query = f"SELECT * FROM {table} WHERE date_time > '{date_period}' " \
                f" AND buoy_id = {buoy_id};"

        raw_data = pd.read_sql_query(query, conn)

        return raw_data



def delete_data(conn_qc, table, date_min, buoy_id):

    cursor = conn_qc.cursor()

    cursor.execute(f"DELETE FROM {table} WHERE"
                   f" date_time >= '{date_min}' AND"
                   f" buoy_id = {buoy_id}")

    conn_qc.commit()

    print(f"Data from buoy {buoy_id} after {date_min} deleted from database.")

    return


def delete_qc_data(conn_qc, pks_df):

    cursor = conn_qc.cursor()

    buoy_id = pks_df['buoy_id'].unique()[0]
    ids_pk = pks_df['id'].tolist()

    query = f"DELETE FROM data_buoys WHERE buoy_id =" \
            f" {buoy_id} AND id IN {*ids_pk,}"

    try:
        cursor.execute(query)
        print(f"Row with IDs ({*ids_pk,}) from buoy_id {buoy_id} deleted from Qualified database")

    except Exception as err:
        print(err)
        print("Rollback Transaction.")
        print("Transaction Cancelled. No data deleted.")

    return





def insert_spotter_qc_data(conn_qc, spotter_qc_df):
    cols = spotter_qc_df.columns.tolist()

    cursor = conn_qc.cursor()
    row_index = 0
    for index, row in spotter_qc_df[cols].iterrows():
        buoy_id = row['buoy_id']
        id = row['id']
        date_time = index
        lat = row['lat']
        lon = row['lon']
        sst = row['sst']
        wspd = row['wspd']
        wdir = row['wdir']
        swvht1 = row['swvht']
        tp1 = row['tp']
        mean_tp = row['mean_tp']
        pk_dir = row['pk_dir']
        wvdir1 = row['wvdir']
        pk_wvspread = row['pk_wvspread']
        wvspread1 = row['wvspread']
        flag_sst = row['flag_sst']
        flag_wspd = row['flag_wspd']
        flag_wdir = row['flag_wdir']
        flag_swvht1 = row['flag_swvht']
        flag_tp1 = row['flag_tp']
        flag_mean_tp = row['flag_mean_tp']
        flag_pk_dir = row['flag_pk_dir']
        flag_wvdir1 = row['flag_wvdir']
        flag_pk_wvspread = row['flag_pk_wvspread']
        flag_wvspread1 = row['flag_wvspread']


        spotter_qc_data = {'buoy_id': buoy_id,
                           'id': id,
                            'date': date_time,
                            'lat': lat,
                            'lon': lon,
                            'sst': sst,
                            'wspd': wspd,
                            'wdir': wdir,
                            'swvht1': swvht1,
                            'tp1': tp1,
                            'wvdir1': wvdir1,
                            'wvspread1': wvspread1,
                            'pk_dir': pk_dir,
                            'pk_wvspread': pk_wvspread,
                            'mean_tp': mean_tp,
                            'flag_sst': flag_sst,
                            'flag_wspd': flag_wspd,
                            'flag_wdir': flag_wdir,
                            'flag_swvht1': flag_swvht1,
                            'flag_tp1': flag_tp1,
                            'flag_mean_tp': flag_mean_tp,
                            'flag_pk_dir': flag_pk_dir,
                            'flag_wvdir1': flag_wvdir1,
                            'flag_pk_wvspread': flag_pk_wvspread,
                            'flag_wvspread1': flag_wvspread1,
                        }


        query_insert = """INSERT INTO data_buoys (buoy_id, id, date_time, lat, lon, sst,
                    wspd, wdir, swvht1, tp1, wvdir1, wvspread1, pk_dir, pk_wvspread,
                    mean_tp, flag_sst, flag_wspd, flag_wdir, flag_swvht1, flag_tp1, 
                    flag_wvdir1, flag_wvspread1, flag_pk_dir, flag_pk_wvspread,
                    flag_mean_tp) 
                    VALUES
                    (%(buoy_id)s,%(id)s, %(date)s, %(lat)s, %(lon)s, %(sst)s, %(wspd)s,
                    %(wdir)s, %(swvht1)s, %(tp1)s, %(wvdir1)s, 
                    %(wvspread1)s, %(pk_dir)s,%(pk_wvspread)s, %(mean_tp)s, %(flag_sst)s,
                    %(flag_wspd)s, %(flag_wdir)s, %(flag_swvht1)s, %(flag_tp1)s,
                    %(flag_wvdir1)s, %(flag_wvspread1)s, %(flag_pk_dir)s,
                    %(flag_pk_wvspread)s, %(flag_mean_tp)s);"""


        try:
            cursor.execute(query_insert, spotter_qc_data)
            print("Row %s inserted on Qualified Database table" % row_index)
        except Exception as err:
            print(err)
            print("Rollback Transaction in row %s \n" % row_index)
            print("Transaction Cancelled.")
            conn_qc.rollback()

            return 0

        row_index += 1

    conn_qc.commit()
    print("Commited!")
    print("All %s rows inserted" % row_index)
    return 1



