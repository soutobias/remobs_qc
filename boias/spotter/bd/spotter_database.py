


###
# Connecting to database
################################################################################
def connect_database_remo():
    """Short summary.

    Returns
    -------
    type
        psycopg2 connection
        connection to REMO db

    """
    import sys
    from os.path import expanduser
    home = expanduser('~')
    sys.path.insert(0, home)
    import user_config as USER_CONFIG

    import psycopg2 as pg
    try:
        conn = pg.connect(user=USER_CONFIG.USER,
                          password=USER_CONFIG.PASSWORD,
                          host=USER_CONFIG.HOST,
                          port=USER_CONFIG.PORT,
                          database=USER_CONFIG.DATABASE)

    except Exception as err:
        print("Error: ", err)

    return conn


################################################################################


def spotter_on(conn):
    import pandas as pd

    query = "SELECT id_buoy FROM buoys WHERE model = 'Spotter' AND" \
            " status = 1;"

    id_buoys = pd.read_sql_query(query, conn)

    return id_buoys


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

    cursor.execute(f"SELECT id_buoy FROM buoys WHERE sat_number = '{spotter_id}'")

    id_buoy = cursor.fetchall()

    return id_buoy


################################################################################


def insert_spotter_status(conn, status_df, id_buoy):
    id = id_buoy
    date_time = status_df['timestamp'][0]
    lat = status_df['latitude'][0].round(6)
    lon = status_df['longitude'][0].round(6)
    bat_pow = status_df['battery_power'][0].round(2)
    bat_vol = status_df['battery_voltage'][0].round(2)
    sol_vol = status_df['solar_voltage'][0].round(2)
    humi = status_df['humidity'][0].round(2)

    cursor = conn.cursor()

    try:
        cursor.execute("""INSERT INTO spotter_status (id_buoy, date_time, latitude,\
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

def insert_spotter_general(conn, spotter_df, id_buoy):
    cols = spotter_df.columns.tolist()

    cursor = conn.cursor()
    for i, row in spotter_df[cols].iterrows():
        id_buoy = id_buoy
        date_time = row['timestamp']
        lat = row['latitude']
        lon = row['longitude']
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

        spotter_data = {'id_buoy': id_buoy, 'date': date_time, 'lat': lat, 'lon': lon,
                        'wspd': wspd, 'wdir': wdir, 'seaId': seaId,
                        'swvht': swvht, 'tp': peak_tp, 'mean_tp': mean_tp,
                        'pk_dir': peak_dir, 'wvdir': wvdir,
                        'pk_wvspread': pk_wvspread,
                        'wvspread': wvspread
                        }


        cursor.execute("""INSERT INTO spotter_general (id_buoy, date_time, lat, lon,
                    swvht, tp, mean_tp, pk_dir, pk_wvspread,
                    wvdir, wvspread, wspd, wdir, sea_surface_id) VALUES
                    (%(id_buoy)s, %(date)s, %(lat)s, %(lon)s, %(swvht)s,
                    %(tp)s, %(mean_tp)s, %(pk_dir)s, %(wvdir)s, 
                    %(pk_wvspread)s, %(wvspread)s,%(wspd)s, %(wdir)s,
                     %(seaId)s);""", spotter_data)

    conn.commit()
    print("Data entered succesfully! %s rows entered" % i)

    return


################################################################################


def check_last_date(conn, table, id_buoy):
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
        cursor.execute(f"SELECT deploy_date FROM {table} WHERE id_buoy = {id_buoy}")
        date = cursor.fetchall()

    else:
        cursor.execute(f"SELECT max(date_time) FROM {table} WHERE id_buoy = {id_buoy}")
        date = cursor.fetchall()

    return date

################################################################################


def raw_data_spotter(id_buoy, conn, interval):
    import pandas as pd

    cursor = conn.cursor()

    query = "SELECT * FROM spotter_general" \
            " WHERE date_time > date_trunc('day', NOW() - interval '30 day') ORDER BY date_time"

    raw_spotter = pd.read_sql_query(query, conn)


    return raw_spotter



def get_declination(conn, id_buoy):
    import pandas as pd

    query = (f"SELECT mag_dec, var_mag_dec FROM buoys WHERE id_buoy = {id_buoy};")

    df = pd.read_sql_query(query, conn)

    return df



def conn_qc_db():
    """Short summary.

    Returns
    -------
    type
        psycopg2 connection
        connection to REMO db

    """

    import psycopg2 as pg

    try:
        conn = pg.connect(user="postgres",
                          password='chm@remobs11',
                          host='localhost',
                          port='5432',
                          database='dw_remo')

    except Exception as err:
        print("Error: ", err)

    return conn



def get_data_spotter(conn, id_buoy, last_date, interval_hour, table):

    import pandas as pd
    from datetime import timedelta

    # Getting data from the last x hours
    date_period = last_date - timedelta(hours = interval_hour)

    query = f"SELECT * FROM {table} WHERE date_time > '{date_period}' AND " \
            f"id_buoy = {id_buoy};"

    raw_data = pd.read_sql_query(query, conn)

    return raw_data

def delete_data(conn_qc, table, date_min, id_buoy):

    cursor = conn_qc.cursor()

    cursor.execute(f"DELETE FROM {table} WHERE"
                   f" date_time >= '{date_min}' AND"
                   f" id_buoy = {id_buoy}")

    conn_qc.commit()

    print(f"Data from buoy {id_buoy} after {date_min} deleted from database.")

    return







def insert_spotter_qc_data(conn_qc, spotter_qc_df):
    cols = spotter_qc_df.columns.tolist()

    cursor = conn_qc.cursor()
    row_index = 0
    for index, row in spotter_qc_df[cols].iterrows():
        id_buoy = row['id_buoy']
        id = row['id']
        date_time = index
        lat = row['lat']
        lon = row['lon']
        wspd = row['wspd']
        wdir = row['wdir']
        swvht1 = row['swvht']
        tp1 = row['tp']
        mean_tp = row['mean_tp']
        pk_dir = row['pk_dir']
        wvdir1 = row['wvdir']
        pk_wvspread = row['pk_wvspread']
        wvspread1 = row['wvspread']
        flag_wspd = row['flag_wspd']
        flag_wdir = row['flag_wdir']
        flag_swvht1 = row['flag_swvht']
        flag_tp1 = row['flag_tp']
        flag_mean_tp = row['flag_mean_tp']
        flag_pk_dir = row['flag_pk_dir']
        flag_wvdir1 = row['flag_wvdir']
        flag_pk_wvspread = row['flag_pk_wvspread']
        flag_wvspread1 = row['flag_wvspread']


        spotter_qc_data = {'id_buoy': id_buoy,
                           'id': id,
                            'date': date_time,
                            'lat': lat,
                            'lon': lon,
                            'wspd': wspd,
                            'wdir': wdir,
                            'swvht1': swvht1,
                            'tp1': tp1,
                            'wvdir1': wvdir1,
                            'wvspread1': wvspread1,
                            'pk_dir': pk_dir,
                            'pk_wvspread': pk_wvspread,
                            'mean_tp': mean_tp,
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


        query_insert = """INSERT INTO data_buoys (id_buoy, id, date_time, lat, lon,
                    wspd, wdir, swvht1, tp1, wvdir1, wvspread1, pk_dir, pk_wvspread,
                    mean_tp, flag_wspd, flag_wdir, flag_swvht1, flag_tp1, 
                    flag_wvdir1, flag_wvspread1, flag_pk_dir, flag_pk_wvspread,
                    flag_mean_tp) 
                    VALUES
                    (%(id_buoy)s,%(id)s, %(date)s, %(lat)s, %(lon)s, %(wspd)s,
                    %(wdir)s, %(swvht1)s, %(tp1)s, %(wvdir1)s, 
                    %(wvspread1)s, %(pk_dir)s,%(pk_wvspread)s, %(mean_tp)s,
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

            return

        row_index += 1

    conn_qc.commit()
    print("Commited!")
    print("All %s rows inserted" % row_index)
    return



