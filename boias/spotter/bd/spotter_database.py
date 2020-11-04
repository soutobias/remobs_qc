


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
        mean_dir = row['meanDirection']
        peak_dir_spread = row['peakDirectionalSpread']
        mean_dir_spread = row['meanDirectionalSpread']

        spotter_data = {'id': id_buoy, 'date': date_time, 'lat': lat, 'lon': lon,
                        'wspd': wspd, 'wdir': wdir, 'seaId': seaId,
                        'swvht': swvht, 'peak_tp': peak_tp, 'mean_tp': mean_tp,
                        'pk_dir': peak_dir, 'mn_dir': mean_dir,
                        'pk_dir_spread': peak_dir_spread,
                        'mn_dir_spread': mean_dir_spread
                        }


        cursor.execute("""INSERT INTO spotter_general (id_buoy, date_time, lat, lon,
                    swvht, peak_tp, mean_tp, peak_dir, peak_dir_spread,
                    mean_dir, mean_dir_spread, wspd, wdir, sea_surface_id) VALUES
                    (%(id)s, %(date)s, %(lat)s, %(lon)s, %(swvht)s,
                    %(peak_tp)s, %(mean_tp)s, %(pk_dir)s, %(mn_dir)s, 
                    %(pk_dir_spread)s, %(mn_dir_spread)s,%(wspd)s, %(wdir)s,
                     %(seaId)s);""", spotter_data)

    conn.commit()
    print("Data entered succesfully! %s rows entered" % i + 1)

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


def insert_spotter_qc_data(conn_qc, spotter_qc_df):
    cols = spotter_df_df.columns.tolist()

    cursor = conn.cursor()
    for i, row in spotter_df_qc[cols].iterrows():
        id_buoy = id_buoy
        date_time = row['timestamp']
        lat = row['latitude']
        lon = row['longitude']
        wspd = row['wspd']
        wdir = row['wdir']
        swvht = row['swvht']
        tp = row['peakPeriod']
        mean_tp = row['meanPeriod']
        peak_dir = row['peakDirection']
        wvdir = row['meanDirection']
        peak_dir_spread = row['peakDirectionalSpread']
        wvspread = row['meanDirectionalSpread']

        flag_wspd = row['flag_wspd']
        flag_wdir = row['flag_wdir']
        flag_swvht = row['flag_swvht']
        flag_tp = row['flag_peakPeriod']
        flag_mean_tp = row['flag_flag_meanPeriod']
        flag_peak_dir = row['flag_peakDirection']
        flag_wvdir = row['flag_meanDirection']
        flag_peak_dir_spread = row['flag_peakDirectionalSpread']
        flag_wvspread = row['flag_meanDirectionalSpread']



        spotter_data = {'id': id_buoy, 'date': date_time, 'lat': lat, 'lon': lon,
                        'wspd': wspd, 'wdir': wdir, 'seaId': seaId,
                        'swvht': swvht, 'peak_tp': peak_tp, 'mean_tp': mean_tp,
                        'pk_dir': peak_dir, 'mn_dir': mean_dir,
                        'pk_dir_spread': peak_dir_spread,
                        'mn_dir_spread': mean_dir_spread,

                        'flag_wspd': wspd, 'flag_wdir': wdir,
                        'flag_swvht': swvht, 'flag_peak_tp': peak_tp, 'flag_mean_tp': mean_tp,
                        'flag_pk_dir': peak_dir, 'flag_mn_dir': mean_dir,
                        'flag_pk_dir_spread': peak_dir_spread,
                        'flag_mn_dir_spread': mean_dir_spread,


                        }


        cursor.execute("""INSERT INTO spotter_general (id_buoy, date_time, lat, lon,
                    swvht, peak_tp, mean_tp, peak_dir, peak_dir_spread,
                    mean_dir, mean_dir_spread, wspd, wdir, sea_surface_id) VALUES
                    (%(id)s, %(date)s, %(lat)s, %(lon)s, %(swvht)s,
                    %(peak_tp)s, %(mean_tp)s, %(pk_dir)s, %(mn_dir)s, 
                    %(pk_dir_spread)s, %(mn_dir_spread)s,%(wspd)s, %(wdir)s,
                     %(seaId)s);""", spotter_data)

    return



