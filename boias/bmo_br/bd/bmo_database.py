
###
# Connecting to database
###############################################################################
def connect_database_remo():
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
                          database='remobs_raw')

    except Exception as err:
        print("Error: ", err)

    return conn


###############################################################################
def bmo_on(conn):
    import pandas as pd

    query = "SELECT id_buoy FROM buoys WHERE model = 'BMO-BR' AND" \
            " status = 1;"

    id_buoys = pd.read_sql_query(query, conn)

    return id_buoys


def check_buoy_id(conn, bmo_name):
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

    cursor.execute(f"SELECT id_buoy FROM buoys WHERE name_buoy = '{bmo_name}'")

    id_buoy = cursor.fetchall()

    return id_buoy


###############################################################################

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

###############################################################################
###############################################################################


def get_data_table_db(conn, id_buoy, last_date, interval_hour, table):

    import pandas as pd
    from datetime import timedelta
    cursor = conn.cursor()

    # Getting data from the last x hours
    date_period = last_date - timedelta(hours = interval_hour)

    query = f"SELECT * FROM {table} WHERE date_time > '{date_period}' ;"

    raw_data = pd.read_sql_query(query, conn)

    return raw_data





###############################################################################
###############################################################################

def insert_data_bmo_message(conn, bmo_message_df, id_buoy):
    """

    @param conn: connector of Remo Database
    @param message_df: BMO message Dataframe
    @param buoy_id: Id of buoy
    @return: None
    """
    cursor = conn.cursor()
    cols = bmo_message_df.columns.tolist()

    for i, row in bmo_message_df[cols].iterrows():

        bmo_message_data = {'id_buoy': id_buoy,
                            'date_time' : row['date_time'],
                            'year': row['year'],
                            'month': row['month'],
                            'day': row['day'],
                            'hour': row['hour'],
                            'minute': row['minute'],
                            'lat': row['latitude'],
                            'lon': row['longitude'],
                            'bat': row['battery'],
                            'wspd1': row['wspd1'],
                            'gust1': row['gust1'],
                            'wdir1': row['wdir1'],
                            'wspd2': row['wspd2'],
                            'gust2': row['gust2'],
                            'wdir2': row['wdir2'],
                            'atmp': row['atmp'],
                            'rh': row['rh'],
                            'dewpt': row['dewpt'],
                            'press': row['press'],
                            'sst': row['sst'],
                            'compass': row['compass'],
                            'arad': row['arad'],
                            'cspd1': row['cspd1'],
                            'cdir1': row['cdir1'],
                            'cspd2' : row['cspd2'],
                            'cdir2' : row['cdir2'],
                            'cspd3': row['cspd3'],
                            'cdir3': row['cdir3'],
                            'cspd4': row['cspd4'],
                            'cdir4': row['cdir4'],
                            'cspd5': row['cspd5'],
                            'cdir5': row['cdir5'],
                            'cspd6': row['cspd6'],
                            'cdir6': row['cdir6'],
                            'cspd7': row['cspd7'],
                            'cdir7': row['cdir7'],
                            'cspd8': row['cspd8'],
                            'cdir8': row['cdir8'],
                            'cspd9': row['cspd9'],
                            'cdir9' : row['cdir9'],
                            'cspd10': row['cspd10'],
                            'cdir10': row['cdir10'],
                            'cspd11': row['cspd11'],
                            'cdir11': row['cdir11'],
                            'cspd12': row['cspd12'],
                            'cdir12': row['cdir12'],
                            'cspd13': row['cspd13'],
                            'cdir13': row['cdir13'],
                            'cspd14': row['cspd14'],
                            'cdir14': row['cdir14'],
                            'cspd15': row['cspd15'],
                            'cdir15': row['cdir15'],
                            'cspd16': row['cspd16'],
                            'cdir16' : row['cdir16'],
                            'cspd17' : row['cspd17'],
                            'cdir17' : row['cdir17'],
                            'cspd18' : row['cspd18'],
                            'cdir18' : row['cdir18'],
                            'swvht1' : row['swvht1'],
                            'tp1' : row['tp1'],
                            'mxwvht1' : row['mxwvht1'],
                            'wvdir1' : row['wvdir1'],
                            'wvspread1' : row['wvspread1'],
                            'swvht2' : row['swvht2'],
                            'tp2' : row['tp2'],
                            'wvdir2' : row['wvdir2']
                            }

        query_insert_data = """INSERT INTO bmo_message (id_buoy, date_time, 
        year, month, day, hour, minute, lat, lon, battery, wspd1, gust1, wdir1,
        wspd2, gust2, wdir2, atmp, rh, dewpt, pres, sst, compass, arad,
        cspd1, cdir1, cspd2, cdir2, cspd3, cdir3, cspd4, cdir4, cspd5, cdir5,
        cspd6, cdir6, cspd7, cdir7, cspd8, cdir8, cspd9, cdir9, cspd10, cdir10,
        cspd11, cdir11, cspd12, cdir12, cspd13, cdir13, cspd14, cdir14, cspd15,
        cdir15, cspd16, cdir16, cspd17, cdir17, cspd18, cdir18, swvht1, tp1,
        mxwvht1, wvdir1, wvspread1, swvht2, tp2, wvdir2) VALUES
        (%(id_buoy)s, %(date_time)s, %(year)s, %(month)s, %(day)s, %(hour)s, %(minute)s,
        %(lat)s, %(lon)s, %(bat)s, %(wspd1)s, %(gust1)s, %(wdir1)s, %(wspd2)s, 
        %(gust2)s, %(wdir2)s, %(atmp)s, %(rh)s, %(dewpt)s, %(press)s, %(sst)s, 
        %(compass)s, %(arad)s, %(cspd1)s, %(cdir1)s, %(cspd2)s, %(cdir2)s,
        %(cspd3)s, %(cdir3)s, %(cspd4)s, %(cdir4)s, %(cspd5)s, %(cdir5)s,
        %(cspd6)s, %(cdir6)s, %(cspd7)s, %(cdir7)s,
        %(cspd8)s, %(cdir8)s, %(cspd9)s, %(cdir9)s, %(cspd10)s, %(cdir10)s,
        %(cspd11)s, %(cdir11)s, %(cspd12)s, %(cdir12)s, %(cspd13)s, %(cdir13)s,
        %(cspd14)s, %(cdir14)s, %(cspd15)s, %(cdir15)s, %(cspd16)s, %(cdir16)s,
        %(cspd17)s, %(cdir17)s, %(cspd18)s, %(cdir18)s,
        %(swvht1)s, %(tp1)s, %(mxwvht1)s, %(wvdir1)s, %(wvspread1)s, %(swvht2)s, 
        %(tp2)s, %(wvdir2)s);"""



        cursor.execute(query_insert_data, bmo_message_data)
        conn.commit()

    print("%s rows inserted on bmo_message table" % i)
    #conn.commit()


###############################################################################

def raw_data_bmo(conn, id_buoy, last_date_general):

    import pandas as pd
    cursor = conn.cursor()

    query = "SELECT * FROM bmo_message WHERE date_time > '%s' ;" % last_date_general

    raw_data = pd.read_sql_query(query, conn)

    return raw_data


###############################################################################

def insert_data_bmo_general(conn, bmo_general_df):
    """

    @param conn: connector of Remo Database
    @param message_df: BMO message Dataframe
    @param buoy_id: Id of buoy
    @return: None
    """
    cursor = conn.cursor()
    cols = bmo_general_df.columns.tolist()
    row_index = 0
    for index, row in bmo_general_df[cols].iterrows():

        bmo_general_df = {'id_buoy': row['id_buoy'],
                            'id' : row['id'],
                            'date_time' : index,
                            'lat': row['lat'],
                            'lon': row['lon'],
                            'bat': row['battery'],
                            'wspd1': row['wspd1'],
                            'gust1': row['gust1'],
                            'wdir1': row['wdir1'],
                            'wspd2': row['wspd2'],
                            'gust2': row['gust2'],
                            'wdir2': row['wdir2'],
                            'atmp': row['atmp'],
                            'rh': row['rh'],
                            'dewpt': row['dewpt'],
                            'pres': row['pres'],
                            'sst': row['sst'],
                            'compass': row['compass'],
                            'arad': row['arad'],
                            'cspd1': row['cspd1'],
                            'cdir1': row['cdir1'],
                            'cspd2' : row['cspd2'],
                            'cdir2' : row['cdir2'],
                            'cspd3': row['cspd3'],
                            'cdir3': row['cdir3'],
                            'swvht1' : row['swvht1'],
                            'tp1' : row['tp1'],
                            'mxwvht1' : row['mxwvht1'],
                            'wvdir1' : row['wvdir1'],
                            'wvspread1' : row['wvspread1'],
                            'swvht2' : row['swvht2'],
                            'tp2' : row['tp2'],
                            'wvdir2' : row['wvdir2']
                            }

        query_insert_data = """INSERT INTO bmo_br (id_buoy, id, 
        date_time, lat, lon, battery, wspd1, gust1, wdir1, wspd2, gust2, wdir2, atmp, 
        rh, dewpt, pres, sst, compass, arad, cspd1, cdir1, cspd2, cdir2, 
        cspd3, cdir3, swvht1, tp1, mxwvht1, wvdir1, wvspread1, swvht2, tp2, wvdir2)
         VALUES
        (%(id_buoy)s, %(id)s, %(date_time)s, %(lat)s, %(lon)s, %(bat)s, %(wspd1)s, 
        %(gust1)s, %(wdir1)s, %(wspd2)s, %(gust2)s, %(wdir2)s, %(atmp)s, 
        %(rh)s, %(dewpt)s, %(pres)s, %(sst)s, %(compass)s, %(arad)s, 
        %(cspd1)s, %(cdir1)s, %(cspd2)s, %(cdir2)s, %(cspd3)s, %(cdir3)s,
        %(swvht1)s, %(tp1)s, %(mxwvht1)s, %(wvdir1)s, %(wvspread1)s, %(swvht2)s, 
        %(tp2)s, %(wvdir2)s);"""


        try:
            cursor.execute(query_insert_data, bmo_general_df)
            print("Row %s inserted on bmo_br_general table" % row_index)
        except Exception as err:
            print(err)
            print("Rollback Transaction in row %s \n" % row_index)
            print("Transaction Cancelled.")
            conn.rollback()
            return

        row_index += 1

    conn.commit()
    print("Commited!")
    print("All %s rows insereted" % row_index)
    return



###############################################################################

def insert_data_bmo_current(conn, bmo_general_df):
    """

    @param conn: connector of Remo Database
    @param message_df: BMO message Dataframe
    @param buoy_id: Id of buoy
    @return: None
    """
    cursor = conn.cursor()
    cols = bmo_general_df.columns.tolist()
    row_index = 0
    for index, row in bmo_general_df[cols].iterrows():

        bmo_general_df = {'id_buoy': row['id_buoy'],
                            'id' : row['id'],
                            'date_time' : index,
                            'lat': row['lat'],
                            'lon': row['lon'],
                            'cspd1': row['cspd1'],
                            'cdir1': row['cdir1'],
                            'cspd2': row['cspd2'],
                            'cdir2': row['cdir2'],
                            'cspd3': row['cspd3'],
                            'cdir3': row['cdir3'],
                            'cspd4': row['cspd4'],
                            'cdir4': row['cdir4'],
                            'cspd5': row['cspd5'],
                            'cdir5': row['cdir5'],
                            'cspd6': row['cspd6'],
                            'cdir6': row['cdir6'],
                            'cspd7': row['cspd7'],
                            'cdir7': row['cdir7'],
                            'cspd8': row['cspd8'],
                            'cdir8': row['cdir8'],
                            'cspd9': row['cspd9'],
                            'cdir9': row['cdir9'],
                            'cspd10': row['cspd10'],
                            'cdir10': row['cdir10'],
                            'cspd11': row['cspd11'],
                            'cdir11': row['cdir11'],
                            'cspd12': row['cspd12'],
                            'cdir12': row['cdir12'],
                            'cspd13': row['cspd13'],
                            'cdir13': row['cdir13'],
                            'cspd14': row['cspd14'],
                            'cdir14': row['cdir14'],
                            'cspd15': row['cspd15'],
                            'cdir15': row['cdir15'],
                            'cspd16': row['cspd16'],
                            'cdir16': row['cdir16'],
                            'cspd17': row['cspd17'],
                            'cdir17': row['cdir17'],
                            'cspd18': row['cspd18'],
                            'cdir18': row['cdir18'],
                             }

        query_insert_data = """INSERT INTO bmo_br_current (id_buoy, id, 
        date_time, lat, lon, cspd1, cdir1, cspd2, cdir2, cspd3, cdir3, cspd4, cdir4, 
        cspd5, cdir5, cspd6, cdir6, cspd7, cdir7, cspd8, cdir8, cspd9, cdir9, cspd10, cdir10, 
        cspd11, cdir11, cspd12, cdir12, cspd13, cdir13, cspd14, cdir14, cspd15, cdir15,
        cspd16, cdir16, cspd17, cdir17, cspd18, cdir18)
         VALUES
        (%(id_buoy)s, %(id)s, %(date_time)s, %(lat)s, %(lon)s, %(cspd1)s, %(cdir1)s, 
        %(cspd2)s, %(cdir2)s, %(cspd3)s, %(cdir3)s ,%(cspd4)s, %(cdir4)s, %(cspd5)s, %(cdir5)s,
        %(cspd6)s, %(cdir6)s ,%(cspd7)s, %(cdir7)s ,%(cspd8)s, %(cdir8)s ,%(cspd9)s, %(cdir9)s,
        %(cspd10)s, %(cdir10)s ,%(cspd11)s, %(cdir11)s ,%(cspd12)s, %(cdir12)s,
        %(cspd13)s, %(cdir13)s ,%(cspd14)s, %(cdir14)s ,%(cspd15)s, %(cdir15)s,
         %(cspd16)s, %(cdir16)s ,%(cspd17)s, %(cdir17)s ,%(cspd18)s, %(cdir18)s);"""


        try:
            cursor.execute(query_insert_data, bmo_general_df)
            print("Row %s inserted on bmo_br_current table" % row_index)
        except Exception as err:
            print(err)
            print("Rollback Transaction in row %s \n" % row_index)
            print("Transaction Cancelled.")
            conn.rollback()
            return



        row_index += 1

    conn.commit()
    print("Commited!")
    print("All %s rows insereted" % row_index)
    return



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






def insert_bmo_qc_data(conn, bmo_qc_data_df):
    """

       @param conn: connector of Remo Qualified Database
       @param message_df: BMO message Dataframe
       @param buoy_id: Id of buoy
       @return: None
       """
    cursor = conn.cursor()
    cols = bmo_qc_data_df.columns.tolist()
    row_index = 0
    for index, row in bmo_qc_data_df[cols].iterrows():

        bmo_qc_df = {'id_buoy': row['id_buoy'],
                            'id' : row['id'],
                            'date_time' : index,
                            'lat': row['lat'],
                            'lon': row['lon'],
                            'bat': row['battery'],
                            'wspd': row['wspd'],
                            'gust': row['gust'],
                            'wdir': row['wdir'],
                            'atmp': row['atmp'],
                            'rh': row['rh'],
                            'dewpt': row['dewpt'],
                            'pres': row['pres'],
                            'sst': row['sst'],
                            'compass': row['compass'],
                            'arad': row['arad'],
                            'cspd1': row['cspd1'],
                            'cdir1': row['cdir1'],
                            'cspd2' : row['cspd2'],
                            'cdir2' : row['cdir2'],
                            'cspd3': row['cspd3'],
                            'cdir3': row['cdir3'],
                            'swvht1' : row['swvht1'],
                            'tp1' : row['tp1'],
                            'mxwvht1' : row['mxwvht1'],
                            'wvdir1' : row['wvdir1'],
                            'wvspread1' : row['wvspread1'],
                            'swvht2' : row['swvht2'],
                            'tp2' : row['tp2'],
                            'wvdir2' : row['wvdir2'],
                            'flag_wspd': row['flag_wspd'],
                            'flag_gust': row['flag_gust'],
                            'flag_wdir': row['flag_wdir'],
                            'flag_atmp': row['flag_atmp'],
                            'flag_rh': row['flag_rh'],
                            'flag_dewpt': row['flag_dewpt'],
                            'flag_pres': row['flag_pres'],
                            'flag_sst': row['flag_sst'],
                            'flag_compass': row['flag_compass'],
                            'flag_arad': row['flag_arad'],
                            'flag_cspd1': row['flag_cspd1'],
                            'flag_cdir1': row['flag_cdir1'],
                            'flag_cspd2': row['flag_cspd2'],
                            'flag_cdir2': row['flag_cdir2'],
                            'flag_cspd3': row['flag_cspd3'],
                            'flag_cdir3': row['flag_cdir3'],
                            'flag_swvht1': row['flag_swvht1'],
                            'flag_tp1': row['flag_tp1'],
                            'flag_mxwvht1': row['flag_mxwvht1'],
                            'flag_wvdir1': row['flag_wvdir1'],
                            'flag_wvspread1': row['flag_wvspread1'],
                            'flag_swvht2': row['flag_swvht2'],
                            'flag_tp2': row['flag_tp2'],
                            'flag_wvdir2': row['flag_wvdir2']
                            }

        query_insert_data = """INSERT INTO data_buoys (id_buoy, id, 
        date_time, lat, lon, battery, wspd, gust, wdir, atmp, 
        rh, dewpt, pres, sst, compass, arad, cspd1, cdir1, cspd2, cdir2, 
        cspd3, cdir3, swvht1, tp1, mxwvht1, wvdir1, wvspread1, swvht2, tp2, wvdir2,
        flag_wspd, flag_gust, flag_wdir, flag_atmp, flag_rh, flag_dewpt, 
        flag_pres, flag_sst, flag_compass, flag_arad, flag_cspd1, flag_cdir1, 
        flag_cspd2, flag_cdir2, flag_cspd3, flag_cdir3, flag_swvht1, flag_tp1, 
        flag_mxwvht1, flag_wvdir1, flag_wvspread1, flag_swvht2, flag_tp2, flag_wvdir2)
         VALUES
        (%(id_buoy)s, %(id)s, %(date_time)s, %(lat)s, %(lon)s, %(bat)s, %(wspd)s, 
        %(gust)s, %(wdir)s, %(atmp)s, %(rh)s, %(dewpt)s, %(pres)s, %(sst)s,
        %(compass)s, %(arad)s, %(cspd1)s, %(cdir1)s, %(cspd2)s, %(cdir2)s,
        %(cspd3)s, %(cdir3)s, %(swvht1)s, %(tp1)s, %(mxwvht1)s, %(wvdir1)s,
        %(wvspread1)s, %(swvht2)s, %(tp2)s, %(wvdir2)s, %(flag_wspd)s, 
        %(flag_gust)s, %(flag_wdir)s, %(flag_atmp)s, %(flag_rh)s, %(flag_dewpt)s,
        %(flag_pres)s, %(flag_sst)s, %(flag_compass)s, %(flag_arad)s, %(flag_cspd1)s, 
        %(flag_cdir1)s, %(flag_cspd2)s, %(flag_cdir2)s, %(flag_cspd3)s, %(flag_cdir3)s,
        %(flag_swvht1)s, %(flag_tp1)s, %(flag_mxwvht1)s, %(flag_wvdir1)s, 
        %(flag_wvspread1)s, %(flag_swvht2)s, %(flag_tp2)s, %(flag_wvdir2)s);"""


        try:
            cursor.execute(query_insert_data, bmo_qc_df)
            print("Row %s inserted on data_buoys table" % row_index)
        except Exception as err:
            print(err)
            print("Rollback Transaction in row %s \n" % row_index)
            print("Transaction Cancelled.")
            conn.rollback()
            return

        row_index += 1

    conn.commit()
    print("Commited!")
    print("All %s rows insereted" % row_index)
    return







