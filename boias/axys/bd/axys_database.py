
#import mysql.connector as MySQLdb
import pandas as pd
#import matplotlib.pyplot as plt
from pandas.io import sql
import sqlalchemy
from time_codes import *
import os
import sys
sys.path.append(os.environ['HOME'])
from user_config import *
import numpy as np

def connect_db(server):
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


def working_buoys(server):

    import pandas as pd
    db = connect_db(server)


    sql = "SELECT * FROM buoys WHERE status=1 AND model = 'Axys'"
    buoys = pd.read_sql_query(sql, db)

    return buoys



def all_buoys(user_config):

    db = connect_db(user_config)

    cur=db.cursor(dictionary=True)

    sql = "SELECT * FROM pnboia_estacao"
    cur.execute(sql)

    buoys = []
    for row in cur.fetchall():
        buoys.append(row)

    return buoys



def insert_raw_data_bd(raw_data, buoy, user_config):

    db = connect_db(user_config)

    cur = db.cursor()

    bd_data = []
    cur.execute("SELECT date_time FROM axys_message WHERE id_buoy = %s\
    ORDER BY date_time DESC limit 5" % buoy)
    for row in cur.fetchall():
        bd_data.append(row)
    print(bd_data)
    if bd_data != []:
        bd_data = bd_data[0]
    for data in raw_data:
        if bd_data != []:
            print(raw_data)
            if data[3] > bd_data[0]:
                sql = "INSERT INTO axys_message (id_buoy,sat_number,lat, lon, date_time, sensor00, sensor01, \
                sensor02, sensor03, sensor04, sensor05, sensor06, sensor07, sensor08, sensor09, \
                sensor10, sensor11, sensor12, sensor13, sensor14, sensor15, sensor16, sensor17, \
                sensor18, sensor19, sensor20, sensor21, sensor22, sensor23, sensor24, sensor25)\
                VALUES ('%s','%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', \
                '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', \
                '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % \
                (buoy, int(data[0]), str(data[1]), str(data[2]), (data[3]), \
                str(data[5]), str(data[6]), str(data[7]), str(data[8]), \
                str(data[9]), str(data[10]), str(data[11]), str(data[12]), \
                str(data[13]), str(data[14]), str(data[15]), str(data[16]), \
                str(data[17]), str(data[18]), str(data[19]), str(data[20]), \
                str(data[21]), str(data[22]), str(data[23]), str(data[24]), \
                str(data[25]), str(data[26]), str(data[27]), str(data[28]), \
                str(data[29]), str(data[30]))
                cur.execute(sql)
                db.commit()
        else:
            print(raw_data)
            sql = "INSERT INTO axys_message (id_buoy,sat_number, lat, lon, date_time, sensor00, sensor01, \
            sensor02, sensor03, sensor04, sensor05, sensor06, sensor07, sensor08, sensor09, \
            sensor10, sensor11, sensor12, sensor13, sensor14, sensor15, sensor16, sensor17, \
            sensor18, sensor19, sensor20, sensor21, sensor22, sensor23, sensor24, sensor25)\
            VALUES ('%s','%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', \
            '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', \
            '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % \
            (buoy, int(data[0]), str(data[1]), str(data[2]), (data[3]), \
            str(data[5]), str(data[6]), str(data[7]), str(data[8]), \
            str(data[9]), str(data[10]), str(data[11]), str(data[12]), \
            str(data[13]), str(data[14]), str(data[15]), str(data[16]), \
            str(data[17]), str(data[18]), str(data[19]), str(data[20]), \
            str(data[21]), str(data[22]), str(data[23]), str(data[24]), \
            str(data[25]), str(data[26]), str(data[27]), str(data[28]), \
            str(data[29]), str(data[30]))
            cur.execute(sql)
            db.commit()



def select_raw_data_bd(buoy, user_config):

    db = connect_db(user_config)


    time_last_month = last_month()

    sql = "SELECT * FROM axys_message WHERE id_buoy = %s and date_time >= '%s' \
    ORDER BY date_time" % (buoy, time_last_month)

    raw_data = pd.read_sql_query(sql, db)

    db.close()

    return raw_data




def select_general_axys_data(buoy, user_config):

    db = connect_db(user_config)


    time_last_month = last_month()

    sql = "SELECT * FROM axys_general WHERE id_buoy = %s and date_time >= '%s' \
    ORDER BY date_time" % (buoy, time_last_month)

    general_data = pd.read_sql_query(sql, db)

    db.close()

    return general_data


def delete_adjusted_old_data(initial_time, buoy, user_config):

    print("Connecting to raw database...\n")
    db = connect_db(user_config)

    cur=db.cursor()

    print("Deleting Raw Data from axys_general table...")
    cur.execute("DELETE FROM axys_general WHERE date_time>='%s' AND id_buoy = '%s'"% (initial_time, buoy))
    # cur.execute("SELECT wmo FROm deriva_estacao")
    db.commit()
    cur.close()
    db.close()




def insert_adjusted_data_bd(qc_data, user_config):

    from sqlalchemy import create_engine

    user = user_config.USER_RAW
    passw = user_config.PASSWORD_RAW
    host = user_config.HOST_RAW
    db = user_config.DATABASE_RAW

    engine = create_engine(f'postgres+psycopg2://{user}:{passw}@{host}/{db}')



    qc_data.to_sql(con=engine, name='axys_general', if_exists='append')

    print('data inserted')




def delete_qc_old_data(initial_time, buoy, user_config):

    db = conn_qc_db(user_config)

    cur=db.cursor()

    cur.execute("DELETE FROM data_buoys WHERE date_time>='%s' AND id_buoy = '%s'"% (initial_time, buoy))
    # cur.execute("SELECT wmo FROm deriva_estacao")
    db.commit()
    cur.close()
    db.close()




def insert_axys_qc_data(qc_data, user_config):
    """

       @param conn: connector of Remo Qualified Database
       @param qc_data: Qualified Axys Data
       @param buoy_id: Id of buoy
       @return: None
       """
    db = conn_qc_db(user_config)

    cursor = db.cursor()
    cols = qc_data.columns.tolist()
    row_index = 0
    for index, row in qc_data[cols].iterrows():

        id = int(row['id_serial'])
        axys_qc_df = {'id_buoy': int(row['id_buoy']),
                            'id' : id,
                            'date_time' : index,
                            'lat': row['lat'].round(6),
                            'lon': row['lon'].round(6),
                            'bat': row['battery'].round(1),
                            'wspd': row['wspd'].round(2),
                            'gust': row['gust'].round(2),
                            'wdir': int(row['wdir']),
                            'atmp': row['atmp'].round(2),
                            'rh': row['rh'].round(2),
                            'dewpt': row['dewpt'].round(2),
                            'pres': row['pres'].round(1),
                            'sst': row['sst'].round(2),
                            'compass': int(row['compass']),
                            'arad': row['arad'].round(2),
                            'cspd1': row['cspd1'].round(2),
                            'cdir1': int(row['cdir1']),
                            'cspd2' : row['cspd2'].round(2),
                            'cdir2' : int(row['cdir2']),
                            'cspd3': row['cspd3'].round(2),
                            'cdir3': int(row['cdir3']),
                            'swvht1' : row['swvht'].round(2),
                            'tp1' : row['tp'].round(),
                            'mxwvht1' : row['mxwvht'].round(2),
                            'wvdir1' : int(row['wvdir']),
                            'wvspread1' : int(row['wvspread']),
                            'flag_wspd': int(row['flag_wspd']),
                            'flag_gust': int(row['flag_gust']),
                            'flag_wdir': int(row['flag_wdir']),
                            'flag_atmp': int(row['flag_atmp']),
                            'flag_rh': int(row['flag_rh']),
                            'flag_dewpt': int(row['flag_dewpt']),
                            'flag_pres': int(row['flag_pres']),
                            'flag_sst': int(row['flag_sst']),
                            'flag_compass': int(row['flag_compass']),
                            'flag_arad': int(row['flag_arad']),
                            'flag_cspd1': int(row['flag_cspd1']),
                            'flag_cdir1': int(row['flag_cdir1']),
                            'flag_cspd2': int(row['flag_cspd2']),
                            'flag_cdir2': int(row['flag_cdir2']),
                            'flag_cspd3': int(row['flag_cspd3']),
                            'flag_cdir3': int(row['flag_cdir3']),
                            'flag_swvht1': int(row['flag_swvht']),
                            'flag_tp1': int(row['flag_tp']),
                            'flag_mxwvht1': int(row['flag_mxwvht']),
                            'flag_wvdir1': int(row['flag_wvdir']),
                            'flag_wvspread1': int(row['flag_wvspread']),
                            }

        for column in axys_qc_df:
            value = axys_qc_df[column]
            if value == -9999 or pd.isnull(value):
                axys_qc_df[column] = None

        query_insert_data = """INSERT INTO data_buoys (id_buoy, id,
        date_time, lat, lon, battery, wspd, gust, wdir, atmp,
        rh, dewpt, pres, sst, compass, arad, cspd1, cdir1, cspd2, cdir2,
        cspd3, cdir3, swvht1, tp1, mxwvht1, wvdir1, wvspread1,
        flag_wspd, flag_gust, flag_wdir, flag_atmp, flag_rh, flag_dewpt,
        flag_pres, flag_sst, flag_compass, flag_arad, flag_cspd1, flag_cdir1,
        flag_cspd2, flag_cdir2, flag_cspd3, flag_cdir3, flag_swvht1, flag_tp1,
        flag_mxwvht1, flag_wvdir1, flag_wvspread1)
         VALUES
        (%(id_buoy)s, %(id)s, %(date_time)s, %(lat)s, %(lon)s, %(bat)s, %(wspd)s,
        %(gust)s, %(wdir)s, %(atmp)s, %(rh)s, %(dewpt)s, %(pres)s, %(sst)s,
        %(compass)s, %(arad)s, %(cspd1)s, %(cdir1)s, %(cspd2)s, %(cdir2)s,
        %(cspd3)s, %(cdir3)s, %(swvht1)s, %(tp1)s, %(mxwvht1)s, %(wvdir1)s,
        %(wvspread1)s, %(flag_wspd)s,
        %(flag_gust)s, %(flag_wdir)s, %(flag_atmp)s, %(flag_rh)s, %(flag_dewpt)s,
        %(flag_pres)s, %(flag_sst)s, %(flag_compass)s, %(flag_arad)s, %(flag_cspd1)s,
        %(flag_cdir1)s, %(flag_cspd2)s, %(flag_cdir2)s, %(flag_cspd3)s, %(flag_cdir3)s,
        %(flag_swvht1)s, %(flag_tp1)s, %(flag_mxwvht1)s, %(flag_wvdir1)s,
        %(flag_wvspread1)s);"""


        try:
            cursor.execute(query_insert_data, axys_qc_df)
            print(f"Row with ID {id} inserted on data_buoys table")
        except Exception as err:
            print(err)
            print(f"Rollback Transaction in row {row_index}"
                  f" ID {id} \n")
            print("Transaction Cancelled.")
            db.rollback()
            return

        row_index += 1

    print("All %s rows inserted" % row_index)
    db.commit()
    print("Commited!")

    return

