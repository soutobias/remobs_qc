
import mysql.connector as MySQLdb
import pandas as pd
import matplotlib.pyplot as plt
from pandas.io import sql
import sqlalchemy
from time_codes import *


def connect_db(user_config):

    return MySQLdb.connect(host = user_config.host,
                             user = user_config.username,
                             password = user_config.password,
                             database = user_config.database,
                             auth_plugin = 'mysql_native_password')

def working_buoys(user_config):

    db = connect_db(user_config)

    cur=db.cursor(dictionary=True)

    sql = "SELECT * FROM pnboia_estacao WHERE sit=1"
    cur.execute(sql)

    buoys = []
    for row in cur.fetchall():
        buoys.append(row)

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
    cur.execute("SELECT data FROM pnboia_raw_rt WHERE argos_id = %s\
    ORDER BY data DESC limit 5" % buoy)
    for row in cur.fetchall():
        bd_data.append(row)
    print(bd_data)
    if bd_data != []:
        bd_data = bd_data[0]
    for data in raw_data:
        if bd_data != []:
            print(raw_data)
            if data[3] > bd_data[0]:
                sql = "INSERT INTO pnboia_raw_rt (argos_id, lat, lon, data, sensor00, sensor01, \
                sensor02, sensor03, sensor04, sensor05, sensor06, sensor07, sensor08, sensor09, \
                sensor10, sensor11, sensor12, sensor13, sensor14, sensor15, sensor16, sensor17, \
                sensor18, sensor19, sensor20, sensor21, sensor22, sensor23, sensor24, sensor25)\
                VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', \
                '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', \
                '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % \
                (int(data[0]), str(data[1]), str(data[2]), (data[3]), \
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
            sql = "INSERT INTO pnboia_raw_rt (argos_id, lat, lon, data, sensor00, sensor01, \
            sensor02, sensor03, sensor04, sensor05, sensor06, sensor07, sensor08, sensor09, \
            sensor10, sensor11, sensor12, sensor13, sensor14, sensor15, sensor16, sensor17, \
            sensor18, sensor19, sensor20, sensor21, sensor22, sensor23, sensor24, sensor25)\
            VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', \
            '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', \
            '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % \
            (int(data[0]), str(data[1]), str(data[2]), (data[3]), \
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

    cur = db.cursor()

    raw_data = []
    time_last_month = last_month()

    cur.execute("SELECT * FROm pnboia_raw_rt wheRe argos_id = %s and data >= '%s' \
    ORdeR by data" % (buoy, time_last_month))

    for row in cur.fetchall():
        raw_data.append(row[:])

    cur.close()
    db.close()
    return raw_data

def delete_qc_old_data(initial_time, buoy, user_config):

    db = connect_db(user_config)

    cur=db.cursor()

    cur.execute("DELETE FROm pnboia WHERE data>='%s' AND estacao_id = '%s'"% (initial_time, buoy))
    # cur.execute("SELECT wmo FROm deriva_estacao")
    db.commit()
    cur.close()
    db.close()

def insert_qc_data_bd(qc_data, user_config):

    con = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}?auth_plugin=mysql_native_password'.
                                            format(user_config.username,
                                                user_config.password,
                                                user_config.host,
                                                user_config.database))


    qc_data.to_sql(con=con, name='pnboia', if_exists='append')

    print('data inserted')
