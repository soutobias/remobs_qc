
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

def get_bufr_data(buoy, last_date, conn):

    if last_date == 1:
        sql = "SELECT * FROM data_buoys INNER JOIN buoys ON \
        data_buoys.id_buoy = buoys.id_boia WHERE data_buoys.id_buoy = %s \
        ORDER BY date_time DESC limit 1" % (buoy)

    else:
        sql = "SELECT * FROM data_buoys INNER JOIN buoys ON \
        data_buoys.id_buoy = buoys.id_boia WHERE data_buoys.id_buoy = %s \
        AND data_buoys.date_time > '%s' \
        ORDER BY date_time DESC" % (buoy, last_date)

    general_data = pd.read_sql_query(sql, conn)

    general_data = general_data.loc[:,~general_data.columns.duplicated()]

    return general_data

