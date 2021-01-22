import os
import sys
import pandas as pd

sys.path.append(os.environ['HOME'])
from user_config import USER_RAW, PASSWORD_RAW, HOST_RAW, DATABASE_RAW, USER_QC, PASSWORD_QC, DATABASE_QC, HOST_QC


import psycopg2 as pg

class db_remo():

    def __init__(self, host=HOST_RAW, db=DATABASE_RAW, user=USER_RAW, pwd=PASSWORD_RAW):
        self.conn = pg.connect(host=host, database=db, user=user, password=pwd)

    def active_buoys(self):
        try:
            query = """SELECT * FROM buoys WHERE status = 1"""
        except Exception as Err:
            return print(Err)
        return pd.read_sql_query(query, self.conn)

    def last_positions(self, buoy_type, buoy_id, n = 5):
        """Select the last N positions of the buoy specified"""

        if buoy_type == 'BMO':
            table = 'bmo_message'
        elif buoy_type == 'AXYS':
            table = 'axys_message'
        elif buoy_type == 'SPOTTER':
            table = 'spotter_general'
        
        try:
            query = f"""SELECT date_time, lat, lon FROM {table} WHERE buoy_id = {buoy_id} 
            ORDER BY date_time DESC LIMIT {n};"""
            
        except Exception as Err:
            return print(Err)

        return pd.read_sql_query(query, self.conn)

    def get_data(self, query):

        try:
            data = pd.read_sql_query(query, self.conn)
        except Exception as Err:
            return print(Err)

        return data


class db_remo_qc():

    def __init__(self, host=HOST_QC, db=DATABASE_QC, user=USER_QC,
                 pwd=PASSWORD_QC):
        self.conn = pg.connect(host=host, database=db, user=user, password=pwd)

    def get_data(self, query):

        try:
            data = pd.read_sql_query(query, self.conn)
        except Exception as Err:
            return print(Err)

        return data

