import os
import sys
import pandas as pd
import psycopg2 as pg

sys.path.append(os.environ['HOME'])

from user_config import USER_RAW, PASSWORD_RAW, HOST_RAW, DATABASE_RAW


class db_tag():

    def __init__(self, host=HOST_RAW, db=DATABASE_RAW, user=USER_RAW, pswd=PASSWORD_RAW):
        self.conn = pg.connect(host=host,
                                database=db,
                                user=user,
                                password=pswd)

    def get_last_time(self, tag_number):

        try:
            query = f"""SELECT max(date_time) FROM tag_location WHERE id_tag = '{tag_number}'"""
        except Exception as Err:
            return print(Err)

        return pd.read_sql_query(query, self.conn)


    def get_buoy_tag(self, tag_number):

        try:
            query = f"""SELECT buoy_id FROM tag_buoys WHERE id_tag = '{tag_number}'"""
        except Exception as Err:
            return print(Err)

        return pd.read_sql_query(query, self.conn)




    def insert_tag_data(self, data, buoy_id):

        cursor = self.conn.cursor()

        data['buoy_id'] = buoy_id

        try:

            sql = """INSERT INTO tag_location(id_tag, buoy_id, date_time, lat, lon) \
                    VALUES(%(tag_id)s, %(buoy_id)s, %(date_time)s, \
                    %(latitude)s, %(longitude)s);"""

            cursor.execute(sql, data)
            print("Tag data inserted on database.")

            self.conn.commit()
            print('Transaction commited.')

        except Exception as Err:
            print(Err)
            print("No data inserted.")
