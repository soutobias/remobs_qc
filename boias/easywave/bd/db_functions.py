import psycopg2 as pg
import pandas as pd
from datetime import datetime
from datetime import timedelta

class remobs_db():

    _db = None
    def __init__(self, host, db, usr, pwd):
        self._db = pg.connect(host=host, database=db, user=usr, password=pwd)

    def db_exec(self, query):
        try:
            cur = self._db.cursor()
            cur.execute(query)
            cur.close()
            self._db.commit()
        except:
            return False
        return True


    def db_select(self, query):
        try:
            data = pd.read_sql_query(query, self._db)
        except:
            return False
        return data


    def get_last_time(self, buoy_id):
        
        query = f"SELECT max(date_time) FROM easywave WHERE buoy_id = {buoy_id};"

        try:
            date = pd.read_sql_query(query, self._db).iloc[0,0]

            if date == None:
                deploy_query = f"SELECT deploy_date FROM buoys WHERE buoy_id = {buoy_id}"
                deploy_date = pd.read_sql_query(deploy_query, self._db).iloc[0,0]
                deploy_date = last_date_dt = datetime.strptime(deploy_date.strftime("%Y-%m-%d %H:%M:%S"),"%Y-%m-%d %H:%M:%S")
                date = deploy_date

        except:
            return False
        return date


    def insert_ew_data(self, df, buoy_id):

        cur = self._db.cursor()
        cols = df.columns.tolist()

        rows = 1
        for i, row in df[cols].iterrows():

            ew_message_data = {'buoy_id': buoy_id,
                    'date_time' : row['date_time'],
                    'year': row['year'],
                    'month': row['month'],
                    'day': row['day'],
                    'hour': row['hour'],
                    'minute': row['minute'],
                    'lat': row['lat'],
                    'lon': row['lon'],
                    'bat': row['battery_v'],
                    'temp_datalogger': row['temp_datalogger'],
                    'swvht': row['swvht'],
                    'tp': row['tp'],
                    'wvdir': row['wvdir']}

            query_insert_data = """INSERT INTO easywave (buoy_id, date_time, 
            year, month, day, hour, minute, lat, lon, battery_voltage,temp_datalogger, swvht, tp, wvdir)
            VALUES (%(buoy_id)s, %(date_time)s, %(year)s, %(month)s, %(day)s, 
            %(hour)s, %(minute)s, %(lat)s, %(lon)s, %(bat)s,%(temp_datalogger)s, %(swvht)s,
             %(tp)s, %(wvdir)s);"""

            try:
                cur.execute(query_insert_data, ew_message_data)
                print("Row %s inserted on easywave table" % rows)

            except Exception as err:
                print("Error on insert data on ew_message table.")
                print('Error: ', err)
                print("Rollback Transaction in row %s \n" % rows)
                print("Transaction Cancelled.")
                self._db.rollback()
                return 0

            rows += 1


        self._db.commit()
        print("Commited!")
        print("All %s rows insereted" % rows)
        return 1



    def ew_on(self):
        

        query = "SELECT buoy_id FROM buoys WHERE model = 'EasyWave' AND" \
                " status = 1;"

        buoy_ids = pd.read_sql_query(query, self._db)

        return buoy_ids


    def get_data_table_db(self, buoy_id, last_date_raw, time_interval):

        date_period = last_date_raw - timedelta(hours = time_interval)

        query = f"""SELECT * FROM easywave WHERE date_time > '{date_period}'
        AND buoy_id = {buoy_id}"""

        raw_data = pd.read_sql_query(query, self._db)

        return raw_data


class remobs_qc_db():

    _db = None
    def __init__(self, host, db, usr, pwd):
        self._db = pg.connect(host=host, database=db, user=usr, password=pwd)

    def db_exec(self, query):
        try:
            cur = self._db.cursor()
            cur.execute(query)
            cur.close()
            self._db.commit()
        except:
            return False
        return True

    def insert_ew_qc_data(self, df_ew_qc):

        cur = self._db.cursor()
        cols = df_ew_qc.columns.tolist()

        buoy_id = int(df_ew_qc['buoy_id'][0])

        rows = 1
        for index, row in df_ew_qc[cols].iterrows():

            id = int(row['id'])

            ew_qc_data = {'buoy_id': buoy_id,
                        'id': id,
                        'date_time' : index,
                        'lat': row['lat'],
                        'lon': row['lon'],
                        'bat': row['battery'],
                        'swvht1': row['swvht'],
                        'tp1': row['tp'],
                        'wvdir1': int(row['wvdir']),
                        'flag_swvht1': int(row['flag_swvht']),
                        'flag_tp1': int(row['flag_tp']),
                        'flag_wvdir1': int(row['flag_wvdir'])}

            query_insert_data = """INSERT INTO data_buoys (buoy_id, id, date_time, 
            lat, lon, battery, swvht1, tp1, wvdir1, flag_swvht1, flag_tp1, flag_wvdir1)
            VALUES (%(buoy_id)s,%(id)s, %(date_time)s, %(lat)s, %(lon)s,
             %(bat)s, %(swvht1)s,%(tp1)s, %(wvdir1)s, 
             %(flag_swvht1)s,%(flag_tp1)s, %(flag_wvdir1)s);"""

            try:
                cur.execute(query_insert_data, ew_qc_data)
                print("Row %s inserted on data_buoys table" % rows)

            except Exception as err:
                print("Error on insert data on data_buoys table.")
                print('Error: ', err)
                print("Rollback Transaction in row %s \n" % rows)
                print("Transaction Cancelled.")
                self._db.rollback()
                return 0

            rows += 1


        self._db.commit()
        print("Commited!")
        print("All %s rows insereted" % rows)
        return 1


    def delete_ew_qc_data(self, df_ids_pk):

        cursor = self._db.cursor()

        buoy_id = df_ids_pk['buoy_id'].unique()[0]
        ids_pk = df_ids_pk['id'].tolist()

        query = f"""DELETE FROM data_buoys WHERE buoy_id = 
                    {buoy_id} AND id IN {*ids_pk,}"""


        try:
            cursor.execute(query)
            print(f"Row with IDs ({*ids_pk,}) from buoy {buoy_id} deleted from Qualified database")

        except Exception as err:
            print(err)
            print("Rollback Transaction.")
            print("Transaction Cancelled. No data deleted.")


        return