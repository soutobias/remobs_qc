
from user_config import *

def insert_remo_model_db(remo_df, server, buoy_id, model):


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



    else:
        raise ValueError("Wrong database specification!")
        return


    if conn.closed == 0:

        cols = remo_df.columns.tolist()

        cursor = conn.cursor()
        for i, row in remo_df[cols].iterrows():
            buoy_id = buoy_id
            model = model
            date_time = row['date_time']
            lat = row['lat']
            lon = row['lon']
            wspd = row['wspd']
            wdir = row['wdir']
            tp = row['tp']
            swvht = row['swvht']
            wvdir= row['wvdir']

            remo_data = {'buoy_id': buoy_id, 'model':model, 'date_time': date_time, 'lat': lat,
                            'lon': lon,
                             'swvht': swvht, 'wvdir': wvdir, 'tp':tp,
                            'wspd': wspd, 'wdir': wdir,

                            }

            cursor.execute("""INSERT INTO models_remo (buoy_id, model, date_time, lat, lon,
                         swvht, tp, wvdir,  wspd, wdir) VALUES
                        (%(buoy_id)s, %(model)s, %(date_time)s, %(lat)s, %(lon)s ,%(swvht)s,
                        %(tp)s,%(wvdir)s, %(wspd)s, %(wdir)s);""", remo_data)

        conn.commit()
        conn.close()
        print("Data entered succesfully! %s rows entered" % i)


def check_last_time_remo(server, model, buoy_id):

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



    else:
        raise ValueError("Wrong database specification!")
        return


    if conn.closed == 0:
        cursor = conn.cursor()

        cursor.execute(f"SELECT max(date_time) FROM models_remo WHERE buoy_id = {buoy_id} AND model = '{model}'")
        date = cursor.fetchall()

        conn.close()
        return date

