

import os
import sys 

home = os.environ['HOME']
sys.path.append(home)

from user_config import *
import psycopg2 as pg 




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





def get_data_spotter(conn, buoy_id, table, last_date, interval_hour):

    import pandas as pd
    from datetime import timedelta

    spotter_vars_str = """date_time, lat, lon, wspd, flag_wspd, wdir, flag_wdir, 
    					sst, flag_sst, swvht1, flag_swvht1, tp1, flag_tp1, 
    					wvdir1, flag_wvdir1, wvspread1, flag_wvspread1,
    					pk_dir, flag_pk_dir, pk_wvspread, flag_pk_wvspread,
    					mean_tp, flag_mean_tp""" 



    # Getting data from the last x hours
    if interval_hour == "ALL":

        query = f"""SELECT {spotter_vars_str} FROM {table} WHERE buoy_id = {buoy_id} AND 
        date_time > '2021-04-18';"""

        data_spotter = pd.read_sql_query(query, conn)

        return data_spotter

    else:
        date_period = last_date - timedelta(hours = interval_hour)

        query = f"SELECT {spotter_vars_str} FROM {table} WHERE date_time > '{date_period}' " \
                f" AND buoy_id = {buoy_id};"

        data_spotter = pd.read_sql_query(query, conn)

        return data_spotter










def get_data_bmo(conn, buoy_id, last_date, table, interval_hour):

    import pandas as pd
    from datetime import timedelta

    bmo_vars_str = """date_time, lat, lon, battery, compass, flag_compass, rh, 
    				  flag_rh, pres, flag_pres, atmp, flag_atmp, dewpt, flag_dewpt,
    				  wspd, flag_wspd, wdir, flag_wdir, gust, flag_gust, arad,
    				  flag_arad, sst, flag_sst, cspd1, flag_cspd1, cdir1, flag_cdir1,
    				  cspd2, flag_cspd2, cdir2, flag_cdir2,cspd3, flag_cspd3, 
    				  cdir3, flag_cdir3, swvht1, flag_swvht1, swvht2, flag_swvht2, 
    				  mxwvht1, flag_mxwvht1, tp1, flag_tp1, tp2, flag_tp2, 
    				  wvdir1, flag_wvdir1, wvdir2, flag_wvdir2, 
    				  wvspread1, flag_wvspread1""" 



    # Getting data from the last x hours
    if interval_hour == "ALL":

        query = f"SELECT {bmo_vars_str} FROM {table} WHERE date_time > '2020-12-10' " \
                f" AND buoy_id = {buoy_id};"

        bmo_data = pd.read_sql_query(query, conn)

        return bmo_data

    else:
        date_period = last_date - timedelta(hours = interval_hour)

        query = f"SELECT * FROM {table} WHERE date_time > '{date_period}' " \
                f" AND buoy_id = {buoy_id};"

        bmo_data = pd.read_sql_query(query, conn)

        return bmo_data









def get_data_axys(conn, buoy_id, last_date, table, interval_hour):

    import pandas as pd
    from datetime import timedelta

    axys_vars_str = """date_time, lat, lon, battery, compass, flag_compass, rh, 
    				  flag_rh, pres, flag_pres, atmp, flag_atmp, dewpt, flag_dewpt,
    				  wspd, flag_wspd, wdir, flag_wdir, gust, flag_gust, arad,
    				  flag_arad, sst, flag_sst, cspd1, flag_cspd1, cspd2, flag_cspd2,
    				  cspd3, flag_cspd3, swvht1, flag_swvht1, 
    				  mxwvht1, flag_mxwvht1, tp1, flag_tp1,
    				  wvdir1, flag_wvdir1, 
    				  wvspread1, flag_wvspread1""" 



    # Getting data from the last x hours
    if interval_hour == "ALL":

        query = f"SELECT {axys_vars_str} FROM {table} WHERE buoy_id = {buoy_id};"

        axys_data = pd.read_sql_query(query, conn)

        return axys_data

    else:
        date_period = last_date - timedelta(hours = interval_hour)

        query = f"SELECT * FROM {table} WHERE date_time > '{date_period}' " \
                f" AND buoy_id = {buoy_id};"

        axys_data = pd.read_sql_query(query, conn)

        return axys_data







def spotter_txt(df_spotter):
	df_spotter.to_csv(EMAIL_SPOTTER_FILE, index = False)


def bmo_txt(df_bmo):
	df_bmo.to_csv(EMAIL_BMO_FILE, index = False)



def axys_txt(df_axys):
	df_axys.to_csv('dados_axys_bs2.txt', index = False)




