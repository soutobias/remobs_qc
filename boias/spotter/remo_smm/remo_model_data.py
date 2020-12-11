import os
import sys

sys.path.append(os.environ['HOME'])

import requests
import pandas as pd
from datetime import datetime
import io


import remo_model_db
from user_config import URL_MODEL

# Link Constructor:

last_data = remo_model_db.check_last_time_remo('PRI')
last_data = last_data[0][0]

if last_data == None or last_data < datetime.today():


    date_url = datetime.today().strftime("%Y%m%d")

    url = URL_MODEL + date_url + '.txt'

    response = requests.get(url, verify = False)

    if response.ok:

        data_remo = response.content
        df_smm = pd.read_csv(io.StringIO(data_remo.decode('utf-8')))
        head = df_smm.columns[0].split(' ')

        date_str = head[2]
        lat_str = head[3]
        lon_str = head[5]

        lat = float(lat_str.split(':')[1])
        lon = float(lon_str.split(':')[1])


        # variables
        time = df_smm.iloc[1,0].split(' ')
        date_time = list()
        for hour in time:
            new_date_time = str.join(' ',(date_str, hour))
            new_date_time = datetime.strptime(new_date_time, '%Y%m%d %HZ')
            date_time.append(new_date_time)

        hs = df_smm.iloc[2,0].split(' ')
        dir_hs = df_smm.iloc[3,0].split(' ')
        tp = df_smm.iloc[4,0].split(' ')
        u10 = df_smm.iloc[5,0].split(' ')
        dir_u10 = df_smm.iloc[6,0].split(' ')

        # remove empty residual string ("")

        list_values = [date_time, hs, dir_hs, tp, u10, dir_u10]
        new_list = list()
        for item in list_values:
            item = list(filter(None, item))
            new_list.append(item)

        df_remo = pd.DataFrame(
            {'date_time': new_list[0],
             'swvht' : new_list[1],
             'wvdir': new_list[2],
             'tp' : new_list[3],
             'wspd': new_list[4],
             'wdir': new_list[5]}
        )

        df_remo['lat'] = lat
        df_remo['lon'] = lon

        remo_model_db.insert_remo_model_db(df_remo, "PRI", 3)

        print("Scrirpt finished.")

else:
    print("No new data for the period.")
    print("Script Finished.")
