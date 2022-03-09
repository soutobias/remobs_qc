
import pandas as pd
import json
import requests
from datetime import datetime, timedelta

def get_token(url_token, payload, headers):

    response = requests.post(url_token, headers=headers, data=payload)

    return response.json()['token']


def get_data_from_url(URL_TOKEN, PAYLOAD_TOKEN, HEADERS_TOKEN, URL_BMO, last_date, id_antenna):

    page = 0
    final_message = pd.DataFrame(columns=['id','date', 'mobile', 'type','data'])
    ids = []
    while page == 0 or len(ids) == 200:
        
        if page == 0:
            start_date = last_date
            
        else:
            last_date_dt = datetime.strptime(last_message_page, "%Y-%m-%dT%H:%M:%S.%f") - timedelta(minutes=1)
            last_date_str = last_date_dt.strftime(format="%Y-%m-%dT%H:%M")
            
            start_date = last_date_str

        payload= {
            "mobile": [id_antenna],
            "start_date":start_date,
            "end_date":(datetime.utcnow() + timedelta(hours=5)).strftime(format="%Y-%m-%dT%H:%M")
            }

        payload = json.dumps(payload)

        token = get_token(URL_TOKEN, PAYLOAD_TOKEN, HEADERS_TOKEN)

        HEADERS_BMO = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
        }


        message_bmo = requests.post(URL_BMO, headers=HEADERS_BMO, data=payload)

        if message_bmo.status_code == 401:
            raise Exception("401. Not authorized.")

        elif message_bmo.status_code != 200:
            raise Exception("Error connection. Script Finished")

        raw_message = pd.json_normalize(message_bmo.json())

        if raw_message.empty:
            return final_message

        filter_cols = ['id','dh_registro', 'mobile', 'data_type','data']
        bmo_message = raw_message[filter_cols].copy()

        bmo_message.rename(columns={'dh_registro':'date', 'data_type':'type'}, inplace=True)

        ids = bmo_message.id.tolist()
        last_message_page = bmo_message.date.tolist()[-1]
        final_message = final_message.append(bmo_message)

        page += 1
        print("Page " + str(page))

    return final_message



def message_bmo(df):

    import pandas as pd

    import re

    # Dataframe columns
    columns_bmo = ['buoy_id', 'year', 'month', 'day', 'hour', 'minute',
                   'latitude',
                   'longitude', 'battery', 'temp_datalogger','wspd1', 'gust1', 'wdir1',
                   'wspd2', 'gust2', 'wdir2', 'atmp', 'rh', 'dewpt',
                   'press', 'sst', 'compass', 'arad', 'cspd1', 'cdir1',
                   'cspd2', 'cdir2', 'cspd3', 'cdir3', 'cspd4', 'cdir4',
                   'cspd5', 'cdir5', 'cspd6', 'cdir6', 'cspd7', 'cdir7',
                   'cspd8', 'cdir8', 'cspd9', 'cdir9', 'cspd10', 'cdir10',
                   'cspd11', 'cdir11', 'cspd12', 'cdir12', 'cspd13', 'cdir13',
                   'cspd14', 'cdir14', 'cspd15', 'cdir15', 'cspd16', 'cdir16',
                   'cspd17', 'cdir17', 'cspd18', 'cdir18', 'swvht1', 'tp1',
                   'mxwvht1', 'wvdir1', 'wvspread1', 'swvht2', 'tp2', 'wvdir2']

    bmo_br = pd.DataFrame(columns=columns_bmo)

    for message in df:


        message_data = re.findall(r"RE(.*)MO", message)
        message_data = message_data[0]


        # Removing '[' and ']'
        message_values = message_data.replace("[", "").replace("]","")

        bmo_values = message_values.split(";")
        bmo_df = pd.DataFrame([bmo_values], columns = columns_bmo)
        bmo_br = bmo_br.append(bmo_df)

    # REMOVING EXTRA COLUMN DATA
    #bmo_br.drop(columns="WHAT_IS", inplace=True) # temporary
    ###
    bmo_br = bmo_br.reset_index(drop = True)

        #Treating values
    return bmo_br


def create_datetime_bmo_message(bmo_br_df):
    from datetime import datetime

    date_df = bmo_br_df[['year', 'month', 'day', 'hour', 'minute']].agg('-'.join, axis = 1)

    date_time = [datetime.strptime(date_str, "%Y-%m-%d-%H-%M") for date_str in date_df]

    return date_time
