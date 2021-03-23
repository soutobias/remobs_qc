import requests
import hashlib
import hmac

from bs4 import BeautifulSoup
from datetime import datetime as dt

import os, sys
sys.path.append(os.environ['HOME'])
sys.path.append(os.environ['HOME'] + '/remobs_qc/boias/axys/tag/')

from bd_tag_wl import db_tag
from user_config import WL_URL, WL_ACCESS_KEY, WL_SECRET_KEY, WL_PPTS


# prepare request


headers = {'X-Access':WL_ACCESS_KEY,
            'X-Hash':''}





for tag_ppt in WL_PPTS:


    tag_buoy_id = int(db_tag().get_buoy_tag(tag_ppt)['buoy_id'][0])

    print(f"Getting data from: \n \
    tag number: {tag_ppt} \n \
    buoy_id: {str(tag_buoy_id)}")

    payload = {'action':'get_deployments', # Action...
                'ptt_decimal':tag_ppt} # Optional parameter

    req = requests.Request('POST', WL_URL, data=payload, headers=headers)

    prepped = req.prepare()
    signature = hmac.new(WL_SECRET_KEY.encode('utf-8'),
                         prepped.body.encode('utf-8'),
                         hashlib.sha256).hexdigest()

    prepped.headers['X-Hash'] = signature

    with requests.Session() as session:

        response = session.send(prepped)


        if response.status_code == 200:
            # Get your data...
            print("Connected to WilfLife.")
            xml = BeautifulSoup(response.content, features='lxml')

            last_time = int(xml.find('location_date').text)
            latitude = float(xml.find('latitude').text)
            longitude = float(xml.find('longitude').text)
            date_time = dt.fromtimestamp(last_time)


            last_date_time = db_tag().get_last_time(tag_ppt)


            print("Last time location tag: " + str(date_time))

            if last_date_time.iloc[0][0] == None or last_date_time.iloc[0][0] < date_time:

                data_dict = dict({'tag_id':tag_ppt,
                                  'latitude': latitude,
                                  'longitude': longitude,
                                  'date_time': date_time})


                db_tag().insert_tag_data(data_dict, tag_buoy_id)

            else:
                print("No new data...")


        else:
            raise Exception("""Something get wrong with your request.\n
            Request Error: """ + str(response.status_code))



print("Disconnecting from database...")

db_tag().conn.close()

print("Disconnected.")
print("Script Finished!")
