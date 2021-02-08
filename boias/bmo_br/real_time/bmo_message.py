



def get_xml_from_url(url, id):
    from bs4 import BeautifulSoup as BS
    #from bs4.element import Comment
    import pandas as pd
    import requests


    page = 0
    final_message = []
    ids = []
    while page == 0 or len(ids) == 200:

        if ids == [] and page == 0 and id == None:
            id_message = 0

        elif id and page == 0:
            id_message = id + 1
        else:
            id_message = ids[-1].text
            id_message = int(id_message) + 1

        xml_bmo = requests.get(url + str(id_message))

        if xml_bmo.status_code != 200:
            print("Error connection.")
            print("Script Finished")
            #return

        document = xml_bmo.content
        raw_xml = BS(document, 'xml')

        ids = raw_xml.find_all('id')
        messages = raw_xml.messages.contents
        messages = [message for message in messages if message != ' ']
        [final_message.append(message) for message in messages]

        page += 1





    id_list = []
    date_list = []
    type_list = []
    data_list = []




    n_messages = len(final_message)

    for i in range(n_messages):

        message_data = final_message[i]

        id_num = message_data.id

        message = id_num.parent

        id = message.id.text
        date = message.date.text
        type = message.type.text
        data = message.data.text
        data = data.replace("\n", "")

        id_list.append(id)
        date_list.append(date)
        type_list.append(type)
        data_list.append(data)



    df = pd.DataFrame({'id':id_list,
                       'date': date_list,
                       'type': type_list,
                       'data': data_list})



    return df


def message_bmo(df):

    import pandas as pd

    import re

    # Dataframe columns
    columns_bmo = ['buoy_id', 'year', 'month', 'day', 'hour', 'minute',
                   'latitude',
                   'longitude', 'battery', 'wspd1', 'gust1', 'wdir1',
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

    bmo_br = bmo_br.reset_index(drop = True)

        #Treating values
    return bmo_br


def create_datetime_bmo_message(bmo_br_df):
    from datetime import datetime

    date_df = bmo_br_df[['year', 'month', 'day', 'hour', 'minute']].agg('-'.join, axis = 1)

    date_time = [datetime.strptime(date_str, "%Y-%m-%d-%H-%M") for date_str in date_df]

    return date_time
