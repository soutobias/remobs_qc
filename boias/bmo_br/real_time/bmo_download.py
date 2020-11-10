import sys
import os
cwd = os.getcwd()
sys.path.insert(0, os.environ['HOME'])
sys.path.insert(0, cwd + '/../bd/')
from bmo_database import *
from bmo_message import *
from user_config import url_bmo_bs1_1


url = url_bmo_bs1_1


conn = connect_database_remo('PRI')

last_id_message = get_id_sat_message(conn, 'CHM1')
id_message = last_id_message[0][0]

df_xml = get_xml_from_url(url, id_message)

if df_xml.empty:
    print("No new data!")
    print("Script Finished...")

elif df_xml[df_xml['type']=='remo'].empty:
    print("No new data for remo message!")
    print("Script Finished...")

else:
    messages = df_xml[df_xml['type']=='remo']
    messages = messages['data']

    bmo_br = message_bmo(messages)

    date_time = create_datetime_bmo_message(bmo_br)

    bmo_br.insert(1, 'date_time', date_time)

    # Inserting data on bmo_message table

    status = insert_data_bmo_message(conn, bmo_br, 2)
    if status == 1:
        insert_sat_message_xml(conn, 2, df_xml)


    print("Closing database connection...")
    conn.close()
    print("Finished BMO Message Script.")
