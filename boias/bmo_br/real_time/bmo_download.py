import sys
import os

home_path = os.environ['HOME']
cwd_path = home_path + '/remobs_qc/boias/bmo_br/real_time/'
bd_path = home_path + '/remobs_qc/boias/bmo_br/bd'

sys.path.append(cwd_path)
sys.path.append(bd_path)

from bmo_database import *
from bmo_message import *
from user_config import URL_TOKEN, PAYLOAD_TOKEN, HEADERS_TOKEN, URL_BMO


conn = connect_database_remo('PRI')

last_id_message = get_id_sat_message(conn)
id_message = last_id_message[0][0]

bmo_ons = bmo_on(conn)

if bmo_ons.empty:
    print("No BMO buoys actived. Script finished.")
    sys.exit()

else:

    df_bmo_raw = get_data_from_url(URL_TOKEN, PAYLOAD_TOKEN, HEADERS_TOKEN, URL_BMO, id_message)

    if df_bmo_raw.empty:
        print("No new data!")
        print("Script Finished...")

    else:
        for idx, buoy in bmo_ons.iterrows():
            buoy_id = buoy['buoy_id']
            antena_id = buoy['sat_number']

            df_bmo_buoy = df_bmo_raw[df_bmo_raw['mobile']==antena_id]

            if df_bmo_buoy [df_bmo_buoy['type']=='remo'].empty:
                print("No new data for remo message!")
                print("Script Finished...")

            else:
                df_bmo_buoy = df_bmo_buoy.sort_values(by = 'id')
                messages = df_bmo_buoy[df_bmo_buoy['type']=='remo']
                message_triaxys = df_bmo_buoy[df_bmo_buoy['type']=='axys']

                if not message_triaxys.empty:

                    status_transaction_triaxys = insert_triaxys_message(conn, message_triaxys, buoy_id)

                    if status_transaction_triaxys == 1:
                        print("Triaxys message inserted on database!")
                    elif status_transaction_triaxys == 0:
                        print("Triaxys message NOT inserted on database!")
                else:
                    print(f"No triaxys data for buoyd {buoy_id}")


                messages = messages['data']

                bmo_br = message_bmo(messages)

                date_time = create_datetime_bmo_message(bmo_br)

                bmo_br.insert(1, 'date_time', date_time)

                # Inserting data on bmo_message table

                status = insert_data_bmo_message(conn, bmo_br, buoy_id)
                if status == 1:
                    insert_sat_message_xml(conn, buoy_id, df_bmo_buoy)

                print(f"Finished BMO {buoy_id} Message collection.")


            print("Closing database connection...")
            conn.close()
            print("Finished BMO Message Script.")
