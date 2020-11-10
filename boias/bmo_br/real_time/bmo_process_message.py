
import sys
import os
cwd = os.getcwd()
sys.path.insert(0, cwd)
sys.path.insert(0, cwd + '/../bd/')
from bmo_database import *

from bmo_database import *
from bmo_adjust_data import *


conn = connect_database_remo('PRI')

id_buoy = bmo_on(conn)


for id in id_buoy['id_buoy']:

    last_date_general = check_last_date(conn, 'bmo_message', id)
    last_date_general = last_date_general[0][0]

    raw_data_bmo = raw_data_bmo(conn, id, last_date_general)

    print("Processing General Data...")

    bmo_general = adjust_bmo_general(raw_data_bmo)
    # Inserting General on Database
    insert_data_bmo_general(conn, bmo_general)

    bmo_current = adjust_bmo_current(raw_data_bmo)
    # Inserting Current on Database
    insert_data_bmo_current(conn, bmo_current)