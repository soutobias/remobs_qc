
import sys
import os

home_path = os.environ['HOME']
cwd_path = home_path + '/remobs_qc/boias/bmo_br/real_time/'
bd_path = home_path + '/remobs_qc/boias/bmo_br/bd'

sys.path.append(cwd_path)
sys.path.append(bd_path)

from bmo_database import *

from bmo_database import *
from bmo_adjust_data import *


conn = connect_database_remo('PRI')

buoy_id = bmo_on(conn)


for id in buoy_id['buoy_id']:

    last_date_general = check_last_date(conn, 'bmo_br', id)
    last_date_general = last_date_general[0][0]

    if last_date_general == None:
        last_date_general = check_last_date(conn, 'buoys', id)
        last_date_general = last_date_general[0][0]

    raw_data_bmo = raw_data_bmo(conn, id, last_date_general)

    print("Processing General Data...")

    bmo_general = adjust_bmo_general(raw_data_bmo)
    # Inserting General on Database
    insert_data_bmo_general(conn, bmo_general)

    bmo_current = adjust_bmo_current(raw_data_bmo)
    # Inserting Current on Database
    insert_data_bmo_current(conn, bmo_current)