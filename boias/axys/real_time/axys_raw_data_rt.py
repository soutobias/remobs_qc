# -*- coding: utf-8 -*-
"""
Created on Tue May 03 10:08:32 2016

@author: Tobias
"""

import sys
import os
home_path = os.environ['HOME']
cwd_path = home_path + '/remobs_qc/boias/axys/real_time/'
bd_path = home_path + '/remobs_qc/boias/axys/bd'

sys.path.append(home_path)
sys.path.append(cwd_path)
sys.path.append(bd_path)


import user_config as user_config
#os.chdir( user_config.path )


import axys_database
import telnet_download


buoys = axys_database.working_buoys('PRI')

for buoy in buoys.itertuples():
	print("Collecting data from buoy: \n")
	print(buoy.name_buoy)
	print("")
	print("Downloading...")
	raw_data = telnet_download.download_raw_data(buoy.sat_number, user_config)
	print("Inserting on database...")
	axys_database.insert_raw_data_bd(raw_data, buoy.id_buoy, 'PRI')
	print("\nScript Finished!")