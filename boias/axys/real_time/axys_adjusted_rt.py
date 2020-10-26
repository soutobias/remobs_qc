# -*- coding: utf-8 -*-
"""
created on Tue may 03 10:08:32 2016

@author: Tobias
"""

import sys
import os
cwd = os.getcwd()
sys.path.insert(0, cwd)
sys.path.insert(0, cwd + '/../bd/')
sys.path.insert(0, cwd + '/../limits/')
sys.path.insert(0, cwd + '/../not_real_time/')

sys.path.insert(0, cwd + '/../../../qc_checks/')

from os.path import expanduser
home = expanduser("~")
sys.path.insert(0,home)
import user_config1 as user_config
os.chdir( user_config.path )


from axys_quality_control import *
import axys_database
import time_codes
from adjust_data import *

buoys = axys_database.working_buoys(user_config)

for buoy in buoys:
    print(buoy["nome"])

    raw_data = axys_database.select_raw_data_bd(buoy["argos_num"], user_config)

    adjusted_data = adjust_data(raw_data)

    adjusted_data = adjust_different_message_data(adjusted_data)

    axys_database.delete_adjusted_old_data(str(adjusted_data.index[0]), buoy["estacao_id"], user_config)

    axys_database.insert_adjusted_data_bd(adjusted_data, user_config)
