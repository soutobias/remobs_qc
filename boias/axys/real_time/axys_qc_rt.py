# -*- coding: utf-8 -*-
"""
created on Tue may 03 10:08:32 2016

@author: Tobias
"""


def rename_merge(data, flag):


    flag.columns = ['flag_atmp', 'flag_rh', 'flag_dewpt', 'flag_pres', 'flag_sst', 'flag_compass', 'flag_arad', 'flag_cspd1',
       'flag_cdir1', 'flag_cspd2', 'flag_cdir2', 'flag_cspd3', 'flag_cdir3', 'flag_swvht', 'flag_mxwvht', 'flag_tp',
       'flag_wvdir', 'flag_wvspread', 'flag_wspd', 'flag_wdir', 'flag_gust']

    return pd.merge(data, flag, left_index=True, right_index=True, how='outer')


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

    adjusted_data = axys_database.select_adjusted_data_bd(buoy["argos_num"], user_config)

    (flag_data, qc_data) = qualitycontrol(adjusted_data, buoy)

    qc_data = rotate_data(qc_data, flag_data, buoy)

    qc_data = rename_merge(qc_data, flag_data)

    axys_database.delete_qc_old_data(str(qc_data.index[0]), buoy["estacao_id"], user_config)

    qc_data["estacao_id"] = buoy["estacao_id"]

    qc_data.reset_index().set_index(["data", "estacao_id"])

    axys_database.insert_qc_data_bd(qc_data, user_config)
