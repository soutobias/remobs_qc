# -*- coding: utf-8 -*-
"""
Created on Tue May 03 10:08:32 2016

@author: Tobias
"""

import sys
import os
cwd = os.getcwd()
sys.path.insert(0, cwd)
sys.path.insert(0, cwd + '/../bd/')
sys.path.insert(0, cwd + '/../limits/')
sys.path.insert(0, cwd + '/../../../qc_checks/')

from os.path import expanduser
home = expanduser("~")
sys.path.insert(0,home)
import user_config1 as user_config
os.chdir( user_config.path )


import axys_database
import telnet_download


buoys = axys_database.working_buoys(user_config)

for buoy in buoys:
    print(buoy["nome"])
    raw_data = telnet_download.download_raw_data(buoy["argos_num"], user_config)
    axys_database.insert_raw_data_bd(raw_data, buoy["argos_num"], user_config)
