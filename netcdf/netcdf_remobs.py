# -*- coding: utf-8 -*-
"""
Created on Wed Jun 11 09:51:37 2014

@author: soutobias
"""

import numpy as np
import math
from datetime import datetime, timedelta

import os
import sys

home_path = os.environ['HOME']
sys.path.append(home_path)

import bd_function as bd
import pandas as pd

import netcdf_new

conn = bd.conn_qc_db('PRI')

buoys = bd.working_buoys(conn)

cf = bd.cf_names(conn)

date_month = datetime.now().strftime("%Y-%m-01 00:00:00")

for i in range(len(buoys)):

    df = bd.get_netcdf_data(buoys["id"][i], date_month, conn)

    netcdf_new.generate_netcdf(df, buoys, i, cf)
