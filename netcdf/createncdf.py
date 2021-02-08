# -*- coding: utf-8 -*-
"""
Created on Wed Jun 04 09:07:08 2014

@author: soutobias
"""
import datetime
from time import gmtime, strftime
import time
from netCDF4 import Dataset
import netCDF4
import numpy as np
import pandas as pd

from datetime import datetime, timedelta

def time_dimension(df, NC):

    date_time = (df['date_time'] - datetime(1970,1,1)).dt.total_seconds()

    NC.createDimension('time', len(date_time))
    times = NC.createVariable('Time', 'i4', 'time')
    times.long_name = 'time'
    times.standard_name = 'time'
    times.units = 'seconds since 1970-01-01 00:00:00 UTC'
    times[:] = date_time

    return times, NC

def create_variable(df, names_dict, name, NC):

    var = NC.createVariable(name, 'f4', ('time'))
    var.long_name = names_dict.loc[(names_dict['short_names'] == name)].long_names.iloc[0]
    var.standard_name = name
    var.units = names_dict.loc[(names_dict['short_names'] == name)].units.iloc[0]
    variables = df[name]
    var[:] = variables

    return NC

def flag_variable(df, names_dict, name, NC):

    vardqc = NC.createVariable(name + '_qc', 'i4', ('time'), zlib=True)
    vardqc.long_name = names_dict.loc[(names_dict['short_names'] == name)].long_names.iloc[0] + "_quality_control"
    vardqc.standard_name = name + '_qc'
    vardqc.flag_values = 'see PNBOIA QC Manual'
    vardqc.flag_meanings = 'see PNBOIA QC Manual'

    vardqc[:] = df["flag_" + name]

    return NC
