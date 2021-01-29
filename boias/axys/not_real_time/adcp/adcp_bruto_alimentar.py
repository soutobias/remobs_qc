# -*- coding: utf-8 -*-
"""
Created on Tue Oct 14 07:38:43 2014

@author: soutobias
"""

import re
import time
from datetime import *
import numpy as np
import operator
from numpy import *
from qualitycontrol import *
import mysql.connector as MySQLdb
from pandas.io import sql
import sqlalchemy
import pandas as pd


import sys
import os
from os.path import expanduser
home = expanduser("~")
sys.path.insert(0,home)
import user_config
os.chdir( user_config.path )

##############################################################################
#
# ETAPA 0 - PREPARACAO DO CODIGO
#
#Preparacao do codigo: define as variaveis principais do codigo e e onde a rotina
#deve ser modifcada para alteracao de parametros (p.ex. tempo real ou nao)
#
##############################################################################

filenames=['riogrande','itajai','santos','cabofrio2','vitoria','portoseguro','fortaleza','niteroi']
ids = [5, 4, 10, 13, 14, 15, 17, 9]


dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')

for i in range(len(filenames)):

    print('adcp_' + filenames[i] + '.xls')
    df = pd.read_excel('adcp/' + filenames[i] + '.xls')

    df['data'] = [datetime.strptime(str(int(df.ano[i])) +  str(int(df.mes[i])).zfill(2) + str(int(df.dia[i])).zfill(2) + str(int(df.hora[i])).zfill(2),'%Y%m%d%H') for i in range(len(df))]

    df['estacao_id'] = ids[i]

    df=df.replace(-9999,np.NaN)
    df=df.replace(-99999,np.NaN)
    df=df.replace(-99999.0,np.NaN)
    df=df.replace(-9999.0,np.NaN)

    del df['ano']
    del df['mes']
    del df['dia']
    del df['hora']

    df.columns = ['lon', 'lat', 'battery', 'wspd1', 'gust1', 'wdir1', 'wspd2', 'gust2',
       'wdir2', 'atmp', 'rh', 'dewpt', 'pres', 'sst', 'compass', 'arad',
       'cspd1', 'cdir1', 'cspd2', 'cdir2', 'cspd3', 'cdir3', 'swvht', 'mxwhht',
       'tp', 'wvdir', 'wvspread', 'date_time', 'buoy_id']


    print("Inserting on database...")

    axys_database.insert_raw_old_data_bd(df)

    print("\nScript Finished!")

