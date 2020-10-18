# -*- coding: utf-8 -*-
"""
Created on Tue Oct 14 07:38:43 2014

@author: soutobias
"""

import re
import time
import datetime
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


def consulta_estacao(boia):

    db = MySQLdb.connect(host = user_config.host,
                         user = user_config.username,
                         password = user_config.password,
                         database = user_config.database)

    cur=db.cursor()

    argosbruto=[]
    cur.execute("SELECT estacao_id FROm pnboia_estacao wheRe nome='%s'" %boia)
    for row in cur.fetchall():
        argosbruto.append(row[:])

    cur.close()
    db.close()

    return argosbruto[0][0]

filenames=['riogrande','itajai','santos','cabofrio2','vitoria','portoseguro','fortaleza','niteroi']


dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')

for file in filenames:
    print('adcp_'+file+'.xls')
    df = pd.read_excel('adcp_'+file+'.xls')

    df['data'] = [datetime.datetime.strptime(str(int(df.ano[i])) +  str(int(df.mes[i])).zfill(2) + str(int(df.dia[i])).zfill(2) + str(int(df.hora[i])).zfill(2),'%Y%m%d%H') for i in range(len(df))]


    df=df.replace(-9999,np.NaN)
    df=df.replace(-99999,np.NaN)
    df=df.replace(-99999.0,np.NaN)
    df=df.replace(-9999.0,np.NaN)

    del df['ano']
    del df['mes']
    del df['dia']
    del df['hora']

    estacaoid=consulta_estacao(file)

    df['estacao_id'] = [estacaoid for i in range(len(df))]

    df = df.set_index('data')

    con = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                            format(user_config.username,
                                                user_config.password,
                                                user_config.host,
                                                user_config.database))

    df.to_sql(con=con, name='pnboia_adcp_bruto', if_exists='append')



