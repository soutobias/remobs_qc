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




con = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                        format(user_config.username,
                                            user_config.password,
                                            user_config.host,
                                            user_config.database))

sql_query = pd.read_sql_query('SELECT * FROM pnboia_adcp_bruto', con)
df.to_csv ('dados.csv', index = False)
df = pd.DataFrame(sql_query)
    df.to_sql(con=con, name='pnboia_adcp_bruto', if_exists='append')



df = pd.read_csv('dados.csv')
df.to_sql(con=con, name='pnboia_adcp_bruto', if_exists='append')
