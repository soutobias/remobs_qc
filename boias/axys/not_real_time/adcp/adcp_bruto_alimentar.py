# -*- coding: utf-8 -*-
"""
Created on Tue Oct 14 07:38:43 2014

@author: soutobias
"""



from datetime import datetime
import numpy as np
import pandas as pd

import sys
import os
home_path = os.environ['HOME']
cwd_path_2 = home_path + '/remobs_qc/boias/axys/real_time/'
cwd_path = home_path + '/remobs_qc/boias/axys/not_real_time/'
cwd_path = home_path + '/remobs_qc/boias/axys/not_real_time/adcp/'

bd_path = home_path + '/remobs_qc/boias/axys/bd'

sys.path.append(home_path)
sys.path.append(cwd_path)
sys.path.append(cwd_path_2)
sys.path.append(bd_path)

import user_config as user_config
import axys_database


##############################################################################
#
# ETAPA 0 - PREPARACAO DO CODIGO
#
#Preparacao do codigo: define as variaveis principais do codigo e e onde a rotina
#deve ser modifcada para alteracao de parametros (p.ex. tempo real ou nao)
#
##############################################################################

# filenames=['riogrande','itajai','santos','cabofrio2','vitoria','portoseguro','fortaleza','niteroi']
# ids = [5, 4, 10, 13, 14, 15, 17, 9]

# dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')

df = pd.read_csv('dados.csv')


axys_database.insert_raw_old_adcp_bd(df)


