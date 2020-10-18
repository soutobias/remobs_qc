

import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np
import re
from math import radians, cos, sin, asin, sqrt
import csv
import operator
from pandas.io import sql
import sqlalchemy

def bancodedados():

    local="pnboia-uol.mysql.uhserver.com"
    usr="pnboia"
    password="Ch@tasenha1"
    data_base="pnboia_uol"

    return local,usr,password,data_base


filenames=['it','rg','ocas','itaoca','itaguai','ni','sa','cf','minuano','cf2','vi','po','re','fo']
#filenames=['re','fo']

dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
x=1
for file in filenames:
    print(file+'.xlsx')
    df = pd.read_excel(file+'.xlsx')

    df.rename(columns={'#ano': 'ano'}, inplace=True)

    df['data'] = [datetime.strptime(str(int(df.ano[i])) +  str(int(df.mes[i])).zfill(2) + str(int(df.dia[i])).zfill(2) + str(int(df.hora[i])).zfill(2),'%Y%m%d%H') for i in range(len(df))]
    df['estacao_id'] = [x for i in range(len(df))]
    x=x+1
    df = df.set_index('data')

    df=df.replace(-9999,np.NaN)
    df=df.replace(-99999,np.NaN)
    df=df.replace(-99999.0,np.NaN)
    df=df.replace(-9999.0,np.NaN)


    del df['ano']
    del df['mes']
    del df['dia']
    del df['hora']

    (local,usr,password,data_base)=bancodedados()

#    con = MySQLdb.connect(local,usr,password,data_base)

    con = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(usr, password,
                                                          local, data_base))

    df.to_sql(con=con, name='pnboia_bruto', if_exists='append')


