

from datetime import datetime
import numpy as np
import pandas as pd

import sys
import os
home_path = os.environ['HOME']
cwd_path_2 = home_path + '/remobs_qc/boias/axys/real_time/'
cwd_path = home_path + '/remobs_qc/boias/axys/not_real_time/'
bd_path = home_path + '/remobs_qc/boias/axys/bd'



sys.path.append(home_path)
sys.path.append(cwd_path)
sys.path.append(cwd_path_2)


sys.path.append(bd_path)


import user_config as user_config
#os.chdir( user_config.path )


import axys_database



def bancodedados():

    local="pnboia-uol.mysql.uhserver.com"
    usr="pnboia"
    password="Ch@tasenha1"
    data_base="pnboia_uol"

    return local,usr,password,data_base


filenames=['it','rg','ocas','itaoca','itaguai','ni','sa','cf','minuano','cf2','vi','po','re','fo']
ids = [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]

# filenames=['rg','ocas','itaoca','itaguai','ni','sa','cf','minuano','cf2','vi','po','re','fo']
# ids = [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]

#filenames=['re','fo']

dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
x=1
for i in range(len(filenames)):
    print(filenames[i]+'.xls')
    df = pd.read_excel('dados/' + filenames[i] + '.xls')

    df.rename(columns={'#ano': 'ano'}, inplace=True)


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
