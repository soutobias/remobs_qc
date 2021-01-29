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
bd_path = home_path + '/remobs_qc/boias/axys/bd'
limits_path = home_path + '/remobs_qc/boias/axys/limits'


qc_path = home_path + '/remobs_qc/qc_checks'


sys.path.append(qc_path)
sys.path.append(home_path)
sys.path.append(cwd_path)
sys.path.append(cwd_path_2)
sys.path.append(limits_path)

sys.path.append(bd_path)

from pnboia_qualitycontrol import qualitycontrol

import user_config as user_config
#os.chdir( user_config.path )

import axys_database
import time_codes
from pnboia_adjusted_data import *



def rename_merge(data, flag):

    data['swvht1'] = data['swvht1']
    data['mxwvht1'] = data['mxwvht']
    data['tp1'] = data['tp']
    data['wvdir1'] = data['wvdir']
    data['wvspread1'] = data['wvspread']

    del data['swvht1']
    del data['mxwvht']
    del data['tp']
    del data['wvdir']
    del data['wvspread']


    flag.columns = ['flag_atmp', 'flag_rh', 'flag_dewpt', 'flag_pres', 'flag_sst', 'flag_compass', 'flag_arad', 'flag_cspd1',
       'flag_cdir1', 'flag_cspd2', 'flag_cdir2', 'flag_cspd3', 'flag_cdir3', 'flag_swvht1', 'flag_mxwvht1', 'flag_tp1',
       'flag_wvdir1', 'flag_wvspread1', 'flag_wspd', 'flag_wdir', 'flag_gust']

    return pd.merge(data, flag, left_index=True, right_index=True, how='outer')

buoys = axys_database.old_buoys()

for buoy in buoys:
    print(buoy["name_buoy"])


    raw_data = axys_database.get_old_data_db(buoy["buoy_id"])


    raw_data = raw_data.replace([-9999, 9999, 99.99, None, "-9999", "-99999", '', '99.99', '-9999.0', -9999.0, -9999] , np.NaN)

    raw_data.set_index("date_time", inplace = True)

    print("Qualifying General Data...")

    (flag_data, qc_data) = qualitycontrol(raw_data, buoy)
    qc_data = rotate_data(qc_data, flag_data, buoy)

    qc_data = rename_merge(qc_data, flag_data)

    print("Deleting old Qualified Data...")
    axys_database.delete_qc_pnboia_old_data(str(qc_data.index[0]), buoy["buoy_id"], 'PRI')

    qc_data.reset_index().set_index(["date_time"])

    del qc_data['id']

    print("Inserting data on database...")
    axys_database.insert_pnboia_qc_data(qc_data)

    print("Script Finished!")


# dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')

# df['estacao_id'] = [boias[s][0] for i in range(len(df))]
# df = df.set_index('data')

# df=df.replace(-9999,np.NaN)
# df=df.replace(-99999,np.NaN)
# df=df.replace(-99999.0,np.NaN)
# df=df.replace(-9999.0,np.NaN)

# df.Wspdflagid = df.Wspdflagid.astype(int)
# df.Wdirflagid = df.Wdirflagid.astype(int)
# df.Gustflagid = df.Gustflagid.astype(int)
# df.Atmpflagid = df.Atmpflagid.astype(int)
# df.Presflagid = df.Presflagid.astype(int)
# df.Dewpflagid = df.Dewpflagid.astype(int)
# df.Humiflagid = df.Humiflagid.astype(int)
# df.Wtmpflagid = df.Wtmpflagid.astype(int)
# df.Cvel1flagid = df.Cvel1flagid.astype(int)
# df.Cdir1flagid = df.Cdir1flagid.astype(int)
# df.Cvel2flagid = df.Cvel2flagid.astype(int)
# df.Cdir2flagid = df.Cdir2flagid.astype(int)
# df.Cvel3flagid = df.Cvel3flagid.astype(int)
# df.Cdir3flagid = df.Cdir3flagid.astype(int)
# df.Wvhtflagid = df.Wvhtflagid.astype(int)
# df.Wmaxflagid = df.Wmaxflagid.astype(int)
# df.Dpdflagid = df.Dpdflagid.astype(int)
# df.Mwdflagid = df.Mwdflagid.astype(int)

# stop

# inserir_dados_banco(df)

# print('done reading in data')
