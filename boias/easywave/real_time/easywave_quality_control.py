
import numpy as np


import sys
import os

home_path = os.environ['HOME']
limits_path = home_path + '/remobs_qc/boias/easywave/limits'
qc_path = home_path + '/remobs_qc/qc_checks'


sys.path.append(limits_path)
sys.path.append(qc_path)

import ew_limits as limits
import ocean_data_qc as qc


def arredondar(num):
    return float( '%.0f' % ( num ) )


def definition_flag_pandas(df):

    flag_data = df [['swvht', 'tp', 'wvdir']] * 0

    flag_data = flag_data.replace(np.nan, 0).astype(int)

    return flag_data



def qualitycontrol(df, buoy):

    flag_data = definition_flag_pandas(df)

    ##############################################
    #RUN THE QC CHECKS
    ##############################################

    parameters = ['swvht', 'tp', 'wvdir']

    for parameter in parameters:
        # missing value checks
        flag_data = qc.mis_value_check(df, limits.mis_value_limits, flag_data, parameter)
        # coarse range check
        flag_data = qc.range_check(df, limits.range_limits, flag_data, parameter)
        # soft range check
        flag_data = qc.range_check_climate(df, limits.climate_limits, flag_data, parameter)
        # soft range check
       # flag_data = qc.range_check_std(df, limits.std_mean_values, flag_data, parameter)


    #Stucksensorcheck
    for parameter in parameters:
        flag_data = qc.stuck_sensor_check(df, limits.stuck_limits, flag_data, parameter)


    #Time continuity check
    flag_data = qc.t_continuity_check(df, limits.sigma_limits, limits.continuity_limits, flag_data, "swvht")


    print("EasyWave Data Qualified!")

    return flag_data, df


