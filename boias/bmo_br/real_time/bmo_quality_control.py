
import numpy as np



import sys
import os
home_path = os.environ['HOME']
limits_path = home_path + '/remobs_qc/boias/bmo_br/limits'
qc_path = home_path + '/remobs_qc/qc_checks'

sys.path.append(limits_path)
sys.path.append(qc_path)

import bmo_limits as limits
import ocean_data_qc as qc


def arredondar(num):
    return float( '%.0f' % ( num ) )


def definition_flag_pandas(df):

    flag_data = df [['wspd1', 'gust1', 'wdir1', 'wspd2', 'gust2',
       'wdir2', 'atmp', 'rh', 'dewpt', 'pres', 'sst', 'compass', 'arad',
       'cspd1', 'cdir1', 'cspd2', 'cdir2', 'cspd3', 'cdir3', 'swvht1',
       'mxwvht1','wvdir1', 'wvspread1', 'tp1', 'swvht2','tp2', 'wvdir2']] * 0

    flag_data = flag_data.replace(np.nan, 0).astype(int)

    return flag_data

def qualitycontrol(df, buoy):

    flag_data = definition_flag_pandas(df)

    ##############################################
    #RUN THE QC CHECKS
    ##############################################

    parameters = ['wdir1', 'wspd1', 'gust1', 'wdir2', 'wspd2', 'gust2',
                  'swvht1', 'swvht1', 'mxwvht1', 'tp1', 'tp2', 'wvdir1',
                  'wvdir2', 'pres', 'rh', 'atmp', 'sst', 'dewpt', 'cspd1',
                  'cdir1', 'cspd2', 'cdir2', 'cspd3', 'cdir3']

    for parameter in parameters:
        # missing value checks
        flag_data = qc.mis_value_check(df, limits.mis_value_limits, flag_data, parameter)
        # coarse range check
        flag_data = qc.range_check(df, limits.range_limits, flag_data, parameter)
        # soft range check
        flag_data = qc.range_check_climate(df, limits.climate_limits, flag_data, parameter)
        # soft range check
        # flag_data = qc.range_check_std(df, limits.std_mean_values, flag_data, parameter)



    #Significance wave height vs Max wave height
    flag_data = qc.swvht_mxwvht_check(df, flag_data, "swvht1", "mxwvht1")

    #Wind speed vs Gust speed
    flag_data = qc.wind_speed_gust_check(df, flag_data, "wspd1", "gust1")
    flag_data = qc.wind_speed_gust_check(df, flag_data, "wspd2", "gust2")


    #Dew point and Air temperature check
    (flag_data, df) = qc.dewpt_atmp_check(df, flag_data, "dewpt", "atmp")

    #Check of effects of battery voltage in sensors
    flag_data = qc.bat_sensor_check(df, flag_data, "battery", "pres")

    #Stucksensorcheck
    for parameter in parameters:
        flag_data = qc.stuck_sensor_check(df, limits.stuck_limits, flag_data, parameter)


    # comparison with scaterometer data
    parameters = ["wspd1", "wspd2", "wdir1", "wdir2", "gust1", "gust2"]
    #flag_data = qc.ascat_anemometer_comparison(df, flag_data, parameters, buoy["nome"])

    df = qc.convert_10_meters(df, flag_data, 3.6, "wspd1", "gust1")

    df = qc.convert_10_meters(df, flag_data, 3.2, "wspd2", "gust2")

    (df, flag_data) = qc.related_meas_check(df, flag_data, parameters)

    #Time continuity check
    flag_data = qc.t_continuity_check(df, limits.sigma_limits, limits.continuity_limits, flag_data, "swvht1")
    flag_data = qc.t_continuity_check(df, limits.sigma_limits, limits.continuity_limits, flag_data, "swvht2")
    flag_data = qc.t_continuity_check(df, limits.sigma_limits, limits.continuity_limits, flag_data, "rh")
    flag_data = qc.t_continuity_check(df, limits.sigma_limits, limits.continuity_limits, flag_data, "pres")
    flag_data = qc.t_continuity_check(df, limits.sigma_limits, limits.continuity_limits, flag_data, "atmp")
    flag_data = qc.t_continuity_check(df, limits.sigma_limits, limits.continuity_limits, flag_data, "wspd")
    flag_data = qc.t_continuity_check(df, limits.sigma_limits, limits.continuity_limits, flag_data, "sst")




   #Frontal passage exception 1 for time continuity
    flag_data = qc.front_except_check1(df, flag_data, "wdir", "atmp")
    # (Wdirflag,Wdirflagid)=qc.frontexcepcheck2(Epoch,Wdir,Wdirflag,Atmpflag,Atmpflagid)

    #Frontal passage exception 3 for time continuity
    flag_data = qc.front_except_check3(df, flag_data, "wspd", "atmp")



    # stop

    # #Frontal passage exception 4 for time continuity
    #flag_data = qc.front_except_check4(df, flag, 'pres', 'wspd')

    # #Frontal passage exception 5 for time continuity
    # flag_data["pres"] = qc.front_except_check5(df["pres"], flag_data["pres"])

    # #Frontal passage exception 6 for time continuity
    # flag_data["swvht"] = qc.front_except_check6(df["wspd"], flag_data[["wspd", "swvht"]])


    # #related measurement check
    # flag_data[["cspd1","cdir1"]] = qc.front_except_check6(flag_data[["cspd1", "cdir1"]])
    # flag_data[["cspd2","cdir2"]] = qc.front_except_check6(flag_data[["cspd2", "cdir2"]])
    # flag_data[["cspd3","cdir3"]] = qc.front_except_check6(flag_data[["cspd3", "cdir3"]])
    # flag_data[["gust","wspd", "wdir"]] = qc.front_except_check6(flag_data[["gust","wspd", "wdir"]])

    print("BMO Data Qualified!")

    return flag_data, df