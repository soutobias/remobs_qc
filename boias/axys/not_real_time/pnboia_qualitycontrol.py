# -*- coding: utf-8 -*-
"""
Created on Thu Jun 05 14:47:23 2014

@author: soutobias
"""

import numpy as np
from numpy import *
import ocean_data_qc as qc
import axys_limits


def arredondar(num):
    return float( '%.0f' % ( num ) )


def definition_flag_pandas(df):

    flag_data = df [['wspd1', 'gust1', 'wdir1', 'atmp', 'rh', 'dewpt', 'pres',
                    'sst', 'compass', 'arad', 'cspd1', 'cdir1', 'cspd2', 'cdir2',
                    'cspd3', 'cdir3', 'swvht', 'mxwvht',
                    'tp', 'wvdir', 'wvspread', 'wspd2', 'gust2', 'wdir2']] * 0

    flag_data = flag_data.replace(np.nan, 0).astype(int)

    return flag_data

def qualitycontrol(df, buoy):


    flag_data = definition_flag_pandas(df)

    ##############################################
    #RUN THE QC CHECKS
    ##############################################

    parameters = ['wdir1', 'wspd1', 'gust1', 'swvht', 'mxwvht', 'tp', 'wvdir',
                  'pres', 'rh', 'atmp', 'sst', 'dewpt', 'cspd1', 'cdir1',
                  'cspd2', 'cdir2', 'cspd3', 'cdir3', 'wdir2', 'wspd2', 'gust2']

    for parameter in parameters:
        # missing value checks
        flag_data = qc.mis_value_check(df, axys_limits.mis_value_axys_limits, flag_data, parameter)
        # coarse range check
        flag_data = qc.range_check(df, axys_limits.range_axys_limits, flag_data, parameter)
        # soft range check
        flag_data = qc.range_check_climate(df, axys_limits.climate_axys_limits, flag_data, parameter)



    #Significance wave height vs Max wave height
    flag_data = qc.swvht_mxwvht_check(df, flag_data, "swvht", "mxwvht")

    #Wind speed vs Gust speed
    flag_data = qc.wind_speed_gust_check(df, flag_data, "wspd1", "gust1")
    flag_data = qc.wind_speed_gust_check(df, flag_data, "wspd2", "gust2")

    #Dew point and Air temperature check
    (flag_data, df) = qc.dewpt_atmp_check(df, flag_data, "dewpt", "atmp")

    #Check of effects of battery voltage in sensors
    flag_data = qc.bat_sensor_check(df, flag_data, "battery", "pres")

    #Stucksensorcheck
    for parameter in parameters:
        flag_data = qc.stuck_sensor_check(df, axys_limits.stuck_axys_limits, flag_data, parameter)


    # comparison with scaterometer data
    parameters = ["wspd1", "wspd2", "wdir1", "wdir2", "gust1", "gust2"]
   # flag_data = qc.ascat_anemometer_comparison(df, flag_data, parameters, buoy["nome"])

    flag_data = qc.ascat_anemometer_comparison(df, flag_data, parameters, buoy["name_buoy"])

    df = qc.convert_10_meters(df, flag_data, float(buoy["h_sensor_wind"]), "wspd1", "gust1")

    try:
        df = qc.convert_10_meters(df, flag_data, float(buoy["h_sensor_wind_2"]), "wspd2", "gust2")
    except:
        print("This buoy has only one anemometer")

    (df, flag_data) = qc.related_meas_check(df, flag_data, parameters)

    #Time continuity check
    flag_data = qc.t_continuity_check(df, axys_limits.sigma_axys_limits, axys_limits.continuity_axys_limits, flag_data, "swvht")
    flag_data = qc.t_continuity_check(df, axys_limits.sigma_axys_limits, axys_limits.continuity_axys_limits, flag_data, "rh")
    flag_data = qc.t_continuity_check(df, axys_limits.sigma_axys_limits, axys_limits.continuity_axys_limits, flag_data, "pres")
    flag_data = qc.t_continuity_check(df, axys_limits.sigma_axys_limits, axys_limits.continuity_axys_limits, flag_data, "atmp")
    flag_data = qc.t_continuity_check(df, axys_limits.sigma_axys_limits, axys_limits.continuity_axys_limits, flag_data, "wspd")
    flag_data = qc.t_continuity_check(df, axys_limits.sigma_axys_limits, axys_limits.continuity_axys_limits, flag_data, "sst")



    #Frontal passage exception 1 for time continuity
    flag_data = qc.front_except_check1(df, flag_data, "wdir", "atmp")
    # (Wdirflag,Wdirflagid)=qc.frontexcepcheck2(Epoch,Wdir,Wdirflag,Atmpflag,Atmpflagid)

    #Frontal passage exception 3 for time continuity
    flag_data = qc.front_except_check3(df, flag_data, "wspd", "atmp")

    flag_data = qc.front_except_check4(df, flag_data, "pres", "wspd")

    flag_data = qc.front_except_check5(df, flag_data, "pres")

    flag_data = qc.frontexcepcheck6(df, flag_data, "wspd", "swvht")

    return flag_data, df


def intdir2uv(intensidade, direcao):

    import numpy as np

    direcao = np.mod(direcao,360)
    direcao = direcao*np.pi/180

    u=intensidade*np.sin(direcao)
    v=intensidade*np.cos(direcao)

    return u, v

def uv2intdir(u, v):

    import numpy as np
    if u>=0 and v>=0:
        intensidade=np.sqrt((u*u)+(v*v))
        direcao=rad2deg(np.arcsin(v/u))
    elif u<0 and v>=0:
        intensidade=np.sqrt((u*u)+(v*v))
        direcao=rad2deg(np.arcsin(v/(-u)))+90
    elif u<0 and v<0:
        intensidade=np.sqrt((u*u)+(v*v))
        direcao=rad2deg(np.arcsin((-v)/(-u)))+180
    elif u>=0 and v<0:
        intensidade=np.sqrt((u*u)+(v*v))
        direcao=rad2deg(np.arcsin((-v)/u))+270

    return intensidade,direcao
