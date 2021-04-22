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

    flag_data = df [['wspd', 'gust', 'wdir', 'atmp', 'rh', 'dewpt', 'pres',
                    'sst', 'compass', 'arad', 'cspd1', 'cdir1', 'cspd2', 'cdir2',
                    'cspd3', 'cdir3', 'swvht', 'mxwvht',
                    'tp', 'wvdir', 'wvspread']] * 0

    flag_data = flag_data.replace(np.nan, 0).astype(int)

    return flag_data

def qualitycontrol(df, buoy):

    flag_data = definition_flag_pandas(df)

    ##############################################
    #RUN THE QC CHECKS
    ##############################################

    parameters = ['wdir', 'wspd', 'gust', 'swvht', 'mxwvht', 'tp', 'wvdir',
                  'pres', 'rh', 'atmp', 'sst', 'dewpt', 'cspd1', 'cdir1',
                  'cspd2', 'cdir2', 'cspd3', 'cdir3']
    for parameter in parameters:
        # missing value checks
        flag_data = qc.mis_value_check(df, axys_limits.mis_value_axys_limits, flag_data, parameter)
        # coarse range check
        flag_data = qc.range_check(df, axys_limits.range_axys_limits, flag_data, parameter)
        # soft range check
        flag_data = qc.range_check_climate(df, axys_limits.climate_axys_limits, flag_data, parameter)
        # soft range check
        # flag_data = qc.range_check_std(df, axys_limits.std_mean_values, flag_data, parameter)



    #Significance wave height vs Max wave height
    flag_data = qc.swvht_mxwvht_check(df, flag_data, "swvht", "mxwvht")

    #Wind speed vs Gust speed
    flag_data = qc.wind_speed_gust_check(df, flag_data, "wspd", "gust")
    #flag_data = qc.wind_speed_gust_check(df, flag_data, "wspd2", "gust2")


    #Dew point and Air temperature check
    (flag_data, df) = qc.dewpt_atmp_check(df, flag_data, "dewpt", "atmp")

    #Check of effects of battery voltage in sensors
    flag_data = qc.bat_sensor_check(df, flag_data, "battery", "pres")

    #Stucksensorcheck
    for parameter in parameters:
        flag_data = qc.stuck_sensor_check(df, axys_limits.stuck_axys_limits, flag_data, parameter)


    # comparison with scaterometer data
    parameters = ["wspd", "wspd", "wdir"]#, "wdir2", "gust1", "gust2"]
   # flag_data = qc.ascat_anemometer_comparison(df, flag_data, parameters, buoy["nome"])

    df = qc.convert_10_meters(df, flag_data, 4.7, "wspd", "gust")

    #df = qc.convert_10_meters(df, flag_data, 3.4, "wspd2", "gust2")

    #(df, flag_data) = qc.related_meas_check(df, flag_data, parameters)

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

    # stop

    # #Frontal passage exception 4 for time continuity
    # flag_data["wspd"] = qc.front_except_check4(df["pres"], flag_data[["pres", "wdir"]])

    # #Frontal passage exception 5 for time continuity
    # flag_data["pres"] = qc.front_except_check5(df["pres"], flag_data["pres"])

    # #Frontal passage exception 6 for time continuity
    # flag_data["swvht"] = qc.front_except_check6(df["wspd"], flag_data[["wspd", "swvht"]])


    # #related measurement check
    # flag_data[["cspd1","cdir1"]] = qc.front_except_check6(flag_data[["cspd1", "cdir1"]])
    # flag_data[["cspd2","cdir2"]] = qc.front_except_check6(flag_data[["cspd2", "cdir2"]])
    # flag_data[["cspd3","cdir3"]] = qc.front_except_check6(flag_data[["cspd3", "cdir3"]])
    # flag_data[["gust","wspd", "wdir"]] = qc.front_except_check6(flag_data[["gust","wspd", "wdir"]])

    return flag_data, df

def definition_flag_pandas_adcp(df):

    flag_data = df [list(df.columns[4:-1])] * 0

    flag_data = flag_data.replace(np.nan, 0).astype(int)

    return flag_data

def qualitycontrol_adcp(df, buoy):

    flag_data = definition_flag_pandas_adcp(df)

    parameters = list(df.columns[4:-1])

    for parameter in parameters:
        # missing value checks
        if parameter[0:4] == 'cspd':
            df.loc[df[parameter] != -9999, parameter] =  df.loc[df[parameter] != -9999, parameter] * 1000
        flag_data = qc.mis_value_check(df.copy(), limits.mis_value_limits, flag_data, parameter)
        # coarse range check
        flag_data = qc.range_check(df.copy(), limits.range_limits, flag_data, parameter)
        # soft range check
        flag_data = qc.range_check_climate(df.copy(), limits.climate_limits, flag_data, parameter)
        flag_data = qc.stuck_sensor_check(df, limits.stuck_limits, flag_data, parameter)

    for i in range(20):
        value = str(i +1)
        try:
            (df['uu'],df['vv'])=intdir2uv(df['cspd' + value], df['cdir' + value])
            flag_data = qc.tcontinuityadcpcheck(df.copy(), limits.adcp_limits, 3, flag_data, ['uu', "cspd" + value])
            flag_data = qc.tcontinuityadcpcheck(df.copy(), limits.adcp_limits, 3, flag_data, ['vv', "cspd" + value])
        except:
            print('no data for cspd' + value)

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
