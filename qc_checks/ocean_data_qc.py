"""
Spyder Editor

Python Version tested: Python 2.7.3

Required Dependencies: Numpy, time

Description: This module is designed to take Standard Meteorogical Data from
NDBC website in form of time, Wdir, Wspd, Gst, swvht, Dpd, Apd, Mwd, Pres, Atmp,
Wtmp, dewpt, Vis and Tide, and perform QC checks based on NDBC's "Handbook of
Automated Data Quality Control Checks and Procedures". Each check generates a QC
table for the archive used, following the QC Numbering.
5 indicates that the time listed is not valid and is in the future.

Required Input: EPOCH Time, swvht(m), Dpd(s), Apd(s), Mwd(degrees)

Checks Performed:
    Valid Time
    Range Limits
    Climatological Range Limits
    Standard Time Continuity
    Stuck Sensor
    Wave Height Verses Average Wave Period

Flag Convention as follows:
 0 = no QC performed
 1 = good data
 2 = prob. good
 3 = prob. bad
 4 = bad data
 5 = invalid time
 6 = unused
 7 = unused
 8 = interpolated value (unused but planned)

Author: Tobias Ferreira
Organization: Brazilian Navy, BR
Date: August 19th 2020
Version: 1.0

"""

import numpy as np
import time
import math
import pandas as pd


############################ Begin QC ################################

###############################################################################
# Missing value check
# Flag the missing (MISVALUE) or None values
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - var: variable that will be checked
# - misvalue: missing value for the var
# - flag: matrix of flag for the variable
# - idf= flag id. '53' --> letter that represents the flag
#
# Required checks: None
#
# Return: flag, idf
###############################################################################

def mis_value_check(var, limits, flag, parameter):

    try:
        flag.loc[(var[parameter] == limits[parameter]) & (flag[parameter] == 0), parameter] = 1
    except:
        print("No mis_value_limit for " + parameter)
    flag.loc[(var[parameter] == np.nan) & (flag[parameter] == 0), parameter] = 1

    return flag

    #####################
    #end Misvalue check section


###############################################################################
# Range check
# Check to ensure values are within global and equipment ranges (LIMITS)
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - var: variable that will be checked
# - limits: [lower limit,upper limit]
# - flag: matrix of flag for the variable
# - idf= flag id. 'L' --> letter that represents the flag
#
# Required checks: Missing value check
#
# Return: flag, idf
###############################################################################

def range_check(var, limits, flag, parameter):

    try:
        flag.loc[(var[parameter] < limits[parameter][0]) & (flag[parameter] == 0), parameter] = 2
        flag.loc[(var[parameter] > limits[parameter][1]) & (flag[parameter] == 0), parameter] = 2
    except:
        print("No range_limit for " + parameter)

    return flag

    #####################
    #end Range Check section

###############################################################################
# Range check Climatologico
# Check to ensure values are within brazil ranges (LIMITS)
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - var: variable that will be checked
# - limits: [lower limit,upper limit]
# - flag: matrix of flag for the variable
# - idf= flag id. 'L' --> letter that represents the flag
#
# Required checks: Missing value check
#
# Return: flag, idf
###############################################################################

def range_check_climate(var, limits, flag, parameter):

    try:
        flag.loc[(var[parameter] < limits[parameter][0]) & (flag[parameter] == 0), parameter] = 9
        flag.loc[(var[parameter] > limits[parameter][1]) & (flag[parameter] == 0), parameter] = 9
    except:
        print("No range_climate_limit for " + parameter)

    return flag

def range_check_std(var, limits, flag, parameter):

    try:
        max_value = limits[parameter][0] + 3 * limits[parameter][1]
        min_value = limits[parameter][0] + 3 * limits[parameter][1]

        flag.loc[(var[parameter] < max_value) & (flag[parameter] == 0), parameter] = 20
        flag.loc[(var[parameter] > min_value) & (flag[parameter] == 0), parameter] = 20
    except:
        print("No std value for " + parameter)

    return flag

    #####################
    #end Range Check section


###############################################################################
# Wave Significant Height x Wave Max Height
# Compares if the values of wind speed is higher than Gust.
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - swvht: wave significant height
# - mxwvht: max wave height
# - flagv: matrix of flag for swvht
# - idfv= flag id for swvht. '4' --> letter that represents the flag
# - flagm: matrix of flag for mxwvht
# - idfm= flag id for mxwvht. '4' --> letter that represents the flag
#
# Required checks: Range Check, Missing value check
#
# Return: flagv,flagm,idfv,idfm
###############################################################################


def swvht_mxwvht_check(var, flag, swvht_name, mxwvht_name):

    flag.loc[(var[swvht_name] > var[mxwvht_name]) & (flag[swvht_name] == 0), swvht_name] = 4
    flag.loc[(var[swvht_name] > var[mxwvht_name]) & (flag[mxwvht_name] == 0), mxwvht_name] = 4

    return flag

    #####################
    #end swvhtmxwvht check section


###############################################################################
# Wind speed x Gust Check
# Compares if the values of wind speed is higher than Gust. Also verify if
# Gust is less of 0.5
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - wind: wind speed
# - gust: gust speed
# - flagw: matrix of flag for wind speed
# - idfw= flag id for wind speed. '52' --> letter that represents the flag
# - flagw: matrix of flag for wind speed
# - idfg= flag id for gust. '52' --> letter that represents the flag
#
# Required checks: Range Check, Missing value check
#
# Return: flagw,flagg,idfw,idfg
###############################################################################


def wind_speed_gust_check(var, flag, wspd_name, gust_name):

    flag.loc[(var[wspd_name] > var[gust_name]) & (flag[wspd_name] == 0), wspd_name] = 3
    flag.loc[(var[wspd_name] > var[gust_name]) & (flag[gust_name] == 0), gust_name] = 3
    flag.loc[(var[gust_name] < 0.5) & (flag[gust_name] == 0), gust_name] = 3

    return flag


    #####################
    #end windspeedgust check section


###############################################################################
# Dew point x Air temperature Check
# Compares if the dewptoint values is higher than Air temperature values.
# If so, dewpt value will be changed to atmp value and data will be soft flagged
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - dewpt: dew point
# - atmp: air temperatura
# - flagd: matrix of flag for dew point
# - flaga: matrix of flag for air temperature
# - idf= flag id. 'o' --> letter that represents the flag
#
# Required checks: Range Check, Missing value check
#
# Return: flagd,idf
#
###############################################################################

def dewpt_atmp_check(var, flag, dewpt_name, atmp_name):

    flag.loc[(var[dewpt_name] > var[atmp_name]) & (flag[dewpt_name] == 0)  & (flag[atmp_name] == 0), dewpt_name] = 51
    var.loc[(flag[dewpt_name] == 51), dewpt_name] = var[atmp_name]

    return flag, var

    #####################
    #end dewptoint atmp check section

###############################################################################
# Battery x Air Pressure Check
# Pressure will be flagged if batterty is below of 10.5 V.
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - pres: air pressure
# - battery: battery voltage
# - flagp: matrix of flag for air pressure
# - idf= flag id. '5' --> letter that represents the flag
#
# Required checks: Range Check, Missing value check
#
# Return: flagp,idf
#
###############################################################################

def bat_sensor_check(var, flag, battery_name, pres_name):

    flag.loc[(var[battery_name] <= 10.5) & (flag[pres_name] == 0), pres_name] = 5

    return flag


    #####################
    #end battery pressure check section

#######################################################################################
# Stuck Sensor Check
# Compare the values to the NEV next values.
# If the value do not change, it will be flagged
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - var: variable
# - flag: matrix of flag for the variable
# - nev: number of variables that will be compared
# - idf= flag id. '6' --> letter that represents the flag
#
# Required checks: Range Check, Missing value check
#
# Return: flag,idf
#
########################################################################################

def stuck_sensor_check (var, limit, flag, parameter):

    for counter in range(len(var)):
        value = var.loc[(var.index >= var.index[counter]) & (var.index <= var.index[counter] + pd.to_timedelta(limit, unit='h')) & (flag[parameter] == 0) & (flag[parameter] == 6), parameter]
        if value.size == 0:
            continue
        elif np.array(value == value[0]).all() and var.index[-1] - var.index[counter] >= pd.to_timedelta(limit, unit='h'):
            flag.loc[(index), parameter] = 6

    return flag

    #####################
    #end stuck sensor check

def ascat_anemometer_comparison(var, flag, parameters, buoy):

    import anemometers

    try:
        limits = anemometer.anemometer_ascat[buoy]

        for limit in limits:
            if limit["choice"] == 0:
                flag.loc[(flag.index >= limit["begin_date"]) & (flag.index < limit["end_date"]), parameters] = 11
            elif limit["choice"] == 1:
                flag.loc[(flag.index >= limit["begin_date"]) & (flag.index < limit["end_date"]), [parameters[1], parameters[3], parameters[5]]] = 11
            elif limit["choice"] == 2:
                flag.loc[(flag.index >= limit["begin_date"]) & (flag.index < limit["end_date"]), [parameters[0], parameters[2], parameters[4]]] = 11
    except:
        "No ascat data for this buoy"

    return flag

def convert_10_meters(var, flag, height, wspd_name, gust_name):

    var.loc[(flag[wspd_name] == 0), wspd_name] = var[wspd_name] * (10 / height) ** 0.11

    var.loc[(flag[gust_name] == 0), gust_name] = var[gust_name] * (10 / height) ** 0.11

    return var


#######################################################################################
# Related Measurement Check
# Compares the values of the two anemometers to find the best one.
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - Wdir1: wind direction for anemometer 1
# - Wspd1: wind speed for anemometer 1
# - Gust1: gust speed for anemometer 1
# - zwind1: Height of the anemometer 1
# - Wdir2: wind direction for anemometer 2
# - Wspd2: wind speed for anemometer 2
# - Gust2: gust speed for anemometer 2
# - zwind2: Height of the anemometer 2
# - Wspd1flag: matrix of flag for wind speed from anemometer 1
# - Wdir1flag: matrix of flag for wind direction from anemometer 1
# - Gust1flag: matrix of flag for gust speed from anemometer 1
# - Wspd2flag: matrix of flag for wind speed from anemometer 2
# - Wdir2flag: matrix of flag for wind direction from anemometer 2
# - Gust2flag: matrix of flag for gust speed from anemometer 2
# - Wdir1flagid: flag id for Wdir1. '52' --> letter that represents the flag
# - Wspd1flagid: flag id for Wspd1. '52' --> letter that represents the flag
# - Gust1flagid: flag id for Gust1. '52' --> letter that represents the flag
# - Wdir2flagid: flag id for Wdir2. '52' --> letter that represents the flag
# - Wspd2flagid: flag id for Wspd2. '52' --> letter that represents the flag
# - Gust2flagid: flag id for Gust2. '52' --> letter that represents the flag
#
#
# Required checks: Range, Missing value, Wind Speed x Gust
#
# Return: Wdirflag,Wspdflag,Gustflag,Wdir,Wspd,Gust,Wdirflagid,Wspdflagid,Gustflagid
########################################################################################

def related_meas_check(var, flag, parameters):

    var["wspd"] = var[parameters[0]]
    var["wdir"] = var[parameters[2]]
    var["gust"] = var[parameters[4]]

    flag["wspd"] = flag[parameters[0]]
    flag["wdir"] = flag[parameters[2]]
    flag["gust"] = flag[parameters[4]]

    var.loc[(flag[parameters[0]] != 0) & (flag[parameters[1]] == 0), "wspd"] = var[parameters[1]]
    flag.loc[(flag[parameters[0]] != 0) & (flag[parameters[1]] == 0), "wspd"] = flag[parameters[1]]

    var.loc[(flag[parameters[2]] != 0) & (flag[parameters[3]] == 0), "wdir"] = var[parameters[3]]
    flag.loc[(flag[parameters[2]] != 0) & (flag[parameters[3]] == 0), "wdir"] = flag[parameters[3]]

    var.loc[(flag[parameters[4]] != 0) & (flag[parameters[5]] == 0), "gust"] = var[parameters[5]]
    flag.loc[(flag[parameters[4]] != 0) & (flag[parameters[5]] == 0), "gust"] = flag[parameters[5]]

    for parameter in parameters:
        del var[parameter]
        del flag[parameter]

    return var, flag



    #####################
    #end of related measurement check 2


########################################################################################
# Time continuity
# Check is to verify if the data has consistency in the time
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - var: variable
# - flag: matrix of flag for the variable
# - sigma: value for the contnuity equation (normally, is related to std deviation)
# - idf= flag id. '8' --> letter that represents the flag
# - rt: toggle for RTQC. If it is 0, RTQC
#
# Required checks: Range Check, Missing value check, Stuck Sensor check
#
# Return: flag,idf
#########################################################################################


def t_continuity_check(var, sigma, limit, flag, parameter):

    flag['tmp_forward'] = 0
    flag['tmp_backward'] = 0

    for counter in range(len(var)):
        value = var.loc[(var.index >= var.index[counter]) & (var.index <= var.index[counter] + pd.to_timedelta(limit, unit='h')) & (flag[parameter] == 0), parameter]
        if value.size > 1:
            forward_values = np.array(value - value[0])
            backward_values = np.array(value - value[-1])
            delta_times_forward = np.array(value.index - value.index[0])/(10**9)/3600
            delta_times_backward = np.array(value.index - value.index[-1])/(10**9)/3600
            times = np.array(value.index)
            for i in range(len(delta_times_forward) - 1):
                if (0.58 * sigma[parameter] * (np.sqrt(int(delta_times_forward[i + 1])))) < forward_values[i + 1]:
                    flag.loc[(times[i]), "tmp_forward"] = 1
                if (0.58 * sigma[parameter] * (np.sqrt(int(-delta_times_backward[i])))) < backward_values[i]:
                    flag.loc[(times[i]), "tmp_backward"] = 1

    flag.loc[(flag["tmp_backward"] == 1) | (flag["tmp_forward"] == 1), parameter] = 8

    del flag['tmp_forward']
    del flag['tmp_backward']

    return flag

########################################################################################
# Time continuity adcp
# Check is to verify if the data has consistency in the time
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - var: variable
# - flag: matrix of flag for the variable
# - sigma: value for the contnuity equation (normally, is related to std deviation)
# - idf= flag id. '8' --> letter that represents the flag
# - rt: toggle for RTQC. If it is 0, RTQC
#
# Required checks: Range Check, Missing value check, Stuck Sensor check
#
# Return: flag,idf
#########################################################################################


def tcontinuityadcpcheck(Epoch,var,flag,idf,flag_dir,idf_dir):


    fwd_ep_gd,bck_ep_gd = float(Epoch[0]),float(Epoch[-1])
    fwd_gd,bck_gd = var[0],var[-1]
    fwd_gdf,bck_gdf = flag[0],flag[-1]
    fwsp_qc,bksp_qc = [0]*len(Epoch),[0]*len(Epoch)

    for i in range(1,len(Epoch)):
        delta_Epoch = abs(float(Epoch[i]) - fwd_ep_gd)
        deltacurrent= abs(float(var[i]) - float(fwd_gd))
        if delta_Epoch== 3600*3 and flag[i]!= 4 and fwd_gdf!=4:
            if deltacurrent>249.6:
                fwsp_qc[i] = 4
                flag[i] = 4
            elif deltacurrent>213.9:
                fwsp_qc[i] = 3
                flag[i] = 3
            else:
                fwsp_qc[i] = 1
                fwd_gd = var[i]
                fwd_ep_gd = float(Epoch[i])
                fwd_gdf=flag[i]
                continue
        elif delta_Epoch== 3600*2 and flag[i]!= 4 and fwd_gdf!=4:
            if deltacurrent>193.5:
                fwsp_qc[i] = 4
                flag[i] = 4
            elif deltacurrent>165.9:
                fwsp_qc[i] = 3
                flag[i] = 3
            else:
                fwsp_qc[i] = 1
                fwd_gd = var[i]
                fwd_ep_gd = float(Epoch[i])
                fwd_gdf=flag[i]
                continue
        elif delta_Epoch== 3600*1 and flag[i]!= 4 and fwd_gdf!=4:
            if deltacurrent>131.4:
                fwsp_qc[i] = 4
                flag[i] = 4
            elif deltacurrent>112.6:
                fwsp_qc[i] = 3
                flag[i] = 3
            else:
                fwsp_qc[i] = 1
                fwd_gd = var[i]
                fwd_ep_gd = float(Epoch[i])
                fwd_gdf=flag[i]
                continue
        else:
            fwd_gd = var[i]
            fwd_ep_gd = float(Epoch[i])
            fwd_gdf=flag[i]
            continue

    for i in range(-2,-len(Epoch),-1):
        delta_Epoch = abs(float(Epoch[i]) - int(bck_ep_gd))
        deltacurrent= abs(float(var[i]) - float(bck_gd))
        if delta_Epoch == 3600*3 and flag[i] != 4 and bck_gdf!=4:
            if deltacurrent>249.6:
                bksp_qc[i] = 4
            elif deltacurrent>213.9:
                bksp_qc[i] = 3
            else:
                bksp_qc[i] = 1
                bck_gd = var[i]
                bck_ep_gd = float(Epoch[i])
                bck_gdf=flag[i]
        elif delta_Epoch == 3600*2 and flag[i] != 4 and bck_gdf!=4:
            if deltacurrent>193.5:
                bksp_qc[i] = 4
            elif deltacurrent>165.9:
                bksp_qc[i] = 3
            else:
                bksp_qc[i] = 1
                bck_gd = var[i]
                bck_ep_gd = float(Epoch[i])
                bck_gdf=flag[i]
        elif delta_Epoch == 3600*1 and flag[i] != 4 and bck_gdf!=4:
            if deltacurrent>131.4:
                bksp_qc[i] = 4
            elif deltacurrent>112.6:
                bksp_qc[i] = 3
            else:
                bksp_qc[i] = 1
                bck_gd = var[i]
                bck_ep_gd = float(Epoch[i])
                bck_gdf=flag[i]
        else:
            bck_gd = var[i]
            bck_ep_gd = float(Epoch[i])
            bck_gdf=flag[i]

    #TODO: Might consider a toggle since you want to be able to run this in realtime.
    #OK
    for i in range(0, len(Epoch)):
        if fwsp_qc[i] == 4 or bksp_qc[i] == 4:
            flag[i] = 4
            idf[i]='10'
            flag_dir[i]=4
            idf_dir[i]='10'

        elif fwsp_qc[i] == 3 or bksp_qc[i] == 3:
            flag[i] = 3
            idf[i]='61'
            flag_dir[i]=3
            idf_dir[i]='61'

        else:
            continue


    return flag,idf, flag_dir, idf_dir




########################################################################################
# Time continuity adcp
# Check is to verify if the data has consistency in the time
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - var: variable
# - flag: matrix of flag for the variable
# - sigma: value for the contnuity equation (normally, is related to std deviation)
# - idf= flag id. '8' --> letter that represents the flag
# - rt: toggle for RTQC. If it is 0, RTQC
#
# Required checks: Range Check, Missing value check, Stuck Sensor check
#
# Return: flag,idf
#########################################################################################


def currentgradientcheck(Epoch,var,flag,sigma,idf,rt):


    fwd_ep_gd,bck_ep_gd = float(Epoch[0]),float(Epoch[-1])
    fwd_gd,bck_gd = var[0],var[-1]
    fwd_gdf,bck_gdf = flag[0],flag[-1]
    fwsp_qc,bksp_qc = [0]*len(Epoch),[0]*len(Epoch)

    for i in range(1,len(Epoch)):
        delta_Epoch = abs(float(Epoch[i]) - fwd_ep_gd)
        deltacurrent= abs(float(var[i]) - float(fwd_gd))
        if delta_Epoch== 3600*3 and flag[i]!= 4 and fwd_gdf!=4:
            if deltacurrent>24.96:
                fwsp_qc[i] = 4
                flag[i] = 4
            elif deltacurrent>21.39:
                fwsp_qc[i] = 3
                flag[i] = 3
            else:
                fwsp_qc[i] = 1
                fwd_gd = var[i]
                fwd_ep_gd = float(Epoch[i])
                fwd_gdf=flag[i]
                continue
        elif delta_Epoch== 3600*2 and flag[i]!= 4 and fwd_gdf!=4:
            if deltacurrent>19.35:
                fwsp_qc[i] = 4
                flag[i] = 4
            elif deltacurrent>16.59:
                fwsp_qc[i] = 3
                flag[i] = 3
            else:
                fwsp_qc[i] = 1
                fwd_gd = var[i]
                fwd_ep_gd = float(Epoch[i])
                fwd_gdf=flag[i]
                continue
        elif delta_Epoch== 3600*1 and flag[i]!= 4 and fwd_gdf!=4:
            if deltacurrent>13.14:
                fwsp_qc[i] = 4
                flag[i] = 4
            elif deltacurrent>11.26:
                fwsp_qc[i] = 3
                flag[i] = 3
            else:
                fwsp_qc[i] = 1
                fwd_gd = var[i]
                fwd_ep_gd = float(Epoch[i])
                fwd_gdf=flag[i]
                continue
        else:
            fwd_gd = var[i]
            fwd_ep_gd = float(Epoch[i])
            fwd_gdf=flag[i]
            continue

    for i in range(-2,-len(Epoch),-1):
        delta_Epoch = abs(float(Epoch[i]) - int(bck_ep_gd))
        deltacurrent= abs(float(var[i]) - float(bck_gd))
        if delta_Epoch == 3600*3 and flag[i] != 4 and bck_gdf!=4:
            if deltacurrent>24.96:
                bksp_qc[i] = 4
            elif deltacurrent>21.39:
                bksp_qc[i] = 3
            else:
                bksp_qc[i] = 1
                bck_gd = var[i]
                bck_ep_gd = float(Epoch[i])
                bck_gdf=flag[i]
        elif delta_Epoch == 3600*2 and flag[i] != 4 and bck_gdf!=4:
            if deltacurrent>19.35:
                bksp_qc[i] = 4
            elif deltacurrent>16.59:
                bksp_qc[i] = 3
            else:
                bksp_qc[i] = 1
                bck_gd = var[i]
                bck_ep_gd = float(Epoch[i])
                bck_gdf=flag[i]
        elif delta_Epoch == 3600*1 and flag[i] != 4 and bck_gdf!=4:
            if deltacurrent>13.14:
                bksp_qc[i] = 4
            elif deltacurrent>11.26:
                bksp_qc[i] = 3
            else:
                bksp_qc[i] = 1
                bck_gd = var[i]
                bck_ep_gd = float(Epoch[i])
                bck_gdf=flag[i]
        else:
            bck_gd = var[i]
            bck_ep_gd = float(Epoch[i])
            bck_gdf=flag[i]

    #TODO: Might consider a toggle since you want to be able to run this in realtime.
    #OK
    for i in range(0, len(Epoch)):
        if fwsp_qc[i] == 4 or bksp_qc[i] == 4:
            flag[i] = 4
            idf[i]='10'
        elif fwsp_qc[i] == 3 or bksp_qc[i] == 3:
            flag[i] = 3
            idf[i]='61'
        else:
            continue


    return flag,idf

########################################################################################
########################################################################################
# Frontal exception for time continuity checks
# Exception for the time continuity during frontal passages
#
# Required checks: Range Check, Missing value check, Stuck Sensor check, Time Continuity
#
########################################################################################
########################################################################################


# Frontal exception 1
# Relation between wind direction and air temperature 2
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - windd: wind direction for the best anemometer
# - flagw: wind direction flag for the best anemometer
# - flaga: air temperature flag
# - idfa= flag id for air temperature. '53' --> letter that represents the flag
#
# Return: flaga,idfa

def front_except_check1(var, flag, wdir_name, atmp_name):

    var = var.sort_index(ascending=False)

    selected_variable = var.loc[((flag[atmp_name] == 8) | (flag[atmp_name] == 0)) & (flag[wdir_name] == 0)]

    flag.loc[(selected_variable.loc[(selected_variable[wdir_name].diff() > 40) &  (selected_variable[wdir_name].diff() < - 40) & (flag[atmp_name] == 8), atmp_name].index)] == 53

    return flag

    #end of Frontal excepion 1

# Frontal exception 2
# Relation between wind direction and air temperature 2
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - windd: wind direction for the best anemometer
# - flagw: wind direction flag for the best anemometer
# - flaga: air temperature flag
# - idfw= flag id for wind direction. '53' --> letter that represents the flag
#
# Return: flagw,idfw

# def frontexcepcheck2(Epoch,windd,flagw,flaga,idfw):
#
#
#
#    last_gt= Epoch[0]
#    last_gw = windd[0]
#    last_fa= flaga[0]
#    last_fw=flagw[0]
#
#    for i in range(1,len(Epoch)):
#        delta_Epoch = abs(Epoch[i] - last_gt)
#        if delta_Epoch <= 3600*3:
#            if last_fw!=4:
#                if flagw[i]==4:
#                    if idfw[i]=='8':
#                        if last_fa!=4 and abs(windd[i] - last_gw)>40:
#                            flagw[i] = 2
#                            idfw[i]='54'
#                            last_gt= Epoch[i]
#                            last_gw = windd[i]
#                            last_fa= flaga[i]
#                            last_fw=flagw[i]
#                    else:
#                        continue
#                else:
#                    last_gt= Epoch[i]
#                    last_gw = windd[i]
#                    last_fa= flaga[i]
#                    last_fw=flagw[i]
#            else:
#                last_gt= Epoch[i]
#                last_gw = windd[i]
#                last_fa= flaga[i]
#                last_fw=flagw[i]
#        else:
#            last_gt= Epoch[i]
#            last_gw = windd[i]
#            last_fa= flaga[i]
#            last_fw=flagw[i]
#            continue
#
#        return flagw, idfw

    #end of Frontal excepion 2

# Frontal exception 3
# Relation between wind direction and air temperature 2
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - winds: wind speed for the best anemometer
# - flags: wind speed flag for the best anemometer
# - flaga: air temperature flag
# - idfw= flag id for wind direction. '53' --> letter that represents the flag
#
# Return: flagw,idfw


def front_except_check3(var, flag, wspd_name, atmp_name):

    flag.loc[((flag[atmp_name] == 8) & (flag[wspd_name] == 0)) & (var[wspd_name] > 7)] = 55

    return flag

    #end of Frontal excepion 3

# Frontal exception 4
# Relation between low pressure and wind speed
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - pres: air pressure
# - flagp: wind speed flag for the best anemometer
# - flagp: air pressure flag
# - idfw= flag id for wind direction. '53' --> letter that represents the flag
#
# Return: flagw,idfw

def front_except_check4(var, flag, pres_name, wspd_name):

    var = var.sort_index(ascending=False)

    selected_variable = var.loc[((flag[atmp_name] == 8) | (flag[atmp_name] == 0)) & (flag[wdir_name] == 0)]

    flag.loc[(selected_variable.loc[(selected_variable[wdir_name].diff() > 40) &  (selected_variable[wdir_name].diff() < - 40) & (flag[atmp_name] == 8), atmp_name].index)] == 53



    for i in range(0, len(Epoch)):
        if pres[i]<995 and flagp[i]!=4 and idfw[i]=='8':
            flagw[i] = 2
            idfw[i]='56'
        else:
            continue

    return flag

    #end of Frontal excepion 4

# Frontal exception 5
# Relation between two pressures
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - pres: air pressure
# - flagp: air pressure flag
# - idfp= flag id for air pressure. '53' --> letter that represents the flag
#
# Return: flagp,idfp


def frontexcepcheck5(Epoch,pres,flagp,idfp):

    last_gt= Epoch[0]
    last_fp= flagp[0]
    last_gp = pres[0]

    for i in range(1,len(Epoch)):
        delta_Epoch = abs(Epoch[i] - last_gt)
        if delta_Epoch <= 3600*3:
            if last_fp!=4:
                if flagp[i]==4:
                    if idfp[i]=='8':
                        if pres[i]<=1000 and last_gp<=1000:
                            flagp[i] = 2
                            idfp[i]='57'
                            last_gt= Epoch[i]
                            last_fp= flagp[i]
                            last_gp = pres[i]
                        else:
                            continue
                    else:
                        continue
                else:
                    last_gt= Epoch[i]
                    last_fp= flagp[i]
                    last_gp = pres[i]
                    continue
            else:
                last_gt= Epoch[i]
                last_fp= flagp[i]
                last_gp = pres[i]
                continue
        else:
            last_gt= Epoch[i]
            last_fp= flagp[i]
            last_gp = pres[i]
            continue

    return flagp,idfp

    #end of Frontal excepion 5


# Frontal exception 6
# Relation between two pressures
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - winds: wind speed for the best anemometer
# - flags: wind speed flag for the best anemometer
# - flagwh: swvht flag
# - idfwh: flag id for swvht. '53' --> letter that represents the flag
#
# Return: flagwh,idfwh


def frontexcepcheck6(Epoch,winds,flags,idfwh,flagwh):

    for i in range(0, len(Epoch)):
        if winds[i]>=15 and flags[i]!=4 and idfwh[i]=='8':
            flagwh[i] = 2
            idfwh[i]='58'
        else:
            continue


    return flagwh,idfwh

    #end of Frontal excepion 6

# TODO asdasdas

def related1(Epoch,winds,flags,idfwh,flagwh):

    for i in range(0, len(Epoch)):
        winds[i]>=15 and flags[i]!=4 and idfwh[i]=='8'


    flag_data[["gust","wspd", "wdir"]] = qc.front_except_check6(flag_data[["gust","wspd", "wdir"]])


    cspd3flagid[i]='12'


#######################################################################################
# swvht x Average Period check
# Compare the wave significant height and Average period
# If the value do not change, it will be flagged
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - Apd: Average wave period
# - swvht: Significant wave height
# - flagt: matrix of flag for Apd
# - flagh: matrix of flag for swvht
# - idf= flag id for swvht. '7' --> letter that represents the flag
#
# Required checks: Range Check, Missing value check
#
# Return: flag,idf
#
########################################################################################

def hstscheck(Epoch, Apd, swvht, flagt,flagh,idf):

    htresh=[0]*len(Epoch)
    for i in range(len(Epoch)):
        if Apd[i] <= 5 and Apd[i]!=None:
           htresh[i] = (2.55 + (Apd[i]/4))

        elif Apd[i] > 5:
           htresh[i] = ((1.16*Apd[i])-2)

        else:
           continue

    for i in range(len(Epoch)):
        if flagt[i]!=4 and flagh[i]!=4:
            if swvht[i] > htresh[i]:
               flagh[i] = 4
               idf[i]='7'

            else:
               continue



    return flagh, idf





    #####################
    #end Wave Height Versus Average Wave Period check




#############################################################
#############################################################
#END OF CHECKS
#############################################################
#############################################################



###############################################################################
#
#OTHERS CHECKS THAT ARE NOT OK YET
#
#
###############################################################################


def ncepmodelcheck(Epoch,Pres,Atmp,Wdir,Wspd,Presflag,Atmpflag,Wdirflag,Wspdflag):


    lines = open('C:\\ndbc\\data\\NCPE_data.txt', 'rb')

#read in the first 2 lines
    ASCIIs = lines.readline()
    Units = lines.readline()

#declare lists for each column in the file.
    Year,Month,Day,Hour,Minute = [],[],[],[],[]
    ModelWdir,ModelAtmp,ModelPres,ModelWspd = [],[],[],[]


    for line in lines:
        dataline = line.strip()
        columns = dataline.split()

        Year.append(int(columns[0]))
        Month.append(int(columns[1]))
        Day.append(int(columns[2]))
        Hour.append(int(columns[3]))
        Minute.append(int(columns[4]))
        ModelWdir.append(int(columns[0]))
        ModelAtmp.append(int(columns[1]))
        ModelWspd.append(int(columns[2]))
        ModelPres.append(int(columns[3]))

    for i in range(len(Epoch)):
            if abs(ModelPres[i]-Pres[i])>2.5:
                Presflag[i] = 3
            elif abs(ModelAtmp[i]-Atmp[i])>3:
                Atmpflag[i] = 3
            elif Wspd[i]>10 and abs(Wdir[i]-Wdir[i-1])>30:
                Wdirflag[i]=3
            elif Wspd[i]>=5 and Wspd<=10:
                if((Wspd-15.6)/-188)<abs(Wdir[i]-Wdir[i-1]):
                    Wdirflag[i]=3
            elif Wspd[i]>=6 and Wspd[i]<=12 and abs(Wspd[i]-Wspd[i-1])>((Wspd[i]-16.1)/1.67):
                Wspdflag[i]=3
            elif Wspd[i]>12 and abs(Wspd[i]-Wspd[i-1])>2.25:
                Wspdflag[i]=3
            else:
                continue

    return Presflag,Atmpflag,Wdirflag,Wspdflag
########################################################
#end Wave Height Verses Average Wave Period check

#######################################################################################
# Climatological Range Check
# Check to ensure values are within climatological ranges.
#
# Required checks: Range
#
########################################################################################
def climarangecheck(Epoch, swvht, Dpd, Apd,Wspd,Gust,Pres,dewpt,Atmp,Wtmp,swvhtflag, Dpdflag, Apdflag,Wspdflag, Gustflag, Presflag, dewptflag, Atmpflag, Wtmpflag):

# Definition of the ranges. For example: msdswvht=[0,5]. It means that the
# climatological mean is 0 and the standard deviation is 20

    msdswvht=[0,20]
    msdDpd=[1.95,26]
    msdApd=[0,26]
    msdWspd=[0,60]
    msdGust=[0,60]
    msdAtmp=[-40,60]
    msdPres=[500,1100]
    msddewpt=[-30,40]
    msdWtmp=[-4,30]

    for i in range(len(Epoch)):

        if swvht[i] > (msdswvht[0]+(3*msdswvht[1])) or swvht[i] < (msdswvht[0]-(3*msdswvht[1])):
           swvhtflag[i] = 4

        else:
           swvhtflag[i] = 1

    for i in range(len(Epoch)):

        if Dpd[i] >= (msdDpd[0]+(3*msdDpd[1])) or Dpd[i] <= (msdDpd[0]+(3*msdDpd[1])):
           Dpdflag[i] = 4

        else:
           Dpdflag[i] = 1

    for i in range(len(Epoch)):

        if Apd[i] >= (msdApd[0]+(3*msdApd[1])) or Apd[i] <= (msdApd[0]+(3*msdApd[1])):
           Apdflag[i] = 4

        else:
           Apdflag[i] = 1

    for i in range(len(Epoch)):

        if Wspd[i] > (msdWspd[0]+(3*msdWspd[1])) or Wspd[i] < (msdWspd[0]+(3*msdWspd[1])):
           Wspdflag[i] = 4

        else:
           Wspdflag[i] = 1

    for i in range(len(Epoch)):

        if Gust[i] > (msdGust[0]+(3*msdGust[1])) or Gust[i] < (msdGust[0]+(3*msdGust[1])):
           Gustflag[i] = 4

        else:
           Gustflag[i] = 1

    for i in range(len(Epoch)):

        if Atmp[i] > (msdAtmp[0]+(3*msdAtmp[1])) or Atmp[i] < (msdAtmp[0]+(3*msdAtmp[1])):
           Atmpflag[i] = 4

        else:
           Atmpflag[i] = 1

    for i in range(len(Epoch)):

        if Pres[i] > (msdPres[0]+(3*msdPres[1])) or Pres[i] < (msdPres[0]+(3*msdPres[1])):
           Presflag[i] = 4

        else:
           Presflag[i] = 1

    for i in range(len(Epoch)):

        if dewpt[i] > (msddewpt[0]+(3*msddewpt[1])) or dewpt[i] < (msddewpt[0]+(3*msddewpt[1])):
           dewptflag[i] = 4

        else:
           dewptflag[i] = 1

    for i in range(len(Epoch)):

        if Wtmp[i] > (msdWtmp[0]+(3*msdWtmp[1])) or Wtmp[i] < (msdWtmp[0]+(3*msdWtmp[1])):
           Wtmpflag[i] = 4

        else:
           Wtmpflag[i] = 1


    return swvhtflag, Dpdflag, Apdflag, Wspdflag, Gustflag, Presflag, dewptflag, Atmpflag, Wtmpflag
    #end Range Check section

#####################################################


def gustratiocheck(gustfactor,winds,gust,flaggf):


     for i in range(len(gust)):
         xx=math.exp(-0.18*gust[i])
         gzero = 1.98 - ( 1.887*xx)
         ratiomax = 1.5 + (1.0/gzero)
         if winds[i]<0.3:
             ratiomax=ratiomax+5
         elif winds[i]<1.0 and winds[i]>=0.3:
             ratiomax=ratiomax+3
         elif winds[i]<3.0 and winds[i]>=1.0:
             ratiomax=ratiomax+0.7
         elif winds[i]<6.0 and winds[i]>=3.0:
             ratiomax=ratiomax+0.35
         else:
             ratiomax=ratiomax+0.2
             continue

         if gustfactor[i]>ratiomax:
             flaggf[i]=4
         elif gustfactor[i]<=0.9:
             flaggf[i]=4
         else:
             continue

     return flaggf




########################################################################################
# Swell direction check
# Check to determine if the direction of the swell is coming from shore
#
# Required checks: None
#
#########################################################################################

def dircoastcheck(Epoch, Mwd, coastflag):


    meancoast = 150 #direction of the coast

    for i in range(len(Epoch)):

        if Mwd[i] < (meancoast-45) and Mwd[i] > (meancoast+45-180):
           coastflag[i] = 4


        else:
           coastflag[i] = 1

    return coastflag, meancoast

#end Swell direction check

########################################################


########################################################################################
#Wave Height Verses Average Wave Period
#
# Check is to verify if the wave height is consistent with the average wave period
#
# Required checks: None
#
#########################################################################################





