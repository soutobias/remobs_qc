"""
Python Version tested: Python 3 or greater

Required Dependencies: Numpy, time, math, pandas

Description: This module is designed to QC data colected by Fixed Station
(buoys, weather station). QC is based on "Manual de Controle de Qualidade de Dados
do PNBOIA". Each check generates a flag number. Depending the number of the flag,
the data is considered good, suspicious or bad

Flag Convention as follows:
 0 = good data or no QC performed
 1 - 50 = hard flag data. Bad data
 51 - 99 = soft flag data. Suspicious data

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
# - parameter: name of variable that will be checked
# - limits: dataframe with missing values limits
# - flag: dataframe for flag
# - var: dataframe for variables
#
# Required checks: None
#
# Return: flag
# Represented by number "1" -> HARD FLAG
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
# - parameter: name of variable that will be checked
# - limits: dataframe with range check limits
# - flag: dataframe for flag
# - var: dataframe for variables
#
# Required checks: Missing value check
#
# Return: flag
# Represented by number "2" -> HARD FLAG
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
# - parameter: name of variable that will be checked
# - limits: dataframe with climate range check limits
# - flag: dataframe for flag
# - var: dataframe for variables
#
# Required checks: Missing value check, range check
#
# Return: flag
# Represented by number "9" -> HARD FLAG
###############################################################################

def range_check_climate(var, limits, flag, parameter):

    try:
        flag.loc[(var[parameter] < limits[parameter][0]) & (flag[parameter] == 0), parameter] = 9
        flag.loc[(var[parameter] > limits[parameter][1]) & (flag[parameter] == 0), parameter] = 9
    except:
        print("No range_climate_limit for " + parameter)

    return flag


###############################################################################
# Wave Significant Height x Wave Max Height
# Compares if the values of wind speed is higher than Gust.
#
# Required input:
# - swvht_name: name used in the dataframe for Wave Significance Height
# - mxwvht_name: name used in the dataframe for Maximun Wave Height
# - flag: dataframe for flag
# - var: dataframe for variables
#
#
# Required checks: Missing value check, range check, range check climate
#
# Return: flag
# Represented by number "4" -> HARD FLAG
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
# - wspd_name: name used in the dataframe for wind speed
# - gust_name: name used in the dataframe for gust speed
# - flag: dataframe for flag
# - var: dataframe for variables
#
# Required checks: Missing value check, range check, range check climate
#
# Return: flag
# Represented by number "3" -> HARD FLAG
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
# - dewpt_name: name used in the dataframe for dew point
# - atmp_name: name used in the dataframe for air temperature
# - flag: dataframe for flag
# - var: dataframe for variables
#
# Required checks: Missing value check, range check, range check climate
#
# Return: flag
# Represented by number "51" -> SOFT FLAG
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
# - pres_name: name used in the dataframe for pressure
# - flag: dataframe for flag
# - var: dataframe for variables
#
# Required checks: Missing value check, range check, range check climate
#
# Return: flag
# Represented by number "5" -> HARD FLAG
###############################################################################

def bat_sensor_check(var, flag, battery_name, pres_name):

    flag.loc[(var[battery_name] <= 10.5) & (flag[pres_name] == 0), pres_name] = 5

    return flag

    #####################
    #end battery pressure check section

#######################################################################################
# Stuck Sensor Check
# Verify if the value of a parameter repeats until limits times
#
# Required input:
# - parameter: name of variable that will be checked
# - limits: number of repetition used in the test
# - flag: dataframe for flag
# - var: dataframe for variables
#
# Required checks: Missing value check, range check, range check climate
#
# Return: flag
# Represented by number "6" -> HARD FLAG
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


#######################################################################################
# Ascat Anemometer Comparison
# CHM did comparisons between buoy and ASCAT data.
# With this comparison, it was possible to correct wind direction data
#
# Required input:
# - parameters: array of parameters names for wind speed and wind direction
# - flag: dataframe for flag
# - var: dataframe for variables
# - buoy: name of the buoy
#
# Required checks: Missing value check, range check, range check climate,
# stuck sensor, windspeedgust check
#
# Return: flag
# Represented by number "11" -> HARD FLAG
########################################################################################



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


#######################################################################################
# Convert wind to 10 meters
# Using Liu equation to convert wind data to 10 meters
#
# Required input:
# - wspd_name: name used in the dataframe for wind speed
# - gust_name: name used in the dataframe for gust speed
# - flag: dataframe for flag
# - var: dataframe for variables
# - height: height of anemometer sensor in the buoy
#
# Required checks: Missing value check, range check, range check climate,
# stuck sensor, windspeedgust check
#
# Return: var
########################################################################################


def convert_10_meters(var, flag, height, wspd_name, gust_name):

    var.loc[(flag[wspd_name] == 0), wspd_name] = var[wspd_name] * (10 / height) ** 0.11

    var.loc[(flag[gust_name] == 0), gust_name] = var[gust_name] * (10 / height) ** 0.11

    return var


#######################################################################################
# Related Measurement Check
# Compares the values of the two anemometers to find the best one.
#
# Required input:
# - parameters: array of parameters names of wind data
# - flag: dataframe for flag
# - var: dataframe for variables
#
# Required checks: Missing value check, range check, range check climate,
# stuck sensor, windspeedgust check, convert_10_meters
#
# Return: flag, var
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
# - sigma: value considered in the timecontinuity check variation in time
# - limit: number of hour considered in the time continuity test
# - flag: dataframe for flag
# - var: dataframe for variables
# - parameter: name user in the dataframe to represent the parameter
#
# Required checks: Missing value check, range check, range check climate,
# stuck sensor
#
# Return: flag
# Represented by number "8" -> HARD FLAG
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
########################################################################################
# Frontal exception for time continuity checks
# Exception for the time continuity during frontal passages
#
# Required checks: Missing value check, range check, range check climate,
# stuck sensor, time continuity
#
########################################################################################
########################################################################################



########################################################################################
# Frontal exception 1
# Relation between wind direction and air temperature
#
# Required input:
# - wdir_name: name used in the dataframe for wind direction
# - atmp_name: name used in the dataframe for air temperature
# - var: dataframe for variables
# - flag: dataframe for flag
#
# Return: flag
# Represented by number "53" -> SOFT FLAG
########################################################################################


def front_except_check1(var, flag, wdir_name, atmp_name):

    var = var.sort_index(ascending=False)

    selected_variable = var.loc[((flag[atmp_name] == 8) | (flag[atmp_name] == 0)) & (flag[wdir_name] == 0)]

    flag.loc[(selected_variable.loc[(selected_variable[wdir_name].diff() > 40) &  (selected_variable[wdir_name].diff() < - 40) & (flag[atmp_name] == 8), atmp_name].index)] == 53

    return flag

    #end of Frontal excepion 1

########################################################################################
# Frontal exception 3
# Relation between wind speed and air temperature 2
#
# Required input:
# - wspd_name: name used in the dataframe for wind speed
# - atmp_name: name used in the dataframe for air temperature
# - var: dataframe for variables
# - flag: dataframe for flag
#
# Return: flag
# Represented by number "55" -> SOFT FLAG
########################################################################################


def front_except_check3(var, flag, wspd_name, atmp_name):

    flag.loc[((flag[atmp_name] == 8) & (flag[wspd_name] == 0) & (var[wspd_name] > 7)), atmp_name ] = 55

    return flag

    #end of Frontal excepion 3

########################################################################################
# Frontal exception 4
# Relation between low pressure and wind speed
#
# Required input:
# - wspd_name: name used in the dataframe for wind speed
# - pres_name: name used in the dataframe for pressure
# - var: dataframe for variables
# - flag: dataframe for flag
#
# Return: flag
# Represented by number "56" -> SOFT FLAG
########################################################################################

def front_except_check4(var, flag, pres_name, wspd_name):

    var = var.sort_index(ascending=True)

    var['date_time'] = var.index

    var['date_time'] = var.diff()["date_time"] / np.timedelta64(1, 's')

    selected_variable = var.loc[(flag[wspd_name] == 8) & (var["date_time"] < 3700)]

    selected_variable_2 = var.loc[var.index.isin(selected_variable.index + np.timedelta64(-1, "h"))]

    selected_variable["pres_new"] = selected_variable_2["pres"]

    selected_variable = selected_variable.loc[(selected_variable["pres_new"] <= 995)]

    flag.loc[(flag.index.isin(selected_variable.index)), wspd_name] = 56

    del var['date_time']

    return flag
    #end of Frontal excepion 4


########################################################################################
# Frontal exception 5
# Relation between two pressures
#
# Required input:
# - pres_name: name used in the dataframe for pressure
# - var: dataframe for variables
# - flag: dataframe for flag
#
# Return: flag
# Represented by number "57" -> SOFT FLAG
########################################################################################

def front_except_check5(var, flag, pres_name):

    var = var.sort_index(ascending=True)

    var['date_time'] = var.index

    var['date_time'] = var.diff()["date_time"] / np.timedelta64(1, 's')

    selected_variable = var.loc[(flag[pres_name] == 8) & (var["date_time"] < 3700)]

    selected_variable_2 = var.loc[var.index.isin(selected_variable.index + np.timedelta64(-1, "h"))]

    selected_variable["pres_new"] = selected_variable_2["pres"]

    selected_variable = selected_variable.loc[(selected_variable["pres_new"] < 1000)]

    flag.loc[(flag.index.isin(selected_variable.index)), pres_name] = 57

    del var['date_time']

    return flag
    #end of Frontal excepion 5



########################################################################################
# Frontal exception 6
# Relation between significance wave height and wind speed
#
# Required input:
# - wspd_name: name used in the dataframe for wind speed
# - swvht_name: name used in the dataframe for significance wave height
# - var: dataframe for variables
# - flag: dataframe for flag
#
# Return: flag
# Represented by number "58" -> SOFT FLAG
########################################################################################

def frontexcepcheck6(var, flag, wspd_name, swvht_name):

    flag.loc[((flag[swvht_name] == 8) & (var[wspd_name] >= 15)), swvht_name ] = 58

    return flag
    #end of Frontal excepion 6

########################################################################################
# Comparison of Measurement Check
# Relation between related measurement (wind speed, gust, etc)
#
# Required input:
# - parameters: list of parameters names to be related
# - var: dataframe for variables
# - flag: dataframe for flag
#
# Return: flag
# Represented by number "60" -> SOFT FLAG
########################################################################################

def comparison_related_check(var, flag, parameters):

    for i in len(parameters):
        params_test = parameters.remove(i)
        for param in params_test:
            flag.loc[((flag[param] < 51) & (flag[param] > 0) & (flag[parameters[i]] == 0)), parameters[i]] = 60

    return flag


#######################################################################################
# swvht x Average Period check
# Compare the wave significant height and Average period
# If the value do not change, it will be flagged
#
# Required input:
# - mean_tp_name: name used in the dataframe for mean period
# - swvht_name: name used in the dataframe for significance wave height
# - var: dataframe for variables
# - flag: dataframe for flag
#
# Required checks: Range Check, Missing value check
#
# Return: flag
# Represented by number "62" -> SOFT FLAG
########################################################################################

def hsts_check(var, flag, swvht_name, mean_tp_name):


    var["hmax"] = 10000

    var.loc[(var[mean_tp_name] < 5), "hmax"] = 2.55 + (var[mean_tp_name] / 4)

    var.loc[(var[mean_tp_name] >= 5), "hmax"] = (1.16 * var[mean_tp_name]) - 2

    flag.loc[(var[swvht_name] <= var["hmax"]) & (var[swvht_name] == 0)] = 62

    return flag
    #####################
    #end Wave Height Versus Average Wave Period check


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


def tcontinuityadcpcheck(var, sigma, limit, flag, parameter):

    flag['tmp_forward'] = 0
    flag['tmp_backward'] = 0

    for counter in range(len(var)):
        value = var.loc[(var.index >= var.index[counter]) & (var.index <= var.index[counter] + pd.to_timedelta(limit, unit='h')) & (flag[parameter[1]] == 0), parameter[0]]
        if value.size > 1:
            forward_values = np.array(value - value[0])
            backward_values = np.array(value - value[-1])
            delta_times_forward = np.array(value.index - value.index[0])/(10**9)/3600
            delta_times_backward = np.array(value.index - value.index[-1])/(10**9)/3600
            times = np.array(value.index)
            for i in range(len(delta_times_forward) - 1):
                tempo = abs(int(delta_times_forward[i + 1]))
                if tempo != 0:
                    if sigma[str(tempo)]['soft'] < forward_values[i + 1]:
                        flag.loc[(times[i]), "tmp_forward"] = 2
                    if sigma[str(tempo)]['hard'] < forward_values[i + 1]:
                        flag.loc[(times[i]), "tmp_forward"] = 1
                tempo = abs(int(delta_times_backward[i]))
                if tempo != 0:
                    if sigma[str(tempo)]['soft'] < forward_values[i + 1]:
                        flag.loc[(times[i]), "tmp_backward"] = 2
                    if sigma[str(tempo)]['hard'] < backward_values[i]:
                        flag.loc[(times[i]), "tmp_backward"] = 1

    flag.loc[(flag["tmp_backward"] == 2) | (flag["tmp_forward"] == 2), parameter[1]] = 61
    flag.loc[(flag["tmp_backward"] == 1) | (flag["tmp_forward"] == 1), parameter[1]] = 10

    del flag['tmp_forward']
    del flag['tmp_backward']
    del df[parameter[0]]

    return flag


########################################################################################
# IN CONSTRUCTION!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# IN CONSTRUCTION!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
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
    pass


###############################################################################
# IN CONSTRUCTION!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# IN CONSTRUCTION!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#
# Range check Standard Deviation
#
# Check to ensure values are within 3 standard deviation from the historical data (LIMITS)
#
# Required input:
# - parameter: name of variable that will be checked
# - limits: dictionary with standard deviation of buoy data
# - flag: dataframe for flag
# - var: dataframe for variables
#
# Required checks: Missing value check, range check, range check climate
#
# Return: flag
# Represented by number "20" -> HARD FLAG
# IN CONSTRUCTION!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
###############################################################################


def range_check_std(var, limits, flag, parameter):

    try:
        max_value = limits[parameter][0] + 3 * limits[parameter][1]
        min_value = limits[parameter][0] - 3 * limits[parameter][1]

        flag.loc[(var[parameter] < max_value) & (flag[parameter] == 0), parameter] = 20
        flag.loc[(var[parameter] > min_value) & (flag[parameter] == 0), parameter] = 20
    except:
        print("No std value for " + parameter)

    return flag

    #####################
    #end Range Check STD section

