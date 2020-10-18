# -*- coding: utf-8 -*-
"""
Created on Thu Jun 05 14:47:23 2014

@author: soutobias
"""

import numpy as np
from numpy import *
import ocean_data_qc as qc
import limits


def arredondar(num):
    return float( '%.0f' % ( num ) )


def definition_flag_pandas(df):

    flag_data = df [['wspd1', 'gust1', 'wdir1', 'wspd2', 'gust2',
       'wdir2', 'atmp', 'rh', 'dewpt', 'pres', 'sst', 'compass', 'arad',
       'cspd1', 'cdir1', 'cspd2', 'cdir2', 'cspd3', 'cdir3', 'swvht', 'mxwvht',
       'tp', 'wvdir', 'wvspread']] * 0

    flag_data = flag_data.replace(np.nan, 0).astype(int)

    return flag_data

def qualitycontrol(df, buoy):

    flag_data = definition_flag_pandas(df)

    ##############################################
    #RUN THE QC CHECKS
    ##############################################

    parameters = ['wdir1', 'wspd1', 'gust1', 'wdir2', 'wspd2', 'gust2', 'swvht', 'mxwvht', 'tp', 'wvdir', 'pres', 'rh', 'atmp', 'sst', 'dewpt', 'cspd1', 'cdir1', 'cspd2', 'cdir2', 'cspd3', 'cdir3']
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
        flag_data = qc.stuck_sensor_check(df, limits.stuck_limits, flag_data, parameter)


    # comparison with scaterometer data
    parameters = ["wspd1", "wspd2", "wdir1", "wdir2", "gust1", "gust2"]
    flag_data = qc.ascat_anemometer_comparison(df, flag_data, parameters, buoy["nome"])

    df = qc.convert_10_meters(df, flag_data, 4.7, "wspd1", "gust1")

    df = qc.convert_10_meters(df, flag_data, 3.4, "wspd2", "gust2")

    (df, flag_data) = qc.related_meas_check(df, flag_data, parameters)

    #Time continuity check
    flag_data = qc.t_continuity_check(df, limits.sigma_limits, limits.continuity_limits, flag_data, "swvht")
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


def qualitycontrol_adcp(Epoch,cspd1,cdir1,cspd2,cdir2,cspd3,cdir3,cspd4,cdir4,cspd5,cdir5,cspd6,cdir6,cspd7,cdir7,cspd8,cdir8,cspd9,cdir9,cspd10,cdir10,cspd11,cdir11,cspd12,cdir12,cspd13,cdir13,cspd14,cdir14,cspd15,cdir15,cspd16,cdir16,cspd17,cdir17,cspd18,cdir18,cspd19,cdir19,cspd20,cdir20):

    cspd1flag,cdir1flag,cspd2flag,cdir2flag,cspd3flag,cdir3flag,cspd4flag,cdir4flag=[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch)
    cspd5flag,cdir5flag,cspd6flag,cdir6flag,cspd7flag,cdir7flag,cspd8flag,cdir8flag=[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch)
    cspd9flag,cdir9flag,cspd10flag,cdir10flag,cspd11flag,cdir11flag,cspd12flag,cdir12flag=[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch)
    cspd13flag,cdir13flag,cspd14flag,cdir14flag,cspd15flag,cdir15flag,cspd16flag,cdir16flag=[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch)
    cspd17flag,cdir17flag,cspd18flag,cdir18flag,cspd19flag,cdir19flag,cspd20flag,cdir20flag=[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch)

    cspd1flagid,cdir1flagid,cspd2flagid,cdir2flagid,cspd3flagid,cdir3flagid,cspd4flagid,cdir4flagid=[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch)
    cspd5flagid,cdir5flagid,cspd6flagid,cdir6flagid,cspd7flagid,cdir7flagid,cspd8flagid,cdir8flagid=[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch)
    cspd9flagid,cdir9flagid,cspd10flagid,cdir10flagid,cspd11flagid,cdir11flagid,cspd12flagid,cdir12flagid=[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch)
    cspd13flagid,cdir13flagid,cspd14flagid,cdir14flagid,cspd15flagid,cdir15flagid,cspd16flagid,cdir16flagid=[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch)
    cspd17flagid,cdir17flagid,cspd18flagid,cdir18flagid,cspd19flagid,cdir19flagid,cspd20flagid,cdir20flagid=[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch),[0]*len(Epoch)


    (rswvht,rtp,rwvdir,rWspd,rWdir,rGust,rAtmp,rPres,rdewpt,rsst,rApd,rrh,rcspd,rcdir, sigmaswvht,sigmaPres,sigmaAtmp,sigmaWspd,sigmasst,sigmarh,misrh1,misrh2,miscspd,miscspd1,miscdir,misdewpt,misAtmp,missst,rmxwvht)=qc.valoresargos()

    (r1swvht,r1tp,r1Wspd,r1Gust,r1Atmp,r1Pres,r1dewpt,r1sst,r1Apd,r1cspd,r1mxwvht,r2Atmp,r3Atmp)=qc.valoresclima()
    ##############################################
    #RUN THE QC CHECKS
    ##############################################
    print('inicio testes')
    #Missing value check

    (cspd1flag,cspd1flagid)=qc.misvaluecheck(Epoch,cspd1,cspd1flag,-9999,cspd1flagid)
    (cdir1flag,cdir1flagid)=qc.misvaluecheck(Epoch,cdir1,cdir1flag,-9999,cdir1flagid)

    (cspd2flag,cspd2flagid)=qc.misvaluecheck(Epoch,cspd2,cspd2flag,-9999,cspd2flagid)
    (cdir2flag,cdir2flagid)=qc.misvaluecheck(Epoch,cdir2,cdir2flag,-9999,cdir2flagid)

    (cspd3flag,cspd3flagid)=qc.misvaluecheck(Epoch,cspd3,cspd3flag,-9999,cspd3flagid)
    (cdir3flag,cdir3flagid)=qc.misvaluecheck(Epoch,cdir3,cdir3flag,-9999,cdir3flagid)

    (cspd4flag,cspd4flagid)=qc.misvaluecheck(Epoch,cspd4,cspd4flag,-9999,cspd4flagid)
    (cdir4flag,cdir4flagid)=qc.misvaluecheck(Epoch,cdir4,cdir4flag,-9999,cdir4flagid)

    (cspd5flag,cspd5flagid)=qc.misvaluecheck(Epoch,cspd5,cspd5flag,-9999,cspd5flagid)
    (cdir5flag,cdir5flagid)=qc.misvaluecheck(Epoch,cdir5,cdir5flag,-9999,cdir5flagid)

    (cspd6flag,cspd6flagid)=qc.misvaluecheck(Epoch,cspd6,cspd6flag,-9999,cspd6flagid)
    (cdir6flag,cdir6flagid)=qc.misvaluecheck(Epoch,cdir6,cdir6flag,-9999,cdir6flagid)

    (cspd7flag,cspd7flagid)=qc.misvaluecheck(Epoch,cspd7,cspd7flag,-9999,cspd7flagid)
    (cdir7flag,cdir7flagid)=qc.misvaluecheck(Epoch,cdir7,cdir7flag,-9999,cdir7flagid)

    (cspd8flag,cspd8flagid)=qc.misvaluecheck(Epoch,cspd8,cspd8flag,-9999,cspd8flagid)
    (cdir8flag,cdir8flagid)=qc.misvaluecheck(Epoch,cdir8,cdir8flag,-9999,cdir8flagid)

    (cspd9flag,cspd9flagid)=qc.misvaluecheck(Epoch,cspd9,cspd9flag,-9999,cspd9flagid)
    (cdir9flag,cdir9flagid)=qc.misvaluecheck(Epoch,cdir9,cdir9flag,-9999,cdir9flagid)

    (cspd10flag,cspd10flagid)=qc.misvaluecheck(Epoch,cspd10,cspd10flag,-9999,cspd10flagid)
    (cdir10flag,cdir10flagid)=qc.misvaluecheck(Epoch,cdir10,cdir10flag,-9999,cdir10flagid)

    (cspd11flag,cspd11flagid)=qc.misvaluecheck(Epoch,cspd11,cspd11flag,-9999,cspd11flagid)
    (cdir11flag,cdir11flagid)=qc.misvaluecheck(Epoch,cdir11,cdir11flag,-9999,cdir11flagid)

    (cspd12flag,cspd12flagid)=qc.misvaluecheck(Epoch,cspd12,cspd12flag,-9999,cspd12flagid)
    (cdir12flag,cdir12flagid)=qc.misvaluecheck(Epoch,cdir12,cdir12flag,-9999,cdir12flagid)

    (cspd13flag,cspd13flagid)=qc.misvaluecheck(Epoch,cspd13,cspd13flag,-9999,cspd13flagid)
    (cdir13flag,cdir13flagid)=qc.misvaluecheck(Epoch,cdir13,cdir13flag,-9999,cdir13flagid)

    (cspd14flag,cspd14flagid)=qc.misvaluecheck(Epoch,cspd14,cspd14flag,-9999,cspd14flagid)
    (cdir14flag,cdir14flagid)=qc.misvaluecheck(Epoch,cdir14,cdir14flag,-9999,cdir14flagid)

    (cspd15flag,cspd15flagid)=qc.misvaluecheck(Epoch,cspd15,cspd15flag,-9999,cspd15flagid)
    (cdir15flag,cdir15flagid)=qc.misvaluecheck(Epoch,cdir15,cdir15flag,-9999,cdir15flagid)

    (cspd16flag,cspd16flagid)=qc.misvaluecheck(Epoch,cspd16,cspd16flag,-9999,cspd16flagid)
    (cdir16flag,cdir16flagid)=qc.misvaluecheck(Epoch,cdir16,cdir16flag,-9999,cdir16flagid)

    (cspd17flag,cspd17flagid)=qc.misvaluecheck(Epoch,cspd17,cspd17flag,-9999,cspd17flagid)
    (cdir17flag,cdir17flagid)=qc.misvaluecheck(Epoch,cdir17,cdir17flag,-9999,cdir17flagid)

    (cspd18flag,cspd18flagid)=qc.misvaluecheck(Epoch,cspd18,cspd18flag,-9999,cspd18flagid)
    (cdir18flag,cdir18flagid)=qc.misvaluecheck(Epoch,cdir18,cdir18flag,-9999,cdir18flagid)

    (cspd19flag,cspd19flagid)=qc.misvaluecheck(Epoch,cspd19,cspd19flag,-9999,cspd19flagid)
    (cdir19flag,cdir19flagid)=qc.misvaluecheck(Epoch,cdir19,cdir19flag,-9999,cdir19flagid)

    (cspd20flag,cspd20flagid)=qc.misvaluecheck(Epoch,cspd20,cspd20flag,-9999,cspd20flagid)
    (cdir20flag,cdir20flagid)=qc.misvaluecheck(Epoch,cdir20,cdir20flag,-9999,cdir20flagid)

    #Coarse Range check
    (cspd1flag,cspd1flagid) = qc.rangecheck(Epoch, cspd1,[-5000,5000],cspd1flag,cspd1flagid)
    (cdir1flag,cdir1flagid) = qc.rangecheck(Epoch, cdir1,[0,360],cdir1flag,cdir1flagid)

    (cspd2flag,cspd2flagid) = qc.rangecheck(Epoch, cspd2,[-5000,5000],cspd2flag,cspd2flagid)
    (cdir2flag,cdir2flagid) = qc.rangecheck(Epoch, cdir2,[0,360],cdir2flag,cdir2flagid)

    (cspd3flag,cspd3flagid) = qc.rangecheck(Epoch, cspd3,[-5000,5000],cspd3flag,cspd3flagid)
    (cdir3flag,cdir3flagid) = qc.rangecheck(Epoch, cdir3,[0,360],cdir3flag,cdir3flagid)

    (cspd4flag,cspd4flagid) = qc.rangecheck(Epoch, cspd4,[-5000,5000],cspd4flag,cspd4flagid)
    (cdir4flag,cdir4flagid) = qc.rangecheck(Epoch, cdir4,[0,360],cdir4flag,cdir4flagid)

    (cspd5flag,cspd5flagid) = qc.rangecheck(Epoch, cspd5,[-5000,5000],cspd5flag,cspd5flagid)
    (cdir5flag,cdir5flagid) = qc.rangecheck(Epoch, cdir5,[0,360],cdir5flag,cdir5flagid)

    (cspd6flag,cspd6flagid) = qc.rangecheck(Epoch, cspd6,[-5000,5000],cspd6flag,cspd6flagid)
    (cdir6flag,cdir6flagid) = qc.rangecheck(Epoch, cdir6,[0,360],cdir6flag,cdir6flagid)

    (cspd7flag,cspd7flagid) = qc.rangecheck(Epoch, cspd7,[-5000,5000],cspd7flag,cspd7flagid)
    (cdir7flag,cdir7flagid) = qc.rangecheck(Epoch, cdir7,[0,360],cdir7flag,cdir7flagid)

    (cspd8flag,cspd8flagid) = qc.rangecheck(Epoch, cspd8,[-5000,5000],cspd8flag,cspd8flagid)
    (cdir8flag,cdir8flagid) = qc.rangecheck(Epoch, cdir8,[0,360],cdir8flag,cdir8flagid)

    (cspd9flag,cspd9flagid) = qc.rangecheck(Epoch, cspd9,[-5000,5000],cspd9flag,cspd9flagid)
    (cdir9flag,cdir9flagid) = qc.rangecheck(Epoch, cdir9,[0,360],cdir9flag,cdir9flagid)

    (cspd10flag,cspd10flagid) = qc.rangecheck(Epoch, cspd10,[-5000,5000],cspd10flag,cspd10flagid)
    (cdir10flag,cdir10flagid) = qc.rangecheck(Epoch, cdir10,[0,360],cdir10flag,cdir10flagid)

    (cspd11flag,cspd11flagid) = qc.rangecheck(Epoch, cspd11,[-5000,5000],cspd11flag,cspd11flagid)
    (cdir11flag,cdir11flagid) = qc.rangecheck(Epoch, cdir11,[0,360],cdir11flag,cdir11flagid)

    (cspd12flag,cspd12flagid) = qc.rangecheck(Epoch, cspd12,[-5000,5000],cspd12flag,cspd12flagid)
    (cdir12flag,cdir12flagid) = qc.rangecheck(Epoch, cdir12,[0,360],cdir12flag,cdir12flagid)

    (cspd13flag,cspd13flagid) = qc.rangecheck(Epoch, cspd13,[-5000,5000],cspd13flag,cspd13flagid)
    (cdir13flag,cdir13flagid) = qc.rangecheck(Epoch, cdir13,[0,360],cdir13flag,cdir13flagid)

    (cspd14flag,cspd14flagid) = qc.rangecheck(Epoch, cspd14,[-5000,5000],cspd14flag,cspd14flagid)
    (cdir14flag,cdir14flagid) = qc.rangecheck(Epoch, cdir14,[0,360],cdir14flag,cdir14flagid)

    (cspd15flag,cspd15flagid) = qc.rangecheck(Epoch, cspd15,[-5000,5000],cspd15flag,cspd15flagid)
    (cdir15flag,cdir15flagid) = qc.rangecheck(Epoch, cdir15,[0,360],cdir15flag,cdir15flagid)

    (cspd16flag,cspd16flagid) = qc.rangecheck(Epoch, cspd16,[-5000,5000],cspd16flag,cspd16flagid)
    (cdir16flag,cdir16flagid) = qc.rangecheck(Epoch, cdir16,[0,360],cdir16flag,cdir16flagid)

    (cspd17flag,cspd17flagid) = qc.rangecheck(Epoch, cspd17,[-5000,5000],cspd17flag,cspd17flagid)
    (cdir17flag,cdir17flagid) = qc.rangecheck(Epoch, cdir17,[0,360],cdir17flag,cdir17flagid)

    (cspd18flag,cspd18flagid) = qc.rangecheck(Epoch, cspd18,[-5000,5000],cspd18flag,cspd18flagid)
    (cdir18flag,cdir18flagid) = qc.rangecheck(Epoch, cdir18,[0,360],cdir18flag,cdir18flagid)

    (cspd19flag,cspd19flagid) = qc.rangecheck(Epoch, cspd19,[-5000,5000],cspd19flag,cspd19flagid)
    (cdir19flag,cdir19flagid) = qc.rangecheck(Epoch, cdir19,[0,360],cdir19flag,cdir19flagid)

    (cspd20flag,cspd20flagid) = qc.rangecheck(Epoch, cspd20,[-5000,5000],cspd20flag,cspd20flagid)
    (cdir20flag,cdir20flagid) = qc.rangecheck(Epoch, cdir20,[0,360],cdir20flag,cdir20flagid)




    #Soft Range check
    (cspd1flag,cspd1flagid) = qc.rangecheckclima(Epoch, cspd1,[-1500,1500],cspd1flag,cspd1flagid)

    (cspd2flag,cspd2flagid) = qc.rangecheckclima(Epoch, cspd2,[-1500,1500],cspd2flag,cspd2flagid)

    (cspd3flag,cspd3flagid) = qc.rangecheckclima(Epoch, cspd3,[-1500,1500],cspd3flag,cspd3flagid)

    (cspd4flag,cspd4flagid) = qc.rangecheckclima(Epoch, cspd4,[-1500,1500],cspd4flag,cspd4flagid)

    (cspd5flag,cspd5flagid) = qc.rangecheckclima(Epoch, cspd5,[-1500,1500],cspd5flag,cspd5flagid)

    (cspd6flag,cspd6flagid) = qc.rangecheckclima(Epoch, cspd6,[-1500,1500],cspd6flag,cspd6flagid)

    (cspd7flag,cspd7flagid) = qc.rangecheckclima(Epoch, cspd7,[-1500,1500],cspd7flag,cspd7flagid)

    (cspd8flag,cspd8flagid) = qc.rangecheckclima(Epoch, cspd8,[-1500,1500],cspd8flag,cspd8flagid)

    (cspd9flag,cspd9flagid) = qc.rangecheckclima(Epoch, cspd9,[-1500,1500],cspd9flag,cspd9flagid)

    (cspd10flag,cspd10flagid) = qc.rangecheckclima(Epoch, cspd10,[-1500,1500],cspd10flag,cspd10flagid)

    (cspd11flag,cspd11flagid) = qc.rangecheckclima(Epoch, cspd11,[-1500,1500],cspd11flag,cspd11flagid)

    (cspd12flag,cspd12flagid) = qc.rangecheckclima(Epoch, cspd12,[-1500,1500],cspd12flag,cspd12flagid)

    (cspd13flag,cspd13flagid) = qc.rangecheckclima(Epoch, cspd13,[-1500,1500],cspd13flag,cspd13flagid)

    (cspd14flag,cspd14flagid) = qc.rangecheckclima(Epoch, cspd14,[-1500,1500],cspd14flag,cspd14flagid)

    (cspd15flag,cspd15flagid) = qc.rangecheckclima(Epoch, cspd15,[-1500,1500],cspd15flag,cspd15flagid)

    (cspd16flag,cspd16flagid) = qc.rangecheckclima(Epoch, cspd16,[-1500,1500],cspd16flag,cspd16flagid)

    (cspd17flag,cspd17flagid) = qc.rangecheckclima(Epoch, cspd17,[-1500,1500],cspd17flag,cspd17flagid)

    (cspd18flag,cspd18flagid) = qc.rangecheckclima(Epoch, cspd18,[-1500,1500],cspd18flag,cspd18flagid)

    (cspd19flag,cspd19flagid) = qc.rangecheckclima(Epoch, cspd19,[-1500,1500],cspd19flag,cspd19flagid)

    (cspd20flag,cspd20flagid) = qc.rangecheckclima(Epoch, cspd20,[-1500,1500],cspd20flag,cspd20flagid)

    #Stucksensorcheck

    (cspd1flag,cspd1flagid) = qc.stucksensorcheck(Epoch, cspd1,cspd1flag,12,cspd1flagid)

    (cspd2flag,cspd2flagid) = qc.stucksensorcheck(Epoch, cspd2,cspd2flag,12,cspd2flagid)

    (cspd3flag,cspd3flagid) = qc.stucksensorcheck(Epoch, cspd3,cspd3flag,12,cspd3flagid)

    (cspd4flag,cspd4flagid) = qc.stucksensorcheck(Epoch, cspd4,cspd4flag,12,cspd4flagid)

    (cspd5flag,cspd5flagid) = qc.stucksensorcheck(Epoch, cspd5,cspd5flag,12,cspd5flagid)

    (cspd6flag,cspd6flagid) = qc.stucksensorcheck(Epoch, cspd6,cspd6flag,12,cspd6flagid)

    (cspd7flag,cspd7flagid) = qc.stucksensorcheck(Epoch, cspd7,cspd7flag,12,cspd7flagid)

    (cspd8flag,cspd8flagid) = qc.stucksensorcheck(Epoch, cspd8,cspd8flag,12,cspd8flagid)

    (cspd9flag,cspd9flagid) = qc.stucksensorcheck(Epoch, cspd9,cspd9flag,12,cspd9flagid)

    (cspd10flag,cspd10flagid) = qc.stucksensorcheck(Epoch, cspd10,cspd10flag,12,cspd10flagid)

    (cspd11flag,cspd11flagid) = qc.stucksensorcheck(Epoch, cspd11,cspd11flag,12,cspd11flagid)

    (cspd12flag,cspd12flagid) = qc.stucksensorcheck(Epoch, cspd12,cspd12flag,12,cspd12flagid)

    (cspd13flag,cspd13flagid) = qc.stucksensorcheck(Epoch, cspd13,cspd13flag,12,cspd13flagid)

    (cspd14flag,cspd14flagid) = qc.stucksensorcheck(Epoch, cspd14,cspd14flag,12,cspd14flagid)

    (cspd15flag,cspd15flagid) = qc.stucksensorcheck(Epoch, cspd15,cspd15flag,12,cspd15flagid)

    (cspd16flag,cspd16flagid) = qc.stucksensorcheck(Epoch, cspd16,cspd16flag,12,cspd16flagid)

    (cspd17flag,cspd17flagid) = qc.stucksensorcheck(Epoch, cspd17,cspd17flag,12,cspd17flagid)

    (cspd18flag,cspd18flagid) = qc.stucksensorcheck(Epoch, cspd18,cspd18flag,12,cspd18flagid)

    (cspd19flag,cspd19flagid) = qc.stucksensorcheck(Epoch, cspd19,cspd19flag,12,cspd19flagid)

    (cspd20flag,cspd20flagid) = qc.stucksensorcheck(Epoch, cspd20,cspd20flag,12,cspd20flagid)

    u1,v1=[],[]
    u2,v2=[],[]
    u3,v3=[],[]
    u4,v4=[],[]
    u5,v5=[],[]
    u6,v6=[],[]
    u7,v7=[],[]
    u8,v8=[],[]
    u9,v9=[],[]
    u10,v10=[],[]
    u11,v11=[],[]
    u12,v12=[],[]
    u13,v13=[],[]
    u14,v14=[],[]
    u15,v15=[],[]
    u16,v16=[],[]
    u17,v17=[],[]
    u18,v18=[],[]
    u19,v19=[],[]
    u20,v20=[],[]

    for i in range(len(cspd1)):
        (uu,vv)=intdir2uv(cspd1[i], cdir1[i])
        u1.append(uu)
        v1.append(vv)

        (uu,vv)=intdir2uv(cspd2[i], cdir2[i])
        u2.append(uu)
        v2.append(vv)

        (uu,vv)=intdir2uv(cspd3[i], cdir3[i])
        u3.append(uu)
        v3.append(vv)

        (uu,vv)=intdir2uv(cspd4[i], cdir4[i])
        u4.append(uu)
        v4.append(vv)

        (uu,vv)=intdir2uv(cspd5[i], cdir5[i])
        u5.append(uu)
        v5.append(vv)

        (uu,vv)=intdir2uv(cspd6[i], cdir6[i])
        u6.append(uu)
        v6.append(vv)

        (uu,vv)=intdir2uv(cspd7[i], cdir7[i])
        u7.append(uu)
        v7.append(vv)

        (uu,vv)=intdir2uv(cspd8[i], cdir8[i])
        u8.append(uu)
        v8.append(vv)

        (uu,vv)=intdir2uv(cspd9[i], cdir9[i])
        u9.append(uu)
        v9.append(vv)

        (uu,vv)=intdir2uv(cspd10[i], cdir10[i])
        u10.append(uu)
        v10.append(vv)

        (uu,vv)=intdir2uv(cspd11[i], cdir11[i])
        u11.append(uu)
        v11.append(vv)

        (uu,vv)=intdir2uv(cspd12[i], cdir12[i])
        u12.append(uu)
        v12.append(vv)

        (uu,vv)=intdir2uv(cspd13[i], cdir13[i])
        u13.append(uu)
        v13.append(vv)

        (uu,vv)=intdir2uv(cspd14[i], cdir14[i])
        u14.append(uu)
        v14.append(vv)

        (uu,vv)=intdir2uv(cspd15[i], cdir15[i])
        u15.append(uu)
        v15.append(vv)

        (uu,vv)=intdir2uv(cspd16[i], cdir16[i])
        u16.append(uu)
        v16.append(vv)

        (uu,vv)=intdir2uv(cspd17[i], cdir17[i])
        u17.append(uu)
        v17.append(vv)

        (uu,vv)=intdir2uv(cspd18[i], cdir18[i])
        u18.append(uu)
        v18.append(vv)

        (uu,vv)=intdir2uv(cspd19[i], cdir19[i])
        u19.append(uu)
        v19.append(vv)

        (uu,vv)=intdir2uv(cspd20[i], cdir20[i])
        u20.append(uu)
        v20.append(vv)


    #Time continuity check

    (cspd1flag,cspd1flagid,cdir1flag,cdir1flagid) = qc.tcontinuityadcpcheck(Epoch, u1,cspd1flag,cspd1flagid,cdir1flag,cdir1flagid)
    (cspd1flag,cspd1flagid,cdir1flag,cdir1flagid) = qc.tcontinuityadcpcheck(Epoch, v1,cspd1flag,cspd1flagid,cdir1flag,cdir1flagid)

    (cspd2flag,cspd2flagid,cdir2flag,cdir2flagid) = qc.tcontinuityadcpcheck(Epoch, u2,cspd2flag,cspd2flagid,cdir2flag,cdir2flagid)
    (cspd2flag,cspd2flagid,cdir2flag,cdir2flagid) = qc.tcontinuityadcpcheck(Epoch, v2,cspd2flag,cspd2flagid,cdir2flag,cdir2flagid)

    (cspd3flag,cspd3flagid,cdir3flag,cdir3flagid) = qc.tcontinuityadcpcheck(Epoch, u3,cspd3flag,cspd3flagid,cdir3flag,cdir3flagid)
    (cspd3flag,cspd3flagid,cdir3flag,cdir3flagid) = qc.tcontinuityadcpcheck(Epoch, v3,cspd3flag,cspd3flagid,cdir3flag,cdir3flagid)

    (cspd4flag,cspd4flagid,cdir4flag,cdir4flagid) = qc.tcontinuityadcpcheck(Epoch, u4,cspd4flag,cspd4flagid,cdir4flag,cdir4flagid)
    (cspd4flag,cspd4flagid,cdir4flag,cdir4flagid) = qc.tcontinuityadcpcheck(Epoch, v4,cspd4flag,cspd4flagid,cdir4flag,cdir4flagid)

    (cspd5flag,cspd5flagid,cdir5flag,cdir5flagid) = qc.tcontinuityadcpcheck(Epoch, u5,cspd5flag,cspd5flagid,cdir5flag,cdir5flagid)
    (cspd5flag,cspd5flagid,cdir5flag,cdir5flagid) = qc.tcontinuityadcpcheck(Epoch, v5,cspd5flag,cspd5flagid,cdir5flag,cdir5flagid)

    (cspd6flag,cspd6flagid,cdir6flag,cdir6flagid) = qc.tcontinuityadcpcheck(Epoch, u6,cspd6flag,cspd6flagid,cdir6flag,cdir6flagid)
    (cspd6flag,cspd6flagid,cdir6flag,cdir6flagid) = qc.tcontinuityadcpcheck(Epoch, v6,cspd6flag,cspd6flagid,cdir6flag,cdir6flagid)

    (cspd7flag,cspd7flagid,cdir7flag,cdir7flagid) = qc.tcontinuityadcpcheck(Epoch, u7,cspd7flag,cspd7flagid,cdir7flag,cdir7flagid)
    (cspd7flag,cspd7flagid,cdir7flag,cdir7flagid) = qc.tcontinuityadcpcheck(Epoch, v7,cspd7flag,cspd7flagid,cdir7flag,cdir7flagid)

    (cspd8flag,cspd8flagid,cdir8flag,cdir8flagid) = qc.tcontinuityadcpcheck(Epoch, u8,cspd8flag,cspd8flagid,cdir8flag,cdir8flagid)
    (cspd8flag,cspd8flagid,cdir8flag,cdir8flagid) = qc.tcontinuityadcpcheck(Epoch, v8,cspd8flag,cspd8flagid,cdir8flag,cdir8flagid)

    (cspd9flag,cspd9flagid,cdir9flag,cdir9flagid) = qc.tcontinuityadcpcheck(Epoch, u9,cspd9flag,cspd9flagid,cdir9flag,cdir9flagid)
    (cspd9flag,cspd9flagid,cdir9flag,cdir9flagid) = qc.tcontinuityadcpcheck(Epoch, v9,cspd9flag,cspd9flagid,cdir9flag,cdir9flagid)

    (cspd10flag,cspd10flagid,cdir10flag,cdir10flagid) = qc.tcontinuityadcpcheck(Epoch, u10,cspd10flag,cspd10flagid,cdir10flag,cdir10flagid)
    (cspd10flag,cspd10flagid,cdir10flag,cdir10flagid) = qc.tcontinuityadcpcheck(Epoch, v10,cspd10flag,cspd10flagid,cdir10flag,cdir10flagid)

    (cspd11flag,cspd11flagid,cdir11flag,cdir11flagid) = qc.tcontinuityadcpcheck(Epoch, u11,cspd11flag,cspd11flagid,cdir11flag,cdir11flagid)
    (cspd11flag,cspd11flagid,cdir11flag,cdir11flagid) = qc.tcontinuityadcpcheck(Epoch, v11,cspd11flag,cspd11flagid,cdir11flag,cdir11flagid)

    (cspd12flag,cspd12flagid,cdir12flag,cdir12flagid) = qc.tcontinuityadcpcheck(Epoch, u12,cspd12flag,cspd12flagid,cdir12flag,cdir12flagid)
    (cspd12flag,cspd12flagid,cdir12flag,cdir12flagid) = qc.tcontinuityadcpcheck(Epoch, v12,cspd12flag,cspd12flagid,cdir12flag,cdir12flagid)

    (cspd13flag,cspd13flagid,cdir13flag,cdir13flagid) = qc.tcontinuityadcpcheck(Epoch, u13,cspd13flag,cspd13flagid,cdir13flag,cdir13flagid)
    (cspd13flag,cspd13flagid,cdir13flag,cdir13flagid) = qc.tcontinuityadcpcheck(Epoch, v13,cspd13flag,cspd13flagid,cdir13flag,cdir13flagid)

    (cspd14flag,cspd14flagid,cdir14flag,cdir14flagid) = qc.tcontinuityadcpcheck(Epoch, u14,cspd14flag,cspd14flagid,cdir14flag,cdir14flagid)
    (cspd14flag,cspd14flagid,cdir14flag,cdir14flagid) = qc.tcontinuityadcpcheck(Epoch, v14,cspd14flag,cspd14flagid,cdir14flag,cdir14flagid)

    (cspd15flag,cspd15flagid,cdir15flag,cdir15flagid) = qc.tcontinuityadcpcheck(Epoch, u15,cspd15flag,cspd15flagid,cdir15flag,cdir15flagid)
    (cspd15flag,cspd15flagid,cdir15flag,cdir15flagid) = qc.tcontinuityadcpcheck(Epoch, v15,cspd15flag,cspd15flagid,cdir15flag,cdir15flagid)

    (cspd16flag,cspd16flagid,cdir16flag,cdir16flagid) = qc.tcontinuityadcpcheck(Epoch, u16,cspd16flag,cspd16flagid,cdir16flag,cdir16flagid)
    (cspd16flag,cspd16flagid,cdir16flag,cdir16flagid) = qc.tcontinuityadcpcheck(Epoch, v16,cspd16flag,cspd16flagid,cdir16flag,cdir16flagid)

    (cspd17flag,cspd17flagid,cdir17flag,cdir17flagid) = qc.tcontinuityadcpcheck(Epoch, u17,cspd17flag,cspd17flagid,cdir17flag,cdir17flagid)
    (cspd17flag,cspd17flagid,cdir17flag,cdir17flagid) = qc.tcontinuityadcpcheck(Epoch, v17,cspd17flag,cspd17flagid,cdir17flag,cdir17flagid)

    (cspd18flag,cspd18flagid,cdir18flag,cdir18flagid) = qc.tcontinuityadcpcheck(Epoch, u18,cspd18flag,cspd18flagid,cdir18flag,cdir18flagid)
    (cspd18flag,cspd18flagid,cdir18flag,cdir18flagid) = qc.tcontinuityadcpcheck(Epoch, v18,cspd18flag,cspd18flagid,cdir18flag,cdir18flagid)

    (cspd19flag,cspd19flagid,cdir19flag,cdir19flagid) = qc.tcontinuityadcpcheck(Epoch, u19,cspd19flag,cspd19flagid,cdir19flag,cdir19flagid)
    (cspd19flag,cspd19flagid,cdir19flag,cdir19flagid) = qc.tcontinuityadcpcheck(Epoch, v19,cspd19flag,cspd19flagid,cdir19flag,cdir19flagid)

    (cspd20flag,cspd20flagid,cdir20flag,cdir20flagid) = qc.tcontinuityadcpcheck(Epoch, u20,cspd20flag,cspd20flagid,cdir20flag,cdir20flagid)
    (cspd20flag,cspd20flagid,cdir20flag,cdir20flagid) = qc.tcontinuityadcpcheck(Epoch, v20,cspd20flag,cspd20flagid,cdir20flag,cdir20flagid)

    flag=np.array([cspd1flag,cdir1flag,cspd2flag,cdir2flag,cspd3flag,cdir3flag,cspd4flag,cdir4flag,cspd5flag,cdir5flag,cspd6flag,cdir6flag,cspd7flag,cdir7flag,cspd8flag,cdir8flag,cspd9flag,cdir9flag,cspd10flag,cdir10flag,cspd11flag,cdir11flag,cspd12flag,cdir12flag,cspd13flag,cdir13flag,cspd14flag,cdir14flag,cspd15flag,cdir15flag,cspd16flag,cdir16flag,cspd17flag,cdir17flag,cspd18flag,cdir18flag,cspd19flag,cdir19flag,cspd20flag,cdir20flag])
    flagid=np.array([cspd1flagid,cdir1flagid,cspd2flagid,cdir2flagid,cspd3flagid,cdir3flagid,cspd4flagid,cdir4flagid,cspd5flagid,cdir5flagid,cspd6flagid,cdir6flagid,cspd7flagid,cdir7flagid,cspd8flagid,cdir8flagid,cspd9flagid,cdir9flagid,cspd10flagid,cdir10flagid,cspd11flagid,cdir11flagid,cspd12flagid,cdir12flagid,cspd13flagid,cdir13flagid,cspd14flagid,cdir14flagid,cspd15flagid,cdir15flagid,cspd16flagid,cdir16flagid,cspd17flagid,cdir17flagid,cspd18flagid,cdir18flagid,cspd19flagid,cdir19flagid,cspd20flagid,cdir20flagid])

    return flag,flagid, u1,v1

#    for i in range(len(cspd1)):
#        if cspd1flag[i]==4 or cdir1flag[i]==4 or cspd2flag[i]==4 or cdir2flag[i]==4 or cspd3flag[i]==4 or cdir3flag[i]==4 or cspd4flag[i]==4 or cdir4flag[i]==4 or cspd5flag[i]==4 or cdir5flag[i]==4 or cspd6flag[i]==4 or cdir6flag[i]==4 or cspd7flag[i]==4 or cdir7flag[i]==4 or cspd8flag[i]==4 or cdir8flag[i]==4 or cspd9flag[i]==4 or cdir9flag[i]==4 or cspd10flag[i]==4 or cdir10flag[i]==4 or cspd11flag[i]==4 or cdir11flag[i]==4 or cspd12flag[i]==4 or cdir12flag[i]==4 or cspd13flag[i]==4 or cdir13flag[i]==4 or cspd14flag[i]==4 or cdir14flag[i]==4 or cspd15flag[i]==4 or cdir15flag[i]==4 or cspd16flag[i]==4 or cdir16flag[i]==4 or cspd17flag[i]==4 or cdir17flag[i]==4 or cspd18flag[i]==4 or cdir18flag[i]==4 or cspd19flag[i]==4 or cdir19flag[i]==4 or cspd20flag[i]==4 or cdir20flag[i]==4:
#            if cdir1flag[i]!=4:
#                cdir1flagid[i]='60'
#            if cspd1flag[i]!=4:
#                cspd1flagid[i]='60'
#            if cspd2flag[i]!=4:
#                cspd2flagid[i]='60'
#            if cdir2flag[i]!=4:
#                cdir2flagid[i]='60'
#            if cspd3flag[i]!=4:
#                cspd3flagid[i]='60'
#            if cdir3flag[i]!=4:
#                cdir3flagid[i]='60'
#            if cspd4flag[i]!=4:
#                cspd4flagid[i]='60'
#            if cdir4flag[i]!=4:
#                cdir4flagid[i]='60'
#            if cspd5flag[i]!=4:
#                cspd5flagid[i]='60'
#            if cdir5flag[i]!=4:
#                cdir5flagid[i]='60'
#            if cspd6flag[i]!=4:
#                cspd6flagid[i]='60'
#            if cdir6flag[i]!=4:
#                cdir6flagid[i]='60'
#            if cspd7flag[i]!=4:
#                cspd7flagid[i]='60'
#            if cdir7flag[i]!=4:
#                cdir7flagid[i]='60'
#            if cspd8flag[i]!=4:
#                cspd8flagid[i]='60'
#            if cdir8flag[i]!=4:
#                cdir8flagid[i]='60'
#            if cspd9flag[i]!=4:
#                cspd9flagid[i]='60'
#            if cdir9flag[i]!=4:
#                cdir9flagid[i]='60'
#            if cspd10flag[i]!=4:
#                cspd10flagid[i]='60'
#            if cdir10flag[i]!=4:
#                cdir10flagid[i]='60'
#            if cspd11flag[i]!=4:
#                cspd11flagid[i]='60'
#            if cdir11flag[i]!=4:
#                cdir11flagid[i]='60'
#            if cspd12flag[i]!=4:
#                cspd12flagid[i]='60'
#            if cdir12flag[i]!=4:
#                cdir12flagid[i]='60'
#            if cspd13flag[i]!=4:
#                cspd13flagid[i]='60'
#            if cdir13flag[i]!=4:
#                cdir13flagid[i]='60'
#            if cspd14flag[i]!=4:
#                cspd14flagid[i]='60'
#            if cdir14flag[i]!=4:
#                cdir14flagid[i]='60'
#            if cspd15flag[i]!=4:
#                cspd15flagid[i]='60'
#            if cdir15flag[i]!=4:
#                cdir15flagid[i]='60'
#            if cspd16flag[i]!=4:
#                cspd16flagid[i]='60'
#            if cdir16flag[i]!=4:
#                cdir16flagid[i]='60'
#            if cspd17flag[i]!=4:
#                cspd17flagid[i]='60'
#            if cdir17flag[i]!=4:
#                cdir17flagid[i]='60'
#            if cspd18flag[i]!=4:
#                cspd18flagid[i]='60'
#            if cdir18flag[i]!=4:
#                cdir18flagid[i]='60'
#            if cspd19flag[i]!=4:
#                cspd19flagid[i]='60'
#            if cdir19flag[i]!=4:
#                cdir19flagid[i]='60'
#            if cspd20flag[i]!=4:
#                cspd20flagid[i]='60'
#            if cdir20flag[i]!=4:
#                cdir20flagid[i]='60'


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
