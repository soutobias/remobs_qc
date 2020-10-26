"""
Spyder Editor

Python Version tested: Python 2.7.3

Required Dependencies: Numpy, time

Description: This module is designed to take Standard Meteorogical Data from
NDBC website in form of time, Wdir, Wspd, Gst, Wvht, Dpd, Apd, Mwd, Pres, Atmp,
Wtmp, Dewp, Vis and Tide, and perform QC checks based on NDBC's "Handbook of
Automated Data Quality Control Checks and Procedures". Each check generates a QC
table for the archive used, following the QC Numbering.
5 indicates that the time listed is not valid and is in the future.  

Required Input: EPOCH Time, Wvht(m), Dpd(s), Apd(s), Mwd(degrees)

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
Date: May 03th 2014  
Version: 1.1 
    
"""

#TODO: Put your name and date on here. 
#OK

#TODO: How are you handling missing or none values? Is that handled in the data read section? Might cause problems.
# I created a check that flags the missing values (MISVALUECHECK)

import numpy as np
import time
import math


def valores():
    
    rWvht=[0,19.9]
    rDpd=[1.7,30]
    rMwd=[0,360]
    rWspd=[0,59]
    rWdir=[0,360]
    rGust=[0,59]
    rAtmp=[-39,59]
    rPres=[501,1099]
    rDewp=[-29,39]
    rWtmp=[-3,29]
    rApd=[1.7,30]
    
    sigmaWvht=6
    sigmaPres=21
    sigmaAtmp=11
    sigmaWspd=25
    sigmaWtmp=8.6
    
    return rWvht,rDpd,rMwd,rWspd,rWdir,rGust,rAtmp,rPres,rDewp,rWtmp,rApd,sigmaWvht,sigmaPres,sigmaAtmp,sigmaWspd,sigmaWtmp

def valoresargos():

    rWvht=[0,19.9]
    rDpd=[1.7,30]
    rMwd=[0,360]
    rWspd=[0,59]
    rWdir=[0,360]
    rGust=[0,59]
    rAtmp=[-39,59]
    rPres=[501,1099]
    rDewp=[-29,39]
    rWtmp=[-3,29]
    rHumi=[25,102]
    rCvel=[-499,499]
    rCdir=[0,360]
    rApd=[1.7,30]
    
    sigmaWvht=6
    sigmaHumi=20
    sigmaPres=21
    sigmaAtmp=11
    sigmaWspd=25
    sigmaWtmp=8.6
    
    misHumi1=11
    misHumi2=12
    misCvel=409.5
    misCdir=511
    misDewp=-10
    misAtmp=-10
    misWtmp=40.95

    return rWvht,rDpd,rMwd,rWspd,rWdir,rGust,rAtmp,rPres,rDewp,rWtmp,rApd,rHumi,rCvel,rCdir, sigmaWvht,sigmaPres,sigmaAtmp,sigmaWspd,sigmaWtmp,sigmaHumi,misHumi1,misHumi2,misCvel,misCdir,misDewp,misAtmp,misWtmp

############################ Begin QC ################################


###############################################################################
# Valid Time Check 
# Check to see if time is in the future.
#
# Required input:
# - Epoch: Data/Time in Epoch date
#
# Required checks: None
#
# Return: timeflag
###############################################################################

def timecheck(Epoch):

    T_now = time.time() 
    timeflag=[0]*len(Epoch) 
    for i in xrange(len(Epoch)): 

#TODO: should add a safety factor to the future time check in case there is a small drift in time of a couple of minutes; 
      
        if Epoch[i] > T_now+600: #Add 10 minutes to the Time_now to avoid incorrect flag
           timeflag[i]=4
        else:
           continue

    return timeflag

    #####################
    #end Time check section

###############################################################################
# Missing value check 
# Flag the missing (MISVALUE) or None values
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - var: variable that will be checked
# - misvalue: missing value for the var
# - flag: matrix of flag for the variable
# - idf= flag id. 'F' --> letter that represents the flag
#
# Required checks: None
#
# Return: flag, idf
###############################################################################

def misvaluecheck(Epoch,var,flag,misvalue,idf):
    
    for i in xrange(len(Epoch)):
        if var[i]==misvalue or var[i]==None:
            flag[i]=4
            idf[i]='F'
        else:
            continue

    return flag,idf

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

def rangecheck(Epoch,var,limits,flag,idf):

 
    for i in xrange(len(Epoch)): 
        if flag[i]!=4:
            if var[i] > limits[1] or var[i] < limits[0]:
               flag[i] = 4
               idf[i]='L'
           
            else:
                continue

    return flag,idf

    #####################
    #end Range Check section


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
# - idfw= flag id for wind speed. 'V' --> letter that represents the flag
# - flagw: matrix of flag for wind speed
# - idfg= flag id for gust. 'V' --> letter that represents the flag
#
# Required checks: Range Check, Missing value check
#
# Return: flagw,flagg,idfw,idfg
###############################################################################

def windspeedgustcheck(Epoch,wind,gust,flagw,flagg,idfw,idfg):

    for i in xrange(len(Epoch)):
        if flagw[i]!=4 and flagg[i]!=4:
            if wind[i]>gust[i]:
                flagw[i]=4
                idfw[i]='V'
                flagg[i]=4
                idfg[i]='V'
            else:
                continue

    for i in xrange(len(Epoch)):
        if flagg[i]!=4:
            if gust[i]<0.5:
                flagg[i]=4
                idfg[i]='V'
            else:
                continue

    return flagw,flagg,idfw,idfg

    #####################
    #end windspeedgust check section


###############################################################################
# Dew point x Air temperature Check 
# Compares if the Dewpoint values is higher than Air temperature values.
# If so, dewp value will be changed to atmp value and data will be soft flagged
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - dewp: dew point
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

def dewpatmpcheck(Epoch,dewp,atmp,flagd,flaga,idf):

  
    for i in xrange(len(Epoch)):            
        if dewp[i]>atmp[i] and flagd[i]!=4 and flaga[i]!=4:
            dewp[i]=atmp[i]
            flagd[i]=3
            idf[i]='o'
        else:
            continue

    return flagd,idf

    #####################
    #end dewpoint atmp check section



###############################################################################
# Battery x Air Pressure Check 
# Pressure will be flagged if batterty is below of 10.5 V.
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - pres: air pressure
# - battery: battery voltage
# - flagp: matrix of flag for air pressure
# - idf= flag id. 'B' --> letter that represents the flag
#
# Required checks: Range Check, Missing value check
#
# Return: flagp,idf
#
###############################################################################

def batsensorcheck(Epoch,battery,pres,flagp,idf):

     
    for i in xrange(len(Epoch)):
        if battery[i]<=10.5 and battery[i]!=None:
            flagp[i]=4
            idf[i]='B'
        else:
            continue
            
    return flagp,idf

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
# - idf= flag id. 'C' --> letter that represents the flag
#
# Required checks: Range Check, Missing value check
#
# Return: flag,idf
#
########################################################################################
def stucksensorcheck (Epoch,var,flag,nev,idf):

    
    for i in xrange(nev-1,len(Epoch)):
        if flag[i]!=4:
            if (np.array(var[i]) == np.array(var[i-nev+1:i])).all():
                for ii in xrange(nev):               
                    flag[i-nev+ii+1]=4
                    idf[i-nev+ii+1]='C'
                    
                
            else:
                continue
                
    return flag,idf

    #####################
    #end stuck sensor check


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
# - Wdir1flagid: flag id for Wdir1. 'v' --> letter that represents the flag
# - Wspd1flagid: flag id for Wspd1. 'v' --> letter that represents the flag
# - Gust1flagid: flag id for Gust1. 'v' --> letter that represents the flag
# - Wdir2flagid: flag id for Wdir2. 'v' --> letter that represents the flag
# - Wspd2flagid: flag id for Wspd2. 'v' --> letter that represents the flag
# - Gust2flagid: flag id for Gust2. 'v' --> letter that represents the flag
#
#
# Required checks: Range, Missing value, Wind Speed x Gust
#
# Return: Wdirflag,Wspdflag,Gustflag,Wdir,Wspd,Gust,Wdirflagid,Wspdflagid,Gustflagid
########################################################################################

def relatedmeascheck(Epoch,Wdir1,Wspd1,Gust1,zwind1,Wdir2,Wspd2,Gust2,zwind2,Wspd1flag, Wdir1flag,Gust1flag,Wspd2flag, Wdir2flag,Gust2flag,Wdir1flagid,Wspd1flagid,Gust1flagid,Wdir2flagid,Wspd2flagid,Gust2flagid): 

    # Creating the variables for the best anemometer
    Wdir= [0]*len(Epoch)
    Wspd= [0]*len(Epoch)
    Gust= [0]*len(Epoch)
    
    # Creating the flags for the best anemometer
    Wspdflag= [0]*len(Epoch)   
    Gustflag= [0]*len(Epoch)   
    Wdirflag= [0]*len(Epoch)
    Wspdflagid= [0]*len(Epoch)   
    Gustflagid= [0]*len(Epoch)   
    Wdirflagid= [0]*len(Epoch)


    for i in xrange(len(Epoch)):
        if Wspd1flag[i]!=4:
#            Wspd1[i]=Wspd1[i]*((10/zwind1)**0.11)  #Check this for heights. should use 10/Zwspd1
#            Gust1[i]=Gust1[i]*((10/zwind1)**0.11)  #Check this for heights. should use 10/Zwspd1

            if Wspd2flag[i]!=4:
#                Wspd2[i]=Wspd2[i]*((10/zwind2)**0.11)  #Check this for heights. should use 10/Zwspd2
#                Gust2[i]=Gust2[i]*((10/zwind2)**0.11)  #Check this for heights. should use 10/Zwspd2

                if Wspd1flag[i-1]!=4 and Wspd2flag[i-1]!=4:
                    delta_Wdir= abs(Wdir1[i]-Wdir2[i])
                    delta_Wdir1=abs(Wdir1[i]-Wdir1[i-1])
                    delta_Wdir2=abs(Wdir2[i]-Wdir2[i-1])
        
                    if Wspd1[i] > 2.5 and Wspd2[i] > 2.5:
                        if delta_Wdir > 25:
                            if delta_Wdir1<=delta_Wdir2:
                                Wdir[i]=Wdir1[i]
                                Wspd[i]=Wspd1[i]
                                Gust[i]=Gust1[i]
                                Wspdflag[i]=Wspd1flag[i]
                                Gustflag[i]=Wdir1flag[i]
                                Gustflag[i]=Gust1flag[i]
                                Wspdflagid[i]=Wspd1flagid[i]
                                Gustflagid[i]=Wdir1flagid[i]
                                Gustflagid[i]=Gust1flagid[i]
                                
                            else:
                                Wdir[i]=Wdir2[i]
                                Wspd[i]=Wspd2[i]
                                Gust[i]=Gust2[i]
                                Wspdflag[i]=Wspd2flag[i]
                                Gustflag[i]=Wdir2flag[i]
                                Gustflag[i]=Gust2flag[i]
                                Wspdflagid[i]=Wspd2flagid[i]
                                Gustflagid[i]=Wdir2flagid[i]
                                Gustflagid[i]=Gust2flagid[i]                                
                                continue
                        else:
                            if Wspd1[i]>=Wspd2[i]:      #should be sensor hierchy.
                                Wdir[i]=Wdir1[i]
                                Wspd[i]=Wspd1[i]
                                Gust[i]=Gust1[i]
                                Wspdflagid[i]=Wspd1flagid[i]
                                Gustflagid[i]=Wdir1flagid[i]
                                Gustflagid[i]=Gust1flagid[i]
                           
                            else:
                                Wdir[i]=Wdir2[i]
                                Wspd[i]=Wspd2[i]
                                Gust[i]=Gust2[i]
                                Wspdflagid[i]=Wspd2flagid[i]
                                Gustflagid[i]=Wdir2flagid[i]
                                Gustflagid[i]=Gust2flagid[i]  
                                continue
                            continue
                    else:
                        if Wspd1[i]>=Wspd2[i]:                  #should be sensor hierchy.
                            Wdir[i]=Wdir1[i]
                            Wspd[i]=Wspd1[i]
                            Gust[i]=Gust1[i]
                            Wspdflagid[i]=Wspd1flagid[i]
                            Gustflagid[i]=Wdir1flagid[i]
                            Gustflagid[i]=Gust1flagid[i]

                        else:
                            Wdir[i]=Wdir2[i]
                            Wspd[i]=Wspd2[i]
                            Gust[i]=Gust2[i]
                            Wspdflagid[i]=Wspd2flagid[i]
                            Gustflagid[i]=Wdir2flagid[i]
                            Gustflagid[i]=Gust2flagid[i]  
                            continue                                
                        continue                                  
                else:
                    if Wspd1flag[i-1]!=4 and Wspd2flag[i-1]==4:
                        Wdir[i]=Wdir1[i]
                        Wspd[i]=Wspd1[i]
                        Gust[i]=Gust1[i]                        
                        Wspdflagid[i]=Wspd1flagid[i]
                        Gustflagid[i]=Wdir1flagid[i]
                        Gustflagid[i]=Gust1flagid[i]
                        
                    elif Wspd1flag[i-1]==4 and Wspd2flag[i-1]!=4:
                        Wdir[i]=Wdir2[i]
                        Wspd[i]=Wspd2[i]
                        Gust[i]=Gust2[i]
                        Wspdflagid[i]=Wspd2flagid[i]
                        Gustflagid[i]=Wdir2flagid[i]
                        Gustflagid[i]=Gust2flagid[i]  
                    else:
                        if Wspd1[i]>=Wspd2[i]:
                            Wdir[i]=Wdir1[i]
                            Wspd[i]=Wspd1[i]
                            Gust[i]=Gust1[i]                        
                            Wspdflagid[i]=Wspd1flagid[i]
                            Gustflagid[i]=Wdir1flagid[i]
                            Gustflagid[i]=Gust1flagid[i]
                            continue
                        else:
                            Wdir[i]=Wdir2[i]
                            Wspd[i]=Wspd2[i]
                            Gust[i]=Gust2[i]                        
                            Wspdflagid[i]=Wspd2flagid[i]
                            Gustflagid[i]=Wdir2flagid[i]
                            Gustflagid[i]=Gust2flagid[i]  
                            continue                           
                        continue
            else:
                Wdir[i]=Wdir1[i]
                Wspd[i]=Wspd1[i]
                Gust[i]=Gust1[i]
                Wspdflag[i]=Wspd1flag[i]
                Gustflag[i]=Wdir1flag[i]
                Gustflag[i]=Gust1flag[i]
                Wspdflagid[i]=Wspd1flagid[i]
                Gustflagid[i]=Wdir1flagid[i]
                Gustflagid[i]=Gust1flagid[i]
                continue
        else:                    
            if Wspd2flag[i]!=4:
#                Wspd2[i]=Wspd2[i]*((10/zwind2)**0.11)          #Check this for heights. should use 10/zwspd1
#                Gust2[i]=Gust2[i]*((10/zwind2)**0.11)          #Check this for heights. should use 10/zwspd1         
                Wdir[i]=Wdir2[i]
                Wspd[i]=Wspd2[i]
                Gust[i]=Gust2[i]
                Wspdflag[i]=Wspd2flag[i]
                Gustflag[i]=Wdir2flag[i]
                Gustflag[i]=Gust2flag[i]
                Wspdflagid[i]=Wspd2flagid[i]
                Gustflagid[i]=Wdir2flagid[i]
                Gustflagid[i]=Gust2flagid[i]
            else:
                Wdir[i]=Wdir1[i]
                Wspd[i]=Wspd1[i]
                Gust[i]=Gust1[i]
                Wspdflag[i]=Wspd1flag[i]
                Gustflag[i]=Wdir1flag[i]
                Gustflag[i]=Gust1flag[i]
                Wspdflagid[i]=Wspd1flagid[i]
                Gustflagid[i]=Wdir1flagid[i]
                Gustflagid[i]=Gust1flagid[i]
                continue
            continue
        

    return Wdirflag,Wspdflag,Gustflag,Wdir,Wspd,Gust,Wdirflagid,Wspdflagid,Gustflagid

    #####################
    #end of related measurement check


#######################################################################################    
# Wvht x Average Period check
# Compare the wave significant height and Average period
# If the value do not change, it will be flagged
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - Apd: Average wave period
# - Wvht: Significant wave height
# - flagt: matrix of flag for Apd
# - flagh: matrix of flag for Wvht
# - idf= flag id for Wvht. 'W' --> letter that represents the flag
#
# Required checks: Range Check, Missing value check
#
# Return: flag,idf
#
########################################################################################

def hstscheck(Epoch, Apd, Wvht, flagt,flagh,idf): 

    htresh=[0]*len(Epoch)
    for i in xrange(len(Epoch)): 
        if Apd[i] <= 5 and Apd[i]!=None:
           htresh[i] = (2.55 + (Apd[i]/4))
            
        elif Apd[i] > 5:
           htresh[i] = ((1.16*Apd[i])-2)
        
        else:
           continue    

    for i in xrange(len(Epoch)): 
        if flagt[i]!=4 and flagh[i]!=4:
            if Wvht[i] > htresh[i]:
               flagh[i] = 4
               idf[i]='W'
          
            else:
               continue



    return flagh, idf

    #####################
    #end Wave Height Versus Average Wave Period check





########################################################################################
# Time continuity
# Check is to verify if the data has consistency in the time
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - var: variable
# - flag: matrix of flag for the variable
# - sigma: value for the contnuity equation (normally, is related to std deviation)
# - idf= flag id. 'T' --> letter that represents the flag
# - rt: toggle for RTQC. If it is 0, RTQC
#
# Required checks: Range Check, Missing value check, Stuck Sensor check
#
# Return: flag,idf
#########################################################################################


def tcontinuitycheck(Epoch,var,flag,sigma,idf,rt):

    fwd_ep_gd,bck_ep_gd = Epoch[0],Epoch[-1]
    fwd_gd,bck_gd = var[0],var[-1]
    fwd_gdf,bck_gdf = flag[0],flag[-1]
    fwsp_qc,bksp_qc = [0]*len(Epoch),[0]*len(Epoch)
        


#TODO: for loop should be 0,len(epoch) as that is the first value in the array.
#To test the continuity, don't I need to start from the second value?

    for i in xrange(1,len(Epoch)):
        delta_Epoch = abs(Epoch[i] - fwd_ep_gd)
        if delta_Epoch<= 3600*3:
            if flag[i]!= 4:
                if fwd_gdf!=4:
                    sigmat=float(0.58*sigma*(np.sqrt(delta_Epoch/3600)))

                    if abs(var[i] - fwd_gd) > sigmat:
                        fwsp_qc[i] = 4
                        flag[i] = 4
                    else:
                        fwsp_qc[i] = 1
                        fwd_gd = var[i]
                        fwd_ep_gd = Epoch[i]
                        fwd_gdf=flag[i]
                        continue
                else:
                    fwd_gd = var[i]
                    fwd_ep_gd = Epoch[i]
                    fwd_gdf=flag[i]                    
            else:
                continue
        else: 
            fwd_gd = var[i]
            fwd_ep_gd = Epoch[i]
            fwd_gdf=flag[i]   
            continue


  #TODO: Might consider a toggle since you want to be able to run this in realtime.
#OK

    if rt==1:
        for i in xrange(-2,-len(Epoch),-1):
              
            delta_Epoch = abs(Epoch[i] - bck_ep_gd)
            if delta_Epoch <= 3600*3:
                if flag[i] != 4:
                    if bck_gdf!=4:                    
                        sigmat=float(0.58*sigma*(np.sqrt(delta_Epoch/3600)))
                        if abs(var[i] - bck_gd) > sigmat:
                            bksp_qc[i] = 4
                        else:
                            bksp_qc[i] = 1
                            bck_gd = var[i]
                            bck_ep_gd = Epoch[i]
                            bck_gdf=flag[i]
                            continue
                    else:
                        bck_gd = var[i]
                        bck_ep_gd = Epoch[i]
                        bck_gdf=flag[i]
                else:
                    continue
            
            else: 
                bck_gd = var[i]
                bck_ep_gd = Epoch[i]
                bck_gdf=flag[i]
                
                continue
    
    #TODO: Might consider a toggle since you want to be able to run this in realtime. 
    #OK
        for i in xrange(0, len(Epoch)):
            if fwsp_qc[i] == 4 and bksp_qc[i] == 4:
                flag[i] = 4
                idf[i]='T'              
            elif fwsp_qc[i] == 4 and bksp_qc[i] == 1:
                fwsp_qc[i] = 3
                flag[i] = 3             
                idf[i]='t'                 
            elif fwsp_qc[i] == 1 and bksp_qc[i] == 4:
                bksp_qc[i] = 3
                flag[i] = 3            
                idf[i]='t'                 
            elif fwsp_qc[i] == 0 and bksp_qc[i] == 4:
                flag[i] = 4
                idf[i]='T'                  
            elif fwsp_qc[i] == 4 and bksp_qc[i] == 0:
                flag[i] = 4
                idf[i]='T'                 
            else:
                continue
    else:
        for i in xrange(0, len(Epoch)):
            if fwsp_qc[i] == 4:
                flag[i] = 4
                idf[i]='T'
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
# - idfa= flag id for air temperature. 'f' --> letter that represents the flag
#
# Return: flaga,idfa


#TODO: How are you defining windd, atmp, pres? Is is that wspd1 or 2 or good wspd. 
def frontexcepcheck1(Epoch,windd,flagw,flaga,idfa):

    last_gt= Epoch[0]
    last_gw = windd[0]
    last_fa= flaga[0]
    last_fw=flagw[0]

    for i in xrange(1,len(Epoch)):
        delta_Epoch = abs(Epoch[i] - last_gt)
        if delta_Epoch <= 3600*3:
            if last_fa!=4:
                if flaga[i]==4:
                    if idfa[i]=='T':
                        if last_fw!=4 and abs(windd[i] - last_gw)>40:
                            flaga[i] = 2
                            idfa[i]='f'
                            last_gt= Epoch[i]
                            last_gw = windd[i]
                            last_fa= flaga[i]
                            last_fw=flagw[i]
                    else:
                        continue
                else:
                    last_gt= Epoch[i]
                    last_gw = windd[i]
                    last_fa= flaga[i]
                    last_fw=flagw[i]
            else:
                last_gt= Epoch[i]
                last_gw = windd[i]
                last_fa= flaga[i]
                last_fw=flagw[i]            
        else:
            last_gt= Epoch[i]
            last_gw = windd[i]
            last_fa= flaga[i]
            last_fw=flagw[i]
            continue
        
        return flaga, idfa

    #end of Frontal excepion 1

# Frontal exception 2
# Relation between wind direction and air temperature 2
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - windd: wind direction for the best anemometer
# - flagw: wind direction flag for the best anemometer
# - flaga: air temperature flag
# - idfw= flag id for wind direction. 'f' --> letter that represents the flag
#
# Return: flagw,idfw

def frontexcepcheck2(Epoch,windd,flagw,flaga,idfw):

    last_gt= Epoch[0]
    last_gw = windd[0]
    last_fa= flaga[0]
    last_fw=flagw[0]

    for i in xrange(1,len(Epoch)):
        delta_Epoch = abs(Epoch[i] - last_gt)
        if delta_Epoch <= 3600*3:
            if last_fw!=4:
                if flagw[i]==4:
                    if idfw[i]=='T':
                        if last_fa!=4 and abs(windd[i] - last_gw)>40:
                            flagw[i] = 2
                            idfw[i]='f'
                            last_gt= Epoch[i]
                            last_gw = windd[i]
                            last_fa= flaga[i]
                            last_fw=flagw[i]
                    else:
                        continue
                else:
                    last_gt= Epoch[i]
                    last_gw = windd[i]
                    last_fa= flaga[i]
                    last_fw=flagw[i]
            else:
                last_gt= Epoch[i]
                last_gw = windd[i]
                last_fa= flaga[i]
                last_fw=flagw[i]            
        else:
            last_gt= Epoch[i]
            last_gw = windd[i]
            last_fa= flaga[i]
            last_fw=flagw[i]
            continue
        
        return flagw, idfw

    #end of Frontal excepion 2

# Frontal exception 3
# Relation between wind direction and air temperature 2
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - winds: wind speed for the best anemometer
# - flags: wind speed flag for the best anemometer
# - flaga: air temperature flag
# - idfw= flag id for wind direction. 'f' --> letter that represents the flag
#
# Return: flagw,idfw



def frontexcepcheck3(Epoch,winds,atmp,flags,flaga,idfa):

    last_gt= Epoch[0]
    last_fa= flaga[0]

    for i in xrange(1,len(Epoch)):
        delta_Epoch = abs(Epoch[i] - last_gt)
        if delta_Epoch <= 3600*3:
            if last_fa!=4:
                if flaga[i]==4:
                    if idfa[i]=='T':
                        if winds[i]>7 and flags[i]!=4 and abs(atmp[i] - last_fa)>6: 
                            flaga[i] = 2
                            idfa[i]='f'
                            last_gt= Epoch[i]
                            last_fa= flaga[i]
                        else:
                            continue
                    else:
                        continue
                else:
                    last_gt= Epoch[i]
                    last_fa= flaga[i]
                    continue
            else:
                last_gt= Epoch[i]
                last_fa= flaga[i]                      
                continue
        else:
            last_gt= Epoch[i]
            last_fa= flaga[i]                      
            continue

    return flaga,idfa

    #end of Frontal excepion 3

# Frontal exception 4
# Relation between low pressure and wind direction
#
# Required input:
# - Epoch: Data/Time in Epoch date
# - pres: air pressure
# - flagp: wind direction flag for the best anemometer
# - flagp: air pressure flag
# - idfw= flag id for wind direction. 'f' --> letter that represents the flag
#
# Return: flagw,idfw


def frontexcepcheck4(Epoch,pres,flagp,flagw,idfw):

    for i in xrange(0, len(Epoch)):   
        if pres[i]<995 and flagp[i]!=4 and idfw[i]=='T':
            flagw[i] = 2
            idfw[i]='f'
        else:
            continue

    return flagw,idfw

    #end of Frontal excepion 4

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

    for i in xrange(len(Epoch)):
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
def climarangecheck(Epoch, Wvht, Dpd, Apd,Wspd,Gust,Pres,Dewp,Atmp,Wtmp,Wvhtflag, Dpdflag, Apdflag,Wspdflag, Gustflag, Presflag, Dewpflag, Atmpflag, Wtmpflag): 

# Definition of the ranges. For example: msdWvht=[0,5]. It means that the
# climatological mean is 0 and the standard deviation is 20

    msdWvht=[0,20]
    msdDpd=[1.95,26]
    msdApd=[0,26]
    msdWspd=[0,60]
    msdGust=[0,60]
    msdAtmp=[-40,60]
    msdPres=[500,1100]
    msdDewp=[-30,40]
    msdWtmp=[-4,30]
    
    for i in xrange(len(Epoch)): 
    
        if Wvht[i] > (msdWvht[0]+(3*msdWvht[1])) or Wvht[i] < (msdWvht[0]-(3*msdWvht[1])):
           Wvhtflag[i] = 4
       
        else:
           Wvhtflag[i] = 1

    for i in xrange(len(Epoch)): 
    
        if Dpd[i] >= (msdDpd[0]+(3*msdDpd[1])) or Dpd[i] <= (msdDpd[0]+(3*msdDpd[1])):
           Dpdflag[i] = 4
       
        else:
           Dpdflag[i] = 1

    for i in xrange(len(Epoch)): 
    
        if Apd[i] >= (msdApd[0]+(3*msdApd[1])) or Apd[i] <= (msdApd[0]+(3*msdApd[1])):
           Apdflag[i] = 4
       
        else:
           Apdflag[i] = 1

    for i in xrange(len(Epoch)): 
    
        if Wspd[i] > (msdWspd[0]+(3*msdWspd[1])) or Wspd[i] < (msdWspd[0]+(3*msdWspd[1])):
           Wspdflag[i] = 4
       
        else:
           Wspdflag[i] = 1

    for i in xrange(len(Epoch)): 
    
        if Gust[i] > (msdGust[0]+(3*msdGust[1])) or Gust[i] < (msdGust[0]+(3*msdGust[1])):
           Gustflag[i] = 4
       
        else:
           Gustflag[i] = 1
           
    for i in xrange(len(Epoch)): 
    
        if Atmp[i] > (msdAtmp[0]+(3*msdAtmp[1])) or Atmp[i] < (msdAtmp[0]+(3*msdAtmp[1])):
           Atmpflag[i] = 4
       
        else:
           Atmpflag[i] = 1

    for i in xrange(len(Epoch)): 
    
        if Pres[i] > (msdPres[0]+(3*msdPres[1])) or Pres[i] < (msdPres[0]+(3*msdPres[1])):
           Presflag[i] = 4
       
        else:
           Presflag[i] = 1

    for i in xrange(len(Epoch)): 
    
        if Dewp[i] > (msdDewp[0]+(3*msdDewp[1])) or Dewp[i] < (msdDewp[0]+(3*msdDewp[1])):
           Dewpflag[i] = 4
       
        else:
           Dewpflag[i] = 1
 
    for i in xrange(len(Epoch)): 
    
        if Wtmp[i] > (msdWtmp[0]+(3*msdWtmp[1])) or Wtmp[i] < (msdWtmp[0]+(3*msdWtmp[1])):
           Wtmpflag[i] = 4
       
        else:
           Wtmpflag[i] = 1
              

    return Wvhtflag, Dpdflag, Apdflag, Wspdflag, Gustflag, Presflag, Dewpflag, Atmpflag, Wtmpflag 
    #end Range Check section

#####################################################
 
 
def gustratiocheck(gustfactor,winds,gust,flaggf): 


     for i in xrange(len(gust)):
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
        
    for i in xrange(len(Epoch)): 
        
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


 

 
 