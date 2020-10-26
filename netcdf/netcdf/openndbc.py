# -*- coding: utf-8 -*-
"""
Created on Thu Jun 05 14:47:23 2014

@author: soutobias
"""

import numpy as np
import argosqc as qc


def openndbctxt(name):

    lines = open('c:\\ndbc\\data\\'+name+'.txt', 'rb')
        
    header = lines.readline()
    
    if header=='YY MM DD hh WD   WSPD GST  WVHT  DPD   APD  MWD  BAR    ATMP  WTMP  DEWP  VIS\r\n':

        Year,Month,Day,Hour = [],[],[],[]
        Wdir,Wspd,Gust,Wvht,Dpd,Apd= [],[],[],[],[],[]
        Mwd,Pres,Atmp,Wtmp,Dewp,Vis = [],[],[],[],[],[]

        for line in lines:
            dataline = line.strip()
            columns = dataline.split()
    
            Year.append(int(columns[0]))
            Month.append(int(columns[1]))
            Day.append(int(columns[2]))
            Hour.append(int(columns[3]))
            Wspd.append(float(columns[5]))
            Gust.append(float(columns[6]))
            Wvht.append(float(columns[7]))
            Dpd.append(float(columns[8])) 
            Apd.append(float(columns[9]))
            Mwd.append(float(columns[10]))
            Pres.append(float(columns[11]))
            Atmp.append(float(columns[12]))
            Wtmp.append(float(columns[13]))
            Dewp.append(float(columns[14]))
            Vis.append(float(columns[15]))

            if 'MM' in columns[4]:
                Wdir.append(999)
            else:
                Wdir.append(float(columns[4]))
        
        lines.close()

        Wvhtflag = [0]*len(Year)    
        Dpdflag = [0]*len(Year) 
        Apdflag = [0]*len(Year) 
        Mwdflag = [0]*len(Year) 
        Wspdflag = [0]*len(Year) 
        Wdirflag = [0]*len(Year) 
        Gustflag = [0]*len(Year) 
        Presflag = [0]*len(Year) 
        Dewpflag = [0]*len(Year) 
        Atmpflag = [0]*len(Year) 
        Wtmpflag = [0]*len(Year)        
        
        Wspdflagid = ['0']*len(Year) 
        Wdirflagid = ['0']*len(Year) 
        Gustflagid = ['0']*len(Year) 
        Atmpflagid= ['0']*len(Year)   
        Dewpflagid= ['0']*len(Year)   
        Presflagid= ['0']*len(Year)   
        Wtmpflagid= ['0']*len(Year)   
        Wvhtflagid= ['0']*len(Year)   
        Dpdflagid= ['0']*len(Year)    
        Mwdflagid= ['0']*len(Year)
        Apdflagid = ['0']*len(Year)                 
        
        data=np.array([Year,Month,Day,Hour,Wdir,Wspd,Gust,Wvht,Dpd,Apd,Mwd,Pres,Atmp,Wtmp,Dewp,Vis])
        flag=np.array([Wdirflag,Wspdflag,Gustflag,Wvhtflag,Dpdflag,Apdflag,Mwdflag,Presflag,Atmpflag,Wtmpflag,Dewpflag])        
        flagid=np.array([Wdirflagid,Wspdflagid,Gustflagid,Wvhtflagid,Dpdflagid,Apdflagid,Mwdflagid,Presflagid,Atmpflagid,Wtmpflagid,Dewpflagid]) 
        variables=['Year','Month','Day','Hour','Wdir','Wspd','Gust','Wvht','Dpd','Apd','Mwd','Pres','Atmp','Wtmp','Dewp','Vis']
        variables2=['Wdir','Wspd','Gust','Wvht','Dpd','Apd','Mwd','Pres','Atmp','Wtmp','Dewp']

    else:
        if header=='#YY  MM DD hh mm WDIR WSPD GST  WVHT   DPD   APD MWD   PRES  ATMP  WTMP  DEWP  VIS  TIDE\n':
            Units = lines.readline()
        else:
            Units=0

        Year,Month,Day,Hour,Minute = [],[],[],[],[]
        Wdir,Wspd,Gust,Wvht,Dpd,Apd= [],[],[],[],[],[]
        Mwd,Pres,Atmp,Wtmp,Dewp,Vis = [],[],[],[],[],[]
        
        
        for line in lines:
            dataline = line.strip()
            columns = dataline.split()
    
            Year.append(int(columns[0]))
            Month.append(int(columns[1]))
            Day.append(int(columns[2]))
            Hour.append(int(columns[3]))
            Minute.append(int(columns[4]))

            Wspd.append(float(columns[6]))
            Gust.append(float(columns[7]))
            Wvht.append(float(columns[8]))
            Dpd.append(float(columns[9])) 
            Apd.append(float(columns[10]))
            Mwd.append(float(columns[11]))
            Pres.append(float(columns[12]))
            Atmp.append(float(columns[13]))
            Wtmp.append(float(columns[14]))
            Dewp.append(float(columns[15]))
            Vis.append(float(columns[16]))

            if 'MM' in columns[5]:
                Wdir.append(999)
            else:
                Wdir.append(float(columns[5]))

        lines.close()
        
        Wvhtflag = [0]*len(Year)    
        Dpdflag = [0]*len(Year) 
        Apdflag = [0]*len(Year) 
        Mwdflag = [0]*len(Year) 
        Wspdflag = [0]*len(Year) 
        Wdirflag = [0]*len(Year) 
        Gustflag = [0]*len(Year) 
        Presflag = [0]*len(Year) 
        Dewpflag = [0]*len(Year) 
        Atmpflag = [0]*len(Year) 
        Wtmpflag = [0]*len(Year)        
        
        Wspdflagid = ['0']*len(Year) 
        Wdirflagid = ['0']*len(Year) 
        Gustflagid = ['0']*len(Year) 
        Atmpflagid= ['0']*len(Year)   
        Dewpflagid= ['0']*len(Year)   
        Presflagid= ['0']*len(Year)   
        Wtmpflagid= ['0']*len(Year)   
        Wvhtflagid= ['0']*len(Year)   
        Dpdflagid= ['0']*len(Year)    
        Mwdflagid= ['0']*len(Year)
        Apdflagid = ['0']*len(Year)   

        data=np.array([Year,Month,Day,Hour,Minute,Wdir,Wspd,Gust,Wvht,Dpd,Apd,Mwd,Pres,Atmp,Wtmp,Dewp,Vis])
        flag=np.array([Wdirflag,Wspdflag,Gustflag,Wvhtflag,Dpdflag,Apdflag,Mwdflag,Presflag,Atmpflag,Wtmpflag,Dewpflag])        
        flagid=np.array([Wdirflagid,Wspdflagid,Gustflagid,Wvhtflagid,Dpdflagid,Apdflagid,Mwdflagid,Presflagid,Atmpflagid,Wtmpflagid,Dewpflagid]) 
        variables=['Year','Month','Day','Hour','Minute','Wdir','Wspd','Gust','Wvht','Dpd','Apd','Mwd','Pres','Atmp','Wtmp','Dewp','Vis']
        variables2=['Wdir','Wspd','Gust','Wvht','Dpd','Apd','Mwd','Pres','Atmp','Wtmp','Dewp']
           
    return data,flag,flagid,variables,variables2



def qualitycontrol(Epoch,data,flag,flagid,variables,variables2):

    for i in xrange(len(variables)):
        exec("%s=data[i]"% (variables[i]))
    
    for i in xrange(len(variables2)):
        exec("%sflag=flag[i]"% (variables2[i]))
        exec("%sflagid=flagid[i]"% (variables2[i]))
        
    (rWvht,rDpd,rMwd,rWspd,rWdir,rGust,rAtmp,rPres,rDewp,rWtmp,rApd,sigmaWvht,sigmaPres,sigmaAtmp,sigmaWspd,sigmaWtmp)=qc.valores()

    ##############################################
    #RUN THE QC CHECKS
    ##############################################
    
    #time check
    (timeflag) = qc.timecheck(Epoch)
    
    #Missing value check
    (Wdirflag,Wdirflagid)=qc.misvaluecheck(Epoch,Wdir,Wdirflag,99,Wdirflagid)
    (Wspdflag,Wspdflagid)=qc.misvaluecheck(Epoch,Wspd,Wspdflag,99,Wspdflagid)
    (Gustflag,Gustflagid)=qc.misvaluecheck(Epoch,Gust,Gustflag,99,Gustflagid)
    (Mwdflag,Mwdflagid)=qc.misvaluecheck(Epoch,Mwd,Mwdflag,999,Mwdflagid)
    (Wvhtflag,Wvhtflagid)=qc.misvaluecheck(Epoch,Wvht,Wvhtflag,99,Wvhtflagid)
    (Apdflag,Apdflagid)=qc.misvaluecheck(Epoch,Apd,Apdflag,99,Apdflagid)
    (Dpdflag,Dpdflagid)=qc.misvaluecheck(Epoch,Dpd,Dpdflag,99,Dpdflagid)
    (Presflag,Presflagid)=qc.misvaluecheck(Epoch,Pres,Presflag,9999,Presflagid)
    (Dewpflag,Dewpflagid)=qc.misvaluecheck(Epoch,Dewp,Dewpflag,999,Dewpflagid)
    (Atmpflag,Atmpflagid)=qc.misvaluecheck(Epoch,Atmp,Atmpflag,999,Atmpflagid)
    (Wtmpflag,Wtmpflagid)=qc.misvaluecheck(Epoch,Wtmp,Wtmpflag,999,Wtmpflagid)
    
    #Range check
    (Wdirflag,Wdirflagid)=qc.rangecheck(Epoch,Wdir,rWdir,Wdirflag,Wdirflagid)
    (Wspdflag,Wspdflagid)=qc.rangecheck(Epoch,Wspd,rWspd,Wspdflag,Wspdflagid)
    (Gustflag,Gustflagid)=qc.rangecheck(Epoch,Gust,rGust,Gustflag,Gustflagid)
    (Mwdflag,Mwdflagid)=qc.rangecheck(Epoch,Mwd,rMwd,Mwdflag,Mwdflagid)
    (Wvhtflag,Wvhtflagid)=qc.rangecheck(Epoch,Wvht,rWvht,Wvhtflag,Wvhtflagid)
    (Apdflag,Apdflagid)=qc.rangecheck(Epoch,Apd,rApd,Apdflag,Apdflagid)
    (Dpdflag,Dpdflagid)=qc.rangecheck(Epoch,Dpd,rDpd,Dpdflag,Dpdflagid)
    (Presflag,Presflagid)=qc.rangecheck(Epoch,Pres,rPres,Presflag,Presflagid)
    (Dewpflag,Dewpflagid)=qc.rangecheck(Epoch,Dewp,rDewp,Dewpflag,Dewpflagid)
    (Atmpflag,Atmpflagid)=qc.rangecheck(Epoch,Atmp,rAtmp,Atmpflag,Atmpflagid)
    (Wtmpflag,Wtmpflagid)=qc.rangecheck(Epoch,Wtmp,rWtmp,Wtmpflag,Wtmpflagid)
    
    #Wind speed vs Gust speed
    (Wspdflag,Gustflag,Wspdflagid,Gustflagid) = qc.windspeedgustcheck(Epoch,Wspd,Gust,Wspdflag,Gustflag,Wspdflagid,Gustflagid)
    
    #Dew point and Air temperature check
    (Dewpflag,Dewpflagid) = qc.dewpatmpcheck(Epoch,Dewp,Atmp,Dewpflag,Atmpflag,Dewpflagid)
    
    #Hs x Tm check
    (Wvhtflag,Wvhtflagid)=qc.hstscheck(Epoch, Apd, Wvht, Apdflag,Wvhtflag,Wvhtflagid)
    
    #Stucksensorcheck
    (Wvhtflag,Wvhtflagid) = qc.stucksensorcheck(Epoch, Wvht,Wvhtflag,6,Wvhtflagid)
    (Dpdflag,Dpdflagid) = qc.stucksensorcheck(Epoch, Dpd,Dpdflag,6,Dpdflagid)
    (Mwdflag,Mwdflagid) = qc.stucksensorcheck(Epoch,Mwd,Mwdflag,6,Mwdflagid)
    (Apdflag,Apdflagid) = qc.stucksensorcheck(Epoch,Apd,Apdflag,6,Apdflagid)
    (Presflag,Presflagid) = qc.stucksensorcheck(Epoch, Pres,Presflag,6,Presflagid)
    (Dewpflag,Dewpflagid) = qc.stucksensorcheck(Epoch, Dewp,Dewpflag,6,Dewpflagid)
    (Atmpflag,Atmpflagid) = qc.stucksensorcheck(Epoch,Atmp,Atmpflag,6,Atmpflagid)
    (Wtmpflag,Wtmpflagid) = qc.stucksensorcheck(Epoch,Wtmp,Wtmpflag,6,Wtmpflagid)
    (Wspdflag,Wspdflagid) = qc.stucksensorcheck(Epoch, Wspd,Wspdflag,6,Wspdflagid)
    (Wdirflag,Wdirflagid) = qc.stucksensorcheck(Epoch, Wdir,Wdirflag,6,Wdirflagid)
    (Gustflag,Gustflagid) = qc.stucksensorcheck(Epoch, Gust,Gustflag,6,Gustflagid)
    
    #Time continuity check
    (Wvhtflag,Wvhtflagid) = qc.tcontinuitycheck(Epoch,Wvht,Wvhtflag,sigmaWvht,Wvhtflagid,1)
    (Presflag,Presflagid) = qc.tcontinuitycheck(Epoch,Pres,Presflag,sigmaPres,Presflagid,1)
    (Atmpflag,Atmpflagid) = qc.tcontinuitycheck(Epoch,Atmp,Atmpflag,sigmaAtmp,Atmpflagid,1)
    (Wspdflag,Wspdflagid) = qc.tcontinuitycheck(Epoch,Wspd,Wspdflag,sigmaWspd,Wspdflagid,1)
    (Wtmpflag,Wtmpflagid) = qc.tcontinuitycheck(Epoch,Wtmp,Wtmpflag,sigmaWtmp,Wtmpflagid,1)
    
    #Frontal passage exception for time continuity
    (Atmpflag,Atmpflagid)=qc.frontexcepcheck1(Epoch,Wdir,Wdirflag,Atmpflag,Atmpflagid)
    (Wdirflag,Wdirflagid)=qc.frontexcepcheck2(Epoch,Wdir,Wdirflag,Atmpflag,Atmpflagid)
    (Atmpflag,Atmpflagid)=qc.frontexcepcheck3(Epoch,Wspd,Atmp,Wspdflag,Atmpflag,Atmpflagid)
    (Wdirflag,Wdirflagid)=qc.frontexcepcheck4(Epoch,Pres,Presflag,Wdirflag,Wdirflagid)
    
    data=np.array([Year,Month,Day,Hour,Minute,Wdir,Wspd,Gust,Wvht,Dpd,Apd,Mwd,Pres,Atmp,Wtmp,Dewp,Vis])
    flag=np.array([Wdirflag,Wspdflag,Gustflag,Wvhtflag,Dpdflag,Apdflag,Mwdflag,Presflag,Atmpflag,Wtmpflag,Dewpflag])        
    flagid=np.array([Wdirflagid,Wspdflagid,Gustflagid,Wvhtflagid,Dpdflagid,Apdflagid,Mwdflagid,Presflagid,Atmpflagid,Wtmpflagid,Dewpflagid]) 


    return data,flag,flagid



def savetxtndbc(Epoch,data,flag,flagid,variables,variables2):

    for i in xrange(len(variables)):
        exec("%s=data[i]"% (variables[i]))
    
    for i in xrange(len(variables2)):
        exec("%sflag=flag[i]"% (variables2[i]))
        exec("%sflagid=flagid[i]"% (variables2[i]))

    Data = np.array([Year,Month,Day,Hour,Minute,Wvht,Wvhtflag,Dpd,Dpdflag,Apd,Apdflag,Mwd,Mwdflag,Wspd,Wspdflag,Wdir,Wdirflag,Gust,Gustflag,Pres,Presflag,Dewp,Dewpflag,Atmp,Atmpflag,Wtmp,Wtmpflag])
    Data1 = Data.transpose()
    header="Month,Day,Hour,Minute,Wvht,Wvhtflag,Dpd,Dpdflag,Apd,Apdflag,Mwd,Mwdflag,Wspd,Wspdflag,Wdir,Wdirflag,Gust,Gustflag,Pres,Presflag,Dewp,Dewpflag,Atmp,Atmpflag,Wtmp,Wtmpflag"
    np.savetxt("c:\\ndbc\\test\\dataline.csv", Data1,'%s',delimiter=",",header=header)




