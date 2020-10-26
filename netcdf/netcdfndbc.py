# -*- coding: utf-8 -*-
"""
Created on Wed Jun 04 09:04:50 2014

@author: soutobias
"""

from netCDF4 import Dataset
import numpy as np
import createncdf as ncdf
import re
import time

def netcdfndbc(name,Epoch,data,flag,flagid,variables,variables2):

    for i in xrange(len(variables)):
        exec("%s=data[i]"% (variables[i]))
    
    for i in xrange(len(variables2)):
        exec("%sflag=flag[i]"% (variables2[i]))
        exec("%sflagid=flagid[i]"% (variables2[i]))

    NC = Dataset('c:\\ndbc\\netcdf\\'+name+'.nc','w')


    hd_new = re.search('(\d{5})h(\d{2})', name)
    
    wmo = hd_new.group(1)
    year = hd_new.group(2)
    station='Niteroi2'
    depth=17
    Lat=36.785
    Lon=-122.469

    NC.wmo_id = wmo
    
    NC.institution = 'National Data Buoy Center'
    NC.institution_abbreviation = 'NDBC'
    
    # Title
    NC.title = "Meteorological and Oceanographic Data Collected from " \
        "the National Data Buoy Center's Coastal Marine Automated " \
        "Network and Weather Buoys"
        
    NC.summary = "The Coastal-Marine Automated Network (C-MAN) was " \
        "established by NDBC for the NWS in the early 1980's. " \
        "Approximately 50 stations make up the C-MAN and have been " \
        "installed on lighthouses, at capes and beaches, on near shore " \
        "islands, and on offshore platforms.  Over 100 moored weather " \
        "buoys have been deployed in U.S. coastal and offshore waters.  " \
        "C-MAN and weather buoy data typically include barometric " \
        "pressure, wind direction, speed and gust, and air temperature; " \
        "however, some C-MAN stations are equipped to also measure sea " \
        "water temperature, waves, and relative humidity. Weather buoys " \
        "also measure wave energy spectra from which significant wave " \
        "height, dominant wave period, and average wave period are " \
        "derived. The direction of wave propagation is also measured on " \
        "many moored weather buoys."
    
    
    NC.station_name=station
    NC.sea_floor_depth_below_sea_level=depth
    
    NC.qc_manual="http://www.ndbc.noaa.gov/NDBCHandbookofAutomatedDataQualityControl2009.pdf"
    NC.keywords = "Atmospheric Pressure, Sea level Pressure, Atmospheric " \
        "Temperature, Surface Temperature, Dewpoint Temperature, Humidity, " \
        "Surface Winds, Ocean Winds, Ocean Temperature, Sea Surface Temperature," \
        "Ocean Waves,  Wave Height, Wave Period, Ocean Currents."
    
    NC.keywords_vocabulary="GCMD Science Keywords"
    NC.standard_name_vocabulary="CF-16.0"
    
    NC.Metadata_Conventions= "Unidata Dataset Discovery v1.0"
    
    NC.citation="The National Data Buoy Center should be cited as the source " \
        "of these data if used in any publication."
    
    NC.publisher_name="NDBC"
    NC.publisher_url="http://www.ndbc.noaa.gov"
    NC.publisher_email="webmaster.ndbc@noaa.gov"
    NC.nominal_latitude= Lat
    NC.nominal_longitude=Lon
    

    (t, t_now)=ncdf.timeforatributes(Year,Month,Day,Hour,Minute)

    NC.time_coverage_start=t[0]
    NC.time_coverage_end= t[-1]
    NC.date_created= t_now

    
    ###########################
    # CREATE DIMENSIONS
    ###########################

    (times,NC)=ncdf.timedimension(Epoch,NC)
  
    
    ###########################
    # CREATE VARIABLES
    ###########################

    (lat,NC)=ncdf.latvariable(Epoch,Lat,NC)
    
    (lon,NC)=ncdf.lonvariable(Epoch,Lon,NC)
    
    
    (wvht,wvht_qc,wvht_dqc,NC)=ncdf.wvhtvariable(Wvht,Wvhtflag,Wvhtflagid,NC)
    
    (dpd,dpd_qc,dpd_dqc,NC)=ncdf.dpdvariable(Dpd,Dpdflag,Dpdflagid,NC)
    (apd,apd_qc,apd_dqc,NC)=ncdf.apdvariable(Apd,Apdflag,Apdflagid,NC)
    (mwd,mwd_qc,mwd_dqc,NC)=ncdf.mwdvariable(Mwd,Mwdflag,Mwdflagid,NC)
    (atmp,atmp_qc,atmp_dqc,NC)=ncdf.atmpvariable(Atmp,Atmpflag,Atmpflagid,NC)
    (pres,pres_qc,pres_dqc,NC)=ncdf.presvariable(Pres,Presflag,Presflagid,NC)
    (dewp,dewp_qc,dewp_dqc,NC)=ncdf.dewpvariable(Dewp,Dewpflag,Dewpflagid,NC)
    (wtmp,wtmp_qc,wtmp_dqc,NC)=ncdf.wtmpvariable(Wtmp,Wtmpflag,Wtmpflagid,NC)
    (wspd,wspd_qc,wspd_dqc,NC)=ncdf.wspdvariable(Wspd,Wspdflag,Wspdflagid,NC)
    (wdir,wdir_qc,wdir_dqc,NC)=ncdf.wdirvariable(Wdir,Wdirflag,Wdirflagid,NC)
    (gust,gust_qc,gust_dqc,NC)=ncdf.gustvariable(Gust,Gustflag,Gustflagid,NC)


    NC.close()
    print "-- File closed successfully"




