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

def generate_netcdf(df, df_status):

    NC = Dataset(df['name'] + '.nc','w')

    NC.wmo_id = df_status['wmo_number'][0]

    NC.institution = 'Brazilian Navy Hydrographic Center'
    NC.institution_abbreviation = 'CHM'

    # Title
    NC.title = "Meteorological and Oceanographic Data Collected by " \
        "the Programa Nacional de Boias Weather Buoys"

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


    NC.station_name = df_status['name']
    NC.sea_floor_depth_below_sea_level = df_status['depth']

    NC.qc_manual = "https://www.marinha.mil.br/chm/sites/www.marinha.mil.br.chm/files/u1947/controle_de_qualidade_dos_dados.pdf"
    NC.keywords = "Atmospheric Pressure, Sea level Pressure, Atmospheric " \
        "Temperature, Surface Temperature, Dewpoint Temperature, Humidity, " \
        "Surface Winds, Ocean Winds, Ocean Temperature, Sea Surface Temperature," \
        "Ocean Waves,  Wave Height, Wave Period, Ocean Currents."

    NC.keywords_vocabulary = "GCMD Science Keywords"
    NC.standard_name_vocabulary = "CF-76"

    NC.Metadata_Conventions = "Unidata Dataset Discovery v1.0"

    NC.citation="Programa Nacional de Boias should be cited as the source " \
        "of these data if used in any publication."

    NC.publisher_name = "PNBOIA"
    NC.publisher_url = "https://www.marinha.mil.br/chm/dados-do-goos-brasil/pnboia"
    NC.publisher_email = "chm.pnboia@marinha.mil.br"
    NC.nominal_latitude = df_status['lat']
    NC.nominal_longitude = df_status['lon']

    NC.time_coverage_start = df.index[0]
    NC.time_coverage_end = df.index[-1]
    NC.date_created = strftime("%Y-%m-%d %H:%M:%SZ", gmtime())


    ###########################
    # CREATE DIMENSIONS
    ###########################

    (times,NC) = ncdf.time_dimension (df, NC)

    ###########################
    # CREATE VARIABLES
    ###########################

    (lat,NC)=ncdf.latvariable(df, 'lat', NC)

    (lon,NC)=ncdf.latvariable(df, 'lon', NC)


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




