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
from datetime import datetime, timedelta
from time import gmtime, strftime

def generate_netcdf(df, df_status, buoy, cf):

    file_name = df_status['name_buoy'][buoy] + "_" + strftime('%Y-%m', gmtime()) + '.nc'

    NC = Dataset(file_name, 'w')

    NC.wmo_id = df_status['wmo_number'][buoy]

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


    NC.station_name = df_status['name_buoy'][buoy]
    NC.sea_floor_depth_below_sea_level = df_status['depth'][buoy]

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
    NC.nominal_latitude = df_status['lat'][buoy]
    NC.nominal_longitude = df_status['lon'][buoy]


    NC.time_coverage_start = df["date_time"].iloc[0].strftime('%Y-%m-%dT%H:%M:%SZ')
    NC.time_coverage_end = df["date_time"].iloc[-1].strftime('%Y-%m-%dT%H:%M:%SZ')
    NC.date_created = strftime('%Y-%m-%dT%H:%M:%SZ', gmtime())


    ###########################
    # CREATE DIMENSIONS
    ###########################

    (times, NC) = ncdf.time_dimension (df, NC)

    ###########################
    # CREATE VARIABLES
    ###########################

    (NC) = ncdf.create_variable(df, cf, 'lat', NC)

    (NC) = ncdf.create_variable(df, cf, 'lon', NC)


    (NC) = ncdf.create_variable(df, cf, 'sst', NC)

    (NC) = ncdf.create_variable(df, cf, 'swvht1', NC)
    (NC) = ncdf.create_variable(df, cf, 'tp1', NC)
    (NC) = ncdf.create_variable(df, cf, 'wvdir1', NC)

    (NC) = ncdf.create_variable(df, cf, 'wspd', NC)
    (NC) = ncdf.create_variable(df, cf, 'wdir', NC)


    if df_status['model'][0] == "SPOT-0222":
        (NC) = ncdf.create_variable(df, cf, 'pk_dir', NC)

        (NC) = ncdf.create_variable(df, cf, 'pk_wvspread', NC)
        (NC) = ncdf.create_variable(df, cf, 'mean_tp', NC)

    else:
        (NC) = ncdf.create_variable(df, cf, 'rh', NC)
        (NC) = ncdf.create_variable(df, cf, 'pres', NC)
        (NC) = ncdf.create_variable(df, cf, 'atmp', NC)
        (NC) = ncdf.create_variable(df, cf, 'dewpt', NC)
        (NC) = ncdf.create_variable(df, cf, 'wspd', NC)
        (NC) = ncdf.create_variable(df, cf, 'wdir', NC)
        (NC) = ncdf.create_variable(df, cf, 'gust', NC)
        (NC) = ncdf.create_variable(df, cf, 'arad', NC)
        (NC) = ncdf.create_variable(df, cf, 'cspd1', NC)
        (NC) = ncdf.create_variable(df, cf, 'cdir1', NC)
        (NC) = ncdf.create_variable(df, cf, 'cspd2', NC)
        (NC) = ncdf.create_variable(df, cf, 'cdir2', NC)
        (NC) = ncdf.create_variable(df, cf, 'cspd3', NC)
        (NC) = ncdf.create_variable(df, cf, 'cdir3', NC)
        (NC) = ncdf.create_variable(df, cf, 'mxwvht1', NC)
        (NC) = ncdf.create_variable(df, cf, 'tp2', NC)
        (NC) = ncdf.create_variable(df, cf, 'wvspread1', NC)
        (NC) = ncdf.create_variable(df, cf, 'battery', NC)
        (NC) = ncdf.create_variable(df, cf, 'compass', NC)


        if df_status['model'][0] == "BMO-BR":
            (NC) = ncdf.create_variable(df, cf, 'swvht2', NC)
            (NC) = ncdf.create_variable(df, cf, 'wvdir2', NC)


    NC.close()
    print ("-- File closed successfully")




