# -*- coding: utf-8 -*-
"""
Created on Wed Jun 04 09:07:08 2014

@author: soutobias
"""
import datetime
from time import gmtime, strftime
import time
from netCDF4 import Dataset
import netCDF4
import convert
import numpy as np

    
def timeforatributes (Year,Month,Day,Hour,Minute):

    t_now=strftime("%Y-%m-%dT%H:%M:%SZ", gmtime())
    
    t=[0]*len(Year)
    for i in xrange(len(t)):
        t[i]=datetime.datetime(int(Year[i]),int(Month[i]),int(Day[i]),int(Hour[i]),0).strftime('%Y-%m-%dT%H:%M:%SZ')

    return t,t_now


def timedimension(Epoch,NC):
    
    name='time'
        
    NC.createDimension(name, len(Epoch))
    times = NC.createVariable(name, 'i4', (name,))
    times.long_name = name
    times.standard_name = name
    times.units = 'seconds since 1970-01-01 00:00:00 UTC'
    times[:] = Epoch
    
    return times,NC

def latvariable(Epoch,Lat,NC):

    name='latitude'
    
    lat = NC.createVariable(name, 'f4', ('time',))
    lat.long_name=name
    lat.standard_name= name
    lat.units="degrees_north"
    lats = [Lat]*len(Epoch)
    lat[:] = lats

    return lat,NC

def lonvariable(Epoch,Lon,NC):

    name='longitude'
    
    lon = NC.createVariable(name, 'f4', ('time',))
    lon.long_name=name
    lon.standard_name=  name
    lon.units="degrees_east"
    lons = [Lon]*len(Epoch)
    lon[:] = lons

    return lon,NC


def wvhtvariable(Var,flag,idf,NC):

    name="sea_surface_wave_significant_height"
    shortname="significant_wave_height"
    unit="m"    

    var=NC.createVariable(shortname,'f4',('time',))
    var.long_name=name
    var.standard_name=name
    var.short_name=shortname
    var.units=unit
    var[:] = Var

    varqc=NC.createVariable(shortname+'_qc','b',('time',),zlib=True, fill_value= -9)
    varqc.standard_name=name+'_qc'
    varqc.short_name=shortname+'_qc'
    varqc.flag_values = 1,2,3,4
    varqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
    varqc[:] = flag

    vardqc=NC.createVariable(shortname+'_detail_qc','c',('time',),zlib=True, fill_value='_')
    vardqc.standard_name=name+'_detail_qc'
    vardqc.short_name=shortname+'_detail_qc'
    vardqc.flag_values = 'see NDBC QC Manual'
    vardqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
  
    (Idf)=convert.liststr2str(idf)
    
    vardqc[:] = Idf

    return var,varqc,vardqc,NC

def dpdvariable(Var,flag,idf,NC):

    name="sea_surface_wave_period_at_variance_spectral_density_maximum"
    shortname="dominant_period"
    unit="s"

    var=NC.createVariable(shortname,'f4',('time',))
    var.long_name=name
    var.standard_name=name
    var.short_name=shortname
    var.units=unit
    var[:] = Var

    varqc=NC.createVariable(shortname+'_qc','b',('time',),zlib=True, fill_value= -9)
    varqc.standard_name=name+'_qc'
    varqc.short_name=shortname+'_qc'
    varqc.flag_values = 1,2,3,4
    varqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
    varqc[:] = flag

    vardqc=NC.createVariable(shortname+'_detail_qc','c',('time',),zlib=True, fill_value='_')
    vardqc.standard_name=name+'_detail_qc'
    vardqc.short_name=shortname+'_detail_qc'
    vardqc.flag_values = 'see NDBC QC Manual'
    vardqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
  
    (Idf)=convert.liststr2str(idf)
    
    vardqc[:] = Idf

    return var,varqc,vardqc,NC
    
def apdvariable(Var,flag,idf,NC):

    name="sea_surface_wave_mean_period_from_variance_spectral_density_second_frequency_moment"
    shortname="average_period"
    unit="s"
   
    var=NC.createVariable(shortname,'f4',('time',))
    var.long_name=name
    var.standard_name=name
    var.short_name=shortname
    var.units=unit
    var[:] = Var

    varqc=NC.createVariable(shortname+'_qc','b',('time',),zlib=True, fill_value= -9)
    varqc.standard_name=name+'_qc'
    varqc.short_name=shortname+'_qc'
    varqc.flag_values = 1,2,3,4
    varqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
    varqc[:] = flag

    vardqc=NC.createVariable(shortname+'_detail_qc','c',('time',),zlib=True, fill_value='_')
    vardqc.standard_name=name+'_detail_qc'
    vardqc.short_name=shortname+'_detail_qc'
    vardqc.flag_values = 'see NDBC QC Manual'
    vardqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
  
    (Idf)=convert.liststr2str(idf)
    
    vardqc[:] = Idf

    return var,varqc,vardqc,NC

def mwdvariable(Var,flag,idf,NC):

    name="sea_surface_wave_from_direction"
    shortname="mean_wave_direction"
    unit="degree"

    var=NC.createVariable(shortname,'f4',('time',))
    var.long_name=name
    var.standard_name=name
    var.short_name=shortname
    var.units=unit
    var[:] = Var

    varqc=NC.createVariable(shortname+'_qc','b',('time',),zlib=True, fill_value= -9)
    varqc.standard_name=name+'_qc'
    varqc.short_name=shortname+'_qc'
    varqc.flag_values = 1,2,3,4
    varqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
    varqc[:] = flag

    vardqc=NC.createVariable(shortname+'_detail_qc','c',('time',),zlib=True, fill_value='_')
    vardqc.standard_name=name+'_detail_qc'
    vardqc.short_name=shortname+'_detail_qc'
    vardqc.flag_values = 'see NDBC QC Manual'
    vardqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
  
    (Idf)=convert.liststr2str(idf)
    
    vardqc[:] = Idf

    return var,varqc,vardqc,NC

def atmpvariable(Var,flag,idf,NC):

    name="air_temperature"
    shortname="air_temperature"
    unit="K"

    var=NC.createVariable(shortname,'f4',('time',))
    var.long_name=name
    var.standard_name=name
    var.units=unit
    var[:] = Var+273

    varqc=NC.createVariable(shortname+'_qc','b',('time',),zlib=True, fill_value= -9)
    varqc.standard_name=name+'_qc'
    varqc.short_name=shortname+'_qc'
    varqc.flag_values = 1,2,3,4
    varqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
    varqc[:] = flag

    vardqc=NC.createVariable(shortname+'_detail_qc','c',('time',),zlib=True, fill_value='_')
    vardqc.standard_name=name+'_detail_qc'
    vardqc.short_name=shortname+'_detail_qc'
    vardqc.flag_values = 'see NDBC QC Manual'
    vardqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
  
    (Idf)=convert.liststr2str(idf)
    
    vardqc[:] = Idf

    return var,varqc,vardqc,NC

def presvariable(Var,flag,idf,NC):

    name="air_pressure_at_sea_level"
    shortname="air_pressure_at_sea_level"
    unit="Pa"

    var=NC.createVariable(shortname,'i4',('time',))
    var.long_name=name
    var.standard_name=name
    var.units=unit
    var[:] = Var*100

    varqc=NC.createVariable(shortname+'_qc','b',('time',),zlib=True, fill_value= -9)
    varqc.standard_name=name+'_qc'
    varqc.short_name=shortname+'_qc'
    varqc.flag_values = 1,2,3,4
    varqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
    varqc[:] = flag

    vardqc=NC.createVariable(shortname+'_detail_qc','c',('time',),zlib=True, fill_value='_')
    vardqc.standard_name=name+'_detail_qc'
    vardqc.short_name=shortname+'_detail_qc'
    vardqc.flag_values = 'see NDBC QC Manual'
    vardqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
  
    (Idf)=convert.liststr2str(idf)
    
    vardqc[:] = Idf

    return var,varqc,vardqc,NC

def dewpvariable(Var,flag,idf,NC):

    name="dew_point_temperature"
    shortname="dew_point_temperature"
    unit="K"
    
    var=NC.createVariable(shortname,'f4',('time',))
    var.long_name=name
    var.standard_name=name
    var.units=unit
    var[:] = Var

    varqc=NC.createVariable(shortname+'_qc','b',('time',),zlib=True, fill_value= -9)
    varqc.standard_name=name+'_qc'
    varqc.short_name=shortname+'_qc'
    varqc.flag_values = 1,2,3,4
    varqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
    varqc[:] = flag

    vardqc=NC.createVariable(shortname+'_detail_qc','c',('time',),zlib=True, fill_value='_')
    vardqc.standard_name=name+'_detail_qc'
    vardqc.short_name=shortname+'_detail_qc'
    vardqc.flag_values = 'see NDBC QC Manual'
    vardqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
  
    (Idf)=convert.liststr2str(idf)
    
    vardqc[:] = Idf

    return var,varqc,vardqc,NC

def humivariable(Var,flag,idf,NC):

    name="relative_humidity"
    shortname="relative_humidity"
    unit="Percent"

    var=NC.createVariable(shortname,'f4',('time',))
    var.long_name=name
    var.standard_name=name
    var.units=unit
    var[:] = Var

    varqc=NC.createVariable(shortname+'_qc','b',('time',),zlib=True, fill_value= -9)
    varqc.standard_name=name+'_qc'
    varqc.short_name=shortname+'_qc'
    varqc.flag_values = 1,2,3,4
    varqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
    varqc[:] = flag

    vardqc=NC.createVariable(shortname+'_detail_qc','c',('time',),zlib=True, fill_value='_')
    vardqc.standard_name=name+'_detail_qc'
    vardqc.short_name=shortname+'_detail_qc'
    vardqc.flag_values = 'see NDBC QC Manual'
    vardqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
  
    (Idf)=convert.liststr2str(idf)
    
    vardqc[:] = Idf

    return var,varqc,vardqc,NC

def wtmpvariable(Var,flag,idf,NC):

    name="sea_surface_temperature"
    shortname="sea_surface_temperature"
    unit="K"

    var=NC.createVariable(shortname,'f4',('time',))
    var.long_name=name
    var.standard_name=name
    var.units=unit
    var[:] = Var

    varqc=NC.createVariable(shortname+'_qc','b',('time',),zlib=True, fill_value= -9)
    varqc.standard_name=name+'_qc'
    varqc.short_name=shortname+'_qc'
    varqc.flag_values = 1,2,3,4
    varqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
    varqc[:] = flag

    vardqc=NC.createVariable(shortname+'_detail_qc','c',('time',),zlib=True, fill_value='_')
    vardqc.standard_name=name+'_detail_qc'
    vardqc.short_name=shortname+'_detail_qc'
    vardqc.flag_values = 'see NDBC QC Manual'
    vardqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
  
    (Idf)=convert.liststr2str(idf)
    
    vardqc[:] = Idf

    return var,varqc,vardqc,NC

def wspdvariable(Var,flag,idf,NC):

    name="wind_speed"
    shortname="wind_speed"
    unit="m/s"

    var=NC.createVariable(shortname,'f4',('time',))
    var.long_name=name
    var.standard_name=name
    var.units=unit
    var[:] = Var

    varqc=NC.createVariable(shortname+'_qc','b',('time',),zlib=True, fill_value= -9)
    varqc.standard_name=name+'_qc'
    varqc.short_name=shortname+'_qc'
    varqc.flag_values = 1,2,3,4
    varqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
    varqc[:] = flag

    vardqc=NC.createVariable(shortname+'_detail_qc','c',('time',),zlib=True, fill_value='_')
    vardqc.standard_name=name+'_detail_qc'
    vardqc.short_name=shortname+'_detail_qc'
    vardqc.flag_values = 'see NDBC QC Manual'
    vardqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
  
    (Idf)=convert.liststr2str(idf)
    
    vardqc[:] = Idf

    return var,varqc,vardqc,NC

def wdirvariable(Var,flag,idf,NC):

    name="wind_from_direction"
    shortname="wind_direction"
    unit="degrees clockwise from North"

    var=NC.createVariable(shortname,'i4',('time',))
    var.long_name=name
    var.standard_name=name
    var.units=unit
    var[:] = Var

    varqc=NC.createVariable(shortname+'_qc','b',('time',),zlib=True, fill_value= -9)
    varqc.standard_name=name+'_qc'
    varqc.short_name=shortname+'_qc'
    varqc.flag_values = 1,2,3,4
    varqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
    varqc[:] = flag

    vardqc=NC.createVariable(shortname+'_detail_qc','c',('time',),zlib=True, fill_value='_')
    vardqc.standard_name=name+'_detail_qc'
    vardqc.short_name=shortname+'_detail_qc'
    vardqc.flag_values = 'see NDBC QC Manual'
    vardqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
  
    (Idf)=convert.liststr2str(idf)
    
    vardqc[:] = Idf

    return var,varqc,vardqc,NC

def gustvariable(Var,flag,idf,NC):

    name="wind_speed_of_gust"
    shortname="wind_gust"
    unit="m/s"

    var=NC.createVariable(shortname,'f4',('time',))
    var.long_name=name
    var.standard_name=name
    var.units=unit
    var[:] = Var
    
    varqc=NC.createVariable(shortname+'_qc','b',('time',),zlib=True, fill_value= -9)
    varqc.standard_name=name+'_qc'
    varqc.short_name=shortname+'_qc'
    varqc.flag_values = 1,2,3,4
    varqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
    varqc[:] = flag

    vardqc=NC.createVariable(shortname+'_detail_qc','c',('time',),zlib=True, fill_value='_')
    vardqc.standard_name=name+'_detail_qc'
    vardqc.short_name=shortname+'_detail_qc'
    vardqc.flag_values = 'see NDBC QC Manual'
    vardqc.flag_meanings='quality_good out_of_range sensor_nonfunctional questionable'
  
    (Idf)=convert.liststr2str(idf)
    
    vardqc[:] = Idf

    return var,varqc,vardqc,NC



