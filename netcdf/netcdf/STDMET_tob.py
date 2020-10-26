# -*- coding: utf-8 -*-
"""
Created on Wed Apr 30 12:36:14 2014

@author: walt.mccall
"""
#import glob
import argosqc as qc
import time
import numpy as np
from openndbc import *
import convert
from netcdfndbc import netcdfndbc
from netCDF4 import Dataset
import createncdf as ncdf
import re

t = time.time()

name="42039h2012"
#open file and use lines as pointer to file.

(data,flag,flagid,variables,variables2)=openndbctxt(name)

try:
    (Epoch)=convert.time2epoch(data[0],data[1],data[2],data[3],data[4])
except:
    (Epoch)=convert.time2epoch(data[0],data[1],data[2],data[3],0)


(data,flag,flagid)=qualitycontrol(Epoch,data,flag,flagid,variables,variables2)

#save txtfile

savetxtndbc(Epoch,data,flag,flagid,variables,variables2)


#SAVE NETCDF

netcdfndbc(name,Epoch,data,flag,flagid,variables,variables2)


elapsed = time.time() - t
print(elapsed)
Data = []


# close the file to free up memory and the file for editing.


print('done reading in data')

