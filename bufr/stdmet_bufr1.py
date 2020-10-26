# -*- coding: utf-8 -*-
"""
Created on Wed Jun 11 09:51:37 2014

@author: soutobias
"""

import os
import sys
import datetime
import time
import csv
import numpy
import math
from datetime import datetime, timedelta
from netCDF4 import Dataset, num2date
from xml.dom import minidom
from urllib2 import urlopen
from struct import pack #@UnresolvedImport
from bitShift import *

lines = open('C:\\ndbc\\bufr\\4604232014_test.txt', 'rb')

#read in the first 2 lines
ASCIIs = lines.readline()

#declare lists for each column in the file.
Year,Month,Day,Hour,Minute = [],[],[],[],[]
Wdir,Wspd,Gust,Wvht,Dpd,Apd= [],[],[],[],[],[]
Mwd,Pres,Atmp,Wtmp,Dewp,Humi,Tide, Lat,Lon = [],[],[],[],[],[],[],[],[]

#read in the remaining lines from the file and split into individual columns.

for line in lines:
    dataline = line.strip()
    columns = dataline.split()
    

    Year=int(columns[0])
    Month=int(columns[1])
    Day=int(columns[2])
    Hour=int(columns[3])
    Minute=int(columns[4])
    Wdir=float(columns[5])
    Wspd=float(columns[6])
    Gust=float(columns[7])
    Wvht=float(columns[8])
    Dpd=float(columns[9])
    Apd=float(columns[10])
    Mwd=float(columns[11])
    Pres=float(columns[12])
    Atmp=float(columns[13])
    Wtmp=float(columns[14])
    Dewp=float(columns[15])
    Humi=float(columns[16])
    Tide=float(columns[17])
    Lat=float(columns[18])
    Lon=float(columns[19])

lines.close()  

print('done reading in data')

############################################################    
#
#
#   BUFR SECTION 1
#
#
############################################################   


bufrSection1List = []
bufrSection1 = None


# Octect 4
# BUFR master table (zero if standard WMO FM 94 BUFR tables are used)
bufrSection1List.append(pack("B", 0))

# Octect 5-6    
# Identification of originating/generating centre 
# (see Common Code table C-11)
# US National Weather Service - Other
bufrSection1List.append(pack(">H", 9))

# Octect 7-8
# Identification of originating/generating sub-centre 
# (allocated by originating/generating centre - see Common Code table C-12)
bufrSection1List.append(pack(">H", 0))

# Octect 9        
# Update sequence number (zero for original BUFR message)
bufrSection1List.append(pack("B", 0))

# Octect 10        
# Code for presence of optional section (2), 0=Not present
bufrSection1List.append("\x00")

# Octect 11        
# Code for Data Category (1=Surface Data-Sea)
bufrSection1List.append(pack("B", 1))

# Octect 12        
# Code for International data sub-category (3=Moored Buoy)
bufrSection1List.append(pack("B", 3))

# Octect 13        
# Code for local data sub-category (??????)
bufrSection1List.append(pack("B", 3))

# Octect 14
# Version number of master table (21=Version implemented on May 2013)
bufrSection1List.append(pack("B", 19))

# Octect 15
# Version number of local tables
bufrSection1List.append(pack("B", 0))


# Octect 16-22
# Data and time of the measurement)

# Octect 16-17 (Year - 4 digits)
bufrSection1List.append(pack(">H", Year))
# Octect 18 (Month)
bufrSection1List.append(pack("B", Month))
# Octect 19 (Day)
bufrSection1List.append(pack("B", Day))
# Octect 20 (Hour)
bufrSection1List.append(pack("B", Hour))
# Octect 21 (Minute)
bufrSection1List.append(pack("B", Minute))
# Octect 22 (Seconds)
bufrSection1List.append(pack("B", 0))


#Octets 1-3
# Length of the section 1
tmpString = "".join(bufrSection1List)
bufrSection1Length = len(tmpString) + 3

print('bufrSection1Length')
print(bufrSection1Length)

# 'L' generates a 4-byte integer, but only need 3 bytes of it
bufrSection1 = "%s%s" % ((pack(">L", bufrSection1Length)[1:]), tmpString)


############################################################    
#
#
#   BUFR SECTION 3
#
#
############################################################   


bufrSection3List = []
bufrSection3 = None

# Octect 4
# Set to zero (reserved)   
bufrSection3List.append(pack("B", 0))

# Octect 5-6
# Number of data subsets         
bufrSection3List.append(pack(">H", 1))

# Octect 7
# Observed data (Bit1=1), uncompressed (Bit2=0), Bit3-8=0 (reserved). 10000000=128     
bufrSection3List.append(pack("B", 128)) 


#############################

# Octect 8-9
bufrSection3List.append(pack("BB", 1, 87))      # 001087


# Octect 10-11
bufrSection3List.append(pack("BB", 1, 15))      # 001015


# Octect 12-13
bufrSection3List.append(pack("BB", 2, 149))      # 002149

#############################
# Octect 14-15
# Date
fx = (3 * 2**6) + 1 #=193 (decimal)=11000001 (binary)
bufrSection3List.append(pack("BB", fx, 11))      # 301011

# Octect 16-17
# Time
fx = (3 * 2**6) + 1 #=193 (decimal)=11000001 (binary)
bufrSection3List.append(pack("BB", fx, 12))      # 301012

# Octect 18-19
# Latitude and longitude (high accuracy)
fx = (3 * 2**6) + 1 #=193 (decimal)=11000001 (binary)
bufrSection3List.append(pack("BB", fx, 21))      # 301021

#################################################################

# Octect 20-21
# Sequence for STDMET measurements from moored buoys
bufrSection3List.append(pack("BB", 10, 4))      # 010004

# Octect 22-23
# Sequence for STDMET measurements from moored buoys
bufrSection3List.append(pack("BB", 10, 51))      # 010051

# Octect 24-25
# Sequence for STDMET measurements from moored buoys
bufrSection3List.append(pack("BB", 7, 33))      # 007033

# Octect 26-27
# Sequence for STDMET measurements from moored buoys
bufrSection3List.append(pack("BB", 12, 101))      # 012101

# Octect 28-29
# Sequence for STDMET measurements from moored buoys
bufrSection3List.append(pack("BB", 12, 103))      # 012103

# Octect 30-31
# Sequence for STDMET measurements from moored buoys
bufrSection3List.append(pack("BB", 13, 3))      # 013003

# Octect 32-33
# Sequence for STDMET measurements from moored buoys
bufrSection3List.append(pack("BB", 7, 33))      # 007033

# Octect 34-35
# Sequence for STDMET measurements from moored buoys
bufrSection3List.append(pack("BB", 8, 21))      # 008021

# Octect 36-37
# Sequence for STDMET measurements from moored buoys
bufrSection3List.append(pack("BB", 4, 25))      # 004025

# Octect 38-39
# Sequence for STDMET measurements from moored buoys
bufrSection3List.append(pack("BB", 11, 1))      # 011001

# Octect 40-41
# Sequence for STDMET measurements from moored buoys
bufrSection3List.append(pack("BB", 11, 2))      # 011002

# Octect 42-43
# Sequence for STDMET measurements from moored buoys
bufrSection3List.append(pack("BB", 8, 21))      # 008021

# Octect 44-45
# Sequence for STDMET measurements from moored buoys
bufrSection3List.append(pack("BB", 4, 25))      # 004025

# Octect 46-47
# Sequence for STDMET measurements from moored buoys
bufrSection3List.append(pack("BB", 11, 41))      # 011041

# Octect 48-49
# Sequence for STDMET measurements from moored buoys
bufrSection3List.append(pack("BB", 4, 24))      # 004025

# Octect 50-51
# Sequence for STDMET measurements from moored buoys
bufrSection3List.append(pack("BB", 7, 33))      # 007033

# Octect 52-53
# Sequence for STDMET measurements from moored buoys
bufrSection3List.append(pack("BB", 2, 5))      # 002005

# Octect 54-55
# Sequence for STDMET measurements from moored buoys
bufrSection3List.append(pack("BB", 7, 63))      # 007063

# Octect 56-57
# Sequence for STDMET measurements from moored buoys
bufrSection3List.append(pack("BB", 22, 49))      # 022049

#################################################################

# Commented. It has same parameters absent in the STDMET file: spread, Wmax, Wave direction of dominant wave
#        # Octect 18-19
#        # Sequence for representation of basic wave measurements
#        fx = (3 * 2**6) + 6 #=198 (decimal)=11000110 (binary)
#        bufrSection3List.append(pack("BB", fx, 21))      # 306038

# Octect 58-59
# Duration of wave record
bufrSection3List.append(pack("BB", 22, 78))      # 022078

# Octect 60-61        
# Significant wave height
bufrSection3List.append(pack("BB", 22, 70))      # 022070

# Octect 62-63        
# Average wave period
bufrSection3List.append(pack("BB", 22, 74))      # 022074       

# Octect 64-65
# Spectral peak wave period
bufrSection3List.append(pack("BB", 22, 71))      # 022071       

# Octect 66-67
# Mean Wave direction
bufrSection3List.append(pack("BB", 22, 86))      # 022086       


tmpString = "".join(bufrSection3List)
bufrSection3Length = len(tmpString) + 3
print('bufrSection3Length')
print(bufrSection3Length)

# 'L' generates a 4-byte integer, but only need 3 bytes of it
bufrSection3 = "%s%s" % ((pack(">L", bufrSection3Length)[1:]), tmpString)



############################################################    
#
#
#   BUFR SECTION 4
#
#
############################################################    

bufrSection4List = []
bufrSection4 = None
excMessage = None

############################################################    
############################################################ 
# 301087  - Moored Buoy ID {includes subsets}    
############################################################    
############################################################ 


############################################################
# 001087 - WMO Marine Observing platform extended identifier
# Numeric 23 bits
wmoIdNum = 0

WMO_ID = 46042

# TODO: Is this the correct WMO number for the buoy?

wmoIdNum = int(WMO_ID) * (2**(32-23))  # bits 32-24 are empty

# Pack WmoId into 3 bytes
wmoIdPacked = pack(">I", wmoIdNum)

# Store 2 bytes and save the third for the empty bits
bufrSection4List.append(wmoIdPacked[0:2])
unfilledByte = wmoIdPacked[2]
# Bits filled: 1111 1110


############################################################
# 001015 - Station or Site Name
# 160 bits (20 bytes)

# TODO: Is this the correct Station Name for the buoy?

model="monterey"

l = len(model)
    
# check if too short or long
if l > 20:
    model = model[0:21]
elif l < 20:
    # pad with spaces
    model = model + chr(32) * (20 - l)

(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, 7, model, 160)
bufrSection4List.append(string)
# Bits filled: 1111 1110


############################################################
# 002149 - Type of data buoy
# 6 bits - 18 = 3-metre Discus
typeBuoy = chr(18 << (8-6))  # Decimal 18, left shifted 2 bits
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, typeBuoy, 6)
bufrSection4List.append(string)
# Bits filled: 1111 1000



############################################################    
############################################################ 
# 301011  - Date {includes subsets}    
############################################################    
############################################################ 

############################################################
# 004001  - Year (12 bits)
# 004002  - Month (4 bits)
# 004003  - Day   (6 bits)
year = Year * 2 ** 20       # bits 31-21    
month = Month * 2 ** 16     # bits 20-16
day = Day * 2 ** 10         # bits 15-10

# Pack into 4-character string as unsigned integer (big-endian)
string = pack(">I", (year | month | day))
print 'Dbg|string-',":".join("{0:x}".format(ord(c)) for c in string)
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 22)
bufrSection4List.append(string)
# Bits filled: 1110 0000



############################################################    
############################################################ 
# 301012  - Time {includes subsets}    
############################################################    
############################################################ 

############################################################
# 004004  - Hour (5 bits)
# 004005  - Minute (6 bits)
hour = Hour * 2 **11      # bits 15-11    
minute = Minute * 2 ** 5    # bits 10-5
string = pack(">H", (hour | minute))
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 11)
bufrSection4List.append(string)
# Bits filled: 1111 1100



############################################################    
############################################################ 
# 301021  - Latitude & longitude (high accuracy) {includes subsets}
############################################################    
############################################################ 

############################################################    
# 005001  - Latitude (25 bits) (scale 5, reference value -9000000)
# 006001  - Longitude (26 bits) (scale 5, reference value -18000000)

latScaled = (int(Lat * 10 ** 5) + 9000000)<< (32-25) # bits 26-32 are empty
lonScaled = (int(Lon * 10 ** 5) + 18000000)<< (32-26) # bits 27-32 are empty
print 'latitude=', Lat, 'latScaled=', latScaled
print 'longitude=', Lon, 'lonScaled=', lonScaled

string = pack(">I", latScaled)
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 25)
bufrSection4List.append(string)
# Bits filled: 1111 1110

string = pack(">I", lonScaled)
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 26)
bufrSection4List.append(string)
# Bits filled: 1000 0000


   
############################################################    
############################################################    
# 306038  - STDMET for Moored Buoys {includes subsets}
############################################################    
############################################################    

############################################################ 
# 010004  - Pressure (14 bits) (scale -1, and units in Pa)
# 010051  - Pressure at sea-level (14 bits) (scale -1, and units in Pa)

# TODO: For 3-metre buoy, are Pres and Pres SL the same?

PresScaled = (int(Pres*100*10**-1))* 2 ** 18 # bits 31-18
PresslScaled = (int(Pres*100*10**-1))* 2 ** 4 # bits 17-04
string = pack(">I", (PresScaled | PresslScaled))
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 28)
bufrSection4List.append(string)
# Bits filled: 1111 1000


############################################################ 
# 007033  - Height Atmp Sensor Above Water (12 bits) (scale 1, m)

# TODO: Value obtained from the NDBC website

Hsensor=4
HsensorScaled = int(Hsensor*10**1)<<(16-12) # bits 13-16 are empty

string = pack(">H", HsensorScaled)
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 12)
bufrSection4List.append(string)
# Bits filled: 1000 0000


############################################################ 
# 012101  - Air Temperature (16 bits) (scale 2, K)
# 012103  - Dew Point (16 bits) (scale 2, K)

AtmpScaled = (int(Atmp*10**2)+273)* 2 ** 16 # bits 31-16
DewpScaled = (int(Pres*10**2)+273)* 2 ** 0 # bits 15-0

string = pack(">I", (AtmpScaled | DewpScaled))
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 32)
bufrSection4List.append(string)
# Bits filled: 1000 0000


############################################################ 
# 013103  - Relative Humidity (7 bits) (scale 0, %)

HumiScaled = int(Humi*10**0)<<(8-7)

string = pack("B", HumiScaled)
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 7)
bufrSection4List.append(string)
# Bits filled: 0000 0000


############################################################ 
# 007033  - Height Wind Sensor Above Water (12 bits) (scale 1, m)

# TODO: Value obtained from the NDBC website

Hsensor=5
HsensorScaled = int(Hsensor*10**1)<<(16-12) # bits 13-16 are empty

string = pack(">H", HsensorScaled)
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 12)
bufrSection4List.append(string)
# Bits filled: 1111 0000


############################################################ 
# 008021  - Time Significance (5 bits) (scale 1, m)

string = pack("B", 2 << (8-5)) # Decimal 2 (Time Average), left shifted 3 bits
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, typeBuoy, 5)
bufrSection4List.append(string)
# Bits filled: 1000 0000


############################################################ 
# 004025  - Time Period of Displacement (12 bits) (scale 0, minute, reference=-2048)

# TODO: I read on NDBC website that the time average for Wspd is 8 minutes

windavg=8
windavgScaled = (int(windavg) + 2048)<<(16-12) # bits 13-16 are empty

string = pack(">H", windavgScaled)
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 12)
bufrSection4List.append(string)
# Bits filled: 1111 1000


############################################################ 
# 011001  - Wind Direction (9 bits) (scale 0, degree)

WdirScaled = int(Wdir*10**0)<<(16-9) # bits 10-16 are empty

string = pack(">H", WdirScaled)
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 9)
bufrSection4List.append(string)
# Bits filled: 1111 1100


############################################################ 
# 011002  - Wind Speed (12 bits) (scale 1, m/s)

WspdScaled = int(Wspd*10**1)<<(16-12) # bits 13-16 are empty

string = pack(">H", WspdScaled)
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 12)
bufrSection4List.append(string)
# Bits filled: 1100 0000

############################################################ 
# 008021  - Time Significance (5 bits) (scale 1, m)
# Set to missing to cancel the previous value (=31)

# TODO: Missing value or not????????????

string = pack("B", 31 << (8-5)) # Decimal 31 (Missing), left shifted 3 bits
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, typeBuoy, 5)
bufrSection4List.append(string)
# Bits filled: 1000 0000

############################################################ 
# 004025  - Time Period of Displacement (12 bits) (scale 0, minute, reference=-2048)

# TODO: Do I have to use the same value for the wind speed?

windavg=8
windavgScaled = (int(windavg) + 2048)<<(16-12) # bits 13-16 are empty

string = pack(">H", windavgScaled)
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 12)
bufrSection4List.append(string)
# Bits filled: 1111 1000

############################################################ 
# 011041  - Maximum Gust Speed (12 bits) (scale 1, m/s)

GustScaled = int(Gust*10**1)<<(16-12) # bits 13-16 are empty

string = pack(">H", GustScaled)
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 12)
bufrSection4List.append(string)
# Bits filled: 1000 0000


############################################################ 
# 004025  - Time Period of Displacement (12 bits) (scale 0, minute, reference=-2048)
# Set to missing (=0 min)

# TODO: It is write in the STDMET template to set to missing. Is "zero minutes" a missing value?

windavg=0
windavgScaled = (int(windavg) + 2048)<<(16-12) # bits 13-16 are empty

string = pack(">H", windavgScaled)
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 12)
bufrSection4List.append(string)
# Bits filled: 1111 1000


############################################################ 
# 007033  - Height Wind Sensor Above Water (12 bits) (scale 1, m)
# Set to missing (=0 m) ????????? Use insted 5 meters

# TODO: I do not know to set it to missing

Hsensor=5
HsensorScaled = int(Hsensor*10**1)<<(16-12) # bits 13-16 are empty

string = pack(">H", HsensorScaled)
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 12)
bufrSection4List.append(string)
# Bits filled: 1000 0000


############################################################ 
# 002005  - Precision of Temperature (7 bits) (scale 2, K)

# TODO: I tried to find the temperature precision, but I couldn't find it

WtmpPrec=0.01
WtmpPrecScaled = int(WtmpPrec*10**2)<<(8-7) # bit 8 is empty

string = pack("B", WtmpPrecScaled)
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 7)
bufrSection4List.append(string)
# Bits filled: 0000 0000

############################################################ 
# 007063  - Depth Below Sea/Water Surface (20 bits) (scale 2, m)

# TODO: Value obtained from the NDBC website

Depth=0.7 #meters
DepthScaled = int(Depth*10**2)<< (32-20) # bits 21-32 are empty, centimeters

string = pack(">I", DepthScaled)
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 20)
bufrSection4List.append(string)
# Bits filled: 1111 0000

############################################################ 
# 022049  - Sea-surface Temperature (15 bits) (scale 2, K)

WtmpScaled = int((Wtmp+273)*10**2)<< (16-15) # bit 16 is empty

string = pack(">H", WtmpScaled)
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 15)
bufrSection4List.append(string)
# Bits filled: 1110 0000


############################################################    
############################################################    
# Wave Measurements
############################################################    
############################################################    

############################################################ 
# 022078  - Duration/Length of Wave Record (12 bits) (scale 0, seconds)

# TODO: Value obtained from the NDBC website

Duration=1200
DurationScaled = Duration << (16-12)  # bits 13-16 are empty

string = pack(">H", DurationScaled)
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 12)
bufrSection4List.append(string)
# Bits filled: 1111 1110


############################################################ 
# 022070  - Significant wave height (13 bits) (scale 2, meters)

WvhtScaled = int(Wvht*10**2)<< (16-13)  # bits 14-16 are empty

string = pack(">H", WvhtScaled)
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 13)
bufrSection4List.append(string)
# Bits filled: 1111 0000


############################################################ 
# 022074  - Average wave period (9 bits) (scale 1, seconds)

ApdScaled = int(Apd*10**1)<< (16-9)  # bits 10-16 are empty

string = pack(">H", ApdScaled)
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 9)
bufrSection4List.append(string)
# Bits filled: 1111 1000


############################################################ 
# 022071  - Spectral peak wave period (9 bits) (scale 1, seconds)

DpdScaled = int(Dpd*10**1)<< (16-9)  # bits 10-16 are empty

string = pack(">H", DpdScaled)
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 9)
bufrSection4List.append(string)
# Bits filled: 1111 1100


############################################################ 
# 022086  - Mean Direction from which waves are coming (9 bits) (scale 0, degree)

MwdScaled = int(Mwd*10**0)<< (16-9)  # bits 10-16 are empty

string = pack(">H", MwdScaled)
(string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 9)
bufrSection4List.append(string)
# Bits filled: 1111 1110

# Add any leftover bits to the output (as full Byte) 
if unfilledByteBits > 0:
    bufrSection4List.append(unfilledByte)
        
        
tmpString = "".join(bufrSection4List)

# Only 3 bytes are good for length, so shift 8 bits
bufrSection4Length = (len(tmpString) + 4) << 8
bufrSection4 = "%s%s" % ((pack(">I", bufrSection4Length)), tmpString)



#######################################################################
#  Function name:      buildBufrMessage
#    
#  Purpose:            To build a BUFR message for a specific profile
#    
#  Input variables:    NetCDF (netCDF4.Dataset)
#                      Profile Number (numpy.int16)
#    
#  Output variables:   String containing the BUFR message.
#    
#  Notes:              None.
#######################################################################

bufrMessage = []

bufrMessage.append(bufrSection1)
bufrMessage.append(bufrSection3)
bufrMessage.append(bufrSection4)
   

# Section 5
bufrMessage.append('7777')
bufrString="".join(bufrMessage)
bufrMessageLength = len(bufrString) + 8
     
# Calculate full size of message
totalMessageSize = bufrMessageLength + 21  # Add 21 for GTS Headers

################
# BUFR Section 0
################
bufrSection0List=[]

bufrSection0List.append(pack("B",66)) # letter B
bufrSection0List.append(pack("B",85)) # letter U
bufrSection0List.append(pack("B",70)) # letter F
bufrSection0List.append(pack("B",82)) # letter R

bufrSection0List.append((pack(">L", bufrMessageLength))[1:])  # include the length of the total GTS Message

bufrSection0List.append(pack("B", 4))     # BUFR Version Number (currently 4)

bufrSection0="".join(bufrSection0List)

# Write message into the archive
BUFR = open('C:\\ndbc\\bufr\\46042', 'wb')

BUFR.write(bufrSection0)
BUFR.write(bufrString)
BUFR.close()
