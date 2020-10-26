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
from xml.dom import minidom
from struct import pack #@UnresolvedImport
from bitShift import *

wmoId = 46042
name = 'remobs01'
buoy_type = 18 #3M Discus
h_sensor_pres = 0
h_sensor_atmp = 4
h_sensor_wind = 4.7
windavg=10
WtmpPrec=0.01
Depth_atmp =0.7 #meters
Duration_wave=1200


lines = open('4604232014_test.txt', 'rb')

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
bufrSection1List.append(np.binary_repr(0, 8))

# Octect 5
# Identification of originating/generating sub-centre
# (see Common Code table C-11)
# CHM - Other
bufrSection1List.append(np.binary_repr(0, 8))

# Octect 6
# Identification of originating/generating centre
# (see Common Code table C-11)
# CHM - Other
bufrSection1List.append(np.binary_repr(146, 8))

# Octect 7
# Update sequence number (zero for original BUFR message)
bufrSection1List.append(np.binary_repr(0, 8))

# Octect 8
# Code for presence of optional section (2), 0=Not present
bufrSection1List.append(np.binary_repr(0, 8))

# Octect 9
# Code for Data Category (1=Surface Data-Sea)
bufrSection1List.append(np.binary_repr(1, 8))

# Octect 10
# Code for International data sub-category (25=buoy observations)
bufrSection1List.append(np.binary_repr(25, 8))

# Octect 11
# Version number of master table (18=Version implemented on May 2012)
bufrSection1List.append(np.binary_repr(18, 8))

# Octect 12
# Version number of local tables
bufrSection1List.append(np.binary_repr(0, 8))

# Octect 13-17
# Data and time of the measurement)

# Octect 13 (Year - of the century)
bufrSection1List.append(np.binary_repr(Year - 2000, 8))
# Octect 14 (Month)
bufrSection1List.append(np.binary_repr(Month, 8))
# Octect 15 (Day)
bufrSection1List.append(np.binary_repr(Day, 8))
# Octect 16 (Hour)
bufrSection1List.append(np.binary_repr(Hour, 8))
# Octect 17 (Minute)
bufrSection1List.append(np.binary_repr(Minute, 8))

# Octect 18 - shall be included and set to zero
bufrSection1List.append(np.binary_repr(0, 8))


#Octets 1-3
# Length of the section 1
tmpString = "".join(bufrSection1List)
bufrSection1Length = int(len(tmpString)/8 + 3)

print('bufrSection1Length')
print(bufrSection1Length)

# 'L' generates a 4-byte integer, but only need 3 bytes of it
bufrSection1 = "%s%s" % (np.binary_repr(bufrSection1Length, 8 * 3), tmpString)


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
bufrSection3List.append(np.binary_repr(0, 8))

# Octect 5-6
# Number of data subsets
bufrSection3List.append(np.binary_repr(1, 16))

# Octect 7
# Observed data (Bit1=1), uncompressed (Bit2=0), Bit3-8=0 (reserved). 10000000=128
bufrSection3List.append(np.binary_repr(128, 8))

#############################

# Octect 8-9
bufrSection3List.append(np.binary_repr(3, 2))
bufrSection3List.append(np.binary_repr(15, 6))
bufrSection3List.append(np.binary_repr(8, 8)) # 3 15 008


tmpString = "".join(bufrSection3List)
bufrSection3Length = int(len(tmpString)/8 + 3)

print('bufrSection3Length')
print(bufrSection3Length)

# 'L' generates a 4-byte integer, but only need 3 bytes of it
bufrSection3 = "%s%s" % (np.binary_repr(bufrSection3Length, 8 * 3), tmpString)


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
bufrSection4List.append(np.binary_repr(wmoid, 23))

############################################################
# 001015 - Station or Site Name
# 160 bits (20 bytes)
bufrSection4List.append(np.binary_repr(name, 160))

############################################################
# 002149 - Type of data buoy
# 6 bits - 18 = 3-metre Discus
bufrSection4List.append(np.binary_repr(buoy_type, 6))

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
bufrSection4List.append(np.binary_repr(Year, 12))
bufrSection4List.append(np.binary_repr(Month, 4))
bufrSection4List.append(np.binary_repr(Day, 6))

############################################################
############################################################
# 301012  - Time {includes subsets}
############################################################
############################################################

############################################################
# 004004  - Hour (5 bits)
# 004005  - Minute (6 bits)
bufrSection4List.append(np.binary_repr(Hour, 5))
bufrSection4List.append(np.binary_repr(Minute, 6))

# Bits filled: 1111 1100

############################################################
############################################################
# 301021  - Latitude & longitude (high accuracy) {includes subsets}
############################################################
############################################################

############################################################
# 005001  - Latitude (25 bits) (scale 5, reference value -9000000)
# 006001  - Longitude (26 bits) (scale 5, reference value -18000000)

latScaled = (int(Lat * 10 ** 5) + 9000000) # bits 26-32 are empty
lonScaled = (int(Lon * 10 ** 5) + 18000000) # bits 27-32 are empty
print ('latitude=', Lat, 'latScaled=', latScaled)
print ('longitude=', Lon, 'lonScaled=', lonScaled)

bufrSection4List.append(np.binary_repr(latScaled, 25))
bufrSection4List.append(np.binary_repr(lonScaled, 26))

############################################################
############################################################
# 306038  - STDMET for Moored Buoys {includes subsets}
############################################################
############################################################

############################################################
# 010004  - Pressure (14 bits) (scale -1, and units in Pa (mb * 100))
# Null = 0
bufrSection4List.append(np.binary_repr(0, 14))

# 010051  - Pressure at sea-level (14 bits) (scale -1, and units in Pa)
bufrSection4List.append(np.binary_repr(Pres * 100 * 0.1, 14))

############################################################
# 007033  - Height pres Sensor Above Water (12 bits) (scale 1, m)

# TODO: Value obtained from the NDBC website
bufrSection4List.append(np.binary_repr(h_sensor_pres * 10, 12))

############################################################
# 012101  - Air Temperature (16 bits) (scale 2, K)
bufrSection4List.append(np.binary_repr(int(Atmp * 10**2)+273, 16))

# 012103  - Dew Point (16 bits) (scale 2, K)
bufrSection4List.append(np.binary_repr(int(Dewp * 10**2)+273, 16))

############################################################
# 013103  - Relative Humidity (7 bits) (scale 0, %)
bufrSection4List.append(np.binary_repr(Humi, 7))

############################################################
# 007033  - Height atmp Above Water (12 bits) (scale 1, m)
bufrSection4List.append(np.binary_repr(int(h_sensor_atmp * 10**1), 12))

############################################################
# 008021  - Time Significance (5 bits) (scale 1, m)
bufrSection4List.append(np.binary_repr(2, 5))

############################################################
# 004025  - Time Period of Displacement (12 bits) (scale 0, minute, reference=-2048)
windavgScaled = (int(windavg) + 2048)
bufrSection4List.append(np.binary_repr(windavgScaled, 12))

############################################################
# 011001  - Wind Direction (9 bits) (scale 0, degree)
bufrSection4List.append(np.binary_repr(wdir, 9))

############################################################
# 011002  - Wind Speed (12 bits) (scale 1, m/s)
bufrSection4List.append(np.binary_repr(wspd * 10, 12))

############################################################
# 008021  - Time Significance (5 bits) (scale 1, m)
# Set to missing to cancel the previous value (=31)
bufrSection4List.append(np.binary_repr(2, 5))

############################################################
# 004025  - Time Period of Displacement (12 bits) (scale 0, minute, reference=-2048)
windavgScaled = (int(windavg) + 2048)
bufrSection4List.append(np.binary_repr(windavgScaled, 12))

############################################################
# 011041  - Maximum Gust Speed (12 bits) (scale 1, m/s)
bufrSection4List.append(np.binary_repr(Gust * 10, 12))

############################################################
# 004025  - Time Period of Displacement (12 bits) (scale 0, minute, reference=-2048)
# Set to missing (=0 min)
bufrSection4List.append(np.binary_repr(0 + 2048, 12))

############################################################
# 007033  - Height Wind Sensor Above Water (12 bits) (scale 1, m)
# Set to missing (=0 m) ????????? Use insted 5 meters

# TODO: I do not know to set it to missing
bufrSection4List.append(np.binary_repr(int(h_sensor_wind * 10**1), 12))

############################################################
# 002005  - Precision of Temperature (7 bits) (scale 2, K)

# TODO: I tried to find the temperature precision, but I couldn't find it

WtmpPrecScaled = int(WtmpPrec*10**2) # bit 8 is empty
bufrSection4List.append(np.binary_repr(WtmpPrecScaled, 7))

############################################################
# 007063  - Depth Below Sea/Water Surface (20 bits) (scale 2, m)

# TODO: Value obtained from the NDBC website

DepthScaled = int(Depth_atmp * 10**2) # bits 21-32 are empty, centimeters
bufrSection4List.append(np.binary_repr(DepthScaled, 20))

############################################################
# 022049  - Sea-surface Temperature (15 bits) (scale 2, K)

WtmpScaled = int((Wtmp+273)*10**2) # bit 16 is empty
bufrSection4List.append(np.binary_repr(WtmpScaled, 15))


############################################################
# 101000  - Delayed descriptor replacation
bufrSection4List.append(np.binary_repr(0, 1))

# 031001  - Delayed descriptor replacation
bufrSection4List.append(np.binary_repr(0, 1))


############################################################
# 302091  (Sequence for representation of ancillary meteorological observations)
############################################################
############################################################

# 020001 - Horizontal visibility (Operational)
bufrSection4List.append(np.binary_repr(0, 13))

# 004024 - Time period or displacement (Operational)
bufrSection4List.append(np.binary_repr(-2048, 12))

# 013011 - Total precipitation/total water equivalent (Operational)
bufrSection4List.append(np.binary_repr(-1, 14))

############################################################
# 101000  - Delayed descriptor replacation
bufrSection4List.append(np.binary_repr(0, 1))

# 031001  - Delayed descriptor replacation
bufrSection4List.append(np.binary_repr(0, 1))


############################################################
# 302082 (Radiation data)
############################################################
############################################################


# 004025 - Time period or displacement (Operational)
bufrSection4List.append(np.binary_repr(-2048, 12))


# 014002 - Long-wave radiation, integrated over period specified (Operational)
bufrSection4List.append(np.binary_repr(-65536 * 0.001, 17))

# 014004 - Short-wave radiation, integrated over period specified (Operational)
bufrSection4List.append(np.binary_repr(-65536 * 0.001, 17))


# 014016 - Net radiation, integrated over period specified (Operational)
bufrSection4List.append(np.binary_repr(-16384 * 0.0001, 15))

# 014028 - Global solar radiation (high accuracy), integrated over period specified (Operational)
bufrSection4List.append(np.binary_repr(0 * 0.01, 20))

# 014029 - Diffuse solar radiation (high accuracy), integrated over period specified (Operational)
bufrSection4List.append(np.binary_repr(0 * 0.01, 20))


# 014030 - Direct solar radiation (high accuracy), integrated over period specified (Operational)
bufrSection4List.append(np.binary_repr(0 * 0.01, 20))

############################################################
# 101000  - Delayed descriptor replacation
bufrSection4List.append(np.binary_repr(0, 1))

# 031001  - Delayed descriptor replacation
bufrSection4List.append(np.binary_repr(0, 1))




############################################################
############################################################
# Wave Measurements
############################################################
############################################################
# 306039  (Sequence for representation of basic wave measurements)
############################################################
############################################################


############################################################
# 022078  - Duration/Length of Wave Record (12 bits) (scale 0, seconds)

# TODO: Value obtained from the NDBC website

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



# 022073 - Maximum wave height (Operational)


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



# 022076 - Direction from which dominant waves are coming (Operational)

# 022077 - Directional spread of dominant wave (Operational)


############################################################
# 101000  - Delayed descriptor replacation
bufrSection4List.append(np.binary_repr(0, 1))

# 031001  - Delayed descriptor replacation
bufrSection4List.append(np.binary_repr(0, 1))




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
bufrSection0List.append(pack("B", 85)) # letter U
bufrSection0List.append(pack("B",70)) # letter F
bufrSection0List.append(pack("B", 82)) # letter R

bufrSection0List.append((pack(">L", bufrMessageLength))[1:])  # include the length of the total GTS Message

bufrSection0List.append(pack("B", 4))     # BUFR Version Number (currently 4)

bufrSection0="".join(bufrSection0List)

# Write message into the archive
BUFR = open('46042', 'wb')

BUFR.write(bufrSection0)
BUFR.write(bufrString)
BUFR.close()
