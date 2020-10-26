#! /usr/bin/python
'''

******* THIS IS BETA SOFTWARE ******* THIS IS BETA SOFTWARE *******
******* THIS IS BETA SOFTWARE ******* THIS IS BETA SOFTWARE *******
******* THIS IS BETA SOFTWARE ******* THIS IS BETA SOFTWARE *******

@author: Tobias Ferreira

Modified from original code Glider2Bufr, wrote by Bill Smith

Created on Aug 25, 2014

for NOAA/National Data Buoy Center

Purpose:    To create BUFR files for Moored buoys

# REVISIONS:
#   0.1 -  Alpha version 
#       -   

'''

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


def openarchive():
    
    lines = open('C:\\ndbc\\bufr\\izabel.txt', 'r')
    izab=[]
    for line in lines:
        dataline = line.strip()
        columns = dataline.split()
        izab.append(columns[:])

    return izab




############################################################    
#
#
#   BUFR SECTION 0
#
#
############################################################  

def bufrSection0(bufrMessageLength):

    bufrSection0List=[]
    
    bufrSection0List.append(pack("B",66)) # letter B
    bufrSection0List.append(pack("B",85)) # letter U
    bufrSection0List.append(pack("B",70)) # letter F
    bufrSection0List.append(pack("B",82)) # letter R
    
    bufrSection0List.append((pack(">L", bufrMessageLength))[1:])  # include the length of the total GTS Message
    
    bufrSection0List.append(pack("B", 4))     # BUFR Version Number (currently 4)
    
    Section0="".join(bufrSection0List)

    return Section0

############################################################    
#
#
#   BUFR SECTION 1
#
#
############################################################   

def bufrSection1():
    
    bufrSection1List = []
    Section1 = None
    izab=openarchive()
    # Octect 4
    # BUFR master table (zero if standard WMO FM 94 BUFR tables are used)
    bufrSection1List.append(pack("B", 0))
    
    # Octect 5-6    
    # Identification of originating/generating centre 
    # (see Common Code table C-11)
    # US National Weather Service - Other= 9
    bufrSection1List.append(pack(">H", 146))
    
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
    # Version number of master table (22=Version implemented on May 2014)
    #
    # For this CODE, you have to use the master table number 22, because the
    # table D sequences 3 01 126, 3 06 038 and 3 06 039 do Not exist in previous
    # master tables versions.
    bufrSection1List.append(pack("B", 22))
    
    # Octect 15
    # Version number of local tables
    bufrSection1List.append(pack("B", 0))
    
    
    # Octect 16-22
    # Data and time of the measurement)
    
    # Octect 16-17 (Year - 4 digits)
    bufrSection1List.append(pack(">H",int(izab[0][4])))
    # Octect 18 (Month)
    bufrSection1List.append(pack("B",int(izab[0][5])))
    # Octect 19 (Day)
    bufrSection1List.append(pack("B",int(izab[0][6])))
    # Octect 20 (Hour)
    bufrSection1List.append(pack("B",int(izab[0][7])))
    # Octect 21 (Minute)
    bufrSection1List.append(pack("B",int(izab[0][8])))
    # Octect 22 (Seconds)
    bufrSection1List.append(pack("B",0))
    
    
    #Octets 1-3
    # Length of the section 1
    tmpString = "".join(bufrSection1List)
    bufrSection1Length = len(tmpString) + 3
    
    print('bufrSection1Length')
    print(bufrSection1Length)
    
    # 'L' generates a 4-byte integer, but only need 3 bytes of it
    Section1 = "%s%s" % ((pack(">L", bufrSection1Length)[1:]), tmpString)


    return Section1

############################################################    
#
#
#   BUFR SECTION 3
#
#
############################################################   

def bufrSection3():
    
    bufrSection3List = []
    Section3 = None
    izab=openarchive()
    
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
    # Sequence for representation of moored buoy identification
    # 301126
    #
    # This sequence comprises the following sequences:
    # 
    # WMO marine observing platform extended identifier: 001087
    # bufrSection3List.append(pack("BB", 1, 87))
    #
    # Station or site name: 001015
    # bufrSection3List.append(pack("BB", 1, 15))      # 001015
    #
    #Type of data buoy: 002149
    #bufrSection3List.append(pack("BB", 2, 149))      # 002149
    #
    # Date: 301011
    #fx = (3 * 2**6) + 1 #=193 (decimal)=11000001 (binary)
    #bufrSection3List.append(pack("BB", fx, 11))
    # 
    # Time: 301012
    # fx = (3 * 2**6) + 1 #=193 (decimal)=11000001 (binary)
    # bufrSection3List.append(pack("BB", fx, 12))
    #
    # Latitude and longitude (high accuracy): 301021
    # fx = (3 * 2**6) + 1 #=193 (decimal)=11000001 (binary)
    # bufrSection3List.append(pack("BB", fx, 21))
    ################################################################# 

    # Octect 8-9
    fx = (3 * 2**6) + 1 #=193 (decimal)=11000001 (binary)
    bufrSection3List.append(pack("BB", fx, 126))      # 301126
    # WMO marine observing platform extended identifier
    

    #################################################################    
    # Octect 10-11
    # Sequence for STDMET measurements from moored buoys
    # 306038
    #
    # This sequence comprises the following sequences:
    #
    # Pressure: 010004
    #bufrSection3List.append(pack("BB", 10, 4))      # 010004
    # 
    # Pressure reduced to mean sea level: 010051
    #bufrSection3List.append(pack("BB", 10, 51))      # 010051
    #
    # Height of sensor above water surface: 007033
    # bufrSection3List.append(pack("BB", 7, 33))      # 007033
    #
    # air temperature: 012101
    # bufrSection3List.append(pack("BB", 12, 101))      # 012101
    #
    # Dew point temperature: 012103
    # bufrSection3List.append(pack("BB", 12, 103))      # 012103
    #
    # Relative humidity: 013003
    # bufrSection3List.append(pack("BB", 13, 3))      # 013003
    #
    # Height of sensor above water surface
    # bufrSection3List.append(pack("BB", 7, 33))      # 007033
    #
    # Time significance (=2, time average): 008021
    # bufrSection3List.append(pack("BB", 8, 21))      # 008021
    #
    # Time period or displacement: 004025
    # bufrSection3List.append(pack("BB", 4, 25))      # 004025
    #
    # Wind direction: 011001
    # bufrSection3List.append(pack("BB", 11, 1))      # 011001
    #
    # wind speed: 011002
    #bufrSection3List.append(pack("BB", 11, 2))      # 011002
    #
    # Time significance (=2, time average): 008021
    # bufrSection3List.append(pack("BB", 8, 21))      # 008021
    #
    # Time period or displacement
    # bufrSection3List.append(pack("BB", 4, 25))      # 004025
    # 
    # Maximum gust speed
    # bufrSection3List.append(pack("BB", 11, 41))      # 011041
    #
    # Time period or displacement (set to missing)
    #bufrSection3List.append(pack("BB", 4, 24))      # 004025
    #
    # Height of sensor above water surface (set to missing to cancel previous value): 007033
    #bufrSection3List.append(pack("BB", 7, 33))      # 007033
    #
    # Precision of temperature: 002005
    # bufrSection3List.append(pack("BB", 2, 5))      # 002005
    #
    # Depth below sea / water surface (cm): 007063
    # bufrSection3List.append(pack("BB", 7, 63))      # 007063
    #
    # Sea-surface temperature: 022049
    # bufrSection3List.append(pack("BB", 22, 49))      # 022049
    #################################################################   

    # Octect 10-11
    fx = (3 * 2**6) + 6 #=198 (decimal)=11000110 (binary)
    bufrSection3List.append(pack("BB", fx, 38))      # 306038



    #################################################################    
    # Octect 12-13
    # Sequence for representation of basic wave measurements
    # 306039
    # fx = (3 * 2**6) + 6 #=198 (decimal)=11000110 (binary)
    # bufrSection3List.append(pack("BB", fx, 39))      # 306039
    # This sequence comprises the following sequences:

    # 0 22 078: Duration of wave record
    # 0 22 070: Significant wave height
    # 0 22 073: Maximum wave height
    # 0 22 074: Average wave period
    # 0 22 071: Spectral peak wave period
    # 0 22 076: Direction from which dominant waves are coming
    # 0 22 077: Directional spread of dominant wave
    #
    # Considering that there are some parameters that are absent in the stdmet
    # file, like spread, Wmax, Wave direction of dominant wave, I will use the
    # individual representation of each sequence
    #
    #################################################################
    
    # Octect 12-13
    # Duration of wave record
    bufrSection3List.append(pack("BB", 22, 78))      # 022078
    
    # Octect 14-15        
    # Significant wave height
    bufrSection3List.append(pack("BB", 22, 70))      # 022070

    # Octect 18-19
    # Spectral peak wave period
    bufrSection3List.append(pack("BB", 22, 71))      # 022071       
    
    # Octect 20-21
    # Spectral peak wave direction
    bufrSection3List.append(pack("BB", 22, 76))      # 022086       
    
    
    tmpString = "".join(bufrSection3List)
    bufrSection3Length = len(tmpString) + 3
    print('bufrSection3Length')
    print(bufrSection3Length)
    
    # 'L' generates a 4-byte integer, but only need 3 bytes of it
    Section3 = "%s%s" % ((pack(">L", bufrSection3Length)[1:]), tmpString)

    return Section3

############################################################    
#
#
#   BUFR SECTION 4
#
# Required variables:
# wmo - wmo number of the station
# station - station reference name (for example: monterrey)
# Year: year of data collection
# Month: month of data collection
# Day: day of data collection
# Hour: Hour of data collection
# Minute: minute of data collection
# Pres,PresSL= pressure and pressure at sea-level
# Hsensor: height of atmp sensor above water
# Atmp: air temperature
# Dewp: dew-point temperature
# Humi: relative humidity
# Hsensor2: Height of Wind Sensor Above Water
# windavg: time average for Wspd
# Wdir: wind direction
# Wspd: wind speed
# Gust: wind gust speed
# wtmpprec: precision of the water temperature measurement
# depth: depth below the sea surface of the water temperature sensor
# Wtmp: water temperature
# duration: duration of wave record
# Wvht: significance wave height
# Apd: Spectral peak wave period
# Mwd: Mean Direction from which waves are coming
# 
# 
# 
# depth,Hsensor,Hsensor2,windavg,duration
#
############################################################    

def bufrSection4():

    bufrSection4List = []
    Section4 = None
    excMessage = None
    izab=openarchive()
    
############################################################    
# 
# 3 01 126  - Moored Buoy ID {includes subsets}    
#   
############################################################ 
    
    
    ############################################################
    # 001087 - WMO Marine Observing platform extended identifier
    # Numeric 23 bits
    wmoIdNum = 0
    
    WMO_ID = izab[0][2]
    
    
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
    station=izab[0][3]
    l = len(station)
        
    # check if too short or long
    if l > 20:
        station = station[0:21]
    elif l < 20:
        # pad with spaces
        station = station + chr(32) * (20 - l)
    
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, 7, station, 160)
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
    # 301011  - Date {includes subsets}    
    #    # 004001  - Year (12 bits)
    #    # 004002  - Month (4 bits)
    #    # 004003  - Day   (6 bits)
    year1 = int(izab[0][4]) * 2 ** 20       # bits 31-21    
    month1 = int(izab[0][5]) * 2 ** 16     # bits 20-16
    day1 = int(izab[0][6]) * 2 ** 10         # bits 15-10
    
    # Pack into 4-character string as unsigned integer (big-endian)
    string = pack(">I", (year1 | month1 | day1))
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 22)
    bufrSection4List.append(string)
    # Bits filled: 1110 0000
   
 
    ############################################################    
    # 301012  - Time {includes subsets}    
    #    # 004004  - Hour (5 bits)
    #    # 004005  - Minute (6 bits)
    hour1 = int(izab[0][7]) * 2 **11      # bits 15-11    
    minute1 = int(izab[0][8]) * 2 ** 5    # bits 10-5
    string = pack(">H", (hour1 | minute1))
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 11)
    bufrSection4List.append(string)
    # Bits filled: 1111 1100


    ############################################################    
    # 301021  - Latitude & longitude (high accuracy) {includes subsets}
    #    # 005001  - Latitude (25 bits) (scale 5, reference value -9000000)
    #    # 006001  - Longitude (26 bits) (scale 5, reference value -18000000)    
    lat=int(float(izab[0][0])*10**5)
    lon=int(float(izab[0][1])*10**5)
    latScaled = lat + 9000000<< (32-25) # bits 26-32 are empty
    lonScaled = lon + 18000000<< (32-26) # bits 27-32 are empty
    
    string = pack(">I", latScaled)
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 25)
    bufrSection4List.append(string)
    # Bits filled: 1111 1110
    
    string = pack(">I", lonScaled)
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 26)
    bufrSection4List.append(string)
    # Bits filled: 1000 0000
    
############################################################    
#
# 306038  - STDMET for Moored Buoys {includes subsets}
#
############################################################    
    
    ############################################################ 
    # 010004  - Pressure (14 bits) (scale -1, and units in Pa)
    # 010051  - Pressure at sea-level (14 bits) (scale -1, and units in Pa)
    
    # TODO: For 3-metre buoy, are Pres and Pres SL the same?
    Pres=int(izab[0][9])
    PresScaled = (int(Pres*100*10**-1))* 2 ** 18 # bits 31-18
    PresslScaled = (int(Pres*100*10**-1))* 2 ** 4 # bits 17-04
    string = pack(">I", (PresScaled | PresslScaled))
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 28)
    bufrSection4List.append(string)
    # Bits filled: 1111 1000
    
    
    ############################################################ 
    # 007033  - Height Atmp Sensor Above Water (12 bits) (scale 1, m)
    Hsensor=5
    HsensorScaled = int(Hsensor*10**1)<<(16-12) # bits 13-16 are empty
    
    string = pack(">H", HsensorScaled)
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 12)
    bufrSection4List.append(string)
    # Bits filled: 1000 0000
    
    
    ############################################################ 
    # 012101  - Air Temperature (16 bits) (scale 2, K)
    # 012103  - Dew Point (16 bits) (scale 2, K)
    Atmp=float(izab[0][11])
    Dewp=float(izab[0][12])
    AtmpScaled = (int(Atmp*10**2)+273)* 2 ** 16 # bits 31-16
    DewpScaled = (int(Dewp*10**2)+273)* 2 ** 0 # bits 15-0
    
    string = pack(">I", (AtmpScaled | DewpScaled))
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 32)
    bufrSection4List.append(string)
    # Bits filled: 1000 0000
    
    
    ############################################################ 
    # 013103  - Relative Humidity (7 bits) (scale 0, %)
    Humi=float(izab[0][13])
    HumiScaled = int(Humi*10**0)<<(8-7)
    
    string = pack("B", HumiScaled)
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 7)
    bufrSection4List.append(string)
    # Bits filled: 0000 0000
    
    
    ############################################################ 
    # 007033  - Height Wind Sensor Above Water (12 bits) (scale 1, m)

    Hsensor2=5
    HsensorScaled = int(Hsensor2*10**1)<<(16-12) # bits 13-16 are empty
    
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
    windavg=float(izab[0][15])
    windavgScaled = (int(windavg) + 2048)<<(16-12) # bits 13-16 are empty
    
    string = pack(">H", windavgScaled)
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 12)
    bufrSection4List.append(string)
    # Bits filled: 1111 1000
    
    
    ############################################################ 
    # 011001  - Wind Direction (9 bits) (scale 0, degree)
    Wdir=float(izab[0][16])
    WdirScaled = int(Wdir*10**0)<<(16-9) # bits 10-16 are empty
    
    string = pack(">H", WdirScaled)
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 9)
    bufrSection4List.append(string)
    # Bits filled: 1111 1100
    
    
    ############################################################ 
    # 011002  - Wind Speed (12 bits) (scale 1, m/s)
    Wspd=float(izab[0][17])
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
    
    # TODO: Do I use the same value used for the wind speed?
    windavg=0
    windavgScaled = (int(windavg) + 2048)<<(16-12) # bits 13-16 are empty
    
    string = pack(">H", windavgScaled)
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 12)
    bufrSection4List.append(string)
    # Bits filled: 1111 1000
    
    ############################################################ 
    # 011041  - Maximum Gust Speed (12 bits) (scale 1, m/s)
    Gust=0
    GustScaled = int(Gust*10**1)<<(16-12) # bits 13-16 are empty
    
    string = pack(">H", GustScaled)
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 12)
    bufrSection4List.append(string)
    # Bits filled: 1000 0000
    
    
    ############################################################ 
    # 004025  - Time Period of Displacement (12 bits) (scale 0, minute, reference=-2048)
    # Set to missing (=0 min) ??????? Use instead windavg
    
    # TODO: It is write in the STDMET template to set to missing. Is "zero minutes" a missing value?
    
    windavg=0
    windavgScaled = (int(windavg) + 2048)<<(16-12) # bits 13-16 are empty
    
    string = pack(">H", windavgScaled)
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 12)
    bufrSection4List.append(string)
    # Bits filled: 1111 1000
    
    
    ############################################################ 
    # 007033  - Height Wind Sensor Above Water (12 bits) (scale 1, m)
    # Set to missing (=0 m) ????????? Use instead Hsensor
    
    # TODO: It is write in the STDMET template to set to missing. How do I do that?
    Hsensor=5
    HsensorScaled = int(Hsensor*10**1)<<(16-12) # bits 13-16 are empty
    
    string = pack(">H", HsensorScaled)
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 12)
    bufrSection4List.append(string)
    # Bits filled: 1000 0000
    
    
    ############################################################ 
    # 002005  - Precision of Temperature (7 bits) (scale 2, K)
    wtmpprec=0.01
    WtmpPrecScaled = int(wtmpprec*10**2)<<(8-7) # bit 8 is empty
    
    string = pack("B", WtmpPrecScaled)
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 7)
    bufrSection4List.append(string)
    # Bits filled: 0000 0000
    
    ############################################################ 
    # 007063  - Depth Below Sea/Water Surface (20 bits) (scale 2, m)
    depth=100
    DepthScaled = int(depth*10**2)<< (32-20) # bits 21-32 are empty, centimeters
    
    string = pack(">I", DepthScaled)
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 20)
    bufrSection4List.append(string)
    # Bits filled: 1111 0000
    
    ############################################################ 
    # 022049  - Sea-surface Temperature (15 bits) (scale 2, K)
    Wtmp=float(izab[0][21])
    WtmpScaled = int((Wtmp+273)*10**2)<< (16-15) # bit 16 is empty
    
    string = pack(">H", WtmpScaled)
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 15)
    bufrSection4List.append(string)
    # Bits filled: 1110 0000
    
    
############################################################    
#   
# Wave Measurements
#
############################################################    
    
    ############################################################ 
    # 022078  - Duration/Length of Wave Record (12 bits) (scale 0, seconds)
    duration=int(izab[0][22])
    DurationScaled = duration << (16-12)  # bits 13-16 are empty
    
    string = pack(">H", DurationScaled)
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 12)
    bufrSection4List.append(string)
    # Bits filled: 1111 1110
    
    
    ############################################################ 
    # 022070  - Significant wave height (13 bits) (scale 2, meters)
    Wvht=float(izab[0][23])
    WvhtScaled = int(Wvht*10**2)<< (16-13)  # bits 14-16 are empty
    
    string = pack(">H", WvhtScaled)
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 13)
    bufrSection4List.append(string)
    # Bits filled: 1111 0000
    
    
    ############################################################ 
    # 022071  - Spectral peak wave period (9 bits) (scale 1, seconds)
    Dpd=float(izab[0][24])
    DpdScaled = int(Dpd*10**1)<< (16-9)  # bits 10-16 are empty
    
    string = pack(">H", DpdScaled)
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 9)
    bufrSection4List.append(string)
    # Bits filled: 1111 1000
    
    
    ############################################################ 
    # 022086  - Mean Direction from which waves are coming (9 bits) (scale 0, degree)
    Mwd=float(izab[0][25])
    MwdScaled = int(Mwd*10**0)<< (16-9)  # bits 10-16 are empty
    
    string = pack(">H", MwdScaled)
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 9)
    bufrSection4List.append(string)
    # Bits filled: 1111 1100

############################################################    
#   
# Leftover bits and lenght of section 4
#
############################################################  

    # Add any leftover bits to the output (as full Byte) 
    if unfilledByteBits > 0:
        bufrSection4List.append(unfilledByte)
            
    tmpString = "".join(bufrSection4List)
    
    # Only 3 bytes are good for length, so shift 8 bits
    # Octets 1-3 (length) and Octet 4 (set to ZERO: reserved)
    bufrSection4Length = (len(tmpString) + 4) << 8
    Section4 = "%s%s" % ((pack(">I", bufrSection4Length)), tmpString)

    return Section4

#######################################################################
#  Function name:      binString
#    
#  Purpose:            Convert string to binary string of '1's and '0's.
#    
#  Input variables:    string (string)
#    
#  Output variables:   String containing binary representation in ASCII code
#    
#  Notes:              Sample input: 'ABC'
#                      Sample output: '010000010100001001000011'
#######################################################################
def binString(string):
    
    binaryString = []
    
    for ch in string:
        num = ord(ch)
        binStr = numpy.binary_repr(num, 8)
        binaryString.append(binStr)
        
    return "".join(binaryString)

#######################################################################
#  Function name:      bitShift
#    
#  Purpose:            To bit-shift a string for bit packing
#    
#  Input variables:    startByte (string)
#                      startByteBits (int, bits used in startByte)
#                      string (string)
#                      stringBits (int, bits used in stringru29-273_20130131T170415_rt0.nc.cdl)
#    
#  Output variables:   String containing shifted bits
#                      Byte containing leftover (partial) byte
#    
#  Notes:              None.
#######################################################################
def bitShift(startByte, startByteBits, string, stringBits):
    newStringList = []
    
    if len(startByte) != 1:
        raise Exception("Error in bitShift - length of startByte must be 1!")

    if startByteBits < 0: 
        raise Exception("Error in bitShift - startByteBits must be positive!")
    elif startByteBits > 7:
        raise Exception("Error in bitShift - startByteBits must be < 8!")

    #TODO: ADD CHECKS
    newLeftover = binString(startByte)[:startByteBits]
    newStringList.append(newLeftover)

   
    bitsConverted = 0
     
    for ch in string:
        bitStr = binString(ch)
        bitsConverted += 8
        
        if bitsConverted <= stringBits:
            newStringList.append(bitStr)
        else:
            remainingBits = stringBits % 8
            newStringList.append(bitStr[:remainingBits])
            break

    newString = ''.join(newStringList)
    print newString
    
    wholeBytes = len(newString) / 8
    print 'wholeBytes=', wholeBytes
    
    newPacked = []
    for i in range(wholeBytes):
        j = i * 8
        k = j + 8
        byte = newString[j:k]
        newPacked.append(chr(int(byte,2)))

    leftoverBits = (len(newString) % 8)
    
    if leftoverBits == 0:
        newLeftover = chr(0)
    else:
        newLeftoverString = newString[(-leftoverBits):] + '0' * (8 - leftoverBits)
        print 'newLeftoverString=', newLeftoverString
        newLeftover = pack("B", int(newLeftoverString, 2))
                
    newString = ''.join(newPacked)

    
    return (newString, newLeftover, leftoverBits)



########################################################
#
# BEGINING OF THE RUNNING CODE
#
########################################################

(Section1)=bufrSection1()

(Section3)=bufrSection3()

(Section4)=bufrSection4()

#Put the sections together
bufrMessage = []

bufrMessage.append(Section1)
bufrMessage.append(Section3)
bufrMessage.append(Section4)

#Juntando a Secao 5
bufrMessage.append('7777')
bufrString="".join(bufrMessage)
bufrMessageLength = len(bufrString) + 8

# BUFR Section 0
(Section0)=bufrSection0(bufrMessageLength)


# Write message into the archive



BUFR = open('gts_message', 'wb')

BUFR.write(Section0)
BUFR.write(bufrString)
BUFR.close()
    
