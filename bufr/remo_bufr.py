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

    section0="".join(bufrSection0List)

    return section0


############################################################
#
#
#   BUFR SECTION 1
#
#
############################################################

def bufrSection1():


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
    section1 = "%s%s" % (np.binary_repr(bufrSection1Length, 8 * 3), tmpString)

    return section1

############################################################
#
#
#   BUFR SECTION 3
#
#
############################################################

def bufrSection3():

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
    bufrSection3List.append(np.binary_repr(1, 6))
    bufrSection3List.append(np.binary_repr(126, 8)) # 3 01 126

    # Octect 10-11
    bufrSection3List.append(np.binary_repr(3, 2))
    bufrSection3List.append(np.binary_repr(6, 6))
    bufrSection3List.append(np.binary_repr(38, 8)) # 3 06 038

    # Octect 12-13
    bufrSection3List.append(np.binary_repr(3, 2))
    bufrSection3List.append(np.binary_repr(6, 6))
    bufrSection3List.append(np.binary_repr(39, 8)) # 3 06 039

    # Octect 14-15
    bufrSection3List.append(np.binary_repr(3, 2))
    bufrSection3List.append(np.binary_repr(6, 6))
    bufrSection3List.append(np.binary_repr(5, 8)) # 3 06 005



    tmpString = "".join(bufrSection3List)
    bufrSection3Length = int(len(tmpString)/8 + 3)

    print('bufrSection3Length')
    print(bufrSection3Length)

    # 'L' generates a 4-byte integer, but only need 3 bytes of it
    section3 = "%s%s" % (np.binary_repr(bufrSection3Length, 8 * 3), tmpString)

    return section3

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

def bufrSection4(df):

    bufrSection4List = []
    bufrSection4 = None
    excMessage = None

    ############################################################
    #
    # 3 01 126  - Moored Buoy ID {includes subsets}
    #
    ############################################################

    ############################################################
    # 001087 - WMO Marine Observing platform extended identifier
    # Numeric 23 bits
    bufrSection4List.append(np.binary_repr(df['wmo_number'], 23))

    ############################################################
    # 001015 - Station or Site Name
    # 160 bits (20 bytes)
    bufrSection4List.append(np.binary_repr(df['name_buoy'], 160))

    ############################################################
    # 002149 - Type of data buoy
    # 6 bits - 18 = 3-metre Discus
    if df['model'] = "3M":
        bufrSection4List.append(np.binary_repr(18, 6))

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
    bufrSection4List.append(np.binary_repr(df.index.year, 12))
    bufrSection4List.append(np.binary_repr(df.index.month, 4))
    bufrSection4List.append(np.binary_repr(df.index.day, 6))

    ############################################################
    ############################################################
    # 301012  - Time {includes subsets}
    ############################################################
    ############################################################

    ############################################################
    # 004004  - Hour (5 bits)
    # 004005  - Minute (6 bits)
    bufrSection4List.append(np.binary_repr(df.index.hour, 5))
    bufrSection4List.append(np.binary_repr(df.index.minute, 6))

    # Bits filled: 1111 1100

    ############################################################
    ############################################################
    # 301021  - Latitude & longitude (high accuracy) {includes subsets}
    ############################################################
    ############################################################

    ############################################################
    # 005001  - Latitude (25 bits) (scale 5, reference value -9000000)
    # 006001  - Longitude (26 bits) (scale 5, reference value -18000000)

    latScaled = (int(df['lat'] * 10 ** 5) + 9000000) # bits 26-32 are empty
    lonScaled = (int(df['lon'] * 10 ** 5) + 18000000) # bits 27-32 are empty

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
    bufrSection4List.append(np.binary_repr(df['pres'] * 100 * 0.1, 14))

    # 010051  - Pressure at sea-level (14 bits) (scale -1, and units in Pa)
    bufrSection4List.append(np.binary_repr(df['pres'] * 100 * 0.1, 14))

    ############################################################
    # 007033  - Height pres Sensor Above Water (12 bits) (scale 1, m)

    # TODO: Value obtained from the NDBC website
    bufrSection4List.append(np.binary_repr(df['h_pres'] * 10, 12))

    ############################################################
    # 012101  - Air Temperature (16 bits) (scale 2, K)
    bufrSection4List.append(np.binary_repr(int(df['atmp'] * 10**2)+273, 16))

    # 012103  - Dew Point (16 bits) (scale 2, K)
    bufrSection4List.append(np.binary_repr(int(df['dewpt'] * 10**2)+273, 16))

    ############################################################
    # 013103  - Relative Humidity (7 bits) (scale 0, %)
    bufrSection4List.append(np.binary_repr(df['rh'], 7))

    ############################################################
    # 007033  - Height atmp Above Water (12 bits) (scale 1, m)
    bufrSection4List.append(np.binary_repr(int(df['h_atmp'] * 10**1), 12))

    ############################################################
    # 008021  - Time Significance (5 bits) (scale 1, m)
    bufrSection4List.append(np.binary_repr(2, 5))

    ############################################################
    # 004025  - Time Period of Displacement (12 bits) (scale 0, minute, reference=-2048)
    windavgScaled = (int(df['avg_atmp']) + 2048)
    bufrSection4List.append(np.binary_repr(windavgScaled, 12))

    ############################################################
    # 011001  - Wind Direction (9 bits) (scale 0, degree)
    bufrSection4List.append(np.binary_repr(df['wdir'], 9))

    ############################################################
    # 011002  - Wind Speed (12 bits) (scale 1, m/s)
    bufrSection4List.append(np.binary_repr(df['wspd'] * 10, 12))

    ############################################################
    # 008021  - Time Significance (5 bits) (scale 1, m)
    # Set to missing to cancel the previous value (=31)
    bufrSection4List.append(np.binary_repr(2, 5))

    ############################################################
    # 004025  - Time Period of Displacement (12 bits) (scale 0, minute, reference=-2048)
    windavgScaled = (int(df['avg_wind']) + 2048)
    bufrSection4List.append(np.binary_repr(windavgScaled, 12))

    ############################################################
    # 011041  - Maximum Gust Speed (12 bits) (scale 1, m/s)
    bufrSection4List.append(np.binary_repr(df['gust'] * 10, 12))

    ############################################################
    # 004025  - Time Period of Displacement (12 bits) (scale 0, minute, reference=-2048)
    # Set to missing (=0 min)
    bufrSection4List.append(np.binary_repr(df['avg_gust'] + 2048, 12))

    ############################################################
    # 007033  - Height Wind Sensor Above Water (12 bits) (scale 1, m)
    # Set to missing (=0 m) ????????? Use insted 5 meters

    # TODO: I do not know to set it to missing
    bufrSection4List.append(np.binary_repr(int(df['h_wind'] * 10**1), 12))

    ############################################################
    # 002005  - Precision of Temperature (7 bits) (scale 2, K)

    # TODO: I tried to find the temperature precision, but I couldn't find it

    WtmpPrecScaled = int(df['prec_sst']*10**2) # bit 8 is empty
    bufrSection4List.append(np.binary_repr(WtmpPrecScaled, 7))

    ############################################################
    # 007063  - Depth Below Sea/Water Surface (20 bits) (scale 2, m)

    # TODO: Value obtained from the NDBC website

    DepthScaled = int(df['d_sst'] * 10**2) # bits 21-32 are empty, centimeters
    bufrSection4List.append(np.binary_repr(DepthScaled, 20))

    ############################################################
    # 022049  - Sea-surface Temperature (15 bits) (scale 2, K)

    WtmpScaled = int((df['sst']+273)*10**2) # bit 16 is empty
    bufrSection4List.append(np.binary_repr(WtmpScaled, 15))


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
    bufrSection4List.append(np.binary_repr(df['avg_wave'], 12))

    ############################################################
    # 022070  - Significant wave height (13 bits) (scale 2, meters)

    WvhtScaled = int(df['swvht1']*10**2)  # bits 14-16 are empty
    bufrSection4List.append(np.binary_repr(WvhtScaled, 13))

    # 022073 - Maximum wave height (Operational)
    bufrSection4List.append(np.binary_repr(df['mxwvht1']*10**2, 13))


    ############################################################
    # 022074  - Average wave period (9 bits) (scale 1, seconds)

    ApdScaled = int(0*10**1)  # bits 10-16 are empty
    bufrSection4List.append(np.binary_repr(ApdScaled, 9))

    ############################################################
    # 022071  - Spectral peak wave period (9 bits) (scale 1, seconds)

    DpdScaled = int(df['tp1']*10**1)  # bits 10-16 are empty
    bufrSection4List.append(np.binary_repr(DpdScaled, 9))
    # Bits filled: 1111 1100


    # 022076 - Direction from which dominant waves are coming (Operational)
    bufrSection4List.append(np.binary_repr(0, 9))

    # 022077 - Directional spread of dominant wave (Operational)
    bufrSection4List.append(np.binary_repr(0, 9))


    ############################################################
    ###Currents measurements
    ############################################################
    ############################################################
    # 306005  (Sequence for representation of basic current measurements)
    ############################################################
    ############################################################


    # 002031 - DURATION AND TIME OF CURRENT MEASUREMENT
    # 4 = AVERAGED OVER MORE THAN 6 MINUTES, BUT 12 AT THE MOST
    bufrSection4List.append(np.binary_repr(4, 5))

    # 103000 - DELAYED REPLICATION OF 3DESCRIPTORS
    bufrSection4List.append(np.binary_repr(0, 8))

    # 031001 - DELAYED DESCRIPTOR REPLICATION FACTOR
    bufrSection4List.append(np.binary_repr(0, 8))



    # 007062 - DEPTH BELOW SEA/WATER SURFACE
    bufrSection4List.append(np.binary_repr(df['d_curr'] * 10, 17))

    # 022004 - DIRECTION OF CURRENT
    bufrSection4List.append(np.binary_repr(df['cdir1'], 9))

    # 022031 - SPEED OF CURRENT
    bufrSection4List.append(np.binary_repr(df['cspd1'] * 10 ** 2, 13))

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

    return section4


############################################################
#
# Leftover bits and lenght of section 4
#
############################################################

    tmpString = "".join(bufrSection4List)
    bufrSection4Length = int(len(tmpString)/8 + 3)

    print('bufrSection3Length')
    print(bufrSection4Length)

    # 'L' generates a 4-byte integer, but only need 3 bytes of it
    section4 = "%s%s" % (np.binary_repr(bufrSection4Length, 8 * 3), tmpString)

    return section4


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
