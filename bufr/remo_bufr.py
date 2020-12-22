# -*- coding: utf-8 -*-
"""
Created on Wed Jun 11 09:51:37 2014

@author: soutobias
"""

import datetime
import numpy as np
import math
from datetime import datetime, timedelta
import pandas as pd


############################################################
#
#
#   BUFR SECTION 0
#
#
############################################################

def bufrSection0(bufrMessageLength):

    bufrSection0List=[]

    #octet 1 - Letter B
    bufrSection0List.append(np.binary_repr(66, 8))

    #octet 2 - Letter U
    bufrSection0List.append(np.binary_repr(85, 8))

    #octet 3 - Letter F
    bufrSection0List.append(np.binary_repr(70, 8))

    #octet 4 - Letter R
    bufrSection0List.append(np.binary_repr(82, 8))

    #octet 5 - 7 -  include the length of the total GTS Message include section0
    bufrSection0List.append(np.binary_repr(int(bufrMessageLength) + 8, 8 * 3))

    #octet 8 -  # BUFR Version Number (currently 4)
    bufrSection0List.append(np.binary_repr(4, 8)) # letter R

    section0 = "".join(bufrSection0List)

    return section0


############################################################
#
#
#   BUFR SECTION 1
#
#
############################################################

def bufrSection1(df):

    bufrSection1List = []

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

    # Data and time of the measurement)
    # Octect 13 (Year - of the century)
    bufrSection1List.append(np.binary_repr(datetime.now().year - 2000, 8))
    # Octect 14 (Month)
    bufrSection1List.append(np.binary_repr(datetime.now().month, 8))
    # Octect 15 (Day)
    bufrSection1List.append(np.binary_repr(datetime.now().day, 8))
    # Octect 16 (Hour)
    bufrSection1List.append(np.binary_repr(datetime.now().hour, 8))
    # Octect 17 (Minute)
    bufrSection1List.append(np.binary_repr(datetime.now().minute, 8))

    # Octect 18 - shall be included and set to zero
    bufrSection1List.append(np.binary_repr(0, 8))

    #Octets 1-3
    # Length of the section 1
    bufrSection1Length =  int(len(bufrSection1List) / 8 + 3)
    section1 = np.binary_repr(bufrSection1Length, 8 * 3) + ''.join(bufrSection1List)

    return section1

############################################################
#
#
#   BUFR SECTION 3
#
#
############################################################

def bufrSection3(df):

    bufrSection3List = []

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
    if df['model'][0] == "SPOT-0222":
        bufrSection3List.append(np.binary_repr(3, 2))
        bufrSection3List.append(np.binary_repr(2, 6))
        bufrSection3List.append(np.binary_repr(42, 8)) # 3 02 042
    else:
        bufrSection3List.append(np.binary_repr(3, 2))
        bufrSection3List.append(np.binary_repr(6, 6))
        bufrSection3List.append(np.binary_repr(38, 8)) # 3 06 038

    # Octect 12-13
    if df['model'][0] == "SPOT-0222":
        bufrSection3List.append(np.binary_repr(3, 2))
        bufrSection3List.append(np.binary_repr(2, 6))
        bufrSection3List.append(np.binary_repr(21, 8)) # 3 02 021
    else:
        bufrSection3List.append(np.binary_repr(3, 2))
        bufrSection3List.append(np.binary_repr(6, 6))
        bufrSection3List.append(np.binary_repr(39, 8)) # 3 06 039

    # Octect 14-15
    if df['model'][0] == "SPOT-0222":
        bufrSection3List.append(np.binary_repr(3, 2))
        bufrSection3List.append(np.binary_repr(2, 6))
        bufrSection3List.append(np.binary_repr(56, 8)) # 3 02 056
    else:
        bufrSection3List.append(np.binary_repr(3, 2))
        bufrSection3List.append(np.binary_repr(6, 6))
        bufrSection3List.append(np.binary_repr(5, 8)) # 3 06 005

    #Octets 1-3
    # Length of the section 3
    bufrSection3Length =  int(len(bufrSection3List) / 8 + 3)
    section3 = np.binary_repr(bufrSection3Length, 8 * 3) + ''.join(bufrSection3List)


    return section3


############################################################
#
#
#   BUFR SECTION 4
#
#
############################################################

def bufrSection4(df):

    bufrSection4List = []

    # octet 4 - set to 0 (reserved)
    bufrSection4List.append(np.binary_repr(0, 8))

    ############################################################
    #
    # 3 01 126  - Moored Buoy ID {includes subsets}
    #
    ############################################################

    ############################################################
    # 001087 - WMO Marine Observing platform extended identifier
    # Numeric 23 bits
    bufrSection4List.append(np.binary_repr(int(df['wmo_number'][0]), 23))

    ############################################################
    # 001015 - Station or Site Name
    # 160 bits (20 bytes)

    name_station = ''.join(format(ord(x), 'b') for x in df['name_buoy'][0])

    size_name = ''.join("00000000" * (20 - int(len(name_station) / 8))) + name_station

    bufrSection4List.append(name_station)

    ############################################################
    # 002149 - Type of data buoy
    # 6 bits - 18 = 3-metre Discus
    if df['model'][0] == "Axys":
        bufrSection4List.append(np.binary_repr(18, 6))
    elif df['model'][0] == "SPOT-0222":
        bufrSection4List.append(np.binary_repr(9, 6))

    ############################################################
    ############################################################
    # 301011  - Date {includes subsets}
    ############################################################
    ############################################################

    ############################################################
    # 004001  - Year (12 bits)
    # 004002  - Month (4 bits)
    # 004003  - Day   (6 bits)
    bufrSection4List.append(np.binary_repr(df["date_time"][0].year, 12))
    bufrSection4List.append(np.binary_repr(df["date_time"][0].month, 4))
    bufrSection4List.append(np.binary_repr(df["date_time"][0].day, 6))

    ############################################################
    ############################################################
    # 301012  - Time {includes subsets}
    ############################################################
    ############################################################

    ############################################################
    # 004004  - Hour (5 bits)
    # 004005  - Minute (6 bits)
    bufrSection4List.append(np.binary_repr(df["date_time"][0].hour, 5))
    bufrSection4List.append(np.binary_repr(df["date_time"][0].minute, 6))

    # Bits filled: 1111 1100

    ############################################################
    ############################################################
    # 301021  - Latitude & longitude (high accuracy) {includes subsets}
    ############################################################
    ############################################################

    ############################################################
    # 005001  - Latitude (25 bits) (scale 5, reference value -9000000)
    # 006001  - Longitude (26 bits) (scale 5, reference value -18000000)

    latScaled = (int(df['lat'][0] * 10 ** 5) + 9000000) # bits 26-32 are empty
    lonScaled = (int(df['lon'][0] * 10 ** 5) + 18000000) # bits 27-32 are empty

    bufrSection4List.append(np.binary_repr(latScaled, 25))
    bufrSection4List.append(np.binary_repr(lonScaled, 26))


    if df['model'][0] != "SPOT-0222":
        ############################################################
        ############################################################
        # 306038  - STDMET for Moored Buoys {includes subsets}
        ############################################################
        ############################################################

        ############################################################
        # 010004  - Pressure (14 bits) (scale -1, and units in Pa (mb * 100))
        # Null = 0
        bufrSection4List.append(np.binary_repr(int(0 * 100 * 0.1), 14))

        # 010051  - Pressure at sea-level (14 bits) (scale -1, and units in Pa)
        if df["pres"][0] == None:
            value = 0
        else:
            value = df["pres"][0]

        bufrSection4List.append(np.binary_repr(int(value * 100 * 0.1), 14))

        ############################################################
        # 007033  - Height pres Sensor Above Water (12 bits) (scale 1, m)
        if df["h_sensor_pres"][0] == None:
            value = 0
        else:
            value = df["h_sensor_pres"][0]

        bufrSection4List.append(np.binary_repr(int(value * 10), 12))

        ############################################################
        # 012101  - Air Temperature (16 bits) (scale 2, K)
        if df["atmp"][0] == None:
            value = 0
        else:
            value = df["atmp"][0]

        bufrSection4List.append(np.binary_repr(int(value * 10**2) + 273, 16))

        # 012103  - Dew Point (16 bits) (scale 2, K)
        if df["dewpt"][0] == None:
            value = 0
        else:
            value = df["dewpt"][0]


        bufrSection4List.append(np.binary_repr(int(value * 10**2) + 273, 16))

        ############################################################
        # 013103  - Relative Humidity (7 bits) (scale 0, %)
        if df["rh"][0] == None:
            value = 0
        else:
            value = df["rh"][0]

        bufrSection4List.append(np.binary_repr(int(value), 7))

        ############################################################
        # 007033  - Height atmp Above Water (12 bits) (scale 1, m)
        if df["h_sensor_atmp"][0] == None:
            value = 0
        else:
            value = df["h_sensor_atmp"][0]

        bufrSection4List.append(np.binary_repr(int(value * 10**1), 12))

        ############################################################
        # 008021  - Time Significance (5 bits) (scale 1, m) - 2 (reserved)
        bufrSection4List.append(np.binary_repr(2, 5))

        ############################################################
        # 004025  - Time Period of Displacement (12 bits) (scale 0, minute, reference=-2048)
        if df["atmp_avg"][0] == None:
            value = 0
        else:
            value = df["atmp_avg"][0]

        windavgScaled = (value + 2048)
        bufrSection4List.append(np.binary_repr(windavgScaled, 12))

        ############################################################
        # 011001  - Wind Direction (9 bits) (scale 0, degree)
        bufrSection4List.append(np.binary_repr(df['wdir'][0], 9))

        ############################################################
        # 011002  - Wind Speed (12 bits) (scale 1, m/s)
        bufrSection4List.append(np.binary_repr(int(df['wspd'][0] * 10), 12))

        ############################################################
        # 008021  - Time Significance (5 bits) (scale 1, m)
        # Set to missing to cancel the previous value (=31)
        bufrSection4List.append(np.binary_repr(2, 5))

        ############################################################
        # 004025  - Time Period of Displacement (12 bits) (scale 0, minute, reference=-2048)
        windavgScaled = (int(df['wind_avg'][0]) + 2048)
        bufrSection4List.append(np.binary_repr(windavgScaled, 12))

        ############################################################
        # 011041  - Maximum Gust Speed (12 bits) (scale 1, m/s)
        bufrSection4List.append(np.binary_repr(int(df['gust'][0] * 10), 12))

        ############################################################
        # 004025  - Time Period of Displacement (12 bits) (scale 0, minute, reference=-2048)
        # Set to missing (=0 min)
        bufrSection4List.append(np.binary_repr(df['gust_avg'][0] + 2048, 12))

        ############################################################
        # 007033  - Height Wind Sensor Above Water (12 bits) (scale 1, m)
        # Set to missing (=0 m) ????????? Use insted 5 meters
        bufrSection4List.append(np.binary_repr(int(df['h_sensor_wind'][0] * 10**1), 12))

        ############################################################
        # 002005  - Precision of Temperature (7 bits) (scale 2, K)
        WtmpPrecScaled = int(df['wtmp_prec'][0] * 10**2) # bit 8 is empty
        bufrSection4List.append(np.binary_repr(WtmpPrecScaled, 7))

        ############################################################
        # 007063  - Depth Below Sea/Water Surface (20 bits) (scale 2, m)
        DepthScaled = int(df['d_sensor_wtmp'][0] * 10**2) # bits 21-32 are empty, centimeters
        bufrSection4List.append(np.binary_repr(DepthScaled, 20))

        ############################################################
        # 022049  - Sea-surface Temperature (15 bits) (scale 2, K)
        WtmpScaled = int((df['sst'][0] + 273) * 10**2) # bit 16 is empty
        bufrSection4List.append(np.binary_repr(WtmpScaled, 15))

    else:
        ############################################################
        ############################################################
        # 302042  - Wind Data
        ############################################################
        ############################################################

        ############################################################
        # 007032  - Height pres Sensor Above Water (16 bits) (scale 2, m)
        bufrSection4List.append(np.binary_repr(int(df['h_sensor_wind'][0] * 10**2), 16))

        ############################################################
        # 002002  - Type of instrumentation for wind measurement
        bufrSection4List.append(np.binary_repr(4, 4))

        ############################################################
        # 008021  - Time Significance (5 bits) (scale 1, m)
        # Set to missing to cancel the previous value (=31)
        bufrSection4List.append(np.binary_repr(2, 5))


        ############################################################
        # 004025  - Time Period of Displacement (12 bits) (scale 0, minute, reference=-2048)
        windavgScaled = (int(df['wind_avg'][0]) + 2048)
        bufrSection4List.append(np.binary_repr(windavgScaled, 12))


        ############################################################
        # 011001  - Wind Direction (9 bits) (scale 0, degree)
        bufrSection4List.append(np.binary_repr(df['wdir'][0], 9))

        ############################################################
        # 011002  - Wind Speed (12 bits) (scale 1, m/s)
        bufrSection4List.append(np.binary_repr(int(df['wspd'][0] * 10), 12))

        ############################################################
        # 008021  - Time Significance (5 bits) (scale 1, m)
        # Set to missing to cancel the previous value (=31)
        bufrSection4List.append(np.binary_repr(31, 5))

        ############################################################
        # 103002  - Replicate 3 descriptors 2 times
        bufrSection4List.append(np.binary_repr(0, 8))

        ############################################################
        # 004025  - Time Period of Displacement (12 bits) (scale 0, minute, reference=-2048)
        bufrSection4List.append(np.binary_repr(0 + 2048, 12))

        ############################################################
        # 011041  - Maximum Gust Speed (12 bits) (scale 1, m/s)
        bufrSection4List.append(np.binary_repr(0, 9))

        ############################################################
        # 011041  - Maximum Gust Speed (12 bits) (scale 1, m/s)
        bufrSection4List.append(np.binary_repr(0 * 10, 12))


    if df['model'][0] != "SPOT-0222":
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
        bufrSection4List.append(np.binary_repr(df['duration_wave'][0], 12))

        ############################################################
        # 022070  - Significant wave height (13 bits) (scale 2, meters)

        WvhtScaled = int(df['swvht1'][0] * 10**2)  # bits 14-16 are empty
        bufrSection4List.append(np.binary_repr(WvhtScaled, 13))

        # 022073 - Maximum wave height (Operational)
        bufrSection4List.append(np.binary_repr(int(df['mxwvht1'][0] * 10**2), 13))


        ############################################################
        # 022074  - Average wave period (9 bits) (scale 1, seconds)

        ApdScaled = int(0*10**1)  # bits 10-16 are empty
        bufrSection4List.append(np.binary_repr(ApdScaled, 9))

        ############################################################
        # 022071  - Spectral peak wave period (9 bits) (scale 1, seconds)

        DpdScaled = int(df['tp1']*10**1)  # bits 10-16 are empty
        bufrSection4List.append(np.binary_repr(DpdScaled, 9))
        # Bits filled: 1111 1100


        # 022076 - Direction from which domiNonet waves are coming (Operational)
        bufrSection4List.append(np.binary_repr(0, 9))

        # 022077 - Directional spread of dominant wave (Operational)
        bufrSection4List.append(np.binary_repr(0, 9))

    else:
        ############################################################
        ############################################################
        # Wave Measurements
        ############################################################
        ############################################################
        # 302021  (Waves)
        ############################################################
        ############################################################


        # 022001 - Direction of waves
        bufrSection4List.append(np.binary_repr(df["wvdir1"][0], 9))

        ############################################################
        # 022010 - Period of waves
        bufrSection4List.append(np.binary_repr(round(df["tp1"][0]), 6))

        ############################################################
        # 022021 - Height of waves
        WvhtScaled = int(df['swvht1'][0] * 10**1)  # bits 14-16 are empty
        bufrSection4List.append(np.binary_repr(WvhtScaled, 10))


    if df['model'][0] != "SPOT-0222":
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
        bufrSection4List.append(np.binary_repr(int(df['d_curr'] * 10), 17))

        # 022004 - DIRECTION OF CURRENT
        bufrSection4List.append(np.binary_repr(round(df['cdir1'][0]), 9))

        # 022031 - SPEED OF CURRENT
        bufrSection4List.append(np.binary_repr(int(df['cspd1'] * 10 ** 2), 13))

    else:
        ############################################################
        ###Water temperature
        ############################################################
        ############################################################
        # 302056  (Sea / Water temperature)
        ############################################################
        ############################################################

        ############################################################
        # 002038 - Method of water temperature and/or salinity measurement
        bufrSection4List.append(np.binary_repr(14, 4))

        ############################################################
        # 007063  - Depth Below Sea/Water Surface (20 bits) (scale 2, m)
        DepthScaled = int(df['d_sensor_wtmp'][0] * 10**2) # bits 21-32 are empty, centimeters
        bufrSection4List.append(np.binary_repr(DepthScaled, 20))

        ############################################################
        # 022043 - Sea/water temperature
        WtmpScaled = int((df['sst'][0] + 273) * 10**2) # bit 16 is empty
        bufrSection4List.append(np.binary_repr(WtmpScaled, 15))

        ############################################################
        # 007063  - Depth Below Sea/Water Surface (20 bits) (scale 2, m)
        DepthScaled = int(df['d_sensor_wtmp'][0] * 10**2) # bits 21-32 are empty, centimeters
        bufrSection4List.append(np.binary_repr(DepthScaled, 20))


    #Octets 1-3
    # Length of the section 3
    bufrSection4leftovers = len(bufrSection4List) % 8

    bufrSection4octets = int((len(bufrSection4List) - bufrSection4leftovers) / 8 % 2)

    bufrSection4List.append(np.binary_repr(0, bufrSection4octets * 8 + 8 - bufrSection4leftovers))

    bufrSection4Length =  int(len(bufrSection4List) / 8 + 3)
    section4 = np.binary_repr(bufrSection4Length, 8 * 3) + ''.join(bufrSection4List)

    return section4


def bufrSection5():

    section5 = []

    section5.append(np.binary_repr(55, 8))
    section5.append(np.binary_repr(55, 8))
    section5.append(np.binary_repr(55, 8))
    section5.append(np.binary_repr(55, 8))

    return ''.join(section5)
