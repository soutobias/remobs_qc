#! /usr/bin/python
'''
Access and use of this software shall impose the following
obligations and understandings on the user. The user is granted the
right, without any fee or cost, to use, copy, modify, alter, enhance
and distribute this software, and any derivative works thereof, and
its supporting documentation for any purpose whatsoever, provided
that this entire notice appears in all copies of the software,
derivative works and supporting documentation. Further, the user
agrees to credit NOAA/Nation Data Buoy Center in any publications
that result from the use of this software or in any product that includes
this software. The names NOAA and/or National Data Buoy Center, however,
may not be used in any advertising or publicity to endorse or promote any
products or commercial entity unless specific written permission is obtained
from NOAA/National Data Buoy Center. The user also understands that NOAA/
National Data Buoy Center is not obligated to provide the user with any
support, consulting, training or assistance of any kind with regard to the
use, operation and performance of this software nor to provide the user with
any updates, revisions, new versions or "bug fixes".

THIS SOFTWARE IS PROVIDED BY NOAA/National Data Buoy Center "AS IS" AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL NOAA/National Data Buoy Center BE LIABLE
FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
CONTRACT, NEGLIGENCE OR OTHER TORTUOUS ACTION, ARISING OUT OF OR IN
CONNECTION WITH THE ACCESS, USE OR PERFORMANCE OF THIS SOFTWARE.

******* THIS IS BETA SOFTWARE ******* THIS IS BETA SOFTWARE *******
******* THIS IS BETA SOFTWARE ******* THIS IS BETA SOFTWARE *******
******* THIS IS BETA SOFTWARE ******* THIS IS BETA SOFTWARE *******

@author: Bill Smith

Created on Aug 5, 2013

for NOAA/National Data Buoy Center

Purpose:    To create BUFR files from IOOS Glider NetCDF files.

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
from NdbcToolset import Mailer
from NdbcToolset import ExceptionFormatter


#######################################################################
#  Function name:      usage
#    
#  Purpose:            To print usage documentation to the user.
#    
#  Input variables:    None
#    
#  Output variables:   None
#    
#  Notes:              The program exits after printing usage.
#######################################################################
def usage():
    global FILENAME
    global BASENAME
    global ISINTERACTIVE
    global MAILER

    usage = []
    usage.append("Usage: %s {config file} {NetCDF File}\n" % (FILENAME))
    usage.append("    Where 'config file' is a XML formatted file containing\n")
    usage.append("    processing parameters and 'NetCDF File' is a NetCDF file,\n")
    usage.append("    containing glider profiles in the IOOS standard format.\n\n")
    usage.append("    Example usage (assuming current directory):\n")
    usage.append("    ./%s %s.xml ru29-273_20130131T170415_rt0.nc\n"
        % (FILENAME, BASENAME))

    msgString = "".join(usage)

    if ISINTERACTIVE:
        print msgString
    else:
        MAILER.sendMail(msgString, alert='CRITICAL ERROR', file=False)

    os._exit(1)


#######################################################################
#  Function name:      retrieveGliderMetadata
#    
#  Purpose:            To retrieve metadata (attributes) from a NetCDF Dataset.
#    
#  Input variables:    NetCDF Dataset
#    
#  Output variables:   None
#    
#  Notes:              Metadata attributes are stored in global variables.
#######################################################################
def retrieveGliderMetadata(NC):
    global WMOID
    
    # Retrieve variable platform
    platform = NC.variables['platform']
    
    # get wmo_id attribute
    WMOID = getattr(platform, 'wmo_id')
    
    if DEBUG:
        print 'platform=', platform, ', wmo_id=', WMOID
    
    return None

#######################################################################
#  Function name:      retrieveUniqueProfileIds
#    
#  Purpose:            To retrieve a set of Profile Ids from a NetCDF Dataset.
#    
#  Input variables:    NetCDF Dataset
#    
#  Output variables:   Tuple containing the set of profile ids.
#    
#  Notes:              None.
#######################################################################
def retrieveUniqueProfileIds(NC):
   
    # Retrieve variable array 'profile_id' 
    profileIds = NC.variables['profile_id'][:]
   
    # Retrieve variable masked array for 'profile_id' to check for missing.
    profileIdMask = numpy.ma.getmaskarray(profileIds)
   
    profileList = []
    for i in range(len(profileIds)):
        if profileIdMask[i]:    # Missing
            continue
       
        profileList.append(profileIds[i])

    return sorted(set(profileList))

#######################################################################
#  Function name:      retrieveProfileMetadata
#    
#  Purpose:            To retrieve metadata for a specific profile
#    
#  Input variables:    NetCDF (netCDF4.Dataset)
#                      Profile Number (numpy.int16)
#    
#  Output variables:   Tuple containing the set of metadata, which includes
#                      time, latitude and longitude.
#    
#  Notes:              Metadata will be retrieved for the topmost profile
#                      (the one nearest the surface).  This is to provide
#                      the most accurate GPS reading possible.
#######################################################################
def retrieveProfileMetadata(NC, profile):
    global DIRECTION_OF_PROFILE

    profileTime = None
    latitude = None
    longitude = None
    
    # Retrieve variable array 'profile_id' 
    profileIds = NC.variables['profile_id'][:]
    
    times = NC.variables['time']
    timeUnits = getattr(times, 'units')
    
    depths = NC.variables['depth']
    depthFillval = getattr(depths, '_FillValue')
    depths.set_auto_maskandscale(False)

    topLevelIndx = None
    bottomLevelIndx = None
    
    for i in range(len(profileIds)):
        if profileIds[i] == profile:

            if depths[i] == depthFillval:
                continue
            
            if topLevelIndx == None or depths[i] <  depths[topLevelIndx]:
                topLevelIndx = i
                
            if bottomLevelIndx == None or depths[i] >  depths[bottomLevelIndx]:
                bottomLevelIndx = i

    DIRECTION_OF_PROFILE = 3        # Missing
    
    if topLevelIndx > bottomLevelIndx:
        dirX = -1
        DIRECTION_OF_PROFILE = 1    # Downwards
    else:
        dirX = 1
        DIRECTION_OF_PROFILE = 0    # Upwards
        
    # Retrieve time (closest to surface for this profile)
    profileTime = num2date(times[topLevelIndx], timeUnits)
    
    # Get Latitude
    lat = NC.variables['lat']
    latFillval = getattr(lat, '_FillValue')
    lat.set_auto_maskandscale(False)

    lon = NC.variables['lon']
    lonFillval = getattr(lat, '_FillValue')
    lon.set_auto_maskandscale(False)

    # Search and find 1st good position (closest to the surface)
    for i in range(topLevelIndx, bottomLevelIndx + dirX, dirX):
        print 'i=', i
        if lat[i] == latFillval:
            continue
        
        if lon[i] == lonFillval:
            continue
        
        latitude = lat[i]
        longitude = lon[i]
        break
        
    return (profileTime, latitude, longitude)

#######################################################################
#  Function name:      retrieveProfile
#    
#  Purpose:            To retrieve profile data
#    
#  Input variables:    NetCDF (netCDF4.Dataset)
#                      Profile Number (numpy.int16)
#    
#  Output variables:   List of Tuples containing the set of profile data.
#    
#  Notes:              None
#######################################################################
def retrieveProfile(NC, profile):
    
    profiles = []

    # Retrieve variable array 'profile_id' 
    profileIds = NC.variables['profile_id'][:]

    # Get Variables
    depths = NC.variables['depth']
    depthsQc = NC.variables['depth_qc']
    temperatures = NC.variables['temperature']
    temperaturesQc = NC.variables['temperature_qc']
    salinitys = NC.variables['salinity']
    salinitysQc = NC.variables['salinity_qc']
    
    depthFillval = getattr(depths, '_FillValue')
    depthQcFillval = getattr(depthsQc, '_FillValue')
    temperaturesFillval = getattr(depths, '_FillValue')
    temperaturesQcFillval = getattr(depths, '_FillValue')
    salinitysFillval = getattr(depths, '_FillValue')
    salinitysQcFillval = getattr(depths, '_FillValue')

    depths.set_auto_maskandscale(False)
    depthsQc.set_auto_maskandscale(False)
    temperatures.set_auto_maskandscale(False)
    temperaturesQc.set_auto_maskandscale(False)
    salinitys.set_auto_maskandscale(False)
    salinitysQc.set_auto_maskandscale(False)


    for i in range(len(profileIds)):
        if profileIds[i] == profile:

            if depths[i] == depthFillval:
                continue
            
            depth = depths[i]
            
            # Set defaults
            temperature = None
            temperatureQc = None
            salinity = None
            salinityQc = None
            
            if temperatures[i] != temperaturesFillval:
                # convert to Kelvin (scale 3)
                temperature = int(round(temperatures[i] + 273.15) * 1000)
                
            if salinitys[i] != salinitysFillval:
                salinity = int(salinitys[i] * 1000)
                
            profiles.append( (depth, temperature, temperatureQc, 
                salinity, salinityQc))
            
    return profiles

#######################################################################
#  Function name:      buildBufrSection1
#    
#  Purpose:            To build Section1 of the BUFR message
#    
#  Input variables:    NetCDF (netCDF4.Dataset)
#                      Profile Number (numpy.int16)
#                      profileTime (datetime)
#    
#  Output variables:   String containing the BUFR Section 1 message.
#    
#  Notes:              None.
#######################################################################
def buildBufrSection1(NC, profile, profileTime):
    
    bufrSection1List = []
    bufrSection1 = None
    
    try:
        # BUFR master table (zero if standard WMO FM 94 BUFR tables are used)
        bufrSection1List.append(pack("B", 0))
    
        # Identification of originating/generating centre 
        # (see Common Code table C-11)
        bufrSection1List.append(pack(">H", int(ORIGINATING_CENTRE)))
        
        # Identification of originating/generating sub-centre 
        # (allocated by originating/generating centre - see Common Code table C-12)
        bufrSection1List.append(pack(">H", int(ORIGINATING_SUBCENTRE)))
        
        # Update sequence number
        bufrSection1List.append(pack("B", 0))
        
        # Code for presence of optional section (2), 0=Not present
        bufrSection1List.append("\x00")
        
        # Code for Data Category (31=Oceanographic)
        bufrSection1List.append(pack("B", 31))
        
        # Code for International data sub-category (4=Sub-surface floats (profile)
        bufrSection1List.append(pack("B", 4))
        
        # Code for local data sub-category
        bufrSection1List.append(pack("B", 4))

        # Version number of master table (19=Version implemented on 7 November 2012)
        bufrSection1List.append(pack("B", 19))

        # Version number of local tables
        bufrSection1List.append(pack("B", 0))
        
        timeTup = profileTime.timetuple()

        # Year, Month, Day, Hour, Minute, Second
        bufrSection1List.append(pack(">H", timeTup[0]))
        bufrSection1List.append(pack("B", timeTup[1]))
        bufrSection1List.append(pack("B", timeTup[2]))
        bufrSection1List.append(pack("B", timeTup[3]))
        bufrSection1List.append(pack("B", timeTup[4]))
        bufrSection1List.append(pack("B", timeTup[5]))

        tmpString = "".join(bufrSection1List)
        bufrSection1Length = len(tmpString) + 3
        
        # 'L' generates a 4-byte integer, but only need 3 bytes of it
        bufrSection1 = "%s%s" % ((pack(">L", bufrSection1Length)[1:]), tmpString)

    except:
        msgString = EXCFMTR.formatExceptionInfo().replace('\\n', '\n')
        if ISINTERACTIVE:
            print msgString
        else:
            MAILER.sendMail(msgString, "Error", file=False)
        os._exit(1)
    
    
    return(bufrSection1)

#######################################################################
#  Function name:      buildBufrSection3
#    
#  Purpose:            To build Section3 of the BUFR message
#    
#  Input variables:    NetCDF (netCDF4.Dataset)
#                      Profile Number (numpy.int16)
#    
#  Output variables:   String containing the BUFR Section 3 message.
#    
#  Notes:              Using BUFR template for Temperature and salinity
#                      profile observed by sub-surface profiling floats.
#######################################################################
def buildBufrSection3(NC, profile):
    
    bufrSection3List = []
    bufrSection3 = None

    try:
        bufrSection3List.append(pack("B", 0))   # Set to zero (reserved)
        bufrSection3List.append(pack(">H", 1))  # Number of data subsets
        bufrSection3List.append(pack("B", 128)) # Observed data, uncompressed


        # WMO Marine Observing platform extended identifier
        bufrSection3List.append(pack("BB", 1, 87))      # 001087
        
        # Observing platform manufacturers model
        bufrSection3List.append(pack("BB", 1, 85))      # 001085

        # Observing platform manufacturers serial number
        bufrSection3List.append(pack("BB", 1, 86))      # 001086

        # Buoy type
        bufrSection3List.append(pack("BB", 2, 36))      # 002036

        # Data Collection and/or location system
        bufrSection3List.append(pack("BB", 2, 148))      # 002148

        # Type of data buoy
        bufrSection3List.append(pack("BB", 2, 149))      # 002149

        # Float cycle number
        bufrSection3List.append(pack("BB", 22, 55))      # 022055

        # Direction of profile
        bufrSection3List.append(pack("BB", 22, 56))      # 022056

        # Instrument type for water temperature profile measurement
        bufrSection3List.append(pack("BB", 22, 67))      # 022067

        # Date
        fx = (3 * 2**6) + 1
        bufrSection3List.append(pack("BB", fx, 11))      # 301011

        # Time
        fx = (3 * 2**6) + 1
        bufrSection3List.append(pack("BB", fx, 12))      # 301012

        # Latitude and longitude (high accuracy)
        fx = (3 * 2**6) + 1
        bufrSection3List.append(pack("BB", fx, 21))      # 301021

        # Qualifier for quality class
        bufrSection3List.append(pack("BB", 8, 80))       # 008080

        # Global GTSPP quality class
        bufrSection3List.append(pack("BB", 33, 50))      # 033050

        # Delayed replication of 9 descriptors
        fx = (1 * 2**6) + 9
        bufrSection3List.append(pack("BB", fx, 0))       # 109000

        # Extended delayed descriptor replication factor
        bufrSection3List.append(pack("BB", 31, 2))       # 031002

        # Water pressure
        bufrSection3List.append(pack("BB", 7, 65))       # 007065

        # Qualifier for quality class
        bufrSection3List.append(pack("BB", 8, 80))       # 008080

        # Global GTSPP quality class
        bufrSection3List.append(pack("BB", 33, 50))      # 033050

        # Subsurface sea temperature
        bufrSection3List.append(pack("BB", 22, 45))      # 022045

        # Qualifier for quality class
        bufrSection3List.append(pack("BB", 8, 80))       # 008080

        # Global GTSPP quality class
        bufrSection3List.append(pack("BB", 33, 50))      # 033050

        # Salinity
        bufrSection3List.append(pack("BB", 22, 64))      # 022064

        # Qualifier for quality class
        bufrSection3List.append(pack("BB", 8, 80))       # 008080

        # Global GTSPP quality class
        bufrSection3List.append(pack("BB", 33, 50))      # 033050

        tmpString = "".join(bufrSection3List)
        bufrSection3Length = len(tmpString) + 3

        # 'L' generates a 4-byte integer, but only need 3 bytes of it
        bufrSection3 = "%s%s" % ((pack(">L", bufrSection3Length)[1:]), tmpString)

    except:
        msgString = EXCFMTR.formatExceptionInfo().replace('\\n', '\n')
        if ISINTERACTIVE:
            print msgString
        else:
            MAILER.sendMail(msgString, "Error", file=False)
        os._exit(1)

    return bufrSection3

#######################################################################
#  Function name:      buildBufrSection4
#    
#  Purpose:            To build Section4 of the BUFR message
#    
#  Input variables:    NetCDF (netCDF4.Dataset)
#                      Profile Number (numpy.int16)
#                      Profile Time
#                      Latitude
#                      Longitude
#    
#  Output variables:   String containing the BUFR Section 4 message.
#    
#  Notes:              Using BUFR template for Temperature and salinity
#                      profile observed by sub-surface profiling floats.
#                      
#                      Uneven byte boundaries require bit shifting.
#######################################################################
def buildBufrSection4(NC, profile, profileTime, latitude, longitude):
    global LOGFILE
    global WMO_ID
    global DIRECTION_OF_PROFILE
    
    bufrSection4List = []
    bufrSection4 = None
    excMessage = None

    # 001087 - WMO Marine Observing platform extended identifier
    # Numeric 23 bits
    wmoIdNum = 0
    
    #TODO: DEBUG
    WMO_ID = 4891234
    try:
        if not WMO_ID:
            excMessage="'wmo_id' is missing from NetCDF file %s" % (xmlName)
            raise 
        
        excMessage="'wmo_id' in NetCDF file is not numeric! Value='%s'" % (WMO_ID)
        wmoIdNum = int(WMO_ID) * 2  # bit 24 is empty
    except:
        LOGFILE.write("%s - %s\n" %
            (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()), excMessage))

    # Pack WmoId into 3 bytes
    wmoIdPacked = pack(">I", wmoIdNum)
    
    if DEBUG:
        print 'Dbg|WMO ID -' , ":".join("{0:x}".format(ord(c)) for c in wmoIdPacked)

    # Store 2 bytes and save the third for the last bit
    bufrSection4List.append(wmoIdPacked[1:3])
    unfilledByte = wmoIdPacked[3]
    # Bits filled: 1111 1110
   
    if DEBUG:
        print 'Dbg|WMO ID (packed) -',":".join("{0:x}".format(ord(c)) for c in bufrSection4List[0])
        print 'Dbg|leftover partial byte -', ":".join("{0:x}".format(ord(c)) for c in unfilledByte)

 
    # 001085 - Observing platform manufacturers model
    # 160 bits (20 bytes)
    model = chr(32) * 20     # initialize to all spaces

    try:    
        platform = NC.variables['instrument_ctd']
    
        # get model number
        model = getattr(platform, 'make_model')
        
        l = len(model)
        
        # check if too short or long
        if l > 20:
            model = model[0:21]
        elif l < 20:
            # pad with spaces
            model = model + chr(32) * (20 - l)
    except:
        LOGFILE.write("%s - %s\n" %
            (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()),
             'Error, missing or invalid make_model.'))

    if DEBUG:
        print 'Dbg|platform manufacturers model-',":".join("{0:x}".format(ord(c)) for c in model)

    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, 7, model, 160)
    bufrSection4List.append(string)
    # Bits filled: 1111 1110

    
    # 001086 - Observing platform manufacturers serial number
    # 256 bits (32 bytes)
    serialNumber = chr(32) * 32     # initialize to all spaces 

    try:    
        platform = NC.variables['instrument_ctd']
    
        # get model number
        serialNumber = getattr(platform, 'serial_number')
        
        l = len(serialNumber)
        
        # check if too short or long
        if l > 32:
            serialNumber = serialNumber[0:33]
        elif l < 32:
            # pad with spaces
            serialNumber = serialNumber + chr(32) * (32 - l)
    except:
        LOGFILE.write("%s - %s\n" %
            (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()),
             'Error, missing or invalid serial_number.'))

    if DEBUG:
        print 'Dbg|platform serial number-',":".join("{0:x}".format(ord(c)) for c in serialNumber)

    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, serialNumber, 256)
    bufrSection4List.append(string)
    # Bits filled: 1111 1110


    # 002036 - Buoy Type
    # 2 bits - 3 = Missing value
    buoyType = chr(3 << 6)     # Decimal 3, left shifted 6 bits
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, buoyType, 2)
    bufrSection4List.append(string)
    # Bits filled: 1000 0000


    # 002148 - Data collection and/or location system
    # 5 bits - 2 = GPS
    locationSystem = chr(2 << 3)    # Decimal 2, left shifted 2 bits
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, locationSystem, 5)
    # Bits filled: 1111 1100


    # 002149 - Type of data buoy
    # 6 bits - 63 = Missing value
    typeBuoy = chr(63 << 2)  # Decimal 63, left shifted 2 bits
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, typeBuoy, 6)
    bufrSection4List.append(string)
    # Bits filled: 1111 0000

    
    # 022055 - Float cycle number
    # 10 bits
    floatCycleNumber = int(profile) << 6     # Left shifted 6 bits
    floatCycleNumber = pack(">H", floatCycleNumber)

    if DEBUG:
        print 'Dbg|Float cycle number -',":".join("{0:x}".format(ord(c)) for c in floatCycleNumber)
    
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, floatCycleNumber, 10)
    bufrSection4List.append(string)
    # Bits filled: 1111 1100
    if DEBUG:
        print 'Dbg|unfilledByte (Float cycle Number) -',":".join("{0:x}".format(ord(c)) for c in unfilledByte)

    
    # 022056 - Direction of profile
    # 2 bits
    string = pack("B", (DIRECTION_OF_PROFILE << 6))
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 2)
    bufrSection4List.append(string)
    # Bits filled: 0000 0000

    
    # 022067  - Instrument type for water temperature profile measurement
    # 10 bits - 830 = CTD
    string = pack(">H", int(830) << 6)       # Left shifted 6 bits 
    if DEBUG:
        print 'Dbg|Instrument type -',":".join("{0:x}".format(ord(c)) for c in string)
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 10)
    bufrSection4List.append(string)
    # Bits filled: 1100 0000


    # 301011  - Date {includes subsets}
    # 004001  - Year (12 bits)
    # 004002  - Month (4 bits)
    # 004003  - Day   (6 bits)
    year = profileTime.year * 2 ** 20       # bits 31-20    
    month = profileTime.month * 2 ** 16     # bits 19-16
    day = profileTime.day * 2 ** 10         # bits 15-10
    
    # Pack into 4-character string as unsigned integer (big-endian)
    string = pack(">I", (year | month | day))
    print 'Dbg|string-',":".join("{0:x}".format(ord(c)) for c in string)
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 22)
    bufrSection4List.append(string)
    # Bits filled: 0000 0000


    # 301012  - Time {includes subsets}
    # 004004  - Hour (5 bits)
    # 004005  - Minute (6 bits)
    hour = profileTime.hour * 2 ** 11       # bits 15-11    
    minute = profileTime.minute * 2 ** 5    # bits 10-05
    string = pack(">H", (hour | minute))
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 11)
    bufrSection4List.append(string)
    # Bits filled: 1110 0000
   
    
    # 301021  - Latitude & longitude (high accuracy) {includes subsets}
    # 005001  - Latitude (25 bits) (scale 5, reference value -9000000)
    # 006001  - Longitude (26 bits) (scale 5, reference value -18000000)
    
    latScaled = (int(latitude * 10 ** 5) + 9000000) << 7
    lonScaled = (int(longitude * 10 ** 5) + 18000000) << 6
    print 'latitude=', latitude, 'latScaled=', latScaled
    print 'longitude=', longitude, 'lonScaled=', lonScaled

    string = pack(">I", latScaled)
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 25)
    bufrSection4List.append(string)
    # Bits filled: 1111 0000
    
    string = pack(">I", lonScaled)
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 26)
    bufrSection4List.append(string)
    # Bits filled: 1111 1100
    
    
    # 008080  - Qualifier for quality class
    #  6 bits - 20 = Position
    string = pack("B", 20 << 2)         # Left shifted 2 bits
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 6)
    bufrSection4List.append(string)
    # Bits filled: 1111 0000
    
    
    # 033050  - Global GTSPP quality class
    #  4 bits - 1 = Good Position
    string = pack("B", 1 << 4)         # Left shifted 4 bits 
    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, string, 4)
    bufrSection4List.append(string)
    # Bits filled: 0000 0000 (even boundary, no leftover bits)
    
    
    # Retrieve profile
    profile = retrieveProfile(NC, profile)
    print 'len(profile)=', len(profile)

    # 109000  - Delayed replication of 9 descriptors
    # 031002  - Extended delayed descriptor replication factor
    # 16 bits - Count of profile depths
    string = pack(">H", len(profile))    
    bufrSection4List.append(string)
    # Bits filled: 0000 0000 (even Byte boundary, no leftover bits)
    
    bitsRemaining = 0
    
    for measT in profile:
        (depth, temperature, temperatureQc, salinity, salinityQc) = measT

        # 007065  - Water pressure
        # 17 bits
        # convert depth to pressure
        pres =  sw_pres(depth, latitude)
        wp = int(pres * 10) << 15
 
        # 008080  - Qualifier for quality class
        #  6 bits - 10 = Water pressure at a level
        wp = wp | (10 << 9)

        # 033050  - Global GTSPP quality class
        #  4 bits - 0 = Unqualified
#        wp = wp | (0 << 5)

        # Pack into 32 bit unsigned integer
        wpStr = pack(">I", wp)    
        (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, wpStr, 27)
        bufrSection4List.append(string)

        
        # 022045  - Subsurface sea temperature
        # 19 bits 
        swt = temperature << 13     # left-justify the bit string
   
        # 008080  - Qualifier for quality class
        #  6 bits - 11 = Water temperature at a level
        swt = swt | (11 << 7)       # fill in the next 6 bits

        # 033050  - Global GTSPP quality class
        #  4 bits - 0 = Unqualified
        # Pack into 32 bit unsigned integer
        swtStr = pack(">I", swt)    
        (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, swtStr, 29)
        bufrSection4List.append(string)

        
        # 022064  - Salinity
        # 17 bits 
        sal = salinity << 15     # left-justify the bit string
   
        # 008080  - Qualifier for quality class
        #  6 bits - 12 = Salinity at a level
        sal = sal | (12 << 9)

        # 033050  - Global GTSPP quality class
        #  4 bits - 0 = Unqualified
        salStr = pack(">I", sal)   
        (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, salStr, 27)
        bufrSection4List.append(string)


    # Add any leftover bits to the output (as full Byte) 
    if unfilledByteBits > 0:
        bufrSection4List.append(unfilledByte)
        
        
    tmpString = "".join(bufrSection4List)

    # Only 3 bytes are good for length, so shift 8 bits
    bufrSection4Length = (len(tmpString) + 4) << 8
    bufrSection4 = "%s%s" % ((pack(">I", bufrSection4Length)), tmpString)
    return bufrSection4

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
def buildBufrMessage(NC, profile, profileTime, latitude, longitude):
    
    bufrMessage = []
    
    bufrMessage.append(buildBufrSection1(NC, profile, profileTime))
    bufrMessage.append(buildBufrSection3(NC, profile))
    bufrMessage.append(buildBufrSection4(NC, profile, profileTime,
        latitude, longitude))
   
    
    # Section 5
    bufrMessage.append('7777')
    
    return "".join(bufrMessage)

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

    if DEBUG:
        print 'Dbg|bitShift (startByte) -', newLeftover, startByteBits
        print 'Dbg|bitShift (string) -',":".join("{0:x}".format(ord(c)) for c in "".join(string))
        print 'Dbg|bitShift (stringBits) -', stringBits
   
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
    if DEBUG:
        print 'Dbg|bitShift (newString) -',":".join("{0:x}".format(ord(c)) for c in newString)
        print 'Dbg|bitShift (newLeftover) -',":".join("{0:x}".format(ord(c)) for c in "".join(newLeftover))
    
    return (newString, newLeftover, leftoverBits)


# SW_PRES    Pressure from depth
#===========================================================================
# SW_PRES   $Id: sw_pres.m,v 1.1 2003/12/12 04:23:22 pen078 Exp $
#           Copyright (C) CSIRO, Phil Morgan 1993.
#
# USAGE:  pres = sw_pres(depth,lat)
#
# DESCRIPTION:
#    Calculates pressure in dbars from depth in meters.
#
# INPUT:  (all must have same dimensions)
#   depth = depth [metres]
#   lat   = Latitude in decimal degress north [-90..+90]
#           (LAT may have dimensions 1x1 or 1xn where depth(mxn) )
#
# OUTPUT:
#  pres   = Pressure    [db]
#
# AUTHOR:  Phil Morgan 93-06-25  (morgan@ml.csiro.au)
#
# DISCLAIMER:
#   This software is provided "as is" without warranty of any kind.
#   See the file sw_copy.m for conditions of use and licence.
#
# REFERENCES:
#    Saunders, P.M. 1981
#    "Practical conversion of Pressure to Depth"
#    Journal of Physical Oceanography, 11, 573-574
#
# CHECK VALUE:
#    P=7500.00 db for LAT=30 deg, depth=7321.45 meters
#=========================================================================

# Modifications
# 1999-06-25.    Lindsay Pender, Fixed transpose of row vectors.
# 2013-08-22.    W. Smith, converted to Python

def sw_pres(DEPTH,LAT):
    DEG2RAD = math.pi/180
    X       = math.sin(abs(LAT)*DEG2RAD)    # convert to radians
    C1      = 5.92E-3+X**2*5.25E-3
    pres    = ((1-C1)-math.sqrt(((1-C1)**2)-(8.84E-6*DEPTH)))/4.42E-6
    return pres
#===========================================================================


###########################
# Main - Start of program #
###########################
# Setup global environment variables
HOME=os.environ['HOME']
FILENAME=os.path.basename(sys.argv[0])
BASENAME=os.path.splitext(FILENAME)[0]
PID=str(os.getpid())
PROGDIR=os.getcwd()
LOGDIR=PROGDIR + '/LOG'
OUTPUTDIR = None

# Turn on Verbose debugging features?
DEBUG = True

# Globals for BUFR document
WMOID = None
DIRECTION_OF_PROFILE = None

ERRFILENAME='/tmp/' + BASENAME + '_' + PID + '.err'

# Open Error Log file to log non-critical errors
ERRFILE = open(ERRFILENAME, 'w')

NOW=time.time()

# Open Log file
LOGFILENAME='%s/%s_%s.log' % (LOGDIR, BASENAME,
    time.strftime("%Y%m%d", time.gmtime(NOW)))

LOGFILE = open(LOGFILENAME, 'a')
LOGFILE.write("%s - ***** BEGIN PROCESSING, pid=%s\n" %
    (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(NOW)), PID))

if sys.stdout.isatty():
    ISINTERACTIVE = True
else:
    ISINTERACTIVE = False

# Setup Exception Formatter, in case of errors
EXCFMTR = ExceptionFormatter()
MAILER = Mailer(defaultTo="swrt@noaa.gov")

# Check for valid argument count
if len(sys.argv) < 3:
    usage()

# Get parameter file name from command line
xmlName = sys.argv[1]

# Read program parameter file
try:
    XFILE = open(xmlName, 'r')
    XDOC = minidom.parse(XFILE)
    XFILE.close()
except:
    # This is NOT a proper XML document. 
    alert='CRITICAL ERROR'
    msgString = EXCFMTR.formatExceptionInfo().replace('\\n', '\n')

    if ISINTERACTIVE:
        print alert
        print msgString
    else:
        MAILER.sendMail(msgString, alert, file=False)

    os._exit(1)
    
# Retrieve parameter settings
try:
    for node in XDOC.getElementsByTagName('settings'):
        alertMail = node.getAttribute('email')
        outputDir = node.getAttribute('output_directory')
        routingId = node.getAttribute('routing_id')
        originator = node.getAttribute('originator')
        originatingCentre = node.getAttribute('originating_centre')
        originatingSubCentre = node.getAttribute('originating_subcentre')
        break

    # Check for mandatory settings
    if not outputDir:
        raise Exception("Missing 'output_directory' setting in %s" % (xmlName))
             
    OUTPUTDIR = outputDir

    if not routingId:
        raise Exception("Missing 'routing_id' setting in %s" % (xmlName))
        
    ROUTING_ID = routingId
    
    if not originator:
        raise Exception("Missing 'originator' setting in %s" % (xmlName))

    ORIGINATOR = originator

    if not originatingCentre:
        raise Exception("Missing 'originating_centre' setting in %s" % (xmlName))

    ORIGINATING_CENTRE = originatingCentre

    if not originatingSubCentre:
        raise Exception("Missing 'originating_subcentre' setting in %s" % (xmlName))

    ORIGINATING_SUBCENTRE = originatingSubCentre

    # Suppy optional settings
    if alertMail:
        MAILER=Mailer(defaultTo=alertMail)
except:
    msgString = EXCFMTR.formatExceptionInfo().replace('\\n', '\n')
    if ISINTERACTIVE:
        print msgString
    else:
        MAILER.sendMail(msgString, "Critical Error", file=False)
    os._exit(1)

try:
    ncFilename = sys.argv[2]    # Filename or OPENDAP Url
    
    NC = Dataset(ncFilename.encode('latin-1'), 'r')
    print NC.file_format
       
    LOGFILE.write("%s - Reading NetCDF file: %s\n" %
        (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()), ncFilename))
    
    retrieveGliderMetadata(NC)  
     
    profiles = retrieveUniqueProfileIds(NC) 

    # Generate a BUFR message for each profile
    for profile in profiles:
        print 'Profile=', profile
        
        LOGFILE.write("%s - Processing profile: %s\n" %
            (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()), profile))

        (profileTime, latitude, longitude)  =   \
            retrieveProfileMetadata(NC, profile)
        
        TIME_STR = profileTime.strftime("%Y%m%d%H%M")
        
        # compute filename
        filename="T_%s_C_%s_%s.bin" % (ROUTING_ID, \
            ORIGINATOR, TIME_STR)
        
        bufrString = buildBufrMessage(NC, profile, profileTime, latitude, longitude)
        
        # Check for empty return
        if bufrString == None:
            continue
        
        bufrMessageLength = len(bufrString) + 8
        
        fullPathname = "%s/%s" % (OUTPUTDIR, filename)
        BUFR = open(fullPathname,'w')
        
        # Calculate full size of message
        totalMessageSize = bufrMessageLength + 21  # Add 21 for GTS Headers
        
        # Write the BUFR message
        
        # GTS Headers
#        BUFR.write("****%010d****\n" % (totalMessageSize))
#        BUFR.write("%s %s %s\r\r\n" % 
#            (ROUTING_ID, ORIGINATOR, profileTime.strftime("%d%H%M")))
        
        ################
        # BUFR Section 0
        ################
        BUFR.write('BUFR')
        
        # Write message length as 3 bytes
        BUFR.write(pack(">L", bufrMessageLength)[1:])
        BUFR.write(pack("B", 4))     # BUFR Version Number

        BUFR.write(bufrString)
        BUFR.close()

    NC.close()
    sys.exit(1)
    


    # Get data (exit, if none retrieved)
    gribArray = []
    gribDataSize = 0

    # Loop through datasets
    for node in XDOC.getElementsByTagName('netcdf'):
        gribDataSet = node.firstChild.data
        
        #gribData = getGribData(gribDataSet)
#        if gribData:
#            gribArray.append(gribData)
#            gribDataSize += len(gribData)

    # Check if any data
    if not gribArray:
        # NO data.  Send alert and exit
        message = "No data found for %s, %s." % (sys.argv[1], sys.argv[2])
        MAILER.sendMail(message, alert='WARNING', file=False)
        os._exit(0)

    # Begin writing -- open file first
#    GRIB2 = open(fullPathname, 'w')

except:
    # This is NOT a proper XML document. 
    msgString = EXCFMTR.formatExceptionInfo().replace('\\n', '\n')
    ERRFILE.write(msgString)

ERRFILE.close()

# Cleanup tmp files
if os.path.exists(ERRFILENAME):
    if os.path.getsize(ERRFILENAME) > 0:
        MAILER.sendMail(ERRFILENAME, alert='ERROR', file=True)

    os.remove(ERRFILENAME)
