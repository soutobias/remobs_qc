# -*- coding: utf-8 -*-
"""
Created on Mon Jun 09 10:16:18 2014

Code for create basic bufr message


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
from NdbcToolset import Mailer
from NdbcToolset import ExceptionFormatter
import math



def buildBufrSection0(NC, profile, profileTime):

        BUFR.write('BUFR')
        
        # Write message length as 3 bytes
        BUFR.write(pack(">L", bufrMessageLength)[1:])
        BUFR.write(pack("B", 4))     # BUFR Version Number

        BUFR.write(bufrString)
        BUFR.close()


def buildBufrSection1(NC, profile, profileTime):
    
    bufrSection1List = []
    bufrSection1 = None
    
    try:
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
        bufrSection1List.append(pack("B", 21))

        # Octect 15
        # Version number of local tables
        bufrSection1List.append(pack("B", 0))
        

        # Octect 16-22
        # Data and time of the measurement)
        timeTup = profileTime.timetuple()

        # Octect 16-17 (Year - 4 digits)
        bufrSection1List.append(pack(">H", timeTup[0]))
        # Octect 18 (Month)
        bufrSection1List.append(pack("B", timeTup[1]))
        # Octect 19 (Day)
        bufrSection1List.append(pack("B", timeTup[2]))
        # Octect 20 (Hour)
        bufrSection1List.append(pack("B", timeTup[3]))
        # Octect 21 (Minute)
        bufrSection1List.append(pack("B", timeTup[4]))
        # Octect 22 (Seconds)
        bufrSection1List.append(pack("B", timeTup[5]))


        #Octets 1-3
        # Length of the section 1
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


def buildBufrSection3(NC, profile):
    
    bufrSection3List = []
    bufrSection3 = None

    try:

        # Octect 4
        # Set to zero (reserved)   
        bufrSection3List.append(pack("B", 0))
        
        # Octect 5-6
        # Number of data subsets         
        bufrSection3List.append(pack(">H", 1))
        
        # Octect 7
        # Observed data (Bit1=1), uncompressed (Bit2=0), Bit3-8=0 (reserved). 10000000=128     
        bufrSection3List.append(pack("B", 128)) 

        # Octect 8-9
        fx = (3 * 2**6) + 1 #=193 (decimal)=11000001 (binary)
        bufrSection3List.append(pack("BB", fx, 126))      # 301011

        # Octect 10-11
        # Date
        fx = (3 * 2**6) + 1 #=193 (decimal)=11000001 (binary)
        bufrSection3List.append(pack("BB", fx, 11))      # 301011

        # Octect 12-13
        # Time
        fx = (3 * 2**6) + 1 #=193 (decimal)=11000001 (binary)
        bufrSection3List.append(pack("BB", fx, 12))      # 301012

        # Octect 14-15
        # Latitude and longitude (high accuracy)
        fx = (3 * 2**6) + 1 #=193 (decimal)=11000001 (binary)
        bufrSection3List.append(pack("BB", fx, 21))      # 301021

        # Octect 16-17
        # Sequence for STDMET measurements from moored buoys
        fx = (3 * 2**6) + 6 #=198 (decimal)=11000110 (binary)
        bufrSection3List.append(pack("BB", fx, 21))      # 306038

# Commented. It has same parameters absent in the STDMET file: spread, Wmax, Wave direction of dominant wave
#        # Octect 18-19
#        # Sequence for representation of basic wave measurements
#        fx = (3 * 2**6) + 6 #=198 (decimal)=11000110 (binary)
#        bufrSection3List.append(pack("BB", fx, 21))      # 306038

        # Octect 18-19
        # Duration of wave record
        bufrSection3List.append(pack("BB", 22, 78))      # 022078

        # Octect 20-21        
        # Significant wave height
        bufrSection3List.append(pack("BB", 22, 70))      # 022070

        # Octect 22-23        
        # Average wave period
        bufrSection3List.append(pack("BB", 22, 74))      # 022074       

        # Octect 24-25
        # Spectral peak wave period
        bufrSection3List.append(pack("BB", 22, 86))      # 022071       

        # Octect 26-27
        # Mean Wave direction
        bufrSection3List.append(pack("BB", 22, 86))      # 022086       


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
    
    bufrSection4List = []
    bufrSection4 = None
    excMessage = None

    ############################################################
    # Octect 4
    # Set to zero (reserved)   
    bufrSection4List.append(pack("B", 0))


    ############################################################    
    ############################################################ 
    # 301087  - Moored Buoy ID {includes subsets}    
    ############################################################    
    ############################################################ 


    ############################################################
    # 001087 - WMO Marine Observing platform extended identifier
    # Numeric 23 bits
    wmoIdNum = 0
    
    #TODO: DEBUG
    WMO_ID = 46042
    bts=math.ceil(math.log(WMO_ID,2))
    
    wmoIdNum = int(WMO_ID) * (2**(24-bts))  # bit 17-24 is empty

    # Pack WmoId into 3 bytes
    wmoIdPacked = pack(">I", wmoIdNum)

    # Store 2 bytes and save the third for the empty bits
    bufrSection4List.append(wmoIdPacked[1:3])
    unfilledByte = wmoIdPacked[3]
    # Bits filled: 1111 1110

    ############################################################
    # 001015 - Station or Site Name
    # 160 bits (20 bytes)
    
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
    typeBuoy = chr(18 << 3)  # Decimal 18, left shifted 3 bits
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
    year = Year * 2 ** 10       # bits 21-11    
    month = Month * 2 ** 6     # bits 10-6
    day = Day * 2 ** 0         # bits 5-0
    
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
    hour = Hour * 2 ** 6      # bits 10-6    
    minute = Minute * 2 ** 0    # bits 5-0
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
    
    latScaled = (int(lat * 10 ** 5) + 9000000) << 7
    lonScaled = (int(lon * 10 ** 5) + 18000000) << 6
    print 'latitude=', lat, 'latScaled=', latScaled
    print 'longitude=', lon, 'lonScaled=', lonScaled

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
    # 010004  - Pressure (Pa)
    # scale -1, and units in Pa

    PresScaled = (int(Pres*100*10**-1)

    (string, unfilledByte, unfilledByteBits) = bitShift(unfilledByte, unfilledByteBits, typeBuoy, 6)
    bufrSection4List.append(string)
    # Bits filled: 1111 1000


    ############################################################ 
    # 010051  - Pressure at Sea-Level (Pa)


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
    
    




