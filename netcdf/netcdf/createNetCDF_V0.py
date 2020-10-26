#! /usr/bin/python
# -*- coding: iso-8859-1 -*-
'''
Created on Oct 26, 2010

@author: Bill Smith

PURPOSE:    To create version 0 (zero) NetCDF archival files with realtime qc only applied.
'''
import os
import re
import sys
from xml.dom import minidom
import time
import datetime
import dateutil.parser
import traceback
from ParseBulletin import ParsePrivate
from netCDF4 import Dataset
import sqlite

#
# Function to read program configuration file
#
def getConfigProperties():
    global FILENAME
    global ERRFILE
    cfgMap = {}

    try:
#        cfgFilename = os.path.splitext(FILENAME)[0] + '.cfg'
        cfgFilename = sys.argv[1]

        if not os.path.exists(cfgFilename):
            # critical error, send alert and stop program
            LOG = open(ERRFILE, 'w')
            LOG.write("Configuration file '%s' was not found. Process aborted." %
                      (cfgFilename))
            LOG.close()
            sendMail(ERRFILE, alert='CRITICAL ERROR')

            # cleanup & exit
            os.remove(ERRFILE)
            os._exit(1)
    
        CFG = open(cfgFilename,'r')

        for line in CFG:
            # Skip comment lines
            if line.startswith(';'):
                continue

            # Remove trailing (in-line) comments
            p = line.find(';')
            if p <> -1:
                line = line[0:p]

            # Check if we have key/value pair separated by '='
            p = line.find('=')
            if p == -1:
                continue

            arr = line.split('=')
            cfgMap[arr[0].strip()] = arr[1].strip()

        CFG.close()

        # Check if we got any parameters
        if len(cfgMap) == 0:
            raise Exception('Processing Error', 'No valid parameters found')
    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        sendMail(msgString, alert='Error', file=False)
        os._exit(1)

    return cfgMap

#
# Function to email the results
#
def sendMail(mailMsg, alert='Alert', file=True):
    USER=os.environ['LOGNAME']
    HOST=os.environ['HOSTNAME']
    global wmoId
    
    if file:
        LOG = open(mailMsg, 'r')
    else:
        LOG = mailMsg

    mailCmd='/usr/lib/sendmail -t -f ' + USER + '@' + HOST

    MAIL = os.popen(mailCmd, 'w')
    MAIL.write("Subject: " + alert + ' reported by ' + FILENAME
        + ' on ' + HOST + "\n")
    MAIL.write("To: " + "Bill.C.Smith@noaa.gov\n")
    MAIL.write("\n")

    for line in LOG:
        MAIL.write(line)

    if file:
        LOG.close()
        
    try:
        MAIL.write('wmoId=' + wmoId)
    except:
        None

    MAIL.close()

#
# Function to write a message to the LOG file
#
def writeLog(message):
    global LOG
    
    try:
        LOG.write("LOGENTRY: " + message + "\n")
    except:
        None

#
# Format Exception Information
#
def formatExceptionInfo(maxTBlevel=5):
    cla, exc, trbk = sys.exc_info()
    excName = cla.__name__
    try:
        excArgs = exc.__dict__["args"]
    except KeyError:
        excArgs = "<no args>"
    excTb = traceback.format_tb(trbk, maxTBlevel)
    return (excName, excArgs, excTb)

#
# print usage statement
#
def usage():
    print "Usage: %s [configuration file] [station id]" % \
        (os.path.basename(sys.argv[0]))

#
# Read XMLdoc and create a dictionary xref of payloads -> stations
#
def getPayloadStationDic(xmlDoc):

    payloadStationDic = {}
    
    for stationNode in xmlDoc.getElementsByTagName('station'):
        stationId = stationNode.getAttribute('id')
        
        '''
        Get list of payloads for this station
        '''
        for stationChildNode in stationNode.childNodes:
            nodeName = stationChildNode.localName
            if nodeName != 'payload':
                continue
            
            pyldId = stationChildNode.getAttribute('id')
            payloadStationDic[pyldId] = stationId
            
    return payloadStationDic

#
# Parse XMLdocument Node and find the payloads on this station
#
def getPayloads(stationNode):

    payloads = set()
    
    try:
        for stationChildNode in stationNode.childNodes:
            if stationChildNode.localName == 'payload': 
                payloadId = stationChildNode.getAttribute('id')
                
                for payloadChildNode in stationChildNode.childNodes:
                    if payloadChildNode.localName == 'configuration':
                        status = payloadChildNode.getAttribute('status')
                        
                        if status == 'Deployed/Powered Up':
                            payloads.add(payloadId)
                            break
                break
        
    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        sendMail(msgString, alert='Error', file=False)
        os._exit(1)
            
    payloadList = sorted(payloads)
    return payloadList

#
# Parse XMLdocument Node and find the current deployment
#
def getDeploymentNode(stationNode, pyldId):

    deploymentDic = {}
    deploymentNode = None
    configId = None
    
    try:
        for stationChildNode in stationNode.childNodes:
            if stationChildNode.localName == 'sensor_metadata': 
                configId = stationChildNode.getAttribute('config_id')
                deploymentDic[configId] = stationChildNode
    
        for stationChildNode in stationNode.childNodes:
            if stationChildNode.localName == 'payload': 
                payloadId = stationChildNode.getAttribute('id')
                if payloadId != pyldId:
                    continue
                
                for payloadChildNode in stationChildNode.childNodes:
                    if payloadChildNode.localName == 'configuration':
                        configId = payloadChildNode.getAttribute('config_id')
                        #TODO: add code to parse status & status_date
                        break
                break
        
        if configId != None:
            try:
                deploymentNode = deploymentDic[configId]
            except:
                None
    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        sendMail(msgString, alert='Error', file=False)
        os._exit(1)
            
    return deploymentNode
   
#
# Parse XMLdocument wave sensor node and find the frequency_width array
#
def getFrequencyBounds(waveSensorNode):
    
    freqWidthList = None
    
    for waveChildNode in waveSensorNode.childNodes:
        if waveChildNode.localName == 'frequency_width':
            freqWidthList = waveChildNode.firstChild.nodeValue.strip().split(';')
            break
        
    return freqWidthList
   
#
### Read a BULLETINS record
#
def get_a_record(mockfile, sep="\x03"):
    buffer = ""
    while True:
        idx = buffer.find(sep) + len(sep)
        while idx >= len(sep):
            yield buffer[:idx]
            buffer = buffer[idx:]
            idx = buffer.find(sep) + len(sep)
        rl = mockfile.readline()
        if rl == "":
            break
        else:
            buffer = '%s%s' % (buffer, rl)
    yield buffer
    raise StopIteration

#
# compute simple quality flag (0,1,2,3)
#
def qualityFlag(qcFlags):
    m1 = re.search('D', qcFlags)
    if m1:
        return 2

    m2 = re.search('[A-Z]', qcFlags)
    if m2:
        return 1

    return 0

#
# Create and initialize a NetCDF document
#
def createNetCDF(xmlDoc, wmoId, deploymentId):
    global ARCHIVESTOP
    global configProperties
    
    try:
        for stationNode in xmlDoc.getElementsByTagName('station'):
            stationId = stationNode.getAttribute('id')
        
            if stationId == wmoId:
                break
        
        # If this isn't true, cannot continue
        assert stationId == wmoId
        
        # Find deploymentId (configId) of primary payload
        payloadList = getPayloads(stationNode)
        deploymentNode = getDeploymentNode(stationNode, payloadList[0])
        #deploymentId = deploymentNode.getAttribute('config_id')
        
        # Create NetCDF file
        filename = configProperties['netCdfDir'] + '/NDBC_' + wmoId \
            + '_' + ARCHIVESTOP.strftime('%Y%m') \
            + '_D' + str(deploymentId) \
            + '_v00.nc'
               
        print filename      
        NC = Dataset(filename, 'w')
        NC.id = wmoId
        
        # Type/Naming Authority
        try:
            for stationChildNode in stationNode.childNodes:
                if stationChildNode.localName != 'type':
                    continue
                
                type = str(stationChildNode.firstChild.nodeValue.strip())
                
                if type == 'C-MAN Station':                
                    NC.naming_authority = 'NWS'
                    NC.geospatial_vertical_datum = "urn:x-noaa:def:datum:noaa::MSL"
                else:
                    NC.naming_authority = 'WMO'
                    
                    if wmoId[:2] == "45":
                        NC.geospatial_vertical_datum = "urn:x-noaa:def:datum:noaa::MSL"
                    else:
                        NC.geospatial_vertical_datum = "urn:ogc:def:datum:epsg::5113"

        except:
            None

        NC.ioos_id = 'urn:ioos:station:wmo:' + wmoId.lower()
        NC.wmo_id = wmoId
        
        NC.institution = 'National Data Buoy Center'
        NC.institution_abbreviation = 'NDBC'
        
        # Title
        NC.title = "Meteorological and Oceanographic Data Collected from " \
            "the National Data Buoy Center's Coastal Marine Automated " \
            "Network and Weather Buoys"
            
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
        
        try:
            for stationChildNode in stationNode.childNodes:
                if stationChildNode.localName != 'description':
                    continue
                
                stationName = str(stationChildNode.firstChild.nodeValue.strip())
                NC.station_name = stationName
        except:
            None
        
        # Water Depth
        try:
            for stationChildNode in stationNode.childNodes:
                if stationChildNode.localName != 'water_depth':
                    continue
                
                water_depth = float(stationChildNode.firstChild.nodeValue.strip())
                NC.sea_floor_depth_below_sea_level = water_depth
        except:
            None
            
        # Site elevation
        try:
            site_elevation = 0.0    # Assume sea-level elevation (ocean buoys)
            
            for stationChildNode in stationNode.childNodes:
                if stationChildNode.localName != 'site_elevation':
                    continue
                
                site_elevation = float(stationChildNode.firstChild.nodeValue.strip())
                
            NC.site_elevation = site_elevation
        except:
            None

        # QC Manual
        NC.qc_manual = 'http://www.ndbc.noaa.gov/NDBCHandbookofAutomatedDataQualityControl2009.pdf'
        
        # Key words
        NC.keywords = 'Atmospheric Pressure, Sea level Pressure, ' \
            'Atmospheric Temperature, Surface Temperature, ' \
            'Dewpoint Temperature, Humidity, Surface Winds, ' \
            'Ocean Winds, Ocean Temperature, Sea Surface Temperature, ' \
            'Ocean Waves,  Wave Height, Wave Period, Wave Spectra, ' \
            'Longwave Radiation, Shortwave Radiation, Conductivity, ' \
            'Salinity,  Ocean Currents'
        NC.keywords_vocabulary = 'GCMD Science Keywords'
        NC.standard_name_vocabulary = "CF-16.0"
        NC.scientific_project = 'None'
        NC.restrictions = 'There are no restrictions placed on these data.'
        NC.Metadata_Conventions = "Unidata Dataset Discovery v1.0"
        
        NC.cdm_data_type = "station"
        NC.history = (os.path.basename(filename)[:-6] + "history.txt").encode('latin-1')
        NC.format_version = ""
        NC.processing_level = "0"
        NC.distribution_statement = "There are no restrictions placed on these data."
        NC.citation = "The National Data Buoy Center should be cited as the " \
            "source of these data if used in any publication."
            
        NC.publisher_name = "NDBC"
        NC.publisher_url = "http://www.ndbc.noaa.gov"
        NC.publisher_email = "webmaster.ndbc@noaa.gov"

        
        return (NC, stationNode, filename)   
            
    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        sendMail(msgString, alert='Error', file=False)
        os._exit(1)

def createPayloadGroups(NC, stationNode):
    global ARCHIVESTART
    global ARCHIVESTOP
    
    payloadSet = set()
    hullIdDic = {}
    
    try:
        startTime = time.mktime(ARCHIVESTART.timetuple())
        stopTime = time.mktime(ARCHIVESTOP.timetuple())

        for stationChildNode in stationNode.childNodes:
            if stationChildNode.localName == 'payload':
                payloadId = stationChildNode.getAttribute('id')
                payloadSet.add(payloadId)
                
        payloadList = sorted(payloadSet)
        payloadNumber = 1
        for payloadId in payloadList:
            groupName = "payload_%d" % (payloadNumber)
            pyldGroup = NC.createGroup(groupName)
            pyldGroup.payload_id = str(payloadId)
            payloadNumber += 1
            
            # deployment configurations
            for stationChildNode in stationNode.childNodes:
                if stationChildNode.localName == 'payload':
                    payloadId2 = stationChildNode.getAttribute('id')
                    if payloadId2 == payloadId:
                        configSet = set()
                        for payloadChildNode in stationChildNode.childNodes:
                            if payloadChildNode.localName == 'parameters':
                                try:
                                    configId = payloadChildNode.getAttribute('config_id')
                                    hullId = payloadChildNode.getAttribute('hull_id')
                                    if hullId:
                                        hullIdDic[configId] = hullId
                                except:
                                    None
                                
                            if payloadChildNode.localName == 'configuration':
                                configId = payloadChildNode.getAttribute('config_id')
                                configSet.add(configId)
                        
#                        print 'configSet=', configSet
                        configList = sorted(configSet)
                        deploymentNumber = 1

                        #TODO: Add code to select configId for multiple deployments in a month
                            
                        for configId in configList:
#                            deploymentName = 'deployment_%d' % (deploymentNumber)
#                            deploymentGroup = pyldGroup.createGroup(deploymentName)
#                            deploymentGroup.id = str(configId)
                            dn = getDeploymentNode(stationNode, payloadId)
                            
                            for deploymentChildNode in dn.childNodes:
                                nodeName = deploymentChildNode.localName
                                if nodeName != 'payload':
                                    continue
                                
                                    

                                installDateStr = deploymentChildNode.getAttribute('install_date')
                                if installDateStr:
                                    installDateTup = time.strptime(installDateStr, '%Y-%m-%dT%H:%M:%SZ')
                                    installDate = time.mktime(installDateTup)
                                    if installDate >= stopTime:
                                        continue
            
                                for name in ['description', 'manufacturer', 'part_number',
                                 'serial_number', 'install_date']:
                                
                                    try:
                                        pyldGroup.__setattr__(name, str(deploymentChildNode.getAttribute(name)))
                                    except:
                                        None
                                        
                                # Add Hull Id
                                if configId in hullIdDic:
                                    hullId = hullIdDic[configId]
                                    pyldGroup.__setattr__('hull_id', str(hullId))
                                
                            break


    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        sendMail(msgString, alert='Error', file=False)
        os._exit(1)
#
# Get the default spatial coordinates (latitude, longitude, site_elevation)
#        
def getSpatialDefaults(stationNode):
    
    minLatitude = None
    maxLatitude = None
    minLongitude = None
    maxLongitude = None
    minVertical = 0.0
    maxVertical = 0.0
    
    try:
        for stationChildNode in stationNode.childNodes:
            if stationChildNode.localName == 'latitude':
                minLatitude = float(stationChildNode.firstChild.nodeValue.strip())
                maxLatitude = minLatitude
            elif stationChildNode.localName == 'longitude':
                minLongitude = float(stationChildNode.firstChild.nodeValue.strip())
                maxLongitude = minLongitude
            elif stationChildNode.localName == 'site_elevation':
                minVertical = float(stationChildNode.firstChild.nodeValue.strip())
                maxVertical = minVertical

        return (minLatitude, maxLatitude, minLongitude, maxLongitude, minVertical, maxVertical)
        
    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        sendMail(msgString, alert='Error', file=False)
        os._exit(1)

#
# Write Spatial coordinates to the NetCDF document
#
def putSpatialCoordinates(NC, minLat, maxLat, minLon, maxLon, vertMin, vertMax):
    
    try:
        if minLat:
            NC.geospatial_lat_min = minLat
            
        if maxLat:
            NC.geospatial_lat_max = maxLat
        
        NC.geospatial_lat_units = 'degrees'
        
        if minLon:
            NC.geospatial_lon_min = minLon
            
        if maxLon:
            NC.geospatial_lon_max = maxLon
        
        NC.geospatial_lon_units = 'degrees'
            
        NC.geospatial_vertical_min = vertMin
        NC.geospatial_vertical_max = vertMax
        NC.geospatial_vertical_positive = 'up'
        NC.geospatial_vertial_units = 'meters above mean sea level'
            
    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        sendMail(msgString, alert='Error', file=False)
        os._exit(1)

#
# create NetCDF Indexes
#    
def createIndexes(NC, SD, stationNode):
    global ARCHIVESTART
    global ARCHIVESTOP
    global TIMEKEYS
    global TIME10KEYS
    global TIMEMKEYS
    global WAVETIMEKEYS
    global WAVEPAYLOADTIMEKEYS
    global WAVEFREQUENCYKEYS
    global ADCPDEPTHKEYS

    timeSet = set()     # Time for Standard Met
    timeWSet = set()    # Time for Wave measurements
    time10Set = set()   # Time for Continuous Winds
    timeMSet = set()    # Time for Max hourly measurements
    timeMBndsDic = {}   # Time cell boundaries for Max hourly measurements
    WAVETIMEKEYS = set()
    WAVEFREQUENCYKEYS = {}
#    adcpDepthSet = set()    # Depths for Acoustic Doppler Current Profiles
    
    payloadSet = set()
    
    try:
        startTime = time.mktime(ARCHIVESTART.timetuple())
        stopTime = time.mktime(ARCHIVESTOP.timetuple())
        # make sure at beginning of file
        SD.seek(0)
            
        # loop thru all bulletin records
        for bullRec in get_a_record(SD):
            myObj = ParsePrivate(bullRec)
            eodTime = myObj.getEndDataTime()
            
            if eodTime == None:
                continue
            
            # Is the current timeset within bounds?
            if eodTime < startTime:
                continue
            
            if eodTime > stopTime:
                continue
            
            # Add time to standard met
            timeSet.add(eodTime)
            
            pyldId = myObj.getPayloadId()
            payloadSet.add(pyldId)
            
            myMeas = myObj.getMeasurements()
            
            try:
                # Check continuous winds
                if myMeas['CWS1'] or myMeas['OWS1']:
                    cwindTimes = myObj.get10MinTimes()
                    for i in range(6):
                        time10Set.add(cwindTimes[i])
            except:
                None

            try:
                if myMeas['MXGT1'] and myMeas['MXMIN1']:
                    myTime = myObj.getMinuteTime(myMeas['MXMIN1'][0])
                    if myTime:
                        timeMSet.add(myTime)
                        timeMBndsDic[myTime] = eodTime
            except:
                None

            try:
                if myMeas['MXGT2'] and myMeas['MXMIN2']:
                    myTime = myObj.getMinuteTime(myMeas['MXMIN2'][0])
                    if myTime:
                        timeMSet.add(myTime)
                        timeMBndsDic[myTime] = eodTime
            except:
                None

            try:
                if myMeas['MX1MGT1'] and myMeas['MX1MMIN1']:
                    myTime = myObj.getMinuteTime(myMeas['MX1MMIN1'][0])
                    if myTime:
                        timeMSet.add(myTime)
                        timeMBndsDic[myTime] = eodTime
            except:
                None

            try:
                if myMeas['MX1MGT2'] and myMeas['MX1MMIN2']:
                    myTime = myObj.getMinuteTime(myMeas['MX1MMIN2'][0])
                    if myTime:
                        timeMSet.add(myTime)
                        timeMBndsDic[myTime] = eodTime
            except:
                None

            try:
                if myMeas['MN1MSLP1'] and myMeas['MSLPMIN1']:
                    myTime = myObj.getMinuteTime(myMeas['MN1MSLP1'][0])
                    if myTime:
                        timeMSet.add(myTime)
                        timeMBndsDic[myTime] = eodTime
            except:
                None

            try:
                if myMeas['MN1MSLP2'] and myMeas['MSLPMIN2']:
                    myTime = myObj.getMinuteTime(myMeas['MN1MSLP2'][0])
                    if myTime:
                        timeMSet.add(myTime)
                        timeMBndsDic[myTime] = eodTime
            except:
                None
                
            # Acoustic Doppler Current Profiler
#            try:
#                bn01Dist = int(myMeas['BN01DIST'])
#                bnNumber = int(myMeas['BNNUMBER'])
#                bnLength = int(myMeas['BNLENGTH'])
                
#                for i in range(bnNumber):
#                    depth = i * bnLength + bn01Dist
#                    adcpDepthSet.add(depth)
#            except:
#                None

        # Make sure we have data.
        if not payloadSet:
            raise IOError, "No data found for this station."
        
        payloads = sorted(payloadSet)
        deploymentNode = getDeploymentNode(stationNode, payloads[0])

        # Get met_duration
        metDuration = int(float(deploymentNode.getAttribute('met_duration')))
        
        #        
        # Create NetCDF Dimensions
        #
        
        # Standard met time dimensions
        TIMEKEYS = sorted(timeSet)
        
        print 'len(timeKeys)=', len(timeSet)

        NC.createDimension('time', len(TIMEKEYS))
#        NC.createDimension('time_bnds', 2)

        # Continuous winds time dimensions
        if len(time10Set) > 0:
            TIME10KEYS = sorted(time10Set)
            print 'len(time10Keys)=', len(time10Set)
            NC.createDimension('time10', len(TIME10KEYS))
#            NC.createDimension('time10_bnds', 2)
        
        # Max hourly time dimensions
        if len(timeMSet) > 0:
            TIMEMKEYS = sorted(timeMSet)
            print 'len(timeMKeys)=', len(timeMSet)
            NC.createDimension('timem', len(TIMEMKEYS))
#            NC.createDimension('timem_bnds', 2)
        
        # ADCP Depths
#        if len(adcpDepthSet) > 0:
#            ADCPDEPTHKEYS = sorted(adcpDepthSet)
#            print 'len(ADCPDEPTHKEYS)=', len(adcpDepthSet)
#            NC.createDimension('adcp_depth', len(ADCPDEPTHKEYS))

        #        
        # Create NetCDF Variables
        #
        
        # Standard met time variables
        times = NC.createVariable('time', 'i4', ('time',))
        times.long_name = 'time'
        times.standard_name = 'time'
        times.units = 'seconds since 1970-01-01 00:00:00 UTC'
        times[:] = TIMEKEYS

#        timesb = NC.createVariable('time_bnds',
#            'i4', ('time','time_bnds'))
#        timesb.units = 'seconds since 1970-01-01 00:00:00 UTC'

#        for t in range(len(TIMEKEYS)):
#            timesb[t,0] = TIMEKEYS[t] - (metDuration * 60)
#            timesb[t,1] = TIMEKEYS[t]

        # Continuous winds time variables
        if len(time10Set) > 0:
            times10 = NC.createVariable('time10', 'i4', ('time10',))
            times10.long_name = 'ten_minute_time'
#            times10.bounds = 'time10_bnds'
            times10.units = 'seconds since 1970-01-01 00:00:00 UTC'
            times10[:] = TIME10KEYS

            # create the time index bounds for times10
#            times10b = NC.createVariable('time10_bnds',
#                'i4', ('time10','time10_bnds'))
#            times10b.units = 'seconds since 1970-01-01 00:00:00 UTC'
            
#            for t in range(len(TIME10KEYS)):
#                times10b[t,0] = TIME10KEYS[t] - 600
#                times10b[t,1] = TIME10KEYS[t]
            
        # Create time index for irregularly reporting variables like peak hourly winds
        if len(timeMSet) > 0:
            timesM = NC.createVariable('timem', 'i4', ('timem',))
            timesM.long_name = 'max_hourly_measured_time'
            timesM.units = 'seconds since 1970-01-01 00:00:00 UTC'
#            timesM.bounds = 'timem_bnds'
            try:
                timesM[:] = TIMEMKEYS
            except:
                print 'len(TIMEMKEYS)=', len(TIMEMKEYS)
                print 'type(TIMEMKEYS)=', type(TIMEMKEYS)
                print 'TIMEMKEYS=', TIMEMKEYS
            
            # create time index bounds
#            timesMb = NC.createVariable('timem_bnds',
#                'i4', ('timem','timem_bnds'))
#            timesMb.units = 'seconds since 1970-01-01 00:00:00 UTC'

#            for t in range(len(TIMEMKEYS)):
#                try:
#                    timeKey = TIMEMKEYS[t]
#                    startTime = timeMBndsDic[timeKey]
#                    timesMb[t,1] = startTime
#                    timesMb[t,0] = startTime - 3600
#                except:
#                    None
            
        # Create depth index for ADCP
#        if len(adcpDepthSet) > 0:
#            depthAdcp = NC.createVariable('adcp_depth', 'i4', ('adcp_depth',))
#            depthAdcp.long_name = 'adcp_depth'
#            depthAdcp.units = 'm'
#            try:
#                depthAdcp[:] = ADCPDEPTHKEYS
#            except:
#                print 'len(ADCPDEPTHKEYS)=', len(ADCPDEPTHKEYS)
#                print 'type(ADCPDEPTHKEYS)=', type(ADCPDEPTHKEYS)
#                print 'ADCPDEPTHKEYS=', ADCPDEPTHKEYS

        
        # Create wave indexes, if exists
        for pyldId in payloadSet:
            
            print payloadSet
            dn = getDeploymentNode(stationNode, pyldId)
            
            for deploymentChildNode in dn.childNodes:
                print 'localName=', deploymentChildNode.localName
                if deploymentChildNode.localName == 'wave_sensor':
                    waveSystemType = deploymentChildNode.getAttribute('type')
                    waveDuration = int(float(deploymentChildNode.getAttribute('wave_duration')))
                    print waveSystemType, waveDuration
                    
                    waveTimeVarName = "time_%s_%s" % (waveSystemType.lower(), waveDuration)
                    print waveTimeVarName
                    
                    WAVEPAYLOADTIMEKEYS[pyldId] = waveTimeVarName

                    # Add Wave time variables
                    if waveTimeVarName not in WAVETIMEKEYS:
                        WAVETIMEKEYS.add(waveTimeVarName)

                        NC.createDimension(waveTimeVarName, len(TIMEKEYS))
                        
                        timesW = NC.createVariable(waveTimeVarName, 'i4', \
                            (waveTimeVarName,))
                        timesW.long_name = 'time'
                        timesW.units = 'seconds since 1970-01-01 00:00:00 UTC'
#                        timesW.bounds = (waveTimeVarName + '_bnds').encode('latin-1')
                        timesW[:] = TIMEKEYS

#                        timesWb = NC.createVariable(waveTimeVarName + '_bnds',
#                            'i4', (waveTimeVarName,'time_bnds'))
#                        timesWb.units = 'seconds since 1970-01-01 00:00:00 UTC'

#                        for t in range(len(TIMEKEYS)):
#                            timesWb[t,0] = TIMEKEYS[t] - (waveDuration * 60)
#                            timesWb[t,1] = TIMEKEYS[t]
                        
                    for waveChildNode in deploymentChildNode.childNodes:
                        if waveChildNode.localName == 'center_frequencies':
                            freqList = waveChildNode.firstChild.nodeValue.strip().split(';')
                            dimName = "wave_%s" % waveSystemType.lower()
                            
                            '''
                                If the wave spectral frequencies exist and are not already
                                defined in WAVEFREQUENCYKEYS.
                            '''
                            if len(freqList) > 0 and dimName not in WAVEFREQUENCYKEYS: 
                                dimBounds = dimName + '_bnds'

                                NC.createDimension(dimName, len(freqList))
                                NC.createDimension(dimBounds, 2)

                                indexF = NC.createVariable(dimName, 'f4', \
                                    (dimName,))
                                indexF.long_name = 'sea_surface_wave_frequency'
                                indexF.bounds = dimBounds.encode('latin-1')
                                indexF.units = 'Hz'
                                
                                indexFb = NC.createVariable(dimBounds,
                                    'f4', (dimName, dimBounds))                   
                                indexFb.units = 'Hz'

                                for i in range(len(freqList)):
                                    f = float(freqList[i])
                                    indexF[i] = f
                                    
                                WAVEFREQUENCYKEYS[dimName] = freqList
                                
                                # Set frequency boundaries
                                freqBndsList = getFrequencyBounds(deploymentChildNode)
                                if freqBndsList and len(freqBndsList) > 0:
                                    for i in range(len(freqList)):
                                        f = float(freqList[i])
                                        w = float(freqBndsList[i])
                                        b0 = f - (w/2)
                                        b1 = f + (w/2)
                                        indexFb[i,0] = b0
                                        indexFb[i,1] = b1
    
    except IOError:
        return -1
    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        sendMail(msgString, alert='Error', file=False)
        os._exit(1)
        
    return 0

#
# Search group and find variable or return None if it doesn't exist
#
def getVariable(NCG, variableName):
    
    ncVariable = None
    
    try:
        ncVariable = NCG.variables[variableName]
    except:
        None

    return ncVariable

#
# Search NetCDF document & find the specified sensor for this payload    
#            
def getSensor(NC, pyldId, dn, sensorName, sensorNumber):
    global ARCHIVESTART
    global ARCHIVESTOP
    
    ncGroupSensor = None
    
    try:
        startTime = time.mktime(ARCHIVESTART.timetuple())
        stopTime = time.mktime(ARCHIVESTOP.timetuple())

        # get configid from XML Document deployment node
        dnId = dn.getAttribute('config_id')
        
        for group1 in NC.groups.values():
            ncAttrPyldId = getattr(group1, 'payload_id')

            if ncAttrPyldId != pyldId:
                # this is not the payload you're looking for
                continue
            
            # found it
            groupName = '%s_%d' % (sensorName, sensorNumber)
                
            try:
                ncGroupSensor = group1.groups[groupName]
            except:
                None
                
            if ncGroupSensor == None:
                # First time, need to create group
                groupName = '%s_%d' % (sensorName, sensorNumber)
                newGroup = group1.createGroup(groupName)
#                print groupName
                
                # find sensor
                for deploymentChildNode in dn.childNodes:
                    nodeName = deploymentChildNode.localName
                    if nodeName != sensorName:
                        continue
            
                    # check position
                    positionStr = deploymentChildNode.getAttribute('position')
                    
                    if len(positionStr) > 0:
                        # make sure we have correct sensor number
                        position = int(positionStr)
                        if position != sensorNumber:
                            continue
                    elif sensorNumber > 1:
                        # No position, skip if position is blank
                        continue
                        
                    installDateStr = deploymentChildNode.getAttribute('install_date')
#                    print 'installDateStr=[', installDateStr, ']'
                    if installDateStr:
                        installDateTup = time.strptime(installDateStr, '%Y-%m-%dT%H:%M:%SZ')
                        installDate = time.mktime(installDateTup)
#                        print 'installDate, stopTime=', installDate, stopTime
                        if installDate >= stopTime:
                            continue
                        
                    removalDateStr = deploymentChildNode.getAttribute('removal_date')    
                    if removalDateStr:
#                        print 'removalDateStr=',removalDateStr
                        removalDateTup = time.strptime(removalDateStr, '%Y-%m-%dT%H:%M:%SZ')
                        removalDate = time.mktime(removalDateTup)
                        if removalDate <= startTime:
                            continue
                    
                    for name in ['description', 'manufacturer', 'part_number',
                                 'serial_number', 'install_date', 'height_of_instrument',
                                 'sampling_period', 'sampling_rate', 'calibration_date']:
                        try: 
                            # Special condition for Wave System part numbers
                            if (name == 'part_number' and sensorName == 'wave_sensor'):
                                newGroup.__setattr__('sensor_type', str(deploymentChildNode.getAttribute(name)))
                            else:
                                newGroup.__setattr__(name, str(deploymentChildNode.getAttribute(name)))
                        except:
                            None
                            
                    if sensorName == 'wave_sensor':
                        newGroup.__setattr__('type', str(deploymentChildNode.getAttribute('type')))
                        newGroup.__setattr__('wave_processing', str(deploymentChildNode.getAttribute('wave_processing')))
                        
                ncGroupSensor = newGroup
    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        writeLog(msgString)

    return ncGroupSensor
        
#
# Search NetCDF document & find Wind sensor for this payload    
#            
def getWind(NC, pyldId, dn, sensorNumber):
    global ARCHIVESTART
    global ARCHIVESTOP

    ncGroupWind = None
    
    try:
        startTime = time.mktime(ARCHIVESTART.timetuple())
        stopTime = time.mktime(ARCHIVESTOP.timetuple())

        # get configid from XML Document deployment node
        dnId = dn.getAttribute('config_id')
        
        for group1 in NC.groups.values():
            ncAttrPyldId = getattr(group1, 'payload_id')

            if ncAttrPyldId != pyldId:
                # this is not the payload you're looking for
                continue
            
            # found it
            groupName = 'anemometer_%d' % (sensorNumber)
                
            try:
                ncGroupWind = group1.groups[groupName]
            except:
                None
                
            if ncGroupWind == None:
                # First time, need to create group
                groupName = 'anemometer_%d' % (sensorNumber)
#                print 'creating new group:', groupName
                newGroup = group1.createGroup(groupName)
                
                # find wind
                for deploymentChildNode in dn.childNodes:
                    nodeName = deploymentChildNode.localName
                    if nodeName != 'wind':
                        continue
            
                    # check position
                    positionStr = deploymentChildNode.getAttribute('position')
                    
#                    print 'position=', positionStr, ', sensorNumber=', sensorNumber
                    
                    if len(positionStr) > 0:
                        # make sure we have correct sensor number
                        position = int(positionStr)
                        if position != sensorNumber:
#                            print 'position != sensorNumber'
                            continue

                    installDateStr = deploymentChildNode.getAttribute('install_date')
                    if installDateStr:
                        installDateTup = time.strptime(installDateStr, '%Y-%m-%dT%H:%M:%SZ')
                        installDate = time.mktime(installDateTup)
                        if installDate >= stopTime:
                            continue
                        
                    for name in ['description', 'manufacturer', 'part_number',
                                 'serial_number', 'install_date', 'height_of_instrument',
                                 'sampling_period', 'sampling_rate', 'calibration_date']:
                        try:
                            newGroup.__setattr__(name, str(deploymentChildNode.getAttribute(name)))
                        except:
                            None
                        
                ncGroupWind = newGroup
    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        sendMail(msgString, alert='Error', file=False)
        os._exit(1)

    return ncGroupWind

#
# put wind data in NetCDF document
#
def putWinds(NC, SD, stationNode, dataObj):
    global TIMEKEYS
    global TIME10KEYS
    global TIMEMKEYS

    eodTime = dataObj.getEndDataTime()
    timeIndx = TIMEKEYS.index(eodTime)
    pyldId = dataObj.getPayloadId()
    dn = getDeploymentNode(stationNode, pyldId)
    myMeas = dataObj.getMeasurements()
    released =  dataObj.getGtsReleasedMeasurements()

    #
    # Anemometer 1
    #
    try:
        wspd1 = myMeas['WSPD1']
        wdir1 = myMeas['WDIR1']
        gust1 = myMeas['GUST1']
                
        ncWind1 = getWind(NC, pyldId, dn, 1)
        assert ncWind1 != None
                
        # Wind speed
        if wspd1[1] != 'M':     
            name= 'wind_speed'
            shortName = 'wind_speed'
    
            ncWspd1 = getVariable(ncWind1, shortName)
                    
            if ncWspd1 == None:
                # Add it
                ncWspd1 = ncWind1.createVariable(shortName,
                    'f4', ('time',), zlib=True)
                ncWspd1.long_name = name
                ncWspd1.standard_name = name
                ncWspd1.units = 'm/s'
                    
            ncWspd1[timeIndx] = float(wspd1[0])

            # QC flags                    
            ncWspd1Qc = getVariable(ncWind1, shortName + '_qc') 
                    
            if ncWspd1Qc == None:
                # Add it
                ncWspd1Qc = ncWind1.createVariable(shortName + '_qc', 'b',
                    ('time',), zlib=True, fill_value= -9)
                ncWspd1Qc.flag_values = 0,1,2,3
                ncWspd1Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

            ncWspd1Qc[timeIndx] = qualityFlag(wspd1[1])
                    
            # NDBC QC flags
            ncWspd1QcDet = getVariable(ncWind1, shortName + '_detail_qc')
            if ncWspd1QcDet == None:
                # Add it
                ncWspd1QcDet = ncWind1.createVariable(shortName + '_detail_qc', 'c',
                    ('time',), zlib=True, fill_value= '_')
                ncWspd1QcDet.flag_values = 'see NDBC QC Manual'

            ncWspd1QcDet[timeIndx] = wspd1[1][0]
            
            # Release flag
            ncWspd1Rel = getVariable(ncWind1, shortName + '_release') 
                    
            if ncWspd1Rel == None:
                # Add it
                ncWspd1Rel = ncWind1.createVariable(shortName + '_release', 
                    'b', ('time',), zlib=True, fill_value= -9)
                ncWspd1Rel.flag_values = 0,1
                ncWspd1Rel.flag_meanings = 'not_released released'
                ncWspd1Rel.comment = 'indicates datum was publicly released in realtime'

            if 'WSPD1' in released:
                ncWspd1Rel[timeIndx] = 1
            else:
                ncWspd1Rel[timeIndx] = 0

        # wind direction
        if wdir1[1] != 'M':
            name= 'wind_from_direction'
            shortName = 'wind_direction'

            ncWdir1 = getVariable(ncWind1, shortName)
                    
            if ncWdir1 == None:
                # Add it
                ncWdir1 = ncWind1.createVariable(shortName, 'i2', ('time',), zlib=True)
                ncWdir1.long_name = name
                ncWdir1.standard_name = name
                ncWdir1.units = 'degrees clockwise from North'
                        
            ncWdir1[timeIndx] = int(float(wdir1[0]))
                
            # QC flags                    
            ncWdir1Qc = getVariable(ncWind1, shortName + '_qc') 
                    
            if ncWdir1Qc == None:
                # Add it
                ncWdir1Qc = ncWind1.createVariable(shortName + '_qc', 'b',
                    ('time',), zlib=True, fill_value= -9)
                ncWdir1Qc.flag_values = 0,1,2,3
                ncWdir1Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

            ncWdir1Qc[timeIndx] = qualityFlag(wdir1[1])
                    
            # NDBC QC flags
            ncWdir1QcDet = getVariable(ncWind1, shortName + '_detail_qc')
            if ncWdir1QcDet == None:
                # Add it
                ncWdir1QcDet = ncWind1.createVariable(shortName + '_detail_qc', 'c',
                    ('time',), zlib=True, fill_value= '_')
                ncWdir1QcDet.flag_values = 'see NDBC QC Manual'

            ncWdir1QcDet[timeIndx] = wdir1[1][0]

            # Release flag
            ncWdir1Rel = getVariable(ncWind1, shortName + '_release') 
                    
            if ncWdir1Rel == None:
                # Add it
                ncWdir1Rel = ncWind1.createVariable(shortName + '_release', 
                    'b', ('time',), zlib=True, fill_value= -9)
                ncWdir1Rel.flag_values = 0,1
                ncWdir1Rel.flag_meanings = 'not_released released'
                ncWdir1Rel.comment = 'indicates datum was publicly released in realtime'

            if 'WDIR1' in released:
                ncWdir1Rel[timeIndx] = 1
            else:
                ncWdir1Rel[timeIndx] = 0

        # Gust
        if gust1[1] != 'M':         
            name= 'wind_speed_of_gust'
            shortName = 'wind_gust'

            ncGust1 = getVariable(ncWind1, shortName)
                    
            if ncGust1 == None:
                # Add it
                ncGust1 = ncWind1.createVariable(shortName, 'f4', ('time',), zlib=True)
                ncGust1.long_name = name
                ncGust1.standard_name = name
                ncGust1.units = 'm/s'
                    
            ncGust1[timeIndx] = float(gust1[0])
                
            # QC flags                    
            ncGust1Qc = getVariable(ncWind1, shortName + '_qc') 
                    
            if ncGust1Qc == None:
                # Add it
                ncGust1Qc = ncWind1.createVariable(shortName + '_qc', 'b',
                    ('time',), zlib=True, fill_value= -9)
                ncGust1Qc.flag_values = 0,1,2,3
                ncGust1Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

            ncGust1Qc[timeIndx] = qualityFlag(gust1[1])
                    
            # NDBC QC flags
            ncGust1QcDet = getVariable(ncWind1, shortName + '_detail_qc')
            if ncGust1QcDet == None:
                # Add it
                ncGust1QcDet = ncWind1.createVariable(shortName + '_detail_qc', 'c',
                    ('time',), zlib=True, fill_value= '_')
                ncGust1QcDet.flag_values = 'see NDBC QC Manual'

            ncGust1QcDet[timeIndx] = gust1[1][0]

            # Release flag
            ncGust1Rel = getVariable(ncWind1, shortName + '_release') 
                    
            if ncGust1Rel == None:
                # Add it
                ncGust1Rel = ncWind1.createVariable(shortName + '_release', 
                    'b', ('time',), zlib=True, fill_value= -9)
                ncGust1Rel.flag_values = 0,1
                ncGust1Rel.flag_meanings = 'not_released released'
                ncGust1Rel.comment = 'indicates datum was publicly released in realtime'

            if 'GUST1' in released:
                ncGust1Rel[timeIndx] = 1
            else:
                ncGust1Rel[timeIndx] = 0

        # Continuous Wind Speed
        try:
            contWinds = False
            
            for i in range(6):
                try:
                    cspd = myMeas['CWS%d' % (i + 1)]
                    if cspd[1][0] != 'M':
                        contWinds = True    
                except:
                    None
                
            if contWinds:
                name= 'wind_speed'
                shortName = 'continuous_wind_speed'

                ncCwspd1 = getVariable(ncWind1, shortName)
                if ncCwspd1 == None:
                    # Add it
                    ncCwspd1 = ncWind1.createVariable(shortName, 'f4',
                        ('time10',), zlib=True)
                    ncCwspd1.long_name = name
                    ncCwspd1.standard_name = name
                    ncCwspd1.units = 'm/s'
                    ncCwspd1.comment = 'Ten-minute average wind speed values'
                            
                # QC flags                    
                ncCwspd1Qc = getVariable(ncWind1, shortName + '_qc') 
                  
                if ncCwspd1Qc == None:
                    # Add it
                    ncCwspd1Qc = ncWind1.createVariable(shortName + '_qc', 'b',
                        ('time10',), zlib=True, fill_value= -9)
                    ncCwspd1Qc.flag_values = 0,1,2,3
                    ncCwspd1Qc.flag_meanings = \
                        'quality_good out_of_range sensor_nonfunctional questionable'
                    
                # NDBC QC flags
                ncCwspd1QcDet = getVariable(ncWind1, shortName + '_detail_qc')
                if ncCwspd1QcDet == None:
                    # Add it
                    ncCwspd1QcDet = ncWind1.createVariable(shortName + '_detail_qc', 'c',
                        ('time10',), zlib=True, fill_value= '_')
                    ncCwspd1QcDet.flag_values = 'see NDBC QC Manual'

                # Release flag
                ncCwspd1Rel = getVariable(ncWind1, shortName + '_release') 
                    
                if ncCwspd1Rel == None:
                    # Add it
                    ncCwspd1Rel = ncWind1.createVariable(shortName + '_release', 
                        'b', ('time',), zlib=True, fill_value= -9)
                    ncCwspd1Rel.flag_values = 0,1
                    ncCwspd1Rel.flag_meanings = 'not_released released'
                    ncCwspd1Rel.comment = 'indicates datum was publicly released in realtime'

                # Continuous wind direction                        
                name= 'wind_from_direction'
                shortName = 'continuous_wind_direction'

                ncCwdir1 = getVariable(ncWind1, shortName)
                if ncCwdir1 == None:
                    # Add it
                    ncCwdir1 = ncWind1.createVariable(shortName, 'f4',
                        ('time10',), zlib=True)
                    ncCwdir1.long_name = name
                    ncCwdir1.standard_name = name
                    ncCwdir1.units = 'degrees clockwise from North'
                    ncCwdir1.comment = 'Ten-minute average wind direction'

                # QC flags                    
                ncCwdir1Qc = getVariable(ncWind1, shortName + '_qc') 
                    
                if ncCwdir1Qc == None:
                    # Add it
                    ncCwdir1Qc = ncWind1.createVariable(shortName + '_qc', 'b',
                        ('time10',), zlib=True, fill_value= -9)
                    ncCwdir1Qc.flag_values = 0,1,2,3
                    ncCwdir1Qc.flag_meanings = \
                        'quality_good out_of_range sensor_nonfunctional questionable'
                    
                # NDBC QC flags
                ncCwdir1QcDet = getVariable(ncWind1, shortName + '_detail_qc')
                if ncCwdir1QcDet == None:
                    # Add it
                    ncCwdir1QcDet = ncWind1.createVariable(shortName + '_detail_qc', 'c',
                        ('time10',), zlib=True, fill_value= '_')
                    ncCwdir1QcDet.flag_values = 'see NDBC QC Manual'

                # Release flag
                ncCwdir1Rel = getVariable(ncWind1, shortName + '_release') 
                    
                if ncCwdir1Rel == None:
                    # Add it
                    ncCwdir1Rel = ncWind1.createVariable(shortName + '_release', 
                        'b', ('time',), zlib=True, fill_value= -9)
                    ncCwdir1Rel.flag_values = 0,1
                    ncCwdir1Rel.flag_meanings = 'not_released released'
                    ncCwdir1Rel.comment = 'indicates datum was publicly released in realtime'

                cwindTimes = dataObj.get10MinTimes()
                for i in range(6):
                    time10Indx = TIME10KEYS.index(cwindTimes[i])
            
                    cspd = myMeas['CWS%d' % (i + 1)]
                    cdir = myMeas['CWD%d' % (i + 1)]
                    
                    if cspd[1][0] != 'M':    
                        ncCwspd1[time10Indx] = float(cspd[0])
                        ncCwspd1Qc[time10Indx] = qualityFlag(cspd[1])
                        ncCwspd1QcDet[time10Indx] = cspd[1][0]
                     
                        if 'CWS%d' % (i + 1) in released:
                            ncCwspd1Rel[timeIndx] = 1
                        else:
                            ncCwspd1Rel[timeIndx] = 0

                    if cdir[1][0] != 'M':
                        ncCwdir1[time10Indx] = float(cdir[0])
                        ncCwdir1Qc[time10Indx] = qualityFlag(cdir[1])
                        ncCwdir1QcDet[time10Indx] = cdir[1][0]
                   
                        if 'CWD%d' % (i + 1) in released:
                            ncCwdir1Rel[timeIndx] = 1
                        else:
                            ncCwdir1Rel[timeIndx] = 0
            # Hourly max gust
            try:
                mxmin1 = myMeas['MXMIN1']
                mxgt1 = myMeas['MXGT1']
                mxdir1 = myMeas['DIRMXGT1']
            except:
                None

            try:
                # Make sure we have time
                assert 'mxmin1' in vars() and mxmin1[1] != 'M'

                hourlyGustTime = dataObj.getMinuteTime(mxmin1[0])
                hourlyGustTimeIndx = TIMEMKEYS.index(hourlyGustTime)
                
                # Hourly Max Gust
                if 'mxgt1' in vars() and mxgt1[1] != 'M':
                    name = 'wind_speed_of_gust'
                    shortName = 'hourly_max_gust'
                
                    ncMxgt1 = getVariable(ncWind1, shortName)
                    
                    if ncMxgt1 == None:
                        # Add it
                        ncMxgt1 = ncWind1.createVariable(shortName,'f4', ('timem',), zlib=True)
                        ncMxgt1.long_name = name
                        ncMxgt1.standard_name = name
                        ncMxgt1.short_name = shortName
                        ncMxgt1.units = 'm/s'
                   
                    ncMxgt1[hourlyGustTimeIndx] = float(mxgt1[0])

                    # QC flags                    
                    ncMxgt1Qc = getVariable(ncWind1, shortName + '_qc') 
                    
                    if ncMxgt1Qc == None:
                        # Add it
                        ncMxgt1Qc = ncWind1.createVariable(shortName + '_qc', 
                            'b', ('timem',), zlib=True, fill_value= -9)
                        ncMxgt1Qc.long_name = name + '_qc'
                        ncMxgt1Qc.short_name = shortName + '_qc'
                        ncMxgt1Qc.flag_values = 0,1,2,3
                        ncMxgt1Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncMxgt1Qc[hourlyGustTimeIndx] = qualityFlag(mxgt1[1])
                    
                    # NDBC QC flags
                    ncMxgt1QcDet = getVariable(ncWind1, shortName + '_detail_qc')
                    if ncMxgt1QcDet == None:
                        # Add it
                        ncMxgt1QcDet = ncWind1.createVariable(shortName + '_detail_qc',
                            'c', ('timem',), zlib=True, fill_value= '_')
                        ncMxgt1QcDet.long_name = name + '_detail_qc'
                        ncMxgt1QcDet.short_name = shortName + '_detail_qc'
                        ncMxgt1QcDet.flag_values = 'see NDBC QC Manual'

                    ncMxgt1QcDet[hourlyGustTimeIndx] = mxgt1[1][0]

                    # Release flag
                    ncMxgt1Rel = getVariable(ncWind1, shortName + '_release') 
                    
                    if ncMxgt1Rel == None:
                        # Add it
                        ncMxgt1Rel = ncWind1.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncMxgt1Rel.flag_values = 0,1
                        ncMxgt1Rel.flag_meanings = 'not_released released'
                        ncMxgt1Rel.comment = 'indicates datum was publicly released in realtime'

                        if 'MXGT1' in released:
                            ncMxgt1Rel[timeIndx] = 1
                        else:
                            ncMxgt1Rel[timeIndx] = 0
                            
                # Direction of hourly max gust
                if 'mxdir1' in vars() and mxdir1[1] != 'M':
                    name = 'wind_from_direction'
                    shortName = 'direction_of_hourly_max_gust'
                
                    ncMxdir1 = getVariable(ncWind1, shortName)
                    
                    if ncMxdir1 == None:
                        # Add it
                        ncMxdir1 = ncWind1.createVariable(shortName,'f4', ('timem',), zlib=True)
                        ncMxdir1.long_name = name
                        ncMxdir1.standard_name = name
                        ncMxdir1.short_name = shortName
                        ncMxdir1.units = 'm/s'
                   
                    ncMxdir1[hourlyGustTimeIndx] = float(mxdir1[0])

                    # QC flags                    
                    ncMxdir1Qc = getVariable(ncWind1, shortName + '_qc') 
                    
                    if ncMxdir1Qc == None:
                        # Add it
                        ncMxdir1Qc = ncWind1.createVariable(shortName + '_qc', 
                            'b', ('timem',), zlib=True, fill_value= -9)
                        ncMxdir1Qc.long_name = name + '_qc'
                        ncMxdir1Qc.short_name = shortName + '_qc'
                        ncMxdir1Qc.flag_values = 0,1,2,3
                        ncMxdir1Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncMxdir1Qc[hourlyGustTimeIndx] = qualityFlag(mxdir1[1])
                    
                    # NDBC QC flags
                    ncMxdir1QcDet = getVariable(ncWind1, shortName + '_detail_qc')
                    if ncMxdir1QcDet == None:
                        # Add it
                        ncMxdir1QcDet = ncWind1.createVariable(shortName + '_detail_qc',
                            'c', ('timem',), zlib=True, fill_value= '_')
                        ncMxdir1QcDet.long_name = name + '_detail_qc'
                        ncMxdir1QcDet.short_name = shortName + '_detail_qc'
                        ncMxdir1QcDet.flag_values = 'see NDBC QC Manual'

                    ncMxdir1QcDet[hourlyGustTimeIndx] = mxdir1[1][0]
                    
                    # Release flag
                    ncMxdir1Rel = getVariable(ncWind1, shortName + '_release') 
                    
                    if ncMxdir1Rel == None:
                        # Add it
                        ncMxdir1Rel = ncWind1.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncMxdir1Rel.flag_values = 0,1
                        ncMxdir1Rel.flag_meanings = 'not_released released'
                        ncMxdir1Rel.comment = 'indicates datum was publicly released in realtime'

                        if 'DIRMXGT1' in released:
                            ncMxdir1Rel[timeIndx] = 1
                        else:
                            ncMxdir1Rel[timeIndx] = 0

            except:
                None
            
            # Hourly maximum 1-minute wind speed
            try:
                mx1min1 = myMeas['MX1MMIN1']
                mx1wspd1 = myMeas['MX1MGT1']
                mx1wdir1 = myMeas['DMX1MGT1']
            except:
                None

            try:
                # Make sure we have time
                assert 'mx1min1' in vars() and mxmin1[1] != 'M'

                hourlyGustTime = dataObj.getMinuteTime(mx1min1[0])
                hourlyGustTimeIndx = TIMEMKEYS.index(hourlyGustTime)
                
                # Hourly maximum 1-minute wind speed 
                if 'mx1wspd1' in vars() and mx1wspd1[1] != 'M':
                    name = 'wind_speed'
                    shortName = 'max_1_minute_wind_speed'
                
                    ncMx1wspd1 = getVariable(ncWind1, shortName)
                    
                    if ncMx1wspd1 == None:
                        # Add it
                        ncMx1wspd1 = ncWind1.createVariable(shortName,'f4', ('timem',), zlib=True)
                        ncMx1wspd1.long_name = name
                        ncMx1wspd1.standard_name = name
                        ncMx1wspd1.short_name = shortName
                        ncMx1wspd1.units = 'm/s'
                   
                    ncMx1wspd1[hourlyGustTimeIndx] = float(mx1wspd1[0])

                    # QC flags                    
                    ncMx1wspd1Qc = getVariable(ncWind1, shortName + '_qc') 
                    
                    if ncMx1wspd1Qc == None:
                        # Add it
                        ncMx1wspd1Qc = ncWind1.createVariable(shortName + '_qc', 
                            'b', ('timem',), zlib=True, fill_value= -9)
                        ncMx1wspd1Qc.long_name = name + '_qc'
                        ncMx1wspd1Qc.short_name = shortName + '_qc'
                        ncMx1wspd1Qc.flag_values = 0,1,2,3
                        ncMx1wspd1Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncMx1wspd1Qc[hourlyGustTimeIndx] = qualityFlag(mx1wspd1[1])
                    
                    # NDBC QC flags
                    ncMx1wspd1QcDet = getVariable(ncWind1, shortName + '_detail_qc')
                    if ncMx1wspd1QcDet == None:
                        # Add it
                        ncMx1wspd1QcDet = ncWind1.createVariable(shortName + '_detail_qc',
                            'c', ('timem',), zlib=True, fill_value= '_')
                        ncMx1wspd1QcDet.long_name = name + '_detail_qc'
                        ncMx1wspd1QcDet.short_name = shortName + '_detail_qc'
                        ncMx1wspd1QcDet.flag_values = 'see NDBC QC Manual'

                    ncMx1wspd1QcDet[hourlyGustTimeIndx] = mx1wspd1[1][0]

                    # Release flag
                    ncMx1wspd1Rel = getVariable(ncWind1, shortName + '_release') 
                    
                    if ncMx1wspd1Rel == None:
                        # Add it
                        ncMx1wspd1Rel = ncWind1.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncMx1wspd1Rel.flag_values = 0,1
                        ncMx1wspd1Rel.flag_meanings = 'not_released released'
                        ncMx1wspd1Rel.comment = 'indicates datum was publicly released in realtime'

                        if 'MX1MGT1' in released:
                            ncMx1wspd1Rel[timeIndx] = 1
                        else:
                            ncMx1wspd1Rel[timeIndx] = 0

                # Direction of hourly maximum 1-minute windspeed
                if 'mx1wdir1' in vars() and mx1wdir1[1] != 'M':
                    name = 'wind_from_direction'
                    shortName = 'direction_of_max_1_minute_wind_speed'
                
                    ncMx1wdir1 = getVariable(ncWind1, shortName)
                    
                    if ncMx1wdir1 == None:
                        # Add it
                        ncMx1wdir1 = ncWind1.createVariable(shortName,'f4', ('timem',), zlib=True)
                        ncMx1wdir1.long_name = name
                        ncMx1wdir1.standard_name = name
                        ncMx1wdir1.short_name = shortName
                        ncMx1wdir1.units = 'm/s'
                   
                    ncMx1wdir1[hourlyGustTimeIndx] = float(mx1wdir1[0])

                    # QC flags                    
                    ncMx1wdir1Qc = getVariable(ncWind1, shortName + '_qc') 
                    
                    if ncMx1wdir1Qc == None:
                        # Add it
                        ncMx1wdir1Qc = ncWind1.createVariable(shortName + '_qc', 
                            'b', ('timem',), zlib=True, fill_value= -9)
                        ncMx1wdir1Qc.long_name = name + '_qc'
                        ncMx1wdir1Qc.short_name = shortName + '_qc'
                        ncMx1wdir1Qc.flag_values = 0,1,2,3
                        ncMx1wdir1Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncMx1wdir1Qc[hourlyGustTimeIndx] = qualityFlag(mx1wdir1[1])
                    
                    # NDBC QC flags
                    ncMx1wdir1QcDet = getVariable(ncWind1, shortName + '_detail_qc')
                    if ncMx1wdir1QcDet == None:
                        # Add it
                        ncMx1wdir1QcDet = ncWind1.createVariable(shortName + '_detail_qc',
                            'c', ('timem',), zlib=True, fill_value= '_')
                        ncMx1wdir1QcDet.long_name = name + '_detail_qc'
                        ncMx1wdir1QcDet.short_name = shortName + '_detail_qc'
                        ncMx1wdir1QcDet.flag_values = 'see NDBC QC Manual'

                    ncMx1wdir1QcDet[hourlyGustTimeIndx] = mx1wdir1[1][0]

                    # Release flag
                    ncMx1wdir1Rel = getVariable(ncWind1, shortName + '_release') 
                    
                    if ncMx1wdir1Rel == None:
                        # Add it
                        ncMx1wdir1Rel = ncWind1.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncMx1wdir1Rel.flag_values = 0,1
                        ncMx1wdir1Rel.flag_meanings = 'not_released released'
                        ncMx1wdir1Rel.comment = 'indicates datum was publicly released in realtime'

                        if 'DMX1MGT1' in released:
                            ncMx1wdir1Rel[timeIndx] = 1
                        else:
                            ncMx1wdir1Rel[timeIndx] = 0
          
            except:
                None
                    
        except:
            msgTuple = formatExceptionInfo()
            msgString = str(msgTuple)
            writeLog(msgString)
            
    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        writeLog(msgString)

    #            
    # Anemometer 2
    #
    try:
        wspd2 = myMeas['WSPD2']
        wdir2 = myMeas['WDIR2']
        gust2 = myMeas['GUST2']
                
        ncWind2 = getWind(NC, pyldId, dn, 2)
        assert ncWind2 != None

        # Wind speed
        if wspd2[1] != 'M':      
            name= 'wind_speed'
            shortName = 'wind_speed'
   
            ncWspd2 = getVariable(ncWind2, shortName)
                   
            if ncWspd2 == None:
                # Add it
                ncWspd2 = ncWind2.createVariable(shortName, 'f4', ('time',), zlib=True)
                ncWspd2.long_name = name
                ncWspd2.standard_name = name
                ncWspd2.units = 'm/s'
                    
            ncWspd2[timeIndx] = float(wspd2[0])

            # QC flags                    
            ncWspd2Qc = getVariable(ncWind2, shortName + '_qc') 
                  
            if ncWspd2Qc == None:
                # Add it
                ncWspd2Qc = ncWind2.createVariable(shortName + '_qc', 'b',
                    ('time',), zlib=True, fill_value= -9)
                ncWspd2Qc.flag_values = 0,1,2,3
                ncWspd2Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

            ncWspd2Qc[timeIndx] = qualityFlag(wspd2[1])
                    
            # NDBC QC flags
            ncWspd2QcDet = getVariable(ncWind2, shortName + '_detail_qc')
            if ncWspd2QcDet == None:
                # Add it
                ncWspd2QcDet = ncWind2.createVariable(shortName + '_detail_qc', 'c',
                    ('time',), zlib=True, fill_value= '_')
                ncWspd2QcDet.flag_values = 'see NDBC QC Manual'

            ncWspd2QcDet[timeIndx] = wspd2[1][0]

            # Release flag
            ncWspd2Rel = getVariable(ncWind2, shortName + '_release') 
                    
            if ncWspd2Rel == None:
                # Add it
                ncWspd2Rel = ncWind2.createVariable(shortName + '_release', 
                    'b', ('time',), zlib=True, fill_value= -9)
                ncWspd2Rel.flag_values = 0,1
                ncWspd2Rel.flag_meanings = 'not_released released'
                ncWspd2Rel.comment = 'indicates datum was publicly released in realtime'

            if 'WSPD2' in released:
                ncWspd2Rel[timeIndx] = 1
            else:
                ncWspd2Rel[timeIndx] = 0

        # wind direction
        if wdir2[1] != 'M':
            name= 'wind_from_direction'
            shortName = 'wind_direction'

            ncWdir2 = getVariable(ncWind2, shortName)
                    
            if ncWdir2 == None:
                # Add it
                ncWdir2 = ncWind2.createVariable(shortName, 'i2', ('time',), zlib=True)
                ncWdir2.long_name = name
                ncWdir2.standard_name = name
                ncWdir2.units = 'degrees clockwise from North'
                        
            ncWdir2[timeIndx] = int(float(wdir2[0]))

            # QC flags                    
            ncWdir2Qc = getVariable(ncWind2, shortName + '_qc') 
                    
            if ncWdir2Qc == None:
                # Add it
                ncWdir2Qc = ncWind2.createVariable(shortName + '_qc', 'b',
                    ('time',), zlib=True, fill_value= -9)
                ncWdir2Qc.flag_values = 0,1,2,3
                ncWdir2Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

            ncWdir2Qc[timeIndx] = qualityFlag(wdir2[1])
                    
            # NDBC QC flags
            ncWdir2QcDet = getVariable(ncWind2, shortName + '_detail_qc')
            if ncWdir2QcDet == None:
                # Add it
                ncWdir2QcDet = ncWind2.createVariable(shortName + '_detail_qc', 'c',
                    ('time',), zlib=True, fill_value= '_')
                ncWdir2QcDet.flag_values = 'see NDBC QC Manual'

            ncWdir2QcDet[timeIndx] = wdir2[1][0]
                
            # Release flag
            ncWdir2Rel = getVariable(ncWind2, shortName + '_release') 
                    
            if ncWdir2Rel == None:
                # Add it
                ncWdir2Rel = ncWind2.createVariable(shortName + '_release', 
                    'b', ('time',), zlib=True, fill_value= -9)
                ncWdir2Rel.flag_values = 0,1
                ncWdir2Rel.flag_meanings = 'not_released released'
                ncWdir2Rel.comment = 'indicates datum was publicly released in realtime'

            if 'WDIR2' in released:
                ncWdir2Rel[timeIndx] = 1
            else:
                ncWdir2Rel[timeIndx] = 0

        # Gust
        if gust2[1] != 'M':         
            name= 'wind_speed_of_gust'
            shortName = 'wind_gust'

            ncGust2 = getVariable(ncWind2, shortName)
                    
            if ncGust2 == None:
                # Add it
                ncGust2 = ncWind2.createVariable(shortName, 'f4', ('time',), zlib=True)
                ncGust2.long_name = name
                ncGust2.standard_name = name
                ncGust2.units = 'm/s'
                    
            ncGust2[timeIndx] = float(gust1[0])
                    
            # QC flags                    
            ncGust2Qc = getVariable(ncWind2, shortName + '_qc') 
                    
            if ncGust2Qc == None:
                # Add it
                ncGust2Qc = ncWind2.createVariable(shortName + '_qc', 'b',
                    ('time',), zlib=True, fill_value= -9)
                ncGust2Qc.flag_values = 0,1,2,3
                ncGust2Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

            ncGust2Qc[timeIndx] = qualityFlag(gust2[1])
                    
            # NDBC QC flags
            ncGust2QcDet = getVariable(ncWind2, shortName + '_detail_qc')
            if ncGust2QcDet == None:
                # Add it
                ncGust2QcDet = ncWind2.createVariable(shortName + '_detail_qc', 'c',
                    ('time',), zlib=True, fill_value= '_')
                ncGust2QcDet.flag_values = 'see NDBC QC Manual'

            ncGust2QcDet[timeIndx] = gust2[1][0]
                    
            # Release flag
            ncGust2Rel = getVariable(ncWind2, shortName + '_release') 
                    
            if ncGust2Rel == None:
                # Add it
                ncGust2Rel = ncWind2.createVariable(shortName + '_release', 
                    'b', ('time',), zlib=True, fill_value= -9)
                ncGust2Rel.flag_values = 0,1
                ncGust2Rel.flag_meanings = 'not_released released'
                ncGust2Rel.comment = 'indicates datum was publicly released in realtime'

            if 'GUST2' in released:
                ncGust2Rel[timeIndx] = 1
            else:
                ncGust2Rel[timeIndx] = 0

        # Continuous Wind Speed
        try:
            contWinds = False
            
            for i in range(6):
                try:
                    cspd = myMeas['OWS%d' % (i + 1)]
                    if cspd[1][0] != 'M':
                        contWinds = True    
                except:
                    None

            if contWinds:
                name= 'wind_speed'
                shortName = 'continuous_wind_speed'

                ncCwspd2 = getVariable(ncWind2, shortName)
                if ncCwspd2 == None:
                    # Add it
                    ncCwspd2 = ncWind2.createVariable(shortName, 'f4',
                            ('time10',), zlib=True)
                    ncCwspd2.long_name = name
                    ncCwspd2.standard_name = name
                    ncCwspd2.units = 'm/s'
                    ncCwspd2.comment = 'Ten-minute average wind speed values'
                            
                # QC flags                    
                ncCwspd2Qc = getVariable(ncWind2, shortName + '_qc') 
                  
                if ncCwspd2Qc == None:
                    # Add it
                    ncCwspd2Qc = ncWind2.createVariable(shortName + '_qc', 'b',
                        ('time10',), zlib=True, fill_value= -9)
                    ncCwspd2Qc.flag_values = 0,1,2,3
                    ncCwspd2Qc.flag_meanings = \
                        'quality_good out_of_range sensor_nonfunctional questionable'
                    
                # NDBC QC flags
                ncCwspd2QcDet = getVariable(ncWind2, shortName + '_detail_qc')
                if ncCwspd2QcDet == None:
                    # Add it
                    ncCwspd2QcDet = ncWind2.createVariable(shortName + '_detail_qc', 'c',
                        ('time10',), zlib=True, fill_value= '_')
                    ncCwspd2QcDet.flag_values = 'see NDBC QC Manual'

                # Release flag
                ncCwspd2Rel = getVariable(ncWind2, shortName + '_release') 
                    
                if ncCwspd2Rel == None:
                    # Add it
                    ncCwspd2Rel = ncWind2.createVariable(shortName + '_release', 
                        'b', ('time',), zlib=True, fill_value= -9)
                    ncCwspd2Rel.flag_values = 0,1
                    ncCwspd2Rel.flag_meanings = 'not_released released'
                    ncCwspd2Rel.comment = 'indicates datum was publicly released in realtime'

                # Continuous wind direction                        
                name= 'wind_from_direction'
                shortName = 'continuous_wind_direction'

                ncCwdir2 = getVariable(ncWind2, shortName)
                if ncCwdir2 == None:
                    # Add it
                    ncCwdir2 = ncWind2.createVariable(shortName, 'f4',
                            ('time10',), zlib=True)
                    ncCwdir2.long_name = name
                    ncCwdir2.standard_name = name
                    ncCwdir2.units = 'degrees clockwise from North'
                    ncCwdir2.comment = 'Ten-minute average wind direction'

                # QC flags                    
                ncCwdir2Qc = getVariable(ncWind2, shortName + '_qc') 
                    
                if ncCwdir2Qc == None:
                    # Add it
                    ncCwdir2Qc = ncWind2.createVariable(shortName + '_qc', 'b',
                        ('time10',), zlib=True, fill_value= -9)
                    ncCwdir2Qc.flag_values = 0,1,2,3
                    ncCwdir2Qc.flag_meanings = \
                        'quality_good out_of_range sensor_nonfunctional questionable'
                    
                # NDBC QC flags
                ncCwdir2QcDet = getVariable(ncWind2, shortName + '_detail_qc')
                if ncCwdir2QcDet == None:
                    # Add it
                    ncCwdir2QcDet = ncWind2.createVariable(shortName + '_detail_qc', 'c',
                        ('time10',), zlib=True, fill_value= '_')
                    ncCwdir2QcDet.flag_values = 'see NDBC QC Manual'

                # Release flag
                ncCwdir2Rel = getVariable(ncWind2, shortName + '_release') 
                    
                if ncCwdir2Rel == None:
                    # Add it
                    ncCwdir2Rel = ncWind2.createVariable(shortName + '_release', 
                        'b', ('time',), zlib=True, fill_value= -9)
                    ncCwdir2Rel.flag_values = 0,1
                    ncCwdir2Rel.flag_meanings = 'not_released released'
                    ncCwdir2Rel.comment = 'indicates datum was publicly released in realtime'

                cwindTimes = dataObj.get10MinTimes()
                for i in range(6):
                    time10Indx = TIME10KEYS.index(cwindTimes[i])
            
                    cspd = myMeas['OWS%d' % (i + 1)]
                    cdir = myMeas['OWD%d' % (i + 1)]
                    
                    if cspd[1][0] != 'M':    
                        ncCwspd2[time10Indx] = float(cspd[0])
                        ncCwspd2Qc[time10Indx] = qualityFlag(cspd[1])
                        ncCwspd2QcDet[time10Indx] = cspd[1][0]
                     
                        if 'OWS%d' % (i + 1) in released:
                            ncCwspd2Rel[timeIndx] = 1
                        else:
                            ncCwspd2Rel[timeIndx] = 0

                    if cdir[1][0] != 'M':
                        ncCwdir2[time10Indx] = float(cdir[0])
                        ncCwdir2Qc[time10Indx] = qualityFlag(cdir[1])
                        ncCwdir2QcDet[time10Indx] = cdir[1][0]
                   
                        if 'OWD%d' % (i + 1) in released:
                            ncCwdir2Rel[timeIndx] = 1
                        else:
                            ncCwdir2Rel[timeIndx] = 0
                    
            # Hourly max gust
            try:
                mxmin2 = myMeas['MXMIN2']
                mxgt2 = myMeas['MXGT2']
                mxdir2 = myMeas['DIRMXGT2']
            except:
                None

            try:
                # Make sure we have time
                assert 'mxmin2' in vars() and mxmin2[1] != 'M'

                hourlyGustTime = dataObj.getMinuteTime(mxmin2[0])
                hourlyGustTimeIndx = TIMEMKEYS.index(hourlyGustTime)
                
                # Hourly Max Gust
                if 'mxgt2' in vars() and mxgt2[1] != 'M':
                    name = 'wind_speed_of_gust'
                    shortName = 'hourly_max_gust'
                
                    ncMxgt2 = getVariable(ncWind2, shortName)
                    
                    if ncMxgt2 == None:
                        # Add it
                        ncMxgt2 = ncWind2.createVariable(shortName,'f4', ('timem',), zlib=True)
                        ncMxgt2.long_name = name
                        ncMxgt2.standard_name = name
                        ncMxgt2.short_name = shortName
                        ncMxgt2.units = 'm/s'
                   
                    ncMxgt2[hourlyGustTimeIndx] = float(mxgt2[0])

                    # QC flags                    
                    ncMxgt2Qc = getVariable(ncWind2, shortName + '_qc') 
                    
                    if ncMxgt2Qc == None:
                        # Add it
                        ncMxgt2Qc = ncWind2.createVariable(shortName + '_qc', 
                            'b', ('timem',), zlib=True, fill_value= -9)
                        ncMxgt2Qc.long_name = name + '_qc'
                        ncMxgt2Qc.short_name = shortName + '_qc'
                        ncMxgt2Qc.flag_values = 0,1,2,3
                        ncMxgt2Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncMxgt2Qc[hourlyGustTimeIndx] = qualityFlag(mxgt2[1])
                    
                    # NDBC QC flags
                    ncMxgt2QcDet = getVariable(ncWind2, shortName + '_detail_qc')
                    if ncMxgt2QcDet == None:
                        # Add it
                        ncMxgt2QcDet = ncWind2.createVariable(shortName + '_detail_qc',
                            'c', ('timem',), zlib=True, fill_value= '_')
                        ncMxgt2QcDet.long_name = name + '_detail_qc'
                        ncMxgt2QcDet.short_name = shortName + '_detail_qc'
                        ncMxgt2QcDet.flag_values = 'see NDBC QC Manual'

                    ncMxgt2QcDet[hourlyGustTimeIndx] = mxgt2[1][0]

                    # Release flag
                    ncMxgt2Rel = getVariable(ncWind2, shortName + '_release') 
                    
                    if ncMxgt2Rel == None:
                        # Add it
                        ncMxgt2Rel = ncWind2.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncMxgt2Rel.flag_values = 0,1
                        ncMxgt2Rel.flag_meanings = 'not_released released'
                        ncMxgt2Rel.comment = 'indicates datum was publicly released in realtime'

                    if 'MXGT2' in released:
                        ncMxgt2Rel[timeIndx] = 1
                    else:
                        ncMxgt2Rel[timeIndx] = 0

                # Direction of hourly max gust
                if 'mxdir2' in vars() and mxdir2[1] != 'M':
                    name = 'wind_from_direction'
                    shortName = 'direction_of_hourly_max_gust'
                
                    ncMxdir2 = getVariable(ncWind2, shortName)
                    
                    if ncMxdir2 == None:
                        # Add it
                        ncMxdir2 = ncWind2.createVariable(shortName,'f4', ('timem',), zlib=True)
                        ncMxdir2.long_name = name
                        ncMxdir2.standard_name = name
                        ncMxdir2.short_name = shortName
                        ncMxdir2.units = 'm/s'
                   
                    ncMxdir2[hourlyGustTimeIndx] = float(mxdir2[0])

                    # QC flags                    
                    ncMxdir2Qc = getVariable(ncWind2, shortName + '_qc') 
                    
                    if ncMxdir2Qc == None:
                        # Add it
                        ncMxdir2Qc = ncWind2.createVariable(shortName + '_qc', 
                            'b', ('timem',), zlib=True, fill_value= -9)
                        ncMxdir2Qc.long_name = name + '_qc'
                        ncMxdir2Qc.short_name = shortName + '_qc'
                        ncMxdir2Qc.flag_values = 0,1,2,3
                        ncMxdir2Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncMxdir2Qc[hourlyGustTimeIndx] = qualityFlag(mxdir2[1])
                    
                    # NDBC QC flags
                    ncMxdir2QcDet = getVariable(ncWind2, shortName + '_detail_qc')
                    if ncMxdir2QcDet == None:
                        # Add it
                        ncMxdir2QcDet = ncWind2.createVariable(shortName + '_detail_qc',
                            'c', ('timem',), zlib=True, fill_value= '_')
                        ncMxdir2QcDet.long_name = name + '_detail_qc'
                        ncMxdir2QcDet.short_name = shortName + '_detail_qc'
                        ncMxdir2QcDet.flag_values = 'see NDBC QC Manual'

                    ncMxdir2QcDet[hourlyGustTimeIndx] = mxdir2[1][0]

                    # Release flag
                    ncMxdir2Rel = getVariable(ncWind2, shortName + '_release') 
                    
                    if ncMxdir2Rel == None:
                        # Add it
                        ncMxdir2Rel = ncWind2.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncMxdir2Rel.flag_values = 0,1
                        ncMxdir2Rel.flag_meanings = 'not_released released'
                        ncMxdir2Rel.comment = 'indicates datum was publicly released in realtime'

                    if 'DIRMXGT2' in released:
                        ncMxdir2Rel[timeIndx] = 1
                    else:
                        ncMxdir2Rel[timeIndx] = 0

            except:
                None
            
            # Hourly maximum 1-minute wind speed
            try:
                mx1min2 = myMeas['MX1MMIN2']
                mx1wspd2 = myMeas['MX1MGT2']
                mx1wdir2 = myMeas['DMX1MGT2']
            except:
                None

            try:
                # Make sure we have time
                assert 'mx1min2' in vars() and mxmin2[1] != 'M'

                hourlyGustTime = dataObj.getMinuteTime(mx1min2[0])
                hourlyGustTimeIndx = TIMEMKEYS.index(hourlyGustTime)
                
                # Hourly maximum 1-minute wind speed 
                if 'mx1wspd2' in vars() and mx1wspd2[1] != 'M':
                    name = 'wind_speed'
                    shortName = 'max_1_minute_wind_speed'
                
                    ncMx1wspd2 = getVariable(ncWind2, shortName)
                    
                    if ncMx1wspd2 == None:
                        # Add it
                        ncMx1wspd2 = ncWind2.createVariable(shortName,'f4', ('timem',), zlib=True)
                        ncMx1wspd2.long_name = name
                        ncMx1wspd2.standard_name = name
                        ncMx1wspd2.short_name = shortName
                        ncMx1wspd2.units = 'm/s'
                   
                    ncMx1wspd2[hourlyGustTimeIndx] = float(mx1wspd2[0])

                    # QC flags                    
                    ncMx1wspd2Qc = getVariable(ncWind2, shortName + '_qc') 
                    
                    if ncMx1wspd2Qc == None:
                        # Add it
                        ncMx1wspd2Qc = ncWind2.createVariable(shortName + '_qc', 
                            'b', ('timem',), zlib=True, fill_value= -9)
                        ncMx1wspd2Qc.long_name = name + '_qc'
                        ncMx1wspd2Qc.short_name = shortName + '_qc'
                        ncMx1wspd2Qc.flag_values = 0,1,2,3
                        ncMx1wspd2Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncMx1wspd2Qc[hourlyGustTimeIndx] = qualityFlag(mx1wspd2[1])
                    
                    # NDBC QC flags
                    ncMx1wspd2QcDet = getVariable(ncWind2, shortName + '_detail_qc')
                    if ncMx1wspd2QcDet == None:
                        # Add it
                        ncMx1wspd2QcDet = ncWind2.createVariable(shortName + '_detail_qc',
                            'c', ('timem',), zlib=True, fill_value= '_')
                        ncMx1wspd2QcDet.long_name = name + '_detail_qc'
                        ncMx1wspd2QcDet.short_name = shortName + '_detail_qc'
                        ncMx1wspd2QcDet.flag_values = 'see NDBC QC Manual'

                    ncMx1wspd2QcDet[hourlyGustTimeIndx] = mx1wspd2[1][0]

                    # Release flag
                    ncMx1wspd2Rel = getVariable(ncWind2, shortName + '_release') 
                    
                    if ncMx1wspd2Rel == None:
                        # Add it
                        ncMx1wspd2Rel = ncWind2.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncMx1wspd2Rel.flag_values = 0,1
                        ncMx1wspd2Rel.flag_meanings = 'not_released released'
                        ncMx1wspd2Rel.comment = 'indicates datum was publicly released in realtime'

                    if 'MX1MGT2' in released:
                        ncMx1wspd2Rel[timeIndx] = 1
                    else:
                        ncMx1wspd2Rel[timeIndx] = 0

                # Direction of hourly maximum 1-minute windspeed
                if 'mx1wdir2' in vars() and mx1wdir2[1] != 'M':
                    name = 'wind_from_direction'
                    shortName = 'direction_of_max_1_minute_wind_speed'
                
                    ncMx1wdir2 = getVariable(ncWind2, shortName)
                    
                    if ncMx1wdir2 == None:
                        # Add it
                        ncMx1wdir2 = ncWind2.createVariable(shortName,'f4', ('timem',), zlib=True)
                        ncMx1wdir2.long_name = name
                        ncMx1wdir2.standard_name = name
                        ncMx1wdir2.short_name = shortName
                        ncMx1wdir2.units = 'm/s'
                   
                    ncMx1wdir2[hourlyGustTimeIndx] = float(mx1wdir2[0])

                    # QC flags                    
                    ncMx1wdir2Qc = getVariable(ncWind2, shortName + '_qc') 
                    
                    if ncMx1wdir2Qc == None:
                        # Add it
                        ncMx1wdir2Qc = ncWind2.createVariable(shortName + '_qc', 
                            'b', ('timem',), zlib=True, fill_value= -9)
                        ncMx1wdir2Qc.long_name = name + '_qc'
                        ncMx1wdir2Qc.short_name = shortName + '_qc'
                        ncMx1wdir2Qc.flag_values = 0,1,2,3
                        ncMx1wdir2Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncMx1wdir2Qc[hourlyGustTimeIndx] = qualityFlag(mx1wdir2[1])
                    
                    # NDBC QC flags
                    ncMx1wdir2QcDet = getVariable(ncWind2, shortName + '_detail_qc')
                    if ncMx1wdir2QcDet == None:
                        # Add it
                        ncMx1wdir2QcDet = ncWind2.createVariable(shortName + '_detail_qc',
                            'c', ('timem',), zlib=True, fill_value= '_')
                        ncMx1wdir2QcDet.long_name = name + '_detail_qc'
                        ncMx1wdir2QcDet.short_name = shortName + '_detail_qc'
                        ncMx1wdir2QcDet.flag_values = 'see NDBC QC Manual'

                    ncMx1wdir2QcDet[hourlyGustTimeIndx] = mx1wdir2[1][0]

                    # Release flag
                    ncMx1wdir2Rel = getVariable(ncWind2, shortName + '_release') 
                    
                    if ncMx1wdir2Rel == None:
                        # Add it
                        ncMx1wdir2Rel = ncWind2.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncMx1wdir2Rel.flag_values = 0,1
                        ncMx1wdir2Rel.flag_meanings = 'not_released released'
                        ncMx1wdir2Rel.comment = 'indicates datum was publicly released in realtime'

                    if 'DMX1MGT2' in released:
                        ncMx1wdir2Rel[timeIndx] = 1
                    else:
                        ncMx1wdir2Rel[timeIndx] = 0

            
            except:
                None

        except:
            msgTuple = formatExceptionInfo()
            msgString = str(msgTuple)
            writeLog(msgString)

    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        writeLog(msgString)

#
# put wind data in NetCDF document
#
def putBaro(NC, SD, stationNode, dataObj):
    global TIMEKEYS
    global TIME10KEYS
    global TIMEMKEYS

    eodTime = dataObj.getEndDataTime()
    timeIndx = TIMEKEYS.index(eodTime)
    pyldId = dataObj.getPayloadId()
    dn = getDeploymentNode(stationNode, pyldId)
    myMeas = dataObj.getMeasurements()
    released =  dataObj.getGtsReleasedMeasurements()
    
    #
    # Barometer 1
    #
    try:
        
        if 'BARO1' in myMeas:
            baro1 = myMeas['BARO1']
            
        if 'SBAR1' in myMeas:
            sbar1 = myMeas['SBAR1']
            
        if 'baro1' in vars() or 'sbar1' in vars():
            ncSensor1 = getSensor(NC, pyldId, dn, 'barometer', 1)
            
            # make sure we have a sensor defined
            assert ncSensor1 != None
                
            # sea-level pressure
            if 'baro1' in vars():
                if baro1[1] != 'M':  
                    name= 'air_pressure_at_sea_level'
                    shortName = 'air_pressure_at_sea_level'
       
                    ncBaro1 = getVariable(ncSensor1, name)
                    
                    if ncBaro1 == None:
                        # Add it
                        ncBaro1 = ncSensor1.createVariable(shortName,
                            'i4', ('time',), zlib=True)
                        ncBaro1.long_name = name
                        ncBaro1.standard_name = name
                        ncBaro1.units = 'Pa'
                    
                    ncBaro1[timeIndx] = float(baro1[0]) * 100

                    # QC flags                    
                    ncBaro1Qc = getVariable(ncSensor1, shortName + '_qc') 
                    
                    if ncBaro1Qc == None:
                        # Add it
                        ncBaro1Qc = ncSensor1.createVariable(shortName + '_qc', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncBaro1Qc.flag_values = 0,1,2,3
                        ncBaro1Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncBaro1Qc[timeIndx] = qualityFlag(baro1[1])
                    
                    # NDBC QC flags
                    ncBaro1QcDet = getVariable(ncSensor1, shortName + '_detail_qc')
                    if ncBaro1QcDet == None:
                        # Add it
                        ncBaro1QcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                            'c', ('time',), zlib=True, fill_value= '_')
                        ncBaro1QcDet.flag_values = 'see NDBC QC Manual'

                    ncBaro1QcDet[timeIndx] = baro1[1][0]
                    
                    # Release flag
                    ncBaro1Rel = getVariable(ncSensor1, shortName + '_release') 
                    
                    if ncBaro1Rel == None:
                        # Add it
                        ncBaro1Rel = ncSensor1.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncBaro1Rel.flag_values = 0,1
                        ncBaro1Rel.flag_meanings = 'not_released released'
                        ncBaro1Rel.comment = 'indicates datum was publicly released in realtime'

                    if 'BARO1' in released:
                        ncBaro1Rel[timeIndx] = 1
                    else:
                        ncBaro1Rel[timeIndx] = 0
                        
            # station pressure
            if 'sbar1' in vars():
                if sbar1[1] != 'M':         
                    name= 'air_pressure'
                    shortName = 'air_pressure'

                    ncSbar1 = getVariable(ncSensor1, shortName)
                    
                    if ncSbar1 == None:
                        # Add it
                        ncSbar1 = ncSensor1.createVariable(shortName,
                            'i4', ('time',), zlib=True)
                        ncSbar1.long_name = name
                        ncSbar1.standard_name = name
                        ncSbar1.units = 'Pa'
                    
                    ncSbar1[timeIndx] = float(sbar1[0]) * 100

                    # QC flags                    
                    ncSbar1Qc = getVariable(ncSensor1, shortName + '_qc') 
                    
                    if ncSbar1Qc == None:
                        # Add it
                        ncSbar1Qc = ncSensor1.createVariable(shortName + '_qc', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncSbar1Qc.flag_values = 0,1,2,3
                        ncSbar1Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    '''
                    For buoys, SBAR1 is automatically 'D' flagged to prevent it from being released.
                    To provide valid flags, BARO1 flags are used, if BARO1 exists.
                    '''
                    if 'baro1' in vars():
                        ncSbar1Qc[timeIndx] = qualityFlag(baro1[1])
                    else:
                        ncSbar1Qc[timeIndx] = qualityFlag(sbar1[1])
                    
                    # NDBC QC flags
                    ncSbar1QcDet = getVariable(ncSensor1, shortName + '_detail_qc')
                    if ncSbar1QcDet == None:
                        # Add it
                        ncSbar1QcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                            'c', ('time',), zlib=True, fill_value= '_')
                        ncSbar1QcDet.flag_values = 'see NDBC QC Manual'

                    if 'baro1' in vars():
                        ncSbar1QcDet[timeIndx] = baro1[1][0]
                    else:
                        ncSbar1QcDet[timeIndx] = sbar1[1][0]

                    # Release flag
                    ncSbar1Rel = getVariable(ncSensor1, shortName + '_release') 
                    
                    if ncSbar1Rel == None:
                        # Add it
                        ncSbar1Rel = ncSensor1.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncSbar1Rel.flag_values = 0,1
                        ncSbar1Rel.flag_meanings = 'not_released released'
                        ncSbar1Rel.comment = 'indicates datum was publicly released in realtime'

                    if 'SBAR1' in released:
                        ncSbar1Rel[timeIndx] = 1
                    else:
                        ncSbar1Rel[timeIndx] = 0
                        
            # Minimum 1-minute pressure
            try:
                if myMeas['MN1MSLP1'] and myMeas['MSLPMIN1']:
                    mn1mslp1 = myMeas['MN1MSLP1']
                    mslpmin1 = myMeas['MSLPMIN1']
                    
                    assert mn1mslp1[1] != 'M'
                    assert mslpmin1[1] != 'M'
                    
                    mn1mslp1Time = dataObj.getMinuteTime(mslpmin1[0])
                    mn1mslp1TimeIndx = TIMEMKEYS.index(mn1mslp1Time)
                    
                    name = 'air_pressure'
                    shortName = 'minimum_1_minute_air_pressure'
                
                    ncMn1mslp1 = getVariable(ncSensor1, shortName)

                    if ncMn1mslp1 == None:
                        # Add it
                        ncMn1mslp1 = ncSensor1.createVariable(shortName,'f4', ('timem',), zlib=True)
                        ncMn1mslp1.long_name = name
                        ncMn1mslp1.standard_name = name
                        ncMn1mslp1.short_name = shortName
                        ncMn1mslp1.units = 'Pa'
                   
                    ncMn1mslp1[mn1mslp1TimeIndx] = float(mn1mslp1[0]) * 100

                    # QC flags                    
                    ncMn1mslp1Qc = getVariable(ncSensor1, shortName + '_qc') 
                    
                    if ncMn1mslp1Qc == None:
                        # Add it
                        ncMn1mslp1Qc = ncSensor1.createVariable(shortName + '_qc', 
                            'b', ('timem',), zlib=True, fill_value= -9)
                        ncMn1mslp1Qc.long_name = name + '_qc'
                        ncMn1mslp1Qc.short_name = shortName + '_qc'
                        ncMn1mslp1Qc.flag_values = 0,1,2,3
                        ncMn1mslp1Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncMn1mslp1Qc[mn1mslp1TimeIndx] = qualityFlag(mn1mslp1[1])
                    
                    # NDBC QC flags
                    ncMn1mslp1QcDet = getVariable(ncSensor1, shortName + '_detail_qc')
                    if ncMn1mslp1QcDet == None:
                        # Add it
                        ncMn1mslp1QcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                            'c', ('timem',), zlib=True, fill_value= '_')
                        ncMn1mslp1QcDet.long_name = name + '_detail_qc'
                        ncMn1mslp1QcDet.short_name = shortName + '_detail_qc'
                        ncMn1mslp1QcDet.flag_values = 'see NDBC QC Manual'

                    ncMn1mslp1QcDet[mn1mslp1TimeIndx] = mn1mslp1[1][0]

                    # Release flag
                    ncMn1mslp1Rel = getVariable(ncSensor1, shortName + '_release') 
                    
                    if ncMn1mslp1Rel == None:
                        # Add it
                        ncMn1mslp1Rel = ncSensor1.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncMn1mslp1Rel.flag_values = 0,1
                        ncMn1mslp1Rel.flag_meanings = 'not_released released'
                        ncMn1mslp1Rel.comment = 'indicates datum was publicly released in realtime'

                    if 'MN1MSLP1' in released:
                        ncMn1mslp1Rel[timeIndx] = 1
                    else:
                        ncMn1mslp1Rel[timeIndx] = 0
                        
            except:
                None    

    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        writeLog(msgString)

    #
    # Barometer 2
    #
    try:
        
        if 'BARO2' in myMeas:
            baro2 = myMeas['BARO2']
            
        if 'SBAR2' in myMeas:
            sbar2 = myMeas['SBAR2']
            
        if 'baro2' in vars() or 'sbar2' in vars():
            ncSensor2 = getSensor(NC, pyldId, dn, 'barometer', 2)
            
            # make sure we have a sensor defined
            assert ncSensor2 != None
                
            # sea-level pressure
            if 'baro2' in vars():
                if baro2[1] != 'M':   
                    name= 'air_pressure_at_sea_level'
                    shortName = 'air_pressure_at_sea_level'
      
                    ncBaro2 = getVariable(ncSensor2, shortName)
                    
                    if ncBaro2 == None:
                        # Add it
                        ncBaro2 = ncSensor2.createVariable(shortName,
                            'i4', ('time',), zlib=True)
                        ncBaro2.long_name = name
                        ncBaro2.standard_name = name
                        ncBaro2.units = 'Pa'
                    
                    ncBaro2[timeIndx] = float(baro2[0]) * 100

                    # QC flags                    
                    ncBaro2Qc = getVariable(ncSensor2, shortName + '_qc') 
                    
                    if ncBaro2Qc == None:
                        # Add it
                        ncBaro2Qc = ncSensor2.createVariable(shortName + '_qc', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncBaro2Qc.flag_values = 0,1,2,3
                        ncBaro2Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncBaro2Qc[timeIndx] = qualityFlag(baro2[1])
                    
                    # NDBC QC flags
                    ncBaro2QcDet = getVariable(ncSensor2, shortName + '_detail_qc')
                    if ncBaro2QcDet == None:
                        # Add it
                        ncBaro2QcDet = ncSensor2.createVariable(shortName + '_detail_qc',
                            'c', ('time',), zlib=True, fill_value= '_')
                        ncBaro2QcDet.flag_values = 'see NDBC QC Manual'

                    ncBaro2QcDet[timeIndx] = baro2[1][0]
                    
                    # Release flag
                    ncBaro2Rel = getVariable(ncSensor2, shortName + '_release') 
                    
                    if ncBaro2Rel == None:
                        # Add it
                        ncBaro2Rel = ncSensor2.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncBaro2Rel.flag_values = 0,1
                        ncBaro2Rel.flag_meanings = 'not_released released'
                        ncBaro2Rel.comment = 'indicates datum was publicly released in realtime'

                    if 'BARO2' in released:
                        ncBaro2Rel[timeIndx] = 1
                    else:
                        ncBaro2Rel[timeIndx] = 0
                        
            # station pressure
            if 'sbar2' in vars():
                if sbar2[1] != 'M':         
                    name= 'air_pressure'
                    shortName = 'air_pressure'

                    ncSbar2 = getVariable(ncSensor2, shortName)
                    
                    if ncSbar2 == None:
                        # Add it
                        ncSbar2 = ncSensor2.createVariable(shortName,
                            'i4', ('time',), zlib=True)
                        ncSbar2.long_name = name
                        ncSbar2.standard_name = name
                        ncSbar2.units = 'Pa'
                    
                    ncSbar2[timeIndx] = float(sbar2[0]) * 100

                    # QC flags                    
                    ncSbar2Qc = getVariable(ncSensor2, shortName + '_qc') 
                    
                    if ncSbar2Qc == None:
                        # Add it
                        ncSbar2Qc = ncSensor2.createVariable(shortName + '_qc', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncSbar2Qc.flag_values = 0,1,2,3
                        ncSbar2Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    '''
                    For buoys, SBAR1 is automatically 'D' flagged to prevent it from being released.
                    To provide valid flags, BARO1 flags are used, if BARO1 exists.
                    '''
                    if 'baro2' in vars():
                        ncSbar2Qc[timeIndx] = qualityFlag(baro2[1])
                    else:
                        ncSbar2Qc[timeIndx] = qualityFlag(sbar2[1])
                    
                    # NDBC QC flags
                    ncSbar2QcDet = getVariable(ncSensor2, shortName + '_detail_qc')
                    if ncSbar2QcDet == None:
                        # Add it
                        ncSbar2QcDet = ncSensor2.createVariable(shortName + '_detail_qc',
                            'c', ('time',), zlib=True, fill_value= '_')
                        ncSbar2QcDet.flag_values = 'see NDBC QC Manual'

                    if 'baro2' in vars():
                        ncSbar2QcDet[timeIndx] = baro2[1][0]
                    else:
                        ncSbar2QcDet[timeIndx] = sbar2[1][0]

                    # Release flag
                    ncSbar2Rel = getVariable(ncSensor2, shortName + '_release') 
                    
                    if ncSbar2Rel == None:
                        # Add it
                        ncSbar2Rel = ncSensor2.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncSbar2Rel.flag_values = 0,1
                        ncSbar2Rel.flag_meanings = 'not_released released'
                        ncSbar2Rel.comment = 'indicates datum was publicly released in realtime'

                    if 'SBAR2' in released:
                        ncSbar2Rel[timeIndx] = 1
                    else:
                        ncSbar2Rel[timeIndx] = 0

            # Minimum 1-minute pressure
            try:
                if myMeas['MN1MSLP2'] and myMeas['MSLPMIN2']:
                    mn1mslp2 = myMeas['MN1MSLP2']
                    mslpmin2 = myMeas['MSLPMIN2']
                    
                    assert mn1mslp2[1] != 'M'
                    assert mslpmin2[1] != 'M'
                    
                    mn1mslp2Time = dataObj.getMinuteTime(mslpmin2[0])
                    mn1mslp2TimeIndx = TIMEMKEYS.index(mn1mslp2Time)
                    
                    name = 'air_pressure'
                    shortName = 'minimum_1_minute_air_pressure'
                
                    ncMn1mslp2 = getVariable(ncSensor2, shortName)

                    if ncMn1mslp2 == None:
                        # Add it
                        ncMn1mslp2 = ncSensor2.createVariable(shortName,'f4', ('timem',), zlib=True)
                        ncMn1mslp2.long_name = name
                        ncMn1mslp2.standard_name = name
                        ncMn1mslp2.short_name = shortName
                        ncMn1mslp2.units = 'Pa'
                   
                    ncMn1mslp2[mn1mslp2TimeIndx] = float(mn1mslp2[0]) * 100

                    # QC flags                    
                    ncMn1mslp2Qc = getVariable(ncSensor2, shortName + '_qc') 
                    
                    if ncMn1mslp2Qc == None:
                        # Add it
                        ncMn1mslp2Qc = ncSensor2.createVariable(shortName + '_qc', 
                            'b', ('timem',), zlib=True, fill_value= -9)
                        ncMn1mslp2Qc.long_name = name + '_qc'
                        ncMn1mslp2Qc.short_name = shortName + '_qc'
                        ncMn1mslp2Qc.flag_values = 0,1,2,3
                        ncMn1mslp2Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncMn1mslp2Qc[mn1mslp2TimeIndx] = qualityFlag(mn1mslp2[1])
                    
                    # NDBC QC flags
                    ncMn1mslp2QcDet = getVariable(ncSensor2, shortName + '_detail_qc')
                    if ncMn1mslp2QcDet == None:
                        # Add it
                        ncMn1mslp2QcDet = ncSensor2.createVariable(shortName + '_detail_qc',
                            'c', ('timem',), zlib=True, fill_value= '_')
                        ncMn1mslp2QcDet.long_name = name + '_detail_qc'
                        ncMn1mslp2QcDet.short_name = shortName + '_detail_qc'
                        ncMn1mslp2QcDet.flag_values = 'see NDBC QC Manual'

                    ncMn1mslp2QcDet[mn1mslp2TimeIndx] = mn1mslp2[1][0]

                    # Release flag
                    ncMn1mslp2Rel = getVariable(ncSensor2, shortName + '_release') 
                    
                    if ncMn1mslp2Rel == None:
                        # Add it
                        ncMn1mslp2Rel = ncSensor2.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncMn1mslp2Rel.flag_values = 0,1
                        ncMn1mslp2Rel.flag_meanings = 'not_released released'
                        ncMn1mslp2Rel.comment = 'indicates datum was publicly released in realtime'

                    if 'MN1MSLP2' in released:
                        ncMn1mslp2Rel[timeIndx] = 1
                    else:
                        ncMn1mslp2Rel[timeIndx] = 0
            except:
                None    
                        
    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        writeLog(msgString)

#
# put air_temperature data in NetCDF document
#
def putAirTemperature(NC, SD, stationNode, dataObj):
    global TIMEKEYS
    global TIME10KEYS
    global TIMEMKEYS

    eodTime = dataObj.getEndDataTime()
    timeIndx = TIMEKEYS.index(eodTime)
    pyldId = dataObj.getPayloadId()
    dn = getDeploymentNode(stationNode, pyldId)
    myMeas = dataObj.getMeasurements()
    released =  dataObj.getGtsReleasedMeasurements()

    #
    # Air Temperature 1
    #
    try:
        
        if 'ATMP1' in myMeas:
            atmp1 = myMeas['ATMP1']
            
        if 'DEWPT1' in myMeas:
            dewpt1 = myMeas['DEWPT1']
            
        if 'atmp1' in vars() or 'dewpt1' in vars():
            ncSensor1 = getSensor(NC, pyldId, dn, 'air_temperature_sensor', 1)
            
            # make sure we have a sensor defined
            assert ncSensor1 != None
                
            # air temperature
            if 'atmp1' in vars():
                if atmp1[1] != 'M':         
                    name= 'air_temperature'
                    shortName = 'air_temperature'

                    ncAtmp1 = getVariable(ncSensor1, shortName)
                    
                    if ncAtmp1 == None:
                        # Add it
                        ncAtmp1 = ncSensor1.createVariable(shortName,
                            'f4', ('time',), zlib=True)
                        ncAtmp1.long_name = name
                        ncAtmp1.standard_name = name
                        ncAtmp1.units = 'K'
                    
                    ncAtmp1[timeIndx] = float(atmp1[0]) + 273.15   # In Kelvins

                    # QC flags                    
                    ncAtmp1Qc = getVariable(ncSensor1, shortName + '_qc') 
                    
                    if ncAtmp1Qc == None:
                        # Add it
                        ncAtmp1Qc = ncSensor1.createVariable(shortName + '_qc', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncAtmp1Qc.flag_values = 0,1,2,3
                        ncAtmp1Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncAtmp1Qc[timeIndx] = qualityFlag(atmp1[1])
                    
                    # NDBC QC flags
                    ncAtmp1QcDet = getVariable(ncSensor1, shortName + '_detail_qc')
                    if ncAtmp1QcDet == None:
                        # Add it
                        ncAtmp1QcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                            'c', ('time',), zlib=True, fill_value= '_')
                        ncAtmp1QcDet.flag_values = 'see NDBC QC Manual'

                    ncAtmp1QcDet[timeIndx] = atmp1[1][0]
                    
                    # Release flag
                    ncAtmp1Rel = getVariable(ncSensor1, shortName + '_release') 
                    
                    if ncAtmp1Rel == None:
                        # Add it
                        ncAtmp1Rel = ncSensor1.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncAtmp1Rel.flag_values = 0,1
                        ncAtmp1Rel.flag_meanings = 'not_released released'
                        ncAtmp1Rel.comment = 'indicates datum was publicly released in realtime'

                    if 'ATMP1' in released:
                        ncAtmp1Rel[timeIndx] = 1
                    else:
                        ncAtmp1Rel[timeIndx] = 0
                    
            # dew point
            if 'dewpt1' in vars():
                if dewpt1[1] != 'M':
                    name= 'dew_point_temperature'
                    shortName = 'dew_point_temperature'
         
                    ncDewpt1 = getVariable(ncSensor1, shortName)
                    
                    if ncDewpt1 == None:
                        # Add it
                        ncDewpt1 = ncSensor1.createVariable(shortName,
                            'f4', ('time',), zlib=True)
                        ncDewpt1.long_name = name
                        ncDewpt1.standard_name = name
                        ncDewpt1.units = 'K'
                    
                    ncDewpt1[timeIndx] = float(dewpt1[0]) + 273.15   # In Kelvins

                    # QC flags                    
                    ncDewpt1Qc = getVariable(ncSensor1, shortName + '_qc') 
                    
                    if ncDewpt1Qc == None:
                        # Add it
                        ncDewpt1Qc = ncSensor1.createVariable(shortName + '_qc', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncDewpt1Qc.flag_values = 0,1,2,3
                        ncDewpt1Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncDewpt1Qc[timeIndx] = qualityFlag(dewpt1[1])
                    
                    # NDBC QC flags
                    ncDewpt1QcDet = getVariable(ncSensor1, shortName + '_detail_qc')
                    if ncDewpt1QcDet == None:
                        # Add it
                        ncDewpt1QcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                            'c', ('time',), zlib=True, fill_value= '_')
                        ncDewpt1QcDet.flag_values = 'see NDBC QC Manual'

                    ncDewpt1QcDet[timeIndx] = dewpt1[1][0]

                    # Release flag
                    ncDewpt1Rel = getVariable(ncSensor1, shortName + '_release') 
                    
                    if ncDewpt1Rel == None:
                        # Add it
                        ncDewpt1Rel = ncSensor1.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncDewpt1Rel.flag_values = 0,1
                        ncDewpt1Rel.flag_meanings = 'not_released released'
                        ncDewpt1Rel.comment = 'indicates datum was publicly released in realtime'

                    if 'DEWPT1' in released:
                        ncDewpt1Rel[timeIndx] = 1
                    else:
                        ncDewpt1Rel[timeIndx] = 0
    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        writeLog(msgString)

    #
    # Air Temperature 2
    #
    try:
        if 'ATMP2' in myMeas:
            atmp2 = myMeas['ATMP2']
            
        if 'DEWPT2' in myMeas:
            dewpt2 = myMeas['DEWPT2']
            
        if 'atmp2' in vars() or 'dewpt2' in vars():
            ncSensor2 = getSensor(NC, pyldId, dn, 'air_temperature_sensor', 2)
            
            # make sure we have a sensor defined
            assert ncSensor2 != None
                
            # air temperature
            if 'atmp2' in vars():
                if atmp2[1] != 'M':         
                    name= 'air_temperature'
                    shortName = 'air_temperature'

                    ncAtmp2 = getVariable(ncSensor2, shortName)
                    
                    if ncAtmp2 == None:
                        # Add it
                        ncAtmp2 = ncSensor2.createVariable(shortName,
                            'f4', ('time',), zlib=True)
                        ncAtmp2.long_name = name
                        ncAtmp2.standard_name = name
                        ncAtmp2.units = 'K'
                    
                    ncAtmp2[timeIndx] = float(atmp2[0]) + 273.15   # In Kelvins

                    # QC flags                    
                    ncAtmp2Qc = getVariable(ncSensor2, shortName + '_qc') 
                    
                    if ncAtmp2Qc == None:
                        # Add it
                        ncAtmp2Qc = ncSensor2.createVariable(shortName + '_qc', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncAtmp2Qc.flag_values = 0,1,2,3
                        ncAtmp2Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncAtmp2Qc[timeIndx] = qualityFlag(atmp2[1])
                    
                    # NDBC QC flags
                    ncAtmp2QcDet = getVariable(ncSensor2, shortName + '_detail_qc')
                    if ncAtmp2QcDet == None:
                        # Add it
                        ncAtmp2QcDet = ncSensor2.createVariable(shortName + '_detail_qc',
                            'c', ('time',), zlib=True, fill_value= '_')
                        ncAtmp2QcDet.flag_values = 'see NDBC QC Manual'

                    ncAtmp2QcDet[timeIndx] = atmp2[1][0]
                    
                    # Release flag
                    ncAtmp2Rel = getVariable(ncSensor2, shortName + '_release') 
                    
                    if ncAtmp2Rel == None:
                        # Add it
                        ncAtmp2Rel = ncSensor2.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncAtmp2Rel.flag_values = 0,1
                        ncAtmp2Rel.flag_meanings = 'not_released released'
                        ncAtmp2Rel.comment = 'indicates datum was publicly released in realtime'

                    if 'ATMP2' in released:
                        ncAtmp2Rel[timeIndx] = 1
                    else:
                        ncAtmp2Rel[timeIndx] = 0
                    
            # dew point
            if 'dewpt2' in vars():
                if dewpt2[1] != 'M':
                    name= 'dew_point_temperature'
                    shortName = 'dew_point_temperature'
         
                    ncDewpt2 = getVariable(ncSensor2, shortName)
                    
                    if ncDewpt2 == None:
                        # Add it
                        ncDewpt2 = ncSensor2.createVariable(shortName,
                            'f4', ('time',), zlib=True)
                        ncDewpt2.long_name = name
                        ncDewpt2.standard_name = name
                        ncDewpt2.units = 'K'
                    
                    ncDewpt2[timeIndx] = float(dewpt2[0]) + 273.15   # In Kelvins

                    # QC flags                    
                    ncDewpt2Qc = getVariable(ncSensor2, shortName + '_qc') 
                    
                    if ncDewpt2Qc == None:
                        # Add it
                        ncDewpt2Qc = ncSensor2.createVariable(shortName + '_qc', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncDewpt2Qc.flag_values = 0,1,2,3
                        ncDewpt2Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncDewpt2Qc[timeIndx] = qualityFlag(dewpt2[1])
                    
                    # NDBC QC flags
                    ncDewpt2QcDet = getVariable(ncSensor2, shortName + '_detail_qc')
                    if ncDewpt2QcDet == None:
                        # Add it
                        ncDewpt2QcDet = ncSensor2.createVariable(shortName + '_detail_qc',
                            'c', ('time',), zlib=True, fill_value= '_')
                        ncDewpt2QcDet.flag_values = 'see NDBC QC Manual'

                    ncDewpt2QcDet[timeIndx] = dewpt1[1][0]

                    # Release flag
                    ncDewpt2Rel = getVariable(ncSensor2, shortName + '_release') 
                    
                    if ncDewpt2Rel == None:
                        # Add it
                        ncDewpt2Rel = ncSensor2.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncDewpt2Rel.flag_values = 0,1
                        ncDewpt2Rel.flag_meanings = 'not_released released'
                        ncDewpt2Rel.comment = 'indicates datum was publicly released in realtime'

                    if 'DEWPT2' in released:
                        ncDewpt2Rel[timeIndx] = 1
                    else:
                        ncDewpt2Rel[timeIndx] = 0

    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        writeLog(msgString)

#
# put humidity sensor data in NetCDF document
#
def putHumidity(NC, SD, stationNode, dataObj):
    global TIMEKEYS
    global TIME10KEYS
    global TIMEMKEYS

    eodTime = dataObj.getEndDataTime()
    timeIndx = TIMEKEYS.index(eodTime)
    pyldId = dataObj.getPayloadId()
    dn = getDeploymentNode(stationNode, pyldId)
    myMeas = dataObj.getMeasurements()
    released =  dataObj.getGtsReleasedMeasurements()
    
    #
    # humidity sensor 1
    #
    try:
        
        if 'RRH' in myMeas:
            rrh = myMeas['RRH']
            
#        if 'ATMP2' in myMeas:
#            atmp2 = myMeas['ATMP2']
            
#        if 'DEWPT2' in myMeas:
#            dewpt2 = myMeas['DEWPT2']

#        if 'rrh' in vars() or 'atmp2' in vars() or 'dewpt2' in vars():
        if 'rrh' in vars():
            ncSensor1 = getSensor(NC, pyldId, dn, 'humidity_sensor', 1)
            
            # make sure we have a sensor defined
            assert ncSensor1 != None
                
            # relative humidity
            if 'rrh' in vars():
                if rrh[1] != 'M':
                    name= 'relative_humidity'
                    shortName = 'relative_humidity'
         
                    ncRrh = getVariable(ncSensor1, shortName)
                    
                    if ncRrh == None:
                        # Add it
                        ncRrh = ncSensor1.createVariable(shortName,
                            'f4', ('time',), zlib=True)
                        ncRrh.long_name = name
                        ncRrh.standard_name = name
                        ncRrh.units = 'percent'
                    
                    ncRrh[timeIndx] = float(rrh[0])

                    # QC flags                    
                    ncRrhQc = getVariable(ncSensor1, shortName + '_qc') 
                    
                    if ncRrhQc == None:
                        # Add it
                        ncRrhQc = ncSensor1.createVariable(shortName + '_qc', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncRrhQc.flag_values = 0,1,2,3
                        ncRrhQc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncRrhQc[timeIndx] = qualityFlag(rrh[1])
                    
                    # NDBC QC flags
                    ncRrhQcDet = getVariable(ncSensor1, shortName + '_detail_qc')
                    if ncRrhQcDet == None:
                        # Add it
                        ncRrhQcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                            'c', ('time',), zlib=True, fill_value= '_')
                        ncRrhQcDet.flag_values = 'see NDBC QC Manual'

                    ncRrhQcDet[timeIndx] = rrh[1][0]

                    # Release flag
                    ncRrhRel = getVariable(ncSensor1, shortName + '_release') 
                    
                    if ncRrhRel == None:
                        # Add it
                        ncRrhRel = ncSensor1.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncRrhRel.flag_values = 0,1
                        ncRrhRel.flag_meanings = 'not_released released'
                        ncRrhRel.comment = 'indicates datum was publicly released in realtime'

                    if 'RRH' in released:
                        ncRrhRel[timeIndx] = 1
                    else:
                        ncRrhRel[timeIndx] = 0
                    
            # air temperature
#            if 'atmp2' in vars():
#                if atmp2[1] != 'M':         
#                    name= 'air_temperature'
#                    shortName = 'air_temperature'
#
#                    ncAtmp2 = getVariable(ncSensor1, shortName)
#                    
#                    if ncAtmp2 == None:
#                        # Add it
#                        ncAtmp2 = ncSensor1.createVariable(shortName,
#                            'f4', ('time',), zlib=True)
#                        ncAtmp2.long_name = name
#                        ncAtmp2.standard_name = name
#                        ncAtmp2.units = 'K'
#                    
#                    ncAtmp2[timeIndx] = float(atmp2[0]) + 273.15   # In Kelvins
#
                    # QC flags                    
#                    ncAtmp2Qc = getVariable(ncSensor1, shortName + '_qc') 
                    
#                    if ncAtmp2Qc == None:
                        # Add it
#                        ncAtmp2Qc = ncSensor1.createVariable(shortName + '_qc', 
#                            'b', ('time',), zlib=True, fill_value= -9)
#                        ncAtmp2Qc.flag_values = 0,1,2,3
#                        ncAtmp2Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'
#
#                    ncAtmp2Qc[timeIndx] = qualityFlag(atmp2[1])
                    
                    # NDBC QC flags
#                    ncAtmp2QcDet = getVariable(ncSensor1, shortName + '_detail_qc')
#                    if ncAtmp2QcDet == None:
                        # Add it
#                        ncAtmp2QcDet = ncSensor1.createVariable(shortName + '_detail_qc',
#                            'c', ('time',), zlib=True, fill_value= '_')
#                        ncAtmp2QcDet.flag_values = 'see NDBC QC Manual'

#                    ncAtmp2QcDet[timeIndx] = atmp2[1][0]

                    # Release flag
#                    ncAtmp2Rel = getVariable(ncSensor1, shortName + '_release') 
                    
#                    if ncAtmp2Rel == None:
#                        # Add it
#                        ncAtmp2Rel = ncSensor1.createVariable(shortName + '_release', 
#                            'b', ('time',), zlib=True, fill_value= -9)
#                        ncAtmp2Rel.flag_values = 0,1
#                        ncAtmp2Rel.flag_meanings = 'not_released released'
#                        ncAtmp2Rel.comment = 'indicates datum was publicly released in realtime'

#                    if 'ATMP2' in released:
#                        ncAtmp2Rel[timeIndx] = 1
#                    else:
#                        ncAtmp2Rel[timeIndx] = 0
                    
            # dew point
#            if 'dewpt2' in vars():
#                if dewpt2[1] != 'M':   
#                    name= 'dew_point_temperature'
#                    shortName = 'dew_point_temperature'
      
#                    ncDewpt2 = getVariable(ncSensor1, shortName)
                    
#                    if ncDewpt2 == None:
                        # Add it
#                        ncDewpt2 = ncSensor1.createVariable(shortName,
#                            'f4', ('time',), zlib=True)
#                        ncDewpt2.long_name = name
#                        ncDewpt2.standard_name = name
#                        ncDewpt2.units = 'K'
                    
#                    ncDewpt2[timeIndx] = float(dewpt2[0]) + 273.15   # In Kelvins

                    # QC flags                    
#                    ncDewpt2Qc = getVariable(ncSensor1, shortName + '_qc') 
                    
#                    if ncDewpt2Qc == None:
                        # Add it
#                        ncDewpt2Qc = ncSensor1.createVariable(shortName + '_qc', 
#                            'b', ('time',), zlib=True, fill_value= -9)
#                        ncDewpt2Qc.flag_values = 0,1,2,3
#                        ncDewpt2Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

#                    ncDewpt2Qc[timeIndx] = qualityFlag(dewpt2[1])
                    
                    # NDBC QC flags
#                    ncDewpt2QcDet = getVariable(ncSensor1, shortName + '_qc')
#                    if ncDewpt2QcDet == None:
                        # Add it
#                        ncDewpt2QcDet = ncSensor1.createVariable(shortName + '_qc',
#                            'c', ('time',), zlib=True, fill_value= '_')
#                        ncDewpt2QcDet.flag_values = 'see NDBC QC Manual'

#                    ncDewpt2QcDet[timeIndx] = dewpt2[1][0]
                    
                    # Release flag
#                    ncDewpt2Rel = getVariable(ncSensor1, shortName + '_release') 
                    
#                    if ncDewpt2Rel == None:
                        # Add it
#                        ncDewpt2Rel = ncSensor1.createVariable(shortName + '_release', 
#                            'b', ('time',), zlib=True, fill_value= -9)
#                        ncDewpt2Rel.flag_values = 0,1
#                        ncDewpt2Rel.flag_meanings = 'not_released released'
#                        ncDewpt2Rel.comment = 'indicates datum was publicly released in realtime'

#                    if 'DEWPT2' in released:
#                        ncDewpt2Rel[timeIndx] = 1
#                    else:
#                        ncDewpt2Rel[timeIndx] = 0

    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        writeLog(msgString)

#
# put ocean temperature data in NetCDF document
#
def putOceanTemperature(NC, SD, stationNode, dataObj):
    global TIMEKEYS
    global TIME10KEYS
    global TIMEMKEYS

    eodTime = dataObj.getEndDataTime()
    timeIndx = TIMEKEYS.index(eodTime)
    pyldId = dataObj.getPayloadId()
    dn = getDeploymentNode(stationNode, pyldId)
    myMeas = dataObj.getMeasurements()
    released =  dataObj.getGtsReleasedMeasurements()
    
    #
    # sea surface temperature sensor 1
    #
    try:
        
        if 'WTMP1' in myMeas:
            wtmp1 = myMeas['WTMP1']
            
        if 'wtmp1' in vars():
            ncSensor1 = getSensor(NC, pyldId, dn, 'ocean_temperature_sensor', 1)
            
            # make sure we have a sensor defined
            assert ncSensor1 != None
                
            # sea surface temperature
            if 'wtmp1' in vars():
                if wtmp1[1] != 'M':         
                    name= 'sea_surface_temperature'
                    shortName = 'sea_surface_temperature'
      
                    ncWtmp1 = getVariable(ncSensor1, shortName)
                    
                    if ncWtmp1 == None:
                        # Add it
                        ncWtmp1 = ncSensor1.createVariable(shortName,
                            'f4', ('time',), zlib=True)
                        ncWtmp1.long_name = name
                        ncWtmp1.standard_name = name
                        ncWtmp1.units = 'K'
                   
                    ncWtmp1[timeIndx] = float(wtmp1[0]) + 273.15   # In Kelvins

                    # QC flags                    
                    ncWtmp1Qc = getVariable(ncSensor1, shortName + '_qc') 
                    
                    if ncWtmp1Qc == None:
                        # Add it
                        ncWtmp1Qc = ncSensor1.createVariable(shortName + '_qc', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncWtmp1Qc.flag_values = 0,1,2,3
                        ncWtmp1Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncWtmp1Qc[timeIndx] = qualityFlag(wtmp1[1])
                    
                    # NDBC QC flags
                    ncWtmp1QcDet = getVariable(ncSensor1, shortName + '_detail_qc')
                    if ncWtmp1QcDet == None:
                        # Add it
                        ncWtmp1QcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                            'c', ('time',), zlib=True, fill_value= '_')
                        ncWtmp1QcDet.flag_values = 'see NDBC QC Manual'

                    ncWtmp1QcDet[timeIndx] = wtmp1[1][0]

                    # Release flag
                    ncWtmp1Rel = getVariable(ncSensor1, shortName + '_release') 
                    
                    if ncWtmp1Rel == None:
                        # Add it
                        ncWtmp1Rel = ncSensor1.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncWtmp1Rel.flag_values = 0,1
                        ncWtmp1Rel.flag_meanings = 'not_released released'
                        ncWtmp1Rel.comment = 'indicates datum was publicly released in realtime'

                    if 'WTMP1' in released:
                        ncWtmp1Rel[timeIndx] = 1
                    else:
                        ncWtmp1Rel[timeIndx] = 0

    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        writeLog(msgString)

#
# put ocean current meter data in NetCDF document
#
def putOceanCurrentMeter(NC, SD, stationNode, dataObj):
    global TIMEKEYS
    global TIME10KEYS
    global TIMEMKEYS

    eodTime = dataObj.getEndDataTime()
    timeIndx = TIMEKEYS.index(eodTime)
    pyldId = dataObj.getPayloadId()
    dn = getDeploymentNode(stationNode, pyldId)
    myMeas = dataObj.getMeasurements()
    released =  dataObj.getGtsReleasedMeasurements()
    
    try:
        
        if 'SCMSPD' in myMeas:
            scmspd = myMeas['SCMSPD']
            
        if 'SCMDIR' in myMeas:
            scmdir = myMeas['SCMDIR']
            
        if 'scmspd' in vars() or 'scmdir' in vars():
            ncSensor1 = getSensor(NC, pyldId, dn, 'ocean_current_meter', 1)
            
            # make sure we have a sensor defined
            assert ncSensor1 != None
                
            # current speed
            if 'scmspd' in vars():
                if scmspd[1] != 'M':         
                    name= 'sea_water_speed'
                    shortName = 'sea_water_speed'

                    ncScmspd = getVariable(ncSensor1, shortName)
                    
                    if ncScmspd == None:
                        # Add it
                        ncScmspd = ncSensor1.createVariable(shortName,
                            'f4', ('time',), zlib=True)
                        ncScmspd.long_name = name
                        ncScmspd.standard_name = name
                        ncScmspd.units = 'm/s'
                    
                    ncScmspd[timeIndx] = float(scmspd[0]) * .01

                    # QC flags                    
                    ncScmspdQc = getVariable(ncSensor1, shortName + '_qc') 
                    
                    if ncScmspdQc == None:
                        # Add it
                        ncScmspdQc = ncSensor1.createVariable(shortName + '_qc', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncScmspdQc.flag_values = 0,1,2,3
                        ncScmspdQc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncScmspdQc[timeIndx] = qualityFlag(scmspd[1])
                    
                    # NDBC QC flags
                    ncScmspdQcDet = getVariable(ncSensor1, shortName + '_detail_qc')
                    if ncScmspdQcDet == None:
                        # Add it
                        ncScmspdQcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                            'c', ('time',), zlib=True, fill_value= '_')
                        ncScmspdQcDet.flag_values = 'see NDBC QC Manual'

                    ncScmspdQcDet[timeIndx] = scmspd[1][0]

                    # Release flag
                    ncScmspdRel = getVariable(ncSensor1, shortName + '_release') 
                    
                    if ncScmspdRel == None:
                        # Add it
                        ncScmspdRel = ncSensor1.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncScmspdRel.flag_values = 0,1
                        ncScmspdRel.flag_meanings = 'not_released released'
                        ncScmspdRel.comment = 'indicates datum was publicly released in realtime'

                    if 'SCMSPD' in released:
                        ncScmspdRel[timeIndx] = 1
                    else:
                        ncScmspdRel[timeIndx] = 0
                    
            # current direction
            if 'scmdir' in vars():
                if scmdir[1] != 'M':         
                    name= 'direction_of_sea_water_velocity'
                    shortName = 'direction_of_sea_water_velocity'

                    ncScmdir = getVariable(ncSensor1, shortName)
                    
                    if ncScmdir == None:
                        # Add it
                        ncScmdir = ncSensor1.createVariable(shortName,
                            'i2', ('time',), zlib=True)
                        ncScmdir.long_name = name
                        ncScmdir.standard_name = name
                        ncScmdir.units = 'degrees clockwise from North'
                    
                    ncScmdir[timeIndx] = float(scmdir[0])

                    # QC flags                    
                    ncScmdirQc = getVariable(ncSensor1, shortName + '_qc') 
                    
                    if ncScmdirQc == None:
                        # Add it
                        ncScmdirQc = ncSensor1.createVariable(shortName + '_qc', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncScmdirQc.flag_values = 0,1,2,3
                        ncScmdirQc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncScmdirQc[timeIndx] = qualityFlag(scmdir[1])
                    
                    # NDBC QC flags
                    ncScmdirQcDet = getVariable(ncSensor1, shortName + '_detail_qc')
                    if ncScmdirQcDet == None:
                        # Add it
                        ncScmdirQcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                            'c', ('time',), zlib=True, fill_value= '_')
                        ncScmdirQcDet.flag_values = 'see NDBC QC Manual'

                    ncScmdirQcDet[timeIndx] = scmdir[1][0]

                    # Release flag
                    ncScmdirRel = getVariable(ncSensor1, shortName + '_release') 
                    
                    if ncScmdirRel == None:
                        # Add it
                        ncScmdirRel = ncSensor1.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncScmdirRel.flag_values = 0,1
                        ncScmdirRel.flag_meanings = 'not_released released'
                        ncScmdirRel.comment = 'indicates datum was publicly released in realtime'

                    if 'SCMDIR' in released:
                        ncScmdirRel[timeIndx] = 1
                    else:
                        ncScmdirRel[timeIndx] = 0

    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        writeLog(msgString)

#
# put ocean current meter data in NetCDF document
#
def putOceanCurrentProfile(NC, SD, stationNode, dataObj):
    global TIMEKEYS
    global TIME10KEYS
    global TIMEMKEYS

    eodTime = dataObj.getEndDataTime()
    timeIndx = TIMEKEYS.index(eodTime)
    pyldId = dataObj.getPayloadId()
    dn = getDeploymentNode(stationNode, pyldId)
    myMeas = dataObj.getMeasurements()
    
    try:
        # Need BN01DIST, BNNUMBER and BNLENGTH
        if 'BN01DIST' not in myMeas:
            return
        
        if 'BNNUMBER' not in myMeas:
            return
        
        if 'BNLENGTH' not in myMeas:
            return
        
        bn01Dist = myMeas['BN01DIST']
        bnNumber = myMeas['BNNUMBER']
        bnLength = myMeas['BNLENGTH']
        
        ncSensor1 = getSensor(NC, pyldId, dn, 'ocean_current_profiler', 1)

        # make sure we have a sensor defined
        assert ncSensor1 != None
        
        
    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        writeLog(msgString)

#
# put salinity data in NetCDF document
#
def putSalinity(NC, SD, stationNode, dataObj):
    global TIMEKEYS

    eodTime = dataObj.getEndDataTime()
    timeIndx = TIMEKEYS.index(eodTime)
    pyldId = dataObj.getPayloadId()
    dn = getDeploymentNode(stationNode, pyldId)
    myMeas = dataObj.getMeasurements()
    released =  dataObj.getGtsReleasedMeasurements()
    
    try:
        
        if 'ZTMP1' in myMeas:
            ztmp1 = myMeas['ZTMP1']
            
        if 'ZSAL1' in myMeas:
            zsal1 = myMeas['ZSAL1']
            
        if 'ZCOND1' in myMeas:
            zcond1 = myMeas['ZCOND1']
            
        if 'ZDEP1' in myMeas:
            zdep1 = myMeas['ZDEP1']

        # Check if we have data
        if not 'ztmp1' in vars() and not 'zsal1' in vars():
            return 

        # temperature
        if 'ztmp1' in vars():
            ncSensor1 = getSensor(NC, pyldId, dn, 'ct_sensor', 1)
            
            # make sure we have a sensor defined
            assert ncSensor1 != None
                
            if ztmp1[1] != 'M':         
                name= 'sea_water_temperature'
                shortName = 'sea_temperature'

                ncZtmp1 = getVariable(ncSensor1, shortName)
                    
                if ncZtmp1 == None:
                    # Add it
                    ncZtmp1 = ncSensor1.createVariable(shortName,
                        'f4', ('time',), zlib=True)
                    ncZtmp1.long_name = name
                    ncZtmp1.standard_name = name
                    ncZtmp1.units = 'K'
                    
                ncZtmp1[timeIndx] = float(ztmp1[0]) + 273.15   # In Kelvins

                # QC flags                    
                ncZtmp1Qc = getVariable(ncSensor1, shortName + '_qc') 
                    
                if ncZtmp1Qc == None:
                    # Add it
                    ncZtmp1Qc = ncSensor1.createVariable(shortName + '_qc', 
                        'b', ('time',), zlib=True, fill_value= -9)
                    ncZtmp1Qc.flag_values = 0,1,2,3
                    ncZtmp1Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                ncZtmp1Qc[timeIndx] = qualityFlag(ztmp1[1])
                    
                # NDBC QC flags
                ncZtmp1QcDet = getVariable(ncSensor1, shortName + '_detail_qc')
                if ncZtmp1QcDet == None:
                    # Add it
                    ncZtmp1QcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                        'c', ('time',), zlib=True, fill_value= '_')
                    ncZtmp1QcDet.flag_values = 'see NDBC QC Manual'

                ncZtmp1QcDet[timeIndx] = ztmp1[1][0]

                # Release flag
                ncZtmp1Rel = getVariable(ncSensor1, shortName + '_release') 
                    
                if ncZtmp1Rel == None:
                    # Add it
                    ncZtmp1Rel = ncSensor1.createVariable(shortName + '_release', 
                        'b', ('time',), zlib=True, fill_value= -9)
                    ncZtmp1Rel.flag_values = 0,1
                    ncZtmp1Rel.flag_meanings = 'not_released released'
                    ncZtmp1Rel.comment = 'indicates datum was publicly released in realtime'

                if 'ZTMP1' in released:
                    ncZtmp1Rel[timeIndx] = 1
                else:
                    ncZtmp1Rel[timeIndx] = 0
                    
        # salinity
        if 'zsal1' in vars():
            ncSensor1 = getSensor(NC, pyldId, dn, 'ct_sensor', 1)
            
            # make sure we have a sensor defined
            assert ncSensor1 != None
                
            if zsal1[1] != 'M':         
                name= 'sea_water_salinity'
                shortName = 'salinity'

                ncZsal1 = getVariable(ncSensor1, shortName)
                    
                if ncZsal1 == None:
                    # Add it
                    ncZsal1 = ncSensor1.createVariable(shortName,
                        'f4', ('time',), zlib=True)
                    ncZsal1.long_name = name
                    ncZsal1.standard_name = name
                    ncZsal1.units = '1e-3'
                    
                ncZsal1[timeIndx] = float(zsal1[0])

                # QC flags                    
                ncZsal1Qc = getVariable(ncSensor1, shortName + '_qc') 
                    
                if ncZsal1Qc == None:
                    # Add it
                    ncZsal1Qc = ncSensor1.createVariable(shortName + '_qc', 
                        'b', ('time',), zlib=True, fill_value= -9)
                    ncZsal1Qc.flag_values = 0,1,2,3
                    ncZsal1Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                ncZsal1Qc[timeIndx] = qualityFlag(zsal1[1])
                    
                # NDBC QC flags
                ncZsal1QcDet = getVariable(ncSensor1, shortName + '_detail_qc')
                if ncZsal1QcDet == None:
                    # Add it
                    ncZsal1QcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                        'c', ('time',), zlib=True, fill_value= '_')
                    ncZsal1QcDet.flag_values = 'see NDBC QC Manual'

                ncZsal1QcDet[timeIndx] = zsal1[1][0]

                # Release flag
                ncZsal1Rel = getVariable(ncSensor1, shortName + '_release') 
                    
                if ncZsal1Rel == None:
                    # Add it
                    ncZsal1Rel = ncSensor1.createVariable(shortName + '_release', 
                        'b', ('time',), zlib=True, fill_value= -9)
                    ncZsal1Rel.flag_values = 0,1
                    ncZsal1Rel.flag_meanings = 'not_released released'
                    ncZsal1Rel.comment = 'indicates datum was publicly released in realtime'

                if 'ZSAL1' in released:
                    ncZsal1Rel[timeIndx] = 1
                else:
                    ncZsal1Rel[timeIndx] = 0

        # conductivity
        if 'zcond1' in vars():
            ncSensor1 = getSensor(NC, pyldId, dn, 'ct_sensor', 1)
            
            # make sure we have a sensor defined
            assert ncSensor1 != None
                
            if zcond1[1] != 'M':         
                name= 'sea_water_electrical_conductivity'
                shortName = 'conductivity'

                ncZcond1 = getVariable(ncSensor1, shortName)
                    
                if ncZcond1 == None:
                    # Add it
                    ncZcond1 = ncSensor1.createVariable(shortName,
                        'f4', ('time',), zlib=True)
                    ncZcond1.long_name = name
                    ncZcond1.standard_name = name
                    ncZcond1.units = 'S/m'
                    
                ncZcond1[timeIndx] = float(zcond1[0])

                # QC flags                    
                ncZcond1Qc = getVariable(ncSensor1, shortName + '_qc') 
                    
                if ncZcond1Qc == None:
                    # Add it
                    ncZcond1Qc = ncSensor1.createVariable(shortName + '_qc', 
                        'b', ('time',), zlib=True, fill_value= -9)
                    ncZcond1Qc.flag_values = 0,1,2,3
                    ncZcond1Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                ncZcond1Qc[timeIndx] = qualityFlag(zcond1[1])
                    
                # NDBC QC flags
                ncZcond1QcDet = getVariable(ncSensor1, shortName + '_detail_qc')
                if ncZcond1QcDet == None:
                    # Add it
                    ncZcond1QcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                        'c', ('time',), zlib=True, fill_value= '_')
                    ncZcond1QcDet.flag_values = 'see NDBC QC Manual'

                ncZcond1QcDet[timeIndx] = zcond1[1][0]

                # Release flag
                ncZcond1Rel = getVariable(ncSensor1, shortName + '_release') 
                    
                if ncZcond1Rel == None:
                    # Add it
                    ncZcond1Rel = ncSensor1.createVariable(shortName + '_release', 
                        'b', ('time',), zlib=True, fill_value= -9)
                    ncZcond1Rel.flag_values = 0,1
                    ncZcond1Rel.flag_meanings = 'not_released released'
                    ncZcond1Rel.comment = 'indicates datum was publicly released in realtime'

                if 'ZCOND1' in released:
                    ncZcond1Rel[timeIndx] = 1
                else:
                    ncZcond1Rel[timeIndx] = 0

        # depth
        if 'zdep1' in vars():
            ncSensor1 = getSensor(NC, pyldId, dn, 'ct_sensor', 1)
            
            # make sure we have a sensor defined
            assert ncSensor1 != None
                
            if zdep1[1] != 'M':         
                name= 'depth'
                shortName = 'depth'

                ncZdep1 = getVariable(ncSensor1, shortName)
                    
                if ncZdep1 == None:
                    # Add it
                    ncZdep1 = ncSensor1.createVariable(shortName,
                        'f4', ('time',), zlib=True)
                    ncZdep1.long_name = name
                    ncZdep1.standard_name = name
                    ncZdep1.units = 'm'
                    
                ncZdep1[timeIndx] = float(zdep1[0])

                # QC flags                    
                ncZdep1Qc = getVariable(ncSensor1, shortName + '_qc') 
                    
                if ncZdep1Qc == None:
                    # Add it
                    ncZdep1Qc = ncSensor1.createVariable(shortName + '_qc', 
                        'b', ('time',), zlib=True, fill_value= -9)
                    ncZdep1Qc.flag_values = 0,1,2,3
                    ncZdep1Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                ncZdep1Qc[timeIndx] = qualityFlag(zdep1[1])
                    
                # NDBC QC flags
                ncZdep1QcDet = getVariable(ncSensor1, shortName + '_detail_qc')
                if ncZdep1QcDet == None:
                    # Add it
                    ncZdep1QcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                        'c', ('time',), zlib=True, fill_value= '_')
                    ncZdep1QcDet.flag_values = 'see NDBC QC Manual'

                ncZdep1QcDet[timeIndx] = zdep1[1][0]

                # Release flag
                ncZdep1Rel = getVariable(ncSensor1, shortName + '_release') 
                    
                if ncZdep1Rel == None:
                    # Add it
                    ncZdep1Rel = ncSensor1.createVariable(shortName + '_release', 
                        'b', ('time',), zlib=True, fill_value= -9)
                    ncZdep1Rel.flag_values = 0,1
                    ncZdep1Rel.flag_meanings = 'not_released released'
                    ncZdep1Rel.comment = 'indicates datum was publicly released in realtime'

                if 'ZDEP1' in released:
                    ncZdep1Rel[timeIndx] = 1
                else:
                    ncZdep1Rel[timeIndx] = 0

    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        writeLog(msgString)

#
# put solar radiation data in NetCDF document
#
def putSolarRadiation(NC, SD, stationNode, dataObj):
    global TIMEKEYS
    global TIME10KEYS
    global TIMEMKEYS

    eodTime = dataObj.getEndDataTime()
    timeIndx = TIMEKEYS.index(eodTime)
    pyldId = dataObj.getPayloadId()
    dn = getDeploymentNode(stationNode, pyldId)
    myMeas = dataObj.getMeasurements()
    released =  dataObj.getGtsReleasedMeasurements()
    
    # Shortwave Radiation
    try:
        
        asciiId = None
        
        if 'SRAD1' in myMeas:
            srad1 = myMeas['SRAD1']
            asciiId = 'SRAD1'
        elif 'SWRAD' in myMeas:
            srad1 = myMeas['SWRAD']
            asciiId = 'SWRAD'
        elif 'WHSWR' in myMeas:
            srad1 = myMeas['WHSWR']
            asciiId = 'WHSWR'
            
        if 'srad1' in vars():
            ncSensor1 = getSensor(NC, pyldId, dn, 'solar_radiation_sensor', 1)
            
            # make sure we have a sensor defined
            assert ncSensor1 != None
                
            if srad1[1] != 'M':         
                name= 'downwelling_shortwave_flux_in_air'
                shortName = 'shortwave_radiation'
                
                ncSrad1 = getVariable(ncSensor1, shortName)
                    
                if ncSrad1 == None:
                    # Add it
                    ncSrad1 = ncSensor1.createVariable(shortName,
                        'f4', ('time',), zlib=True)
                    ncSrad1.long_name = name
                    ncSrad1.standard_name = name
                    ncSrad1.short_name = shortName
                    ncSrad1.units = 'W/m**2'
                    
                ncSrad1[timeIndx] = float(srad1[0])

                # QC flags                    
                ncSrad1Qc = getVariable(ncSensor1, shortName + '_qc') 
                    
                if ncSrad1Qc == None:
                    # Add it
                    ncSrad1Qc = ncSensor1.createVariable(shortName + '_qc', 
                        'b', ('time',), zlib=True, fill_value= -9)
                    ncSrad1Qc.flag_values = 0,1,2,3
                    ncSrad1Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                ncSrad1Qc[timeIndx] = qualityFlag(srad1[1])
                    
                # NDBC QC flags
                ncSrad1QcDet = getVariable(ncSensor1, shortName + '_detail_qc')
                if ncSrad1QcDet == None:
                    # Add it
                    ncSrad1QcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                        'c', ('time',), zlib=True, fill_value= '_')
                    ncSrad1QcDet.flag_values = 'see NDBC QC Manual'

                ncSrad1QcDet[timeIndx] = srad1[1][0]

                # Release flag
                ncSrad1Rel = getVariable(ncSensor1, shortName + '_release')
                 
                if ncSrad1Rel == None:
                    # Add it
                    ncSrad1Rel = ncSensor1.createVariable(shortName + '_release', 
                        'b', ('time',), zlib=True, fill_value= -9)
                    ncSrad1Rel.flag_values = 0,1
                    ncSrad1Rel.flag_meanings = 'not_released released'
                    ncSrad1Rel.comment = 'indicates datum was publicly released in realtime'

                if asciiId in released:
                    ncSrad1Rel[timeIndx] = 1
                else:
                    ncSrad1Rel[timeIndx] = 0
                    
    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        writeLog(msgString)

    # Longwave Radiation
    try:
        
        asciiId = None
        
        if 'LWRAD' in myMeas:
            lrad1 = myMeas['LWRAD']
            asciiId = 'LWRAD'
        elif 'WHLWR' in myMeas:
            lrad1 = myMeas['WHLWR']
            asciiId = 'WHLWR'
            
        if 'lrad1' in vars():
            ncSensor1 = getSensor(NC, pyldId, dn, 'solar_radiation_sensor', 1)
            
            # make sure we have a sensor defined
            assert ncSensor1 != None
                
            if lrad1[1] != 'M':         
                name= 'downwelling_longwave_flux_in_air'
                shortName = 'longwave_radiation'
                
                ncLrad1 = getVariable(ncSensor1, shortName)
                    
                if ncLrad1 == None:
                    # Add it
                    ncLrad1 = ncSensor1.createVariable(shortName,
                        'f4', ('time',), zlib=True)
                    ncLrad1.long_name = name
                    ncLrad1.standard_name = name
                    ncLrad1.short_name = shortName
                    ncLrad1.units = 'W/m**2'
                    
                ncLrad1[timeIndx] = float(lrad1[0])

                # QC flags                    
                ncLrad1Qc = getVariable(ncSensor1, shortName + '_qc') 
                    
                if ncLrad1Qc == None:
                    # Add it
                    ncLrad1Qc = ncSensor1.createVariable(shortName + '_qc', 
                        'b', ('time',), zlib=True, fill_value= -9)
                    ncLrad1Qc.flag_values = 0,1,2,3
                    ncLrad1Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                ncLrad1Qc[timeIndx] = qualityFlag(lrad1[1])
                    
                # NDBC QC flags
                ncLrad1QcDet = getVariable(ncSensor1, shortName + '_detail_qc')
                if ncLrad1QcDet == None:
                    # Add it
                    ncLrad1QcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                        'c', ('time',), zlib=True, fill_value= '_')
                    ncLrad1QcDet.flag_values = 'see NDBC QC Manual'

                ncLrad1QcDet[timeIndx] = lrad1[1][0]

                # Release flag
                ncLrad1Rel = getVariable(ncSensor1, shortName + '_release')
                 
                if ncLrad1Rel == None:
                    # Add it
                    ncLrad1Rel = ncSensor1.createVariable(shortName + '_release', 
                        'b', ('time',), zlib=True, fill_value= -9)
                    ncLrad1Rel.flag_values = 0,1
                    ncLrad1Rel.flag_meanings = 'not_released released'
                    ncLrad1Rel.comment = 'indicates datum was publicly released in realtime'

                if asciiId in released:
                    ncLrad1Rel[timeIndx] = 1
                else:
                    ncLrad1Rel[timeIndx] = 0
                    
    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        writeLog(msgString)

#
# put visibility in NetCDF document
#
def putVisibility(NC, SD, stationNode, dataObj):
    global TIMEKEYS
    global TIME10KEYS
    global TIMEMKEYS

    eodTime = dataObj.getEndDataTime()
    timeIndx = TIMEKEYS.index(eodTime)
    pyldId = dataObj.getPayloadId()
    dn = getDeploymentNode(stationNode, pyldId)
    myMeas = dataObj.getMeasurements()
    released =  dataObj.getGtsReleasedMeasurements()
    
    try:
        
        if 'VISIB3' in myMeas:
            visib3 = myMeas['VISIB3']
            
        if 'visib3' in vars():
            ncSensor1 = getSensor(NC, pyldId, dn, 'visibility_sensor', 1)
            
            # make sure we have a sensor defined
            assert ncSensor1 != None
                
            if visib3[1] != 'M':         
                name= 'visibility_in_air'
                shortName = 'visibility'
                
                ncVisib3 = getVariable(ncSensor1, shortName)
                    
                if ncVisib3 == None:
                    # Add it
                    ncVisib3 = ncSensor1.createVariable(shortName,
                        'f4', ('time',), zlib=True)
                    ncVisib3.long_name = name
                    ncVisib3.standard_name = name
                    ncVisib3.short_name = shortName
                    ncVisib3.units = 'm'
                    
                ncVisib3[timeIndx] = float(visib3[0]) * 1609.344

                # QC flags                    
                ncVisib3Qc = getVariable(ncSensor1, shortName + '_qc') 
                    
                if ncVisib3Qc == None:
                    # Add it
                    ncVisib3Qc = ncSensor1.createVariable(shortName + '_qc', 
                        'b', ('time',), zlib=True, fill_value= -9)
                    ncVisib3Qc.flag_values = 0,1,2,3
                    ncVisib3Qc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                ncVisib3Qc[timeIndx] = qualityFlag(visib3[1])
                    
                # NDBC QC flags
                ncVisib3QcDet = getVariable(ncSensor1, shortName + '_detail_qc')
                if ncVisib3QcDet == None:
                    # Add it
                    ncVisib3QcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                        'c', ('time',), zlib=True, fill_value= '_')
                    ncVisib3QcDet.flag_values = 'see NDBC QC Manual'

                ncVisib3QcDet[timeIndx] = visib3[1][0]

                # Release flag
                ncVisib3Rel = getVariable(ncSensor1, shortName + '_release')
                 
                if ncVisib3Rel == None:
                    # Add it
                    ncVisib3Rel = ncSensor1.createVariable(shortName + '_release', 
                        'b', ('time',), zlib=True, fill_value= -9)
                    ncVisib3Rel.flag_values = 0,1
                    ncVisib3Rel.flag_meanings = 'not_released released'
                    ncVisib3Rel.comment = 'indicates datum was publicly released in realtime'

                if 'VISIB3' in released:
                    ncVisib3Rel[timeIndx] = 1
                else:
                    ncVisib3Rel[timeIndx] = 0
                    
    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        writeLog(msgString)
#
# put GPS location data in NetCDF document
#
def putGps(NC, SD, stationNode, dataObj):
    global TIMEKEYS
    global TIME10KEYS
    global TIMEMKEYS

    eodTime = dataObj.getEndDataTime()
    timeIndx = TIMEKEYS.index(eodTime)
    pyldId = dataObj.getPayloadId()
    dn = getDeploymentNode(stationNode, pyldId)
    myMeas = dataObj.getMeasurements()
    released =  dataObj.getGtsReleasedMeasurements()
    
    #
    # GPS
    #
    try:
        
        if 'GPSLAT' in myMeas:
            gpslat = myMeas['GPSLAT']
            
        if 'GPSLON' in myMeas:
            gpslon = myMeas['GPSLON']
            
        if 'gpslat' in vars() or 'gpslon' in vars():
            ncSensor1 = getSensor(NC, pyldId, dn, 'gps', 1)
            
            # make sure we have a sensor defined
            assert ncSensor1 != None
                
            # latitude
            if 'gpslat' in vars():
                if gpslat[1] != 'M':
                    name= 'latitude'
                    shortName = 'latitude'
         
                    ncGpslat = getVariable(ncSensor1, shortName)
                    
                    if ncGpslat == None:
                        # Add it
                        ncGpslat = ncSensor1.createVariable(shortName,
                            'f4', ('time',), zlib=True)
                        ncGpslat.long_name = name
                        ncGpslat.standard_name = name
                        ncGpslat.units = 'degrees_north'
                   
                    ncGpslat[timeIndx] = float(gpslat[0])

                    # QC flags                    
                    ncGpslatQc = getVariable(ncSensor1, shortName + '_qc') 
                    
                    if ncGpslatQc == None:
                        # Add it
                        ncGpslatQc = ncSensor1.createVariable(shortName + '_qc', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncGpslatQc.flag_values = 0,1,2,3
                        ncGpslatQc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncGpslatQc[timeIndx] = qualityFlag(gpslat[1])
                    
                    # NDBC QC flags
                    ncGpslatQcDet = getVariable(ncSensor1, shortName + '_detail_qc')
                    if ncGpslatQcDet == None:
                        # Add it
                        ncGpslatQcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                            'c', ('time',), zlib=True, fill_value= '_')
                        ncGpslatQcDet.flag_values = 'see NDBC QC Manual'

                    ncGpslatQcDet[timeIndx] = gpslat[1][0]

                    # Release flag
                    ncGpslatRel = getVariable(ncSensor1, shortName + '_release')
                 
                    if ncGpslatRel == None:
                        # Add it
                        ncGpslatRel = ncSensor1.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncGpslatRel.flag_values = 0,1
                        ncGpslatRel.flag_meanings = 'not_released released'
                        ncGpslatRel.comment = 'indicates datum was publicly released in realtime'

                    if 'GPSLAT' in released:
                        ncGpslatRel[timeIndx] = 1
                    else:
                        ncGpslatRel[timeIndx] = 0

            # longitude
            if 'gpslon' in vars():
                if gpslon[1] != 'M':         
                    name= 'longitude'
                    shortName = 'longitude'

                    ncGpslon = getVariable(ncSensor1, shortName)
                    
                    if ncGpslon == None:
                        # Add it
                        ncGpslon = ncSensor1.createVariable(shortName,
                            'f4', ('time',), zlib=True)
                        ncGpslon.long_name = name
                        ncGpslon.standard_name = name
                        ncGpslon.units = 'degrees_east'
                   
                    ncGpslon[timeIndx] = float(gpslon[0])

                    # QC flags                    
                    ncGpslonQc = getVariable(ncSensor1, shortName + '_qc') 
                    
                    if ncGpslonQc == None:
                        # Add it
                        ncGpslonQc = ncSensor1.createVariable(shortName + '_qc', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncGpslonQc.flag_values = 0,1,2,3
                        ncGpslonQc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

                    ncGpslonQc[timeIndx] = qualityFlag(gpslon[1])
                    
                    # NDBC QC flags
                    ncGpslonQcDet = getVariable(ncSensor1, shortName + '_detail_qc')
                    if ncGpslonQcDet == None:
                        # Add it
                        ncGpslonQcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                            'c', ('time',), zlib=True, fill_value= '_')
                        ncGpslonQcDet.flag_values = 'see NDBC QC Manual'

                    ncGpslonQcDet[timeIndx] = gpslon[1][0]
    
                    # Release flag
                    ncGpslonRel = getVariable(ncSensor1, shortName + '_release')
                 
                    if ncGpslonRel == None:
                        # Add it
                        ncGpslonRel = ncSensor1.createVariable(shortName + '_release', 
                            'b', ('time',), zlib=True, fill_value= -9)
                        ncGpslonRel.flag_values = 0,1
                        ncGpslonRel.flag_meanings = 'not_released released'
                        ncGpslonRel.comment = 'indicates datum was publicly released in realtime'

                    if 'GPSLON' in released:
                        ncGpslonRel[timeIndx] = 1
                    else:
                        ncGpslonRel[timeIndx] = 0

    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        writeLog(msgString)

#
# put Wave data in NetCDF document
#
def putWave(NC, SD, stationNode, dataObj):
    global TIMEKEYS
    global TIME10KEYS
    global TIMEMKEYS
    global WAVEPAYLOADTIMEKEYS
    global WAVEFREQUENCYKEYS

    eodTime = dataObj.getEndDataTime()
    timeIndx = TIMEKEYS.index(eodTime)
    pyldId = dataObj.getPayloadId()
    dn = getDeploymentNode(stationNode, pyldId)
    myMeas = dataObj.getMeasurements()
    released =  dataObj.getGtsReleasedMeasurements()
    
    #
    # Wave Sensor
    #
    try:
        if 'WVHGT' not in myMeas:
            # Waves undefined for this station
            return
            
        wvhgt = myMeas['WVHGT']
        if wvhgt[1] == 'M':
            # wave data is missing for this report
            return         
            
        ncSensor1 = getSensor(NC, pyldId, dn, 'wave_sensor', 1)
            
        # make sure we have a sensor defined
        assert ncSensor1 != None
                
        # wave height
        name = 'sea_surface_wave_significant_height'
        shortName = 'significant_wave_height'
        timeDim = (WAVEPAYLOADTIMEKEYS[pyldId])
#        timeDim = (WAVEPAYLOADTIMEKEYS[pyldId]).encode('latin-1')

        ncWvhgt = getVariable(ncSensor1, shortName)
                    
        if ncWvhgt == None:
            # Add it
            ncWvhgt = ncSensor1.createVariable(shortName,'f4' \
                ,(timeDim,), zlib=True)
            ncWvhgt.long_name = name
            ncWvhgt.standard_name = name
            ncWvhgt.short_name = shortName
            ncWvhgt.units = 'm'
                   
        ncWvhgt[timeIndx] = float(wvhgt[0])

        # QC flags                    
        ncWvhgtQc = getVariable(ncSensor1, shortName + '_qc') 
                    
        if ncWvhgtQc == None:
            # Add it
            ncWvhgtQc = ncSensor1.createVariable(shortName + '_qc', 
                'b', (timeDim,), zlib=True, fill_value= -9)
            ncWvhgtQc.long_name = name + '_qc'
            ncWvhgtQc.short_name = shortName + '_qc'
            ncWvhgtQc.flag_values = 0,1,2,3
            ncWvhgtQc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

        ncWvhgtQc[timeIndx] = qualityFlag(wvhgt[1])

        # NDBC QC flags
        ncWvhgtQcDet = getVariable(ncSensor1, shortName + '_detail_qc')
        if ncWvhgtQcDet == None:
            # Add it
            ncWvhgtQcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                'c', (timeDim,), zlib=True, fill_value= '_')
            ncWvhgtQcDet.long_name = name + '_detail_qc'
            ncWvhgtQcDet.short_name = shortName + '_detail_qc'
            ncWvhgtQcDet.flag_values = 'see NDBC QC Manual'

        ncWvhgtQcDet[timeIndx] = wvhgt[1][0]

        # Release flag
        ncWvhgtRel = getVariable(ncSensor1, shortName + '_release')
                 
        if ncWvhgtRel == None:
            # Add it
            ncWvhgtRel = ncSensor1.createVariable(shortName + '_release', 
                'b', ('time',), zlib=True, fill_value= -9)
            ncWvhgtRel.flag_values = 0,1
            ncWvhgtRel.flag_meanings = 'not_released released'
            ncWvhgtRel.comment = 'indicates datum was publicly released in realtime'

            if 'WVHGT' in released:
                ncWvhgtRel[timeIndx] = 1
            else:
                ncWvhgtRel[timeIndx] = 0
                    
        # dominate period
        try:
            dompd = myMeas['DOMPD']
        except:
            None

        if 'dompd' in vars() and dompd[1] != 'M':
            name= 'sea_surface_wave_period_at_variance_spectral_density_maximum'
            shortName = 'dominant_period'
            
            ncDompd = getVariable(ncSensor1, shortName)
                    
            if ncDompd == None:
                # Add it
                ncDompd = ncSensor1.createVariable(shortName,'f4', (timeDim,), zlib=True)
                ncDompd.long_name = name
                ncDompd.standard_name = name
                ncDompd.short_name = shortName
                ncDompd.units = 's'
                   
            ncDompd[timeIndx] = float(dompd[0])

            # QC flags                    
            ncDompdQc = getVariable(ncSensor1, shortName + '_qc') 
                    
            if ncDompdQc == None:
                # Add it
                ncDompdQc = ncSensor1.createVariable(shortName + '_qc','b', (timeDim,), zlib=True, fill_value= -9)
                ncDompdQc.long_name = name + '_qc'
                ncDompdQc.short_name = shortName + '_qc'
                ncDompdQc.flag_values = 0,1,2,3
                ncDompdQc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

            ncDompdQc[timeIndx] = qualityFlag(dompd[1])
                    
            # NDBC QC flags
            ncDompdQcDet = getVariable(ncSensor1, shortName + '_detail_qc')
            if ncDompdQcDet == None:
                # Add it
                ncDompdQcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                    'c', (timeDim,), zlib=True, fill_value='_')
                ncDompdQcDet.long_name = name + '_detail_qc'
                ncDompdQcDet.short_name = shortName + '_detail_qc'
                ncDompdQcDet.flag_values = 'see NDBC QC Manual'

            ncDompdQcDet[timeIndx] = dompd[1][0]
    
        # Release flag
        ncDompdRel = getVariable(ncSensor1, shortName + '_release')
                 
        if ncDompdRel == None:
            # Add it
            ncDompdRel = ncSensor1.createVariable(shortName + '_release', 
                'b', ('time',), zlib=True, fill_value= -9)
            ncDompdRel.flag_values = 0,1
            ncDompdRel.flag_meanings = 'not_released released'
            ncDompdRel.comment = 'indicates datum was publicly released in realtime'

            if 'DOMPD' in released:
                ncDompdRel[timeIndx] = 1
            else:
                ncDompdRel[timeIndx] = 0

        # Average wave period
        try:
            avgpd = myMeas['AVGPD']
        except:
            None

        if 'avgpd' in vars() and avgpd[1] != 'M':
            name = 'sea_surface_wave_mean_period_from_variance_spectral_density_second_frequency_moment'
            shortName = 'average_period'
            
            ncAvgpd = getVariable(ncSensor1, shortName)
                    
            if ncAvgpd == None:
                # Add it
                ncAvgpd = ncSensor1.createVariable(shortName,'f4' \
                    , (timeDim,), zlib=True)
                ncAvgpd.long_name = name
                ncAvgpd.standard_name = name
                ncAvgpd.short_name = shortName
                ncAvgpd.units = 's'
                   
            ncAvgpd[timeIndx] = float(avgpd[0])

            # QC flags                    
            ncAvgpdQc = getVariable(ncSensor1, shortName + '_qc') 
                    
            if ncAvgpdQc == None:
                # Add it
                ncAvgpdQc = ncSensor1.createVariable(shortName + '_qc', 
                    'b', (timeDim,), zlib=True, fill_value= -9)
                ncAvgpdQc.long_name = name + '_qc'
                ncAvgpdQc.short_name = shortName + '_qc'
                ncAvgpdQc.flag_values = 0,1,2,3
                ncAvgpdQc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

            ncAvgpdQc[timeIndx] = qualityFlag(avgpd[1])
                    
            # NDBC QC flags
            ncAvgpdQcDet = getVariable(ncSensor1, shortName + '_detail_qc')
            if ncAvgpdQcDet == None:
                # Add it
                ncAvgpdQcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                    'c', (timeDim,), zlib=True, fill_value= '_')
                ncAvgpdQcDet.long_name = name + '_detail_qc'
                ncAvgpdQcDet.short_name = shortName + '_detail_qc'
                ncAvgpdQcDet.flag_values = 'see NDBC QC Manual'

            ncAvgpdQcDet[timeIndx] = avgpd[1][0]
            
        # Release flag
        ncAvgpdRel = getVariable(ncSensor1, shortName + '_release')
                 
        if ncAvgpdRel == None:
            # Add it
            ncAvgpdRel = ncSensor1.createVariable(shortName + '_release', 
                'b', ('time',), zlib=True, fill_value= -9)
            ncAvgpdRel.flag_values = 0,1
            ncAvgpdRel.flag_meanings = 'not_released released'
            ncAvgpdRel.comment = 'indicates datum was publicly released in realtime'

            if 'AVGPD' in released:
                ncAvgpdRel[timeIndx] = 1
            else:
                ncAvgpdRel[timeIndx] = 0

        # mean wave direction
        try:
            mwdir = myMeas['MWDIR']
        except:
            None

        if 'mwdir' in vars() and mwdir[1] != 'M':
            name = 'sea_surface_wave_from_direction'
            shortName = 'mean_wave_direction'
            
            ncMwdir = getVariable(ncSensor1, shortName)
                    
            if ncMwdir == None:
                # Add it
                ncMwdir = ncSensor1.createVariable(shortName,'f4', \
                    (timeDim,), zlib=True)
                ncMwdir.long_name = name
                ncMwdir.standard_name = name
                ncMwdir.short_name = shortName
                ncMwdir.units = 'degree'
                   
            ncMwdir[timeIndx] = float(mwdir[0])

            # QC flags                    
            ncMwdirQc = getVariable(ncSensor1, shortName + '_qc') 
                    
            if ncMwdirQc == None:
                # Add it
                ncMwdirQc = ncSensor1.createVariable(shortName + '_qc', 
                    'b', (timeDim,), zlib=True, fill_value= -9)
                ncMwdirQc.long_name = name + '_qc'
                ncMwdirQc.short_name = shortName + '_qc'
                ncMwdirQc.flag_values = 0,1,2,3
                ncMwdirQc.flag_meanings = 'quality_good out_of_range sensor_nonfunctional questionable'

            ncMwdirQc[timeIndx] = qualityFlag(mwdir[1])
                    
            # NDBC QC flags
            ncMwdirQcDet = getVariable(ncSensor1, shortName + '_detail_qc')
            if ncMwdirQcDet == None:
                # Add it
                ncMwdirQcDet = ncSensor1.createVariable(shortName + '_detail_qc',
                    'c', (timeDim,), zlib=True, fill_value= '_')
                ncMwdirQcDet.long_name = name + '_detail_qc'
                ncMwdirQcDet.short_name = shortName + '_detail_qc'
                ncMwdirQcDet.flag_values = 'see NDBC QC Manual'

            ncMwdirQcDet[timeIndx] = mwdir[1][0]
            
        # Release flag
        ncMwdirRel = getVariable(ncSensor1, shortName + '_release')
                 
        if ncMwdirRel == None:
            # Add it
            ncMwdirRel = ncSensor1.createVariable(shortName + '_release', 
                'b', ('time',), zlib=True, fill_value= -9)
            ncMwdirRel.flag_values = 0,1
            ncMwdirRel.flag_meanings = 'not_released released'
            ncMwdirRel.comment = 'indicates datum was publicly released in realtime'

            if 'MWDIR' in released:
                ncMwdirRel[timeIndx] = 1
            else:
                ncMwdirRel[timeIndx] = 0

        # Wave Spectra
        myWave1 = dataObj.getWave1Measurements()
        
        if myWave1:
            try:
                waveType = getattr(ncSensor1, 'type').lower()
            except:
                waveType = 'NULL'
                print 'waveType NOT found!'
                for name in ncSensor1.ncattrs():
                    print 'Attribute: ', name, '=', getattr(ncSensor1, name)

            dimName = "wave_%s" % (waveType)
            freqList = WAVEFREQUENCYKEYS[dimName]                       

            # c11
            try:
                c11 = myWave1[0]
            except:
                None

            if 'c11' in vars():
                name = 'sea_surface_wave_variance_spectral_density'
                shortName = 'c11'
            
                ncC11 = getVariable(ncSensor1, shortName)
                    
                if ncC11 == None:
                    # Add it
                    ncC11 = ncSensor1.createVariable(shortName,'f4',
                        (timeDim, dimName), zlib=True)
                    ncC11.long_name = name
                    ncC11.standard_name = name
                    ncC11.short_name = shortName
                    ncC11.units = 'm**2/hz'
                
                for j in range(len(freqList)):
                    ncC11[timeIndx, j] = float(c11[j])
            
            # c11m
            try:
                c11m = myWave1[1]
            except:
                None

            if 'c11m' in vars():
                name = 'sea_surface_wave_variance_spectral_density_uncorrected'
                shortName = 'c11m'
            
                ncC11m = getVariable(ncSensor1, shortName)
                    
                if ncC11m == None:
                    # Add it
                    ncC11m = ncSensor1.createVariable(shortName,'f4',
                        (timeDim, dimName), zlib=True)
                    ncC11m.long_name = name
                    ncC11m.short_name = shortName
                    ncC11m.units = 'm**2/hz'
                
                for j in range(len(freqList)):
                    ncC11m[timeIndx, j] = float(c11m[j])
                
            # Wave Directional Spectra
            myWave2 = dataObj.getWave2Measurements()

            # alpha 1
            try:
                alpha1 = myWave2[0]
            except:
                None

            if 'alpha1' in vars():
                name = 'mean_wave_direction_at_specified_frequency'
                shortName = 'alpha1'
            
                ncAlpha1 = getVariable(ncSensor1, shortName)
                    
                if ncAlpha1 == None:
                    # Add it
                    ncAlpha1 = ncSensor1.createVariable(shortName,'f4',
                        (timeDim, dimName), zlib=True)
                    ncAlpha1.long_name = name
                    ncAlpha1.standard_name = name
                    ncAlpha1.short_name = shortName
                    ncAlpha1.units = 'deg'
                
                for j in range(len(freqList)):
                    ncAlpha1[timeIndx, j] = float(alpha1[j])

            # alpha 2
            try:
                alpha2 = myWave2[1]
            except:
                None

            if 'alpha2' in vars():
                name = 'principal_wave_direction_at_specified_frequency'
                shortName = 'alpha2'
            
                ncAlpha2 = getVariable(ncSensor1, shortName)
                    
                if ncAlpha2 == None:
                    # Add it
                    ncAlpha2 = ncSensor1.createVariable(shortName,'f4',
                        (timeDim, dimName), zlib=True)
                    ncAlpha2.long_name = name
                    ncAlpha2.standard_name = name
                    ncAlpha2.short_name = shortName
                    ncAlpha2.units = 'deg'
                
                for j in range(len(freqList)):
                    ncAlpha2[timeIndx, j] = float(alpha2[j])

            # r1
            try:
                r1 = myWave2[2]
            except:
                None

            if 'r1' in vars():
                name = 'first_normalized_polar_coordinate_of_the_Fourier_coefficients'
                shortName = 'r1'
            
                ncR1 = getVariable(ncSensor1, shortName)
                    
                if ncR1 == None:
                    # Add it
                    ncR1 = ncSensor1.createVariable(shortName,'f4',
                        (timeDim, dimName), zlib=True)
                    ncR1.long_name = name
                    ncR1.standard_name = name
                    ncR1.short_name = shortName
                    ncR1.units = 'deg'
                
                for j in range(len(freqList)):
                    ncR1[timeIndx, j] = float(r1[j])

            # r2
            try:
                r2 = myWave2[3]
            except:
                None

            if 'r2' in vars():
                name = 'second_normalized_polar_coordinate_of_the_Fourier_coefficients'
                shortName = 'r2'
            
                ncR2 = getVariable(ncSensor1, shortName)
                    
                if ncR2 == None:
                    # Add it
                    ncR2 = ncSensor1.createVariable(shortName,'f4',
                        (timeDim, dimName), zlib=True)
                    ncR2.long_name = name
                    ncR2.standard_name = name
                    ncR2.short_name = shortName
                    ncR2.units = 'deg'
                
                for j in range(len(freqList)):
                    ncR2[timeIndx, j] = float(r2[j])

            # Wave Directional Spectra
            myWave3 = dataObj.getWave3Measurements()

            # Do I have a wave3 rec
            if myWave3:
                # RHQ
                try:
                    rhq = myWave3[0]
                except:
                    None

 #           if 'alpha1' in vars():
                name = 'rhq_coefficient_for_quad_spectra'
                shortName = 'rhq'
            
                ncRhq1 = getVariable(ncSensor1, shortName)
                    
                if ncRhq1 == None:
                    # Add it
                    ncRhq1 = ncSensor1.createVariable(shortName,'f4',
                        (timeDim, dimName), zlib=True)
                    ncRhq1.long_name = name
#                    ncRhq1.standard_name = name
                    ncRhq1.short_name = shortName
#                    ncRhq1.units = 'none'
                
                for j in range(len(freqList)):
                    try:
                        ncRhq1[timeIndx, j] = float(rhq[j])
                    except:
                        None

                # GAMMA2
                try:
                    gamma2 = myWave3[1]
                except:
                    None

 #           if 'alpha1' in vars():
                name = 'gamma2_coefficient_for_quad_spectra'
                shortName = 'gamma2'
            
                ncGamma21 = getVariable(ncSensor1, shortName)
                    
                if ncGamma21 == None:
                    # Add it
                    ncGamma21 = ncSensor1.createVariable(shortName,'f4',
                        (timeDim, dimName), zlib=True)
                    ncGamma21.long_name = name
#                    ncRhq1.standard_name = name
                    ncGamma21.short_name = shortName
#                    ncRhq1.units = 'none'
                
                for j in range(len(freqList)):
                    try:
                        ncGamma21[timeIndx, j] = float(gamma2[j])
                    except:
                        None

                # GAMMA3
                try:
                    gamma3 = myWave3[2]
                except:
                    None

 #           if 'alpha1' in vars():
                name = 'gamma3_coefficient_for_quad_spectra'
                shortName = 'gamma3'
            
                ncGamma31 = getVariable(ncSensor1, shortName)
                    
                if ncGamma31 == None:
                    # Add it
                    ncGamma31 = ncSensor1.createVariable(shortName,'f4',
                        (timeDim, dimName), zlib=True)
                    ncGamma31.long_name = name
#                    ncRhq1.standard_name = name
                    ncGamma31.short_name = shortName
#                    ncRhq1.units = 'none'
                
                for j in range(len(freqList)):
                    try:
                        ncGamma31[timeIndx, j] = float(gamma3[j])
                    except:
                        None

                # PHIH
                try:
                    phih = myWave3[3]
                except:
                    None

 #           if 'alpha1' in vars():
                name = 'phih_coefficient_for_quad_spectra'
                shortName = 'phih'
            
                ncPhih1 = getVariable(ncSensor1, shortName)
                    
                if ncPhih1 == None:
                    # Add it
                    ncPhih1 = ncSensor1.createVariable(shortName,'f4',
                        (timeDim, dimName), zlib=True)
                    ncPhih1.long_name = name
#                    ncRhq1.standard_name = name
                    ncPhih1.short_name = shortName
#                    ncRhq1.units = 'none'
                
                for j in range(len(freqList)):
                    try:
                        ncPhih1[timeIndx, j] = float(phih[j])
                    except:
                        None

    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        writeLog(msgString)

#
# create NetCDF Variables
#    
def createVariables(NC, SD, stationNode):
    global TIMEKEYS
    global TIME10KEYS
    global TIMEMKEYS

    try:
        # make sure at beginning of file
        SD.seek(0)
            
        # loop thru all bulletin records
        for bullRec in get_a_record(SD):
            myObj = ParsePrivate(bullRec)
            
            # Get timestamp for the data
            eodTime = myObj.getEndDataTime()
            if eodTime == None:
                continue
            
            try:
                timeIndx = TIMEKEYS.index(eodTime)
            except ValueError:
                continue
            
            pyldId = myObj.getPayloadId()
            dn = getDeploymentNode(stationNode, pyldId)
            myMeas = myObj.getMeasurements()
            
            # write wind data to the NetCDF document 
            putWinds(NC, SD, stationNode, myObj)
            
            # write barometer data to the NetCDF document
            putBaro(NC, SD, stationNode, myObj)

            # write air temperature sensor data to the NetCDF document
            putAirTemperature(NC, SD, stationNode, myObj)
            
            # write humidity sensor data to the NetCDF document
            putHumidity(NC, SD, stationNode, myObj)
            
            # write sea surface temperature sensor to NetCDF document
            putOceanTemperature(NC, SD, stationNode, myObj)

            # write current meter sensor data to NetCDF document
            putOceanCurrentMeter(NC, SD, stationNode, myObj)

            # write CTD sensor data to NetCDF document
            putSalinity(NC, SD, stationNode, myObj)

            # write solar radiation data to NetCDF document
            putSolarRadiation(NC, SD, stationNode, myObj)

            # write visibility sensor data to NetCDF document
            putVisibility(NC, SD, stationNode, myObj)

            # write current profiler sensor data to NetCDF document
#            putOceanCurrentProfile(NC, SD, stationNode, myObj)

            # write current meter sensor data to NetCDF document
            putGps(NC, SD, stationNode, myObj)
            
            # write wave data to the NetCDF document
            putWave(NC, SD, stationNode, myObj)

    except:
        msgTuple = formatExceptionInfo()
        msgString = str(msgTuple)
        writeLog(msgString)

#####################################################################
#####################################################################
# Main - Start of program                                           #
#####################################################################
#####################################################################
# Setup global environment variables
HOME=os.environ['HOME']
FILENAME=os.path.basename(sys.argv[0])
PID=str(os.getpid())
PROGDIR=os.getcwd()
ERRFILE='/tmp/' + os.path.splitext(FILENAME)[0] + '_' + PID + '.err'
LOGFILE=PROGDIR + '/' + os.path.splitext(FILENAME)[0] + '.log'

TIMEKEYS = None
TIME10KEYS = None
TIMEMKEYS = None
WAVETIMEKEYS = set()
WAVEPAYLOADTIMEKEYS = {}
WAVEFREQUENCYKEYS = {}
ADCPDEPTHKEYS = None

# Read run parameters
configProperties = getConfigProperties()

try:
    startTime = configProperties['starttime']
    ARCHIVESTART = dateutil.parser.parse(startTime, ignoretz=True)
    ARCHIVESTARTPARM = ARCHIVESTART
    archiveStartTime = time.mktime(ARCHIVESTART.timetuple())
    
    stopTime = configProperties['stoptime']
    ARCHIVESTOP = dateutil.parser.parse(stopTime, ignoretz=True)
    ARCHIVESTOPPARM = ARCHIVESTOP
    archiveStopTime = time.mktime(ARCHIVESTOP.timetuple())

    # Load station metadata
    xmlDoc = minidom.parse('ArchiveExtractionList_Stage2.xml')
    top_element = xmlDoc.documentElement

    if len(sys.argv) > 2 and sys.argv[2]:
        wmoId = sys.argv[2]
    else:
        usage()
        os._exit(1)

    # Get Deployment numbers for the month
    deploymentDbConnection = sqlite.connect('deployment.db')
    cursor = deploymentDbConnection.cursor()
    
    deploymentDi = {}
    
    sql = """
        SELECT deploymentId,deploymentStart,deploymentEnd
        FROM deployment
        WHERE stationId='%s'
        AND deploymentStart < '%s'
        ORDER BY deploymentStart DESC
        """ % (wmoId, ARCHIVESTOP)
    
#    print sql
    
    deploymentId = None
    deploymentStart = None
    deploymentEnd = None
    
    cursor.execute(sql)
    
    for row in cursor.fetchall():
        deploymentId = row[0]
        deploymentStart = row[1]
        
        if row[2]:
            deploymentEnd = row[2]
        else:
            deploymentEnd = "%s" % (ARCHIVESTOP)
            
        deploymentDi[deploymentId] = "%s|%s" % (deploymentStart, deploymentEnd)
        if deploymentStart < "%s" % (ARCHIVESTART):
            break
 
    cursor.close()
    deploymentDbConnection.close()
    
    deployments = deploymentDi.keys()
    deployments.sort()

    print "deployments=", deployments
    
    for key in deployments:
        # Reset Start & Stop on each loop
        ARCHIVESTART = ARCHIVESTARTPARM
        ARCHIVESTOP = ARCHIVESTOPPARM
        
        [startTime, stopTime] = deploymentDi[key].split('|')

        tmpStartFmt = "%s" % (ARCHIVESTARTPARM)
        
        # If deployment start is after archive start, reset archive start
        # for this loop.
        if tmpStartFmt < startTime:
            ARCHIVESTART = dateutil.parser.parse(startTime, ignoretz=True)
                                                 
        archiveStartTime = time.mktime(ARCHIVESTART.timetuple())
        
        tmpStopFmt = "%s" % (ARCHIVESTOPPARM)
        
        # If deployment stop is before archive stop, reset archive stop
        # for this loop.
        if tmpStopFmt > stopTime:
            ARCHIVESTOP = dateutil.parser.parse(stopTime, ignoretz=True)
            
        archiveStopTime = time.mktime(ARCHIVESTOP.timetuple())
            
        print time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(archiveStartTime))
        print time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(archiveStopTime))
        
        # Open station data file
        filename = configProperties['stationDir'] + '/' + wmoId
        print filename
        
        deploymentId = key

        SD = open(filename, 'r')
    
        # open LOG file
        LOGFILE=PROGDIR + '/logs/' + os.path.splitext(FILENAME)[0] \
            + '_' + wmoId + '.log'
        LOG = open(LOGFILE, 'w')

        # create NetCDF file
        (NC, stationNode, netcdfFilename) = createNetCDF(xmlDoc, wmoId, deploymentId)
    
        # create Payload Groups
        createPayloadGroups(NC, stationNode)

        (minLat, maxLat, minLon, maxLon, vertMin, vertMax) = getSpatialDefaults(stationNode)
        NC.nominal_latitude = minLat
        NC.nominal_longitude = minLon
    
        # create Indexes
        rc = createIndexes(NC, SD, stationNode)

        # Nothing to process
        if rc == -1:
            NC.close()
            SD.close()
            if os.path.exists(netcdfFilename):
                os.unlink(netcdfFilename)
            continue
    
        # create & load variables
        createVariables(NC, SD, stationNode)
        
#        vertMax = ADCPDEPTHKEYS[0] * -1
#        vertMin = ADCPDEPTHKEYS[-1] * -1
    
        putSpatialCoordinates(NC, minLat, maxLat, minLon, maxLon, vertMin, vertMax)
    
        NC.time_coverage_start = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(TIMEKEYS[0]))
    
        NC.time_coverage_end = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(TIMEKEYS[-1]))
    
        NC.date_created = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        NC.date_modified = ""
        NC.creator_url = 'http://www.ndbc.noaa.gov'
        NC.creator_email = 'webmaster.ndbc@noaa.gov'
    
        NC.close()
        SD.close()
        
    LOG.close()
except:
    msgTuple = formatExceptionInfo()
    msgString = str(msgTuple)
    sendMail(msgString, alert='Error', file=False)
    os._exit(1)
