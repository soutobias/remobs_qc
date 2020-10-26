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
from struct import pack #@UnresolvedImport


def binString(string):

    binaryString = []

    for ch in string:
        num = ord(ch)
        binStr = numpy.binary_repr(num, 8)
        binaryString.append(binStr)

    return "".join(binaryString)


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
    print(newString)

    wholeBytes = len(newString) / 8
    print('wholeBytes=', wholeBytes)

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
        print('newLeftoverString=', newLeftoverString)
        newLeftover = pack("B", int(newLeftoverString, 2))

    newString = ''.join(newPacked)

    return (newString, newLeftover, leftoverBits)
