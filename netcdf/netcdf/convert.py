# -*- coding: utf-8 -*-
"""
Created on Thu Jun 05 13:50:52 2014

@author: soutobias
"""
import datetime

def time2epoch(Year,Month,Day,Hour,Minute):
    
    
    Epoch = [0]*len(Year)
    
    if Minute==0:
        for i in xrange(len(Year)):
            Epoch[i]=(datetime.datetime(int(Year[i]),int(Month[i]),int(Day[i]),int(Hour[i])) - datetime.datetime(1970,1,1)).total_seconds()
    else:
        for i in xrange(len(Year)):
            Epoch[i]=(datetime.datetime(int(Year[i]),int(Month[i]),int(Day[i]),int(Hour[i]),int(Minute[i])) - datetime.datetime(1970,1,1)).total_seconds()

    return Epoch
    
    
def liststr2str(var):
    
    varstr=""
    for i in xrange(len(var)):
        varstr=str(varstr)+str(var[i])

    return varstr
