# -*- coding: utf-8 -*-
"""
Created on Tue Oct 14 07:38:43 2014

@author: soutobias
"""

import re
import time
import datetime
import numpy as np
import operator
from numpy import *
from qualitycontrol import *
import mysql.connector as MySQLdb
from pandas.io import sql
import sqlalchemy
import pandas as pd

import sys
import os
from os.path import expanduser
home = expanduser("~")
sys.path.insert(0,home)
import user_config
os.chdir( user_config.path2 )

def get_data_db(buoy):

    con = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                            format(user_config.username,
                                                user_config.password,
                                                user_config.host,
                                                user_config.database))


    sql = "SELECT * FROm pnboia_bruto wheRe estacao_id=%s ORdeR by data" % buoy
    return pd.read_sql(sql, con)

def insert_data_db(df):

    con = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                            format(user_config.username,
                                                user_config.password,
                                                user_config.host,
                                                user_config.database))

    df.to_sql(con=con, name='pnboia_qualificado', if_exists='append')


def working_buoys():

    db = MySQLdb.connect(host = user_config.host,
                         user = user_config.username,
                         password = user_config.password,
                         database = user_config.database)

    cur=db.cursor(dictionary=True)

    sql = "SELECT * FROM pnboia_estacao"
    cur.execute(sql)

    buoys = []
    for row in cur.fetchall():
        buoys.append(row)

    return buoys

buoys=working_buoys()

buoy = buoys[0]
# for buoy in buoys:
print(buoy["nome"])

raw_data = get_data_db(buoy["estacao_id"])

raw_data = raw_data.replace([-9999, 9999, 99.99, None, "-9999", "-99999", '', '99.99', '-9999.0', -9999.0, -9999] , np.NaN)

raw_data = raw_data.set_index("data")

# for ii in range(len(variables0)):
#     exec("%stratados=[]"% (variables0[ii]))
#     exec("%stratados1=[]"% (variables0[ii]))

# for ii in range(len(variables3)):
#     exec("%sflagtratados=[]"% (variables3[ii]))
#     exec("%sflagidtratados=[]"% (variables3[ii]))
#     exec("%sflagtratados1=[]"% (variables3[ii]))
#     exec("%sflagidtratados1=[]"% (variables3[ii]))

stop
(flag_data)=qualitycontrol(raw_data,buoy)


for i in range(len(Wdir)):
    aw=int(data[i].year)-2002
    var=Cdir1flag[i]
    if var!=4:
        Cdir1[i]=int(arredondar(float(Cdir1[i])-(float(boias[s][4])+(float(boias[s][5])*aw))))
        if float(Cdir1[i])>360:
            Cdir1[i]=float(Cdir1[i])-360
        elif float(Cdir1[i])<0:
            Cdir1[i]=float(Cdir1[i])+360
    var=Cdir2flag[i]
    if var!=4:
        Cdir2[i]=int(arredondar(float(Cdir2[i])-(float(boias[s][4])+(float(boias[s][5])*aw))))
        if float(Cdir2[i])>360:
            Cdir2[i]=float(Cdir2[i])-360
        elif float(Cdir2[i])<0:
            Cdir2[i]=float(Cdir2[i])+360
    var=Cdir3flag[i]
    if var!=4:
        Cdir3[i]=int(arredondar(float(Cdir3[i])-(float(boias[s][4])+(float(boias[s][5])*aw))))
        if float(Cdir3[i])>360:
            Cdir3[i]=float(Cdir3[i])-360
        elif float(Cdir3[i])<0:
            Cdir3[i]=float(Cdir3[i])+360
    var=Wdirflag[i]
    if var!=4:
        Wdir[i]=int(arredondar(float(Wdir[i])-(float(boias[s][4])+(float(boias[s][5])*aw))))
        if float(Wdir[i])>360:
            Wdir[i]=float(Wdir[i])-360
        elif float(Wdir[i])<0:
            Wdir[i]=float(Wdir[i])+360
    var=Mwdflag[i]
    if var!=4:
        Mwd[i]=int(arredondar(float(Mwd[i])-(float(boias[s][4])+(float(boias[s][5])*aw))))
        if float(Mwd[i])>360:
            Mwd[i]=float(Mwd[i])-360
        elif float(Mwd[i])<0:
            Mwd[i]=float(Mwd[i])+360
    Mwd[i]=float( '%.0f' % ( Mwd[i]))
    Cdir1[i]=float( '%.0f' % ( Cdir1[i]))
    Cdir2[i]=float( '%.0f' % ( Cdir2[i]))
    Cdir3[i]=float( '%.0f' % ( Cdir3[i]))
    bHead[i]=float( '%.0f' % ( bHead[i]))
    Wspd[i]=float( '%.1f' % ( Wspd[i]))
    Wdir[i]=float( '%.0f' % ( Wdir[i]))
    Gust[i]=float( '%.1f' % ( Gust[i]))
    Lat[i]=float( '%.5f' % ( Lat[i]))
    Lon[i]=float( '%.5f' % ( Lon[i]))

    if int(Wspdflagid[i])>0 and int(Wspdflagid[i])<=50:
        Wspd[i]=-9999
    if int(Wdirflagid[i])>0 and int(Wdirflagid[i])<=50:
        Wdir[i]=-9999
    if int(Gustflagid[i])>0 and int(Gustflagid[i])<=50:
        Gust[i]=-9999
    if int(Atmpflagid[i])>0 and int(Atmpflagid[i])<=50:
        Atmp[i]=-9999
    if int(Presflagid[i])>0 and int(Presflagid[i])<=50:
        Pres[i]=-9999
    if int(Dewpflagid[i])>0 and int(Dewpflagid[i])<=50:
        Dewp[i]=-9999
    if int(Humiflagid[i])>0 and int(Humiflagid[i])<=50:
        Humi[i]=-9999
    if int(Wtmpflagid[i])>0 and int(Wtmpflagid[i])<=50:
        Wtmp[i]=-9999
    if int(Cvel1flagid[i])>0 and int(Cvel1flagid[i])<=50:
        Cvel1[i]=-9999
    if int(Cdir1flagid[i])>0 and int(Cdir1flagid[i])<=50:
        Cdir1[i]=-9999
    if int(Cvel2flagid[i])>0 and int(Cvel2flagid[i])<=50:
        Cvel2[i]=-9999
    if int(Cdir2flagid[i])>0 and int(Cdir2flagid[i])<=50:
        Cdir2[i]=-9999
    if int(Cvel3flagid[i])>0 and int(Cvel3flagid[i])<=50:
        Cvel3[i]=-9999
    if int(Cdir3flagid[i])>0 and int(Cdir3flagid[i])<=50:
        Cdir3[i]=-9999
    if int(Wvhtflagid[i])>0 and int(Wvhtflagid[i])<=50:
        Wvht[i]=-9999
    if int(Wmaxflagid[i])>0 and int(Wmaxflagid[i])<=50:
        Wmax[i]=-9999
    if int(Dpdflagid[i])>0 and int(Dpdflagid[i])<=50:
        Dpd[i]=-9999
    if int(Mwdflagid[i])>0 and int(Mwdflagid[i])<=50:
        Mwd[i]=-9999

data1 = np.array([data,Lat,Lon,Battery,bHead,Wspd,Wdir,Gust,Atmp,Pres,Dewp,Humi,Wtmp,Cvel1,Cdir1,Cvel2,Cdir2,Cvel3,Cdir3,Wvht,Wmax,Dpd,Mwd,Spred])

data1 = data1.transpose()
header="Datetime,Lat,Lon,Battery,bHead,Wspd,Wdir,Gust,Atmp,Pres,Dewp,Humi,Wtmp,Cvel1,Cdir1,Cvel2,Cdir2,Cvel3,Cdir3,Wvht,Wmax,Dpd,Mwd,Spread"
np.savetxt("historico_"+str(boias[s][2])+".txt", data1,'%s',delimiter=",",header=header)

df = pd.DataFrame(list(zip(data,Lat,Lon,Battery,bHead,Wspd,Wspdflagid,Wdir,Wdirflagid,
                   Gust,Gustflagid,Atmp,Atmpflagid,Pres,Presflagid,Dewp,Dewpflagid,
                   Humi,Humiflagid,Wtmp,Wtmpflagid,Cvel1,Cvel1flagid,Cdir1,Cdir1flagid,
                   Cvel2,Cvel2flagid,Cdir2,Cdir2flagid,Cvel3,Cvel3flagid,Cdir3,
                   Cdir3flagid,Wvht,Wvhtflagid,Wmax,Wmaxflagid,Dpd,Dpdflagid,Mwd,
                   Mwdflagid,Spred)),
    columns =['data','Lat','Lon','Battery','bHead','Wspd','Wspdflagid','Wdir',
              'Wdirflagid','Gust','Gustflagid','Atmp','Atmpflagid','Pres','Presflagid',
              'Dewp','Dewpflagid','Humi','Humiflagid','Wtmp','Wtmpflagid','Cvel1',
              'Cvel1flagid','Cdir1','Cdir1flagid','Cvel2','Cvel2flagid','Cdir2',
              'Cdir2flagid','Cvel3','Cvel3flagid','Cdir3','Cdir3flagid','Wvht',
              'Wvhtflagid','Wmax','Wmaxflagid','Dpd','Dpdflagid','Mwd','Mwdflagid','Spred'])


dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')

df['estacao_id'] = [boias[s][0] for i in range(len(df))]
df = df.set_index('data')

df=df.replace(-9999,np.NaN)
df=df.replace(-99999,np.NaN)
df=df.replace(-99999.0,np.NaN)
df=df.replace(-9999.0,np.NaN)

df.Wspdflagid = df.Wspdflagid.astype(int)
df.Wdirflagid = df.Wdirflagid.astype(int)
df.Gustflagid = df.Gustflagid.astype(int)
df.Atmpflagid = df.Atmpflagid.astype(int)
df.Presflagid = df.Presflagid.astype(int)
df.Dewpflagid = df.Dewpflagid.astype(int)
df.Humiflagid = df.Humiflagid.astype(int)
df.Wtmpflagid = df.Wtmpflagid.astype(int)
df.Cvel1flagid = df.Cvel1flagid.astype(int)
df.Cdir1flagid = df.Cdir1flagid.astype(int)
df.Cvel2flagid = df.Cvel2flagid.astype(int)
df.Cdir2flagid = df.Cdir2flagid.astype(int)
df.Cvel3flagid = df.Cvel3flagid.astype(int)
df.Cdir3flagid = df.Cdir3flagid.astype(int)
df.Wvhtflagid = df.Wvhtflagid.astype(int)
df.Wmaxflagid = df.Wmaxflagid.astype(int)
df.Dpdflagid = df.Dpdflagid.astype(int)
df.Mwdflagid = df.Mwdflagid.astype(int)

stop

inserir_dados_banco(df)

print('done reading in data')
