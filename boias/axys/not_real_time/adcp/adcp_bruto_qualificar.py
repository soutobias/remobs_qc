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
from pathlib import Path

import sys
import os
from os.path import expanduser
home = expanduser("~")
sys.path.insert(0,home)
import user_config
os.chdir( user_config.path )


def consulta_estacao(boia):


    db = MySQLdb.connect(host = user_config.host,
                         user = user_config.username,
                         password = user_config.password,
                         database = user_config.database)

    cur=db.cursor()

    argosbruto=[]
    cur.execute("SELECT * FROm pnboia_estacao wheRe nome='%s'" %boia)
    for row in cur.fetchall():
        argosbruto.append(row[:])

    cur.close()
    db.close()

    return argosbruto[0]

def consulta_banco(boia):


    db = MySQLdb.connect(host = user_config.host,
                         user = user_config.username,
                         password = user_config.password,
                         database = user_config.database)

    cur=db.cursor()

    argosbruto=[]
    cur.execute("SELECT * FROm pnboia_adcp_bruto \
                wheRe estacao_id=%s\
                ORdeR by data" %boia)
    for row in cur.fetchall():
        argosbruto.append(row[:])

    cur.close()
    db.close()

    return argosbruto

def inserir_dados_banco(df):

    con = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                            format(user_config.username,
                                                user_config.password,
                                                user_config.host,
                                                user_config.database))

    df.to_sql(con=con, name='pnboia_adcp_qualificado', if_exists='append')


#########################################################################
#
# ETAPA 0 - PREPARACAO DO CODIGO
#
#Preparacao do codigo: define as variaveis principais do codigo e e onde a rotina
#deve ser modifcada para alteracao de parametros (p.ex. tempo real ou nao)
#
##############################################################################

#Count for determine how much time it takes for the codes to run
t = time.time()

boias=['riogrande','itajai','santos','cabofrio2','vitoria','portoseguro','fortaleza','niteroi']


variables10=['data','Cvel1','Cdir1','Cvel2','Cdir2','Cvel3','Cdir3','Cvel4','Cdir4','Cvel5','Cdir5','Cvel6','Cdir6','Cvel7','Cdir7','Cvel8','Cdir8','Cvel9','Cdir9','Cvel10','Cdir10','Cvel11','Cdir11','Cvel12','Cdir12','Cvel13','Cdir13','Cvel14','Cdir14','Cvel15','Cdir15','Cvel16','Cdir16','Cvel17','Cdir17','Cvel18','Cdir18','Cvel19','Cdir19','Cvel20','Cdir20']

#lista de variáveis para o arquivo final a ser gerado
variables1=['Cvel1','Cdir1','Cvel2','Cdir2','Cvel3','Cdir3','Cvel4','Cdir4','Cvel5','Cdir5','Cvel6','Cdir6','Cvel7','Cdir7','Cvel8','Cdir8','Cvel9','Cdir9','Cvel10','Cdir10','Cvel11','Cdir11','Cvel12','Cdir12','Cvel13','Cdir13','Cvel14','Cdir14','Cvel15','Cdir15','Cvel16','Cdir16','Cvel17','Cdir17','Cvel18','Cdir18','Cvel19','Cdir19','Cvel20','Cdir20']


for s in range(len(boias)):
    print(boias[s])

    estacaoid=consulta_estacao(boias[s])

    argosbruto=consulta_banco(estacaoid[0])

    for i in range(len(variables10)):
        exec("%s = []" % (variables10[i]))

    for ii in range(len(argosbruto)):
        for i in range(len(variables10)):
            var=argosbruto[ii][i]
            if var==9999 or var==99.99 or var==None or var=='-9999'  or var=='-99999'  or var==-99999 or var=='' or var=='99.99' or var=='-9999.0' or var==-9999.0  or var==-9999:
                exec("%s.append(-9999)"% (variables10[i]))
            else:
                exec("%s.append(argosbruto[ii][i])" %variables10[i])
                continue


    Epoca = [0]*len(argosbruto)

    for i in range(len(argosbruto)):
        Epoca[i]=(datetime.datetime(int(data[i].year),int(data[i].month),int(data[i].day),int(data[i].hour),0) - datetime.datetime(1970,1,1)).total_seconds()

    (flag,flagid,u1,v1)=qualitycontrol_adcp(Epoca,Cvel1,Cdir1,Cvel2,Cdir2,Cvel3,Cdir3,Cvel4,Cdir4,Cvel5,Cdir5,Cvel6,Cdir6,Cvel7,Cdir7,Cvel8,Cdir8,Cvel9,Cdir9,Cvel10,Cdir10,Cvel11,Cdir11,Cvel12,Cdir12,Cvel13,Cdir13,Cvel14,Cdir14,Cvel15,Cdir15,Cvel16,Cdir16,Cvel17,Cdir17,Cvel18,Cdir18,Cvel19,Cdir19,Cvel20,Cdir20)

    #renomeando as variáveis para salvar o arquivo final

    for ii in range(len(variables1)):
        exec("%sflag=flag[ii]"% (variables1[ii]))
        exec("%sflagid=flagid[ii]"% (variables1[ii]))

    for i in range(len(Cdir1)):
        aw=int(data[i].year)-2002
        var=Cdir1flag[i]
        if var!=4:
            Cdir1[i]=int(arredondar(float(Cdir1[i])-(float(estacaoid[4])+(float(estacaoid[5])*aw))))
            if float(Cdir1[i])>360:
                Cdir1[i]=float(Cdir1[i])-360
            elif float(Cdir1[i])<0:
                Cdir1[i]=float(Cdir1[i])+360
        var=Cdir2flag[i]
        if var!=4:
            Cdir2[i]=int(arredondar(float(Cdir2[i])-(float(estacaoid[4])+(float(estacaoid[5])*aw))))
            if float(Cdir2[i])>360:
                Cdir2[i]=float(Cdir2[i])-360
            elif float(Cdir2[i])<0:
                Cdir2[i]=float(Cdir2[i])+360
        var=Cdir3flag[i]
        if var!=4:
            Cdir3[i]=int(arredondar(float(Cdir3[i])-(float(estacaoid[4])+(float(estacaoid[5])*aw))))
            if float(Cdir3[i])>360:
                Cdir3[i]=float(Cdir3[i])-360
            elif float(Cdir3[i])<0:
                Cdir3[i]=float(Cdir3[i])+360

        var=Cdir4flag[i]
        if var!=4:
            Cdir4[i]=int(arredondar(float(Cdir4[i])-(float(estacaoid[4])+(float(estacaoid[5])*aw))))
            if float(Cdir4[i])>360:
                Cdir4[i]=float(Cdir4[i])-360
            elif float(Cdir4[i])<0:
                Cdir4[i]=float(Cdir4[i])+360

        var=Cdir5flag[i]
        if var!=4:
            Cdir5[i]=int(arredondar(float(Cdir5[i])-(float(estacaoid[4])+(float(estacaoid[5])*aw))))
            if float(Cdir5[i])>360:
                Cdir5[i]=float(Cdir5[i])-360
            elif float(Cdir5[i])<0:
                Cdir5[i]=float(Cdir5[i])+360

        var=Cdir6flag[i]
        if var!=4:
            Cdir6[i]=int(arredondar(float(Cdir6[i])-(float(estacaoid[4])+(float(estacaoid[5])*aw))))
            if float(Cdir6[i])>360:
                Cdir6[i]=float(Cdir6[i])-360
            elif float(Cdir6[i])<0:
                Cdir6[i]=float(Cdir6[i])+360

        var=Cdir7flag[i]
        if var!=4:
            Cdir7[i]=int(arredondar(float(Cdir7[i])-(float(estacaoid[4])+(float(estacaoid[5])*aw))))
            if float(Cdir7[i])>360:
                Cdir7[i]=float(Cdir7[i])-360
            elif float(Cdir7[i])<0:
                Cdir7[i]=float(Cdir7[i])+360

        var=Cdir8flag[i]
        if var!=4:
            Cdir8[i]=int(arredondar(float(Cdir8[i])-(float(estacaoid[4])+(float(estacaoid[5])*aw))))
            if float(Cdir8[i])>360:
                Cdir8[i]=float(Cdir8[i])-360
            elif float(Cdir8[i])<0:
                Cdir8[i]=float(Cdir8[i])+360

        var=Cdir9flag[i]
        if var!=4:
            Cdir9[i]=int(arredondar(float(Cdir9[i])-(float(estacaoid[4])+(float(estacaoid[5])*aw))))
            if float(Cdir9[i])>360:
                Cdir9[i]=float(Cdir9[i])-360
            elif float(Cdir9[i])<0:
                Cdir9[i]=float(Cdir9[i])+360

        var=Cdir10flag[i]
        if var!=4:
            Cdir10[i]=int(arredondar(float(Cdir10[i])-(float(estacaoid[4])+(float(estacaoid[5])*aw))))
            if float(Cdir10[i])>360:
                Cdir10[i]=float(Cdir10[i])-360
            elif float(Cdir10[i])<0:
                Cdir10[i]=float(Cdir10[i])+360

        var=Cdir11flag[i]
        if var!=4:
            Cdir11[i]=int(arredondar(float(Cdir11[i])-(float(estacaoid[4])+(float(estacaoid[5])*aw))))
            if float(Cdir11[i])>360:
                Cdir11[i]=float(Cdir11[i])-360
            elif float(Cdir11[i])<0:
                Cdir11[i]=float(Cdir11[i])+360

        var=Cdir12flag[i]
        if var!=4:
            Cdir12[i]=int(arredondar(float(Cdir12[i])-(float(estacaoid[4])+(float(estacaoid[5])*aw))))
            if float(Cdir12[i])>360:
                Cdir12[i]=float(Cdir12[i])-360
            elif float(Cdir12[i])<0:
                Cdir12[i]=float(Cdir12[i])+360

        var=Cdir13flag[i]
        if var!=4:
            Cdir13[i]=int(arredondar(float(Cdir13[i])-(float(estacaoid[4])+(float(estacaoid[5])*aw))))
            if float(Cdir13[i])>360:
                Cdir13[i]=float(Cdir13[i])-360
            elif float(Cdir13[i])<0:
                Cdir13[i]=float(Cdir13[i])+360

        var=Cdir14flag[i]
        if var!=4:
            Cdir14[i]=int(arredondar(float(Cdir14[i])-(float(estacaoid[4])+(float(estacaoid[5])*aw))))
            if float(Cdir14[i])>360:
                Cdir14[i]=float(Cdir14[i])-360
            elif float(Cdir14[i])<0:
                Cdir14[i]=float(Cdir14[i])+360

        var=Cdir15flag[i]
        if var!=4:
            Cdir15[i]=int(arredondar(float(Cdir15[i])-(float(estacaoid[4])+(float(estacaoid[5])*aw))))
            if float(Cdir15[i])>360:
                Cdir15[i]=float(Cdir15[i])-360
            elif float(Cdir15[i])<0:
                Cdir15[i]=float(Cdir15[i])+360

        var=Cdir16flag[i]
        if var!=4:
            Cdir16[i]=int(arredondar(float(Cdir16[i])-(float(estacaoid[4])+(float(estacaoid[5])*aw))))
            if float(Cdir16[i])>360:
                Cdir16[i]=float(Cdir16[i])-360
            elif float(Cdir16[i])<0:
                Cdir16[i]=float(Cdir16[i])+360

        var=Cdir17flag[i]
        if var!=4:
            Cdir17[i]=int(arredondar(float(Cdir17[i])-(float(estacaoid[4])+(float(estacaoid[5])*aw))))
            if float(Cdir17[i])>360:
                Cdir17[i]=float(Cdir17[i])-360
            elif float(Cdir17[i])<0:
                Cdir17[i]=float(Cdir17[i])+360

        var=Cdir18flag[i]
        if var!=4:
            Cdir18[i]=int(arredondar(float(Cdir18[i])-(float(estacaoid[4])+(float(estacaoid[5])*aw))))
            if float(Cdir18[i])>360:
                Cdir18[i]=float(Cdir18[i])-360
            elif float(Cdir18[i])<0:
                Cdir18[i]=float(Cdir18[i])+360

        var=Cdir19flag[i]
        if var!=4:
            Cdir19[i]=int(arredondar(float(Cdir19[i])-(float(estacaoid[4])+(float(estacaoid[5])*aw))))
            if float(Cdir19[i])>360:
                Cdir19[i]=float(Cdir19[i])-360
            elif float(Cdir19[i])<0:
                Cdir19[i]=float(Cdir19[i])+360

        var=Cdir20flag[i]
        if var!=4:
            Cdir20[i]=int(arredondar(float(Cdir20[i])-(float(estacaoid[4])+(float(estacaoid[5])*aw))))
            if float(Cdir20[i])>360:
                Cdir20[i]=float(Cdir20[i])-360
            elif float(Cdir20[i])<0:
                Cdir20[i]=float(Cdir20[i])+360


    import pandas as pd
    df = pd.DataFrame(list(zip(data,Cvel1,Cvel1flagid,Cdir1,Cdir1flagid,Cvel2,
                               Cvel2flagid,Cdir2,Cdir2flagid,Cvel3,Cvel3flagid,Cdir3,
                               Cdir3flagid,Cvel4,Cvel4flagid,Cdir4,Cdir4flagid,Cvel5,
                               Cvel5flagid,Cdir5,Cdir5flagid,Cvel6,Cvel6flagid,Cdir6,
                               Cdir6flagid,Cvel7,Cvel7flagid,Cdir7,Cdir7flagid,Cvel8,
                               Cvel8flagid,Cdir8,Cdir8flagid,Cvel9,Cvel9flagid,Cdir9,
                               Cdir9flagid,Cvel10,Cvel10flagid,Cdir10,Cdir10flagid,Cvel11,
                               Cvel11flagid,Cdir11,Cdir11flagid,Cvel12,Cvel12flagid,Cdir12,
                               Cdir12flagid,Cvel13,Cvel13flagid,Cdir13,Cdir13flagid,Cvel14,
                               Cvel14flagid,Cdir14,Cdir14flagid,Cvel15,Cvel15flagid,Cdir15,
                               Cdir15flagid,Cvel16,Cvel16flagid,Cdir16,Cdir16flagid,Cvel17,
                               Cvel17flagid,Cdir17,Cdir17flagid,Cvel18,Cvel18flagid,Cdir18,
                               Cdir18flagid,Cvel19,Cvel19flagid,Cdir19,Cdir19flagid,Cvel20,
                               Cvel20flagid,Cdir20,Cdir20flagid)),
        columns =['data','Cvel1','Cvel1flagid','Cdir1','Cdir1flagid','Cvel2',
                               'Cvel2flagid','Cdir2','Cdir2flagid','Cvel3','Cvel3flagid','Cdir3',
                               'Cdir3flagid','Cvel4','Cvel4flagid','Cdir4','Cdir4flagid','Cvel5',
                               'Cvel5flagid','Cdir5','Cdir5flagid','Cvel6','Cvel6flagid','Cdir6',
                               'Cdir6flagid','Cvel7','Cvel7flagid','Cdir7','Cdir7flagid','Cvel8',
                               'Cvel8flagid','Cdir8','Cdir8flagid','Cvel9','Cvel9flagid','Cdir9',
                               'Cdir9flagid','Cvel10','Cvel10flagid','Cdir10','Cdir10flagid','Cvel11',
                               'Cvel11flagid','Cdir11','Cdir11flagid','Cvel12','Cvel12flagid','Cdir12',
                               'Cdir12flagid','Cvel13','Cvel13flagid','Cdir13','Cdir13flagid','Cvel14',
                               'Cvel14flagid','Cdir14','Cdir14flagid','Cvel15','Cvel15flagid','Cdir15',
                               'Cdir15flagid','Cvel16','Cvel16flagid','Cdir16','Cdir16flagid','Cvel17',
                               'Cvel17flagid','Cdir17','Cdir17flagid','Cvel18','Cvel18flagid','Cdir18',
                               'Cdir18flagid','Cvel19','Cvel19flagid','Cdir19','Cdir19flagid','Cvel20',
                               'Cvel20flagid','Cdir20','Cdir20flagid'])

    dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')

    df = df.set_index('data')

    df.to_csv("adcptratados_"+str(boias[s])+".csv")

    df['estacao_id'] = [estacaoid[0] for i in range(len(df))]

    df=df.replace(-9999,np.NaN)
    df=df.replace(-99999,np.NaN)
    df=df.replace(-99999.0,np.NaN)
    df=df.replace(-9999.0,np.NaN)

    inserir_dados_banco(df)

    print('done reading in data')


    elapsed = time.time() - t
    print(elapsed)
    print('done reading in data')
