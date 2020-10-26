#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 08:53:47 2020

@author: tobias
"""

def bancodedados():

    local="pnboia-uol.mysql.uhserver.com"
    usr="pnboia"
    password="Ch@tasenha1"
    data_base="pnboia_uol"

    return local,usr,password,data_base


def consulta_estacao(boia):

    (local,usr,password,data_base)=bancodedados()
    
    db = MySQLdb.connect(local,usr,password,data_base)
    
    cur=db.cursor()
    
    argosbruto=[]
    cur.execute("SELECT * FROm pnboia_estacao wheRe nome='%s'" %boia)
    for row in cur.fetchall():
        argosbruto.append(row[:])
    
    cur.close()
    db.close()

    return argosbruto[0]



boias=['riogrande','itajai','santos','cabofrio2','vitoria','portoseguro','fortaleza','niteroi']

# anos=["2009","2010","2011","2012","2013","2014","2015","2016","2017","2018","2019","2020"]
# meses=["01","02","03","04","05","06","07","08","09","10","11","12"]

pastas=["STATUS","WAVE","HNE","UVH","NONDIRSPEC","MEANDIR_MEM4","MEANDIR_KVH","FOURIER","DIRSPEC"


for boia in boias:
    dadosboia=consulta_estacao(boia)

    NC = Dataset(boia + '.nc','w')

    wmo = dadosboias[1]
    station=dadosboias[1]
    depth=dadosboias[1]
    Lat=dadosboias[1]
    Lon=dadosboias[1]

    NC.wmo_id = wmo

    NC.institution = 'Programa Nacional de Boias'
    NC.institution_abbreviation = 'PNBOIA'

# Title
    NC.title = "Wave Data Collected by " \
        "the Programa Nacional de Boias Weather Buoys"
        
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
    
    
    NC.station_name=station
    NC.sea_floor_depth_below_sea_level=depth
    
    NC.keywords = "Atmospheric Pressure, Sea level Pressure, Atmospheric " \
        "Temperature, Surface Temperature, Dewpoint Temperature, Humidity, " \
        "Surface Winds, Ocean Winds, Ocean Temperature, Sea Surface Temperature," \
        "Ocean Waves,  Wave Height, Wave Period, Ocean Currents."
    
    NC.keywords_vocabulary="GCMD Science Keywords"
    # NC.standard_name_vocabulary="CF-16.0"
    
    NC.Metadata_Conventions= "Unidata Dataset Discovery v1.0"
    
    NC.citation="The Programa Nacional de Boias should be cited as the source " \
        "of these data if used in any publication."
    
    NC.publisher_name="PNBOIA"
    NC.publisher_url="https://www.marinha.mil.br/chm/dados-do-goos-brasil/pnboia"
    NC.publisher_email="chm.pnboia@marinha.mil.br"
    NC.nominal_latitude= Lat
    NC.nominal_longitude=Lon
    
    t_now=strftime("%Y-%m-%d %H:%M:%SZ", gmtime())
    
    # NC.time_coverage_start=t[0]
    # NC.time_coverage_end= t[-1]
    NC.date_created= t_now



    anos=next(os.walk('.'))[1]
    for ano in anos:
        cd ano
        meses=next(os.walk('.'))[1]
        for mes in meses:
            cd mes
            cd "STATUS"
            glob_pattern = ano+mes+"*0000.*"
            filelist=glob.glob(glob_pattern)
            sorted(filelist)
            dia=[]
            hora=[]
            diashoras=[]
            for file in filelist:
                hd_new = re.search(ano+mes+'(\d{2})(\d{2})0000.STATUS', file)
                dia.append(hd_new[1])
                hora.append(hd_new[2])
                diashoras.append(hd_new[1]+hd_new[2])
            cd ..
            for diahora in diashoras
                for pasta in pastas:
                    cd pasta
                
                
                
            cd "STATUS"
            
                
            
        


