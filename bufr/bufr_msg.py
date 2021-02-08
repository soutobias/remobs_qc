# -*- coding: utf-8 -*-
"""
Created on Wed Jun 11 09:51:37 2014

@author: soutobias
"""

import numpy as np
import math
from datetime import datetime, timedelta

import os
import sys

home_path = os.environ['HOME']
sys.path.append(home_path)

import bd_function as bd
import pandas as pd

import remo_bufr as bufr

from pybufrkit.encoder import Encoder
import json

conn = bd.conn_qc_db('PRI')

buoys = bd.working_buoys(conn)

for i in range(len(buoys)):

    print(buoys['name_buoy'][i])

    try:
        df = bd.get_bufr_data(buoys["id"][i], 1, conn)

        section0 = bufr.bufr_section0()

        section1 = bufr.bufr_section1()

        section3 = bufr.bufr_section3()

        section4 = bufr.bufr_section4(df)

        section5 = bufr.bufr_section5()

        bufr_message = []

        bufr_message.append(section0)
        bufr_message.append(section1)
        bufr_message.append(section3)
        bufr_message.append(section4)
        bufr_message.append(section5)

        print(bufr_message)
        name_buoy = df["name_buoy"][0] + "_" + datetime.now().strftime("%Y%m%d%H0000") + '.bufr'

        encoder = Encoder()
        bufr_message_new = encoder.process(bufr_message)
        with open(name_buoy + ".bufr", 'wb') as outs:
            outs.write(bufr_message_new.serialized_bytes)

        print('BUFR file created for buoy ' + buoys['name_buoy'][i] + '!')

        add_preamble = "pybufrkit decode %s.bufr | pybufrkit encode - %s_1.bufr --preamble $'IOBI02 SBBR %s\r\r\n'" % (name_buoy, name_buoy, datetime.now().strftime("%d%H00"))
        download_status = os.system(add_preamble)
        print('BUFR preamble added!')
    except:
        print('Can not create BUFR file for buoy ' + buoys['name_buoy'][i] + '!')
