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

conn = bd.conn_qc_db('PRI')

buoys = bd.working_buoys(conn)

print(buoys)

for i in range(len(buoys)):

    df = bd.get_bufr_data(buoys["id_boia"][i], 1, conn)

    Section1 = bufr.bufrSection1(df)

    Section3 = bufr.bufrSection3(df)

    Section4 = bufr.bufrSection4(df)

    Section5 = bufr.bufrSection5()

    bufrMessage = []

    bufrMessage.append(Section1)
    bufrMessage.append(Section3)
    bufrMessage.append(Section4)
    bufrMessage.append(Section5)

    bufrString="".join(bufrMessage)

    bufrMessageLength = len(bufrString) / 8

    # BUFR Section 0
    Section0 = bufr.bufrSection0(bufrMessageLength)

    bufrString = bufrString + ''.join(Section0)

<<<<<<< refs/remotes/origin/bufr-new
    print(bufrString)
=======
        print(bufr_message)
        name_buoy = df["name_buoy"][0] + "_" + datetime.now().strftime("%Y%m%d%H0000")
>>>>>>> local

    name_buoy = df["name_buoy"][0] + "_" + datetime.now().strftime("%Y%m%d%H0000")

<<<<<<< refs/remotes/origin/bufr-new
    bufr_message = open(name_buoy, 'w')

    bufr_message.write(bufrString)
    bufr_message.close()
=======
        bufr_message_new = encoder.process(bufr_message)
        with open(name_buoy + ".bufr", 'wb') as outs:
            outs.write(bufr_message_new.serialized_bytes)

        print('BUFR file created for buoy ' + buoys['name_buoy'][i] + '!')

        add_preamble = "pybufrkit decode %s.bufr | pybufrkit encode - %s_1.bufr --preamble $'IOBI02 SBBR %s\r\r\n'" % (name_buoy, name_buoy, datetime.now().strftime("%d%H00"))
        download_status = os.system(add_preamble)
        print('BUFR preamble added!')
    except:
        print('Can not create BUFR file for buoy ' + buoys['name_buoy'][i] + '!')
>>>>>>> local
