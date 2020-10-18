from telnetlib import Telnet
import datetime
import re
import numpy as np

def download_raw_data(argos_id, user_config):
    program = "05655"
    out = []
    command = "PRV,5655,DS,,"
    usern = user_config.pnboia_user
    passw = user_config.pnboia_psw

    try:
        tn = Telnet(user_config.pnboia_telnet)
    except :
        pass

    #---Login

    try:
        tn.read_until(b"Username: ")
        tn.write(usern.encode('ascii') + b"\n")

        tn.read_until(b"Password: ")
        tn.write(passw.encode('ascii') + b"\n")

        print ("Successful connect to the server...")# ,tn.read_until('SERVER', 5)
        print ("Collecting data from argos_id " + str(argos_id) + "...")

    except:
        pass

    #---command para coleta de telnet_data
    #print command
    print(str(command) + str(argos_id) + "\n\r")
    tn.write(passw.encode('ascii') + b"\n")
    command = command + str(argos_id)
    tn.write(command.encode('ascii') + b"\n")

    telnet_data = tn.read_until(b'SERVER', 5)
    print ("Receive data: \n\r")
    tn.close()
    telnet_data = telnet_data.decode("utf-8")
    if telnet_data[0:7] != "No data":
        telnet_data = telnet_data.replace("\r\n", ";")
        telnet_data = re.sub("\s+", ",", telnet_data.strip())
        telnet_data = telnet_data.replace(";,", ";")
        telnet_data = telnet_data.replace(";;", ";")
        telnet_data = telnet_data.replace("/" + program, program)
        telnet_data = telnet_data.replace(":,", ":")
        telnet_data = telnet_data.replace(";ARGOS,READY;/ARGOS,READY;/","")
        telnet_data = telnet_data.replace(",?,", ",")
        telnet_data = telnet_data.replace(",?", "")
        telnet_data = telnet_data + ";;"
        print (str(telnet_data.count(";")) + " received lines...")
    else:
        print ("No data available or access denied")
    counter = 0
    checkpoint = 0
    result = []
    raw_data = []

    while counter != telnet_data.count(";"):
        line = telnet_data[checkpoint:telnet_data.index(";", checkpoint)]
        checkpoint = telnet_data.index(";", checkpoint) + 1
        counter = counter + 1
        if program in line[0:6]:
            out = []
            out1 = []
            out = line.split(",")
            line = telnet_data[checkpoint:telnet_data.index(";",checkpoint)]
            checkpoint = telnet_data.index(";",checkpoint) + 1
            counter = counter + 1
            if ":" in line:
                out1 = line.split(",")
                line = telnet_data[checkpoint:telnet_data.index(";", checkpoint)]
                checkpoint = telnet_data.index(";", checkpoint) + 1
                counter = counter + 1
                while program not in line[0:6] and counter < telnet_data.count(";") and ":" not in line:
                    out1.extend(line.split(","))
                    line = telnet_data[checkpoint:telnet_data.index(";", checkpoint)]
                    checkpoint = telnet_data.index(";", checkpoint) + 1
                    counter = counter + 1
                result = out
                result.extend(out1)
                if len(result) >= 42:
                    if int(float(result[15])) == 1:
                        raw_data.append([int(result[1]), str(result[8]), str(result[9]), str(result[12]), str(result[13]), \
                            str(result[15]), str(result[16]), str(result[17]), str(result[18]), str(result[19]), \
                            str(result[20]), str(result[21]), str(result[22]), str(result[23]), str(result[24]), \
                            str(result[25]), str(result[26]), str(result[27]), str(result[28]), str(result[29]), \
                            str(result[30]), str(result[31]), str(result[32]), str(result[33]), str(result[34]), \
                            str(result[35]), str(result[36]), str(result[37]), str(result[38]), str(result[39]), \
                            str(result[40])])
                    if int(float(result[15])) == 2:
                        raw_data.append([int(result[1]), str(result[8]), str(result[9]), str(result[12]), str(result[13]), \
                            str(result[15]), str(result[16]), str(result[17]), str(result[18]), str(result[19]), \
                            str(result[20]), str(result[21]), str(result[22]), str(result[23]), str(result[24]), \
                            str(result[25]), str(result[26]), str(result[27]), str(result[28]), str(result[29]), \
                            str(result[30]), str(result[31]), str(result[32]), str(result[33]), str(result[34]), \
                            str(result[35]), str(result[36]), str(result[37]), str(result[38]), str(result[39]), \
                            str(result[40])])
                if len(result) == 40 or len(result) == 41:
                    if int(float(result[8])) == 1:
                        raw_data.append([int(result[1]), np.nan, np.nan, str(result[5]), str(result[6]), str(result[8]), \
                            str(result[9]), str(result[10]), str(result[11]), str(result[12]), str(result[13]), \
                            str(result[14]), str(result[15]), str(result[16]), str(result[17]), str(result[18]), \
                            str(result[19]), str(result[20]), str(result[21]), str(result[22]), str(result[23]), \
                            str(result[24]), str(result[25]), str(result[26]), str(result[27]), str(result[28]), \
                            str(result[29]), str(result[30]), str(result[31]), str(result[32]), str(result[33]), \
                            str(result[34])])
                    if int(float(result[8])) == 2:
                        raw_data.append([int(result[1]), np.nan, np.nan, str(result[5]), str(result[6]), str(result[8]), \
                            str(result[9]), str(result[10]), str(result[11]), str(result[12]), str(result[13]), \
                            str(result[14]), str(result[15]), str(result[16]), str(result[17]), str(result[18]), \
                            str(result[19]), str(result[20]), str(result[21]), str(result[22]), str(result[23]), \
                            str(result[24]), str(result[25]), str(result[26]), str(result[27]), str(result[28]), \
                            str(result[29]), str(result[30]), str(result[31]), str(result[32]), str(result[33]), \
                            str(result[34])])
                if len(result) == 35 or len(result) == 34:
                    if int(float(result[8])) == 1:
                        raw_data.append([int(result[1]), np.nan, np.nan, str(result[5]), str(result[6]), str(result[8]), \
                            str(result[9]), str(result[10]), str(result[11]), str(result[12]), str(result[13]), \
                            str(result[14]), str(result[15]), str(result[16]), str(result[17]), str(result[18]), \
                            str(result[19]), str(result[20]), str(result[21]), str(result[22]), str(result[23]), \
                            str(result[24]), str(result[25]), str(result[26]), str(result[27]), str(result[28]), \
                            str(result[29]), str(result[30]), str(result[31]), str(result[32]), str(result[33]), 0])
                    if int(float(result[8])) == 2:
                        raw_data.append([int(result[1]), np.nan, np.nan, str(result[5]), str(result[6]), str(result[8]), \
                            str(result[9]), str(result[10]), str(result[11]), str(result[12]), str(result[13]), \
                            str(result[14]), str(result[15]), str(result[16]), str(result[17]), str(result[18]), \
                            str(result[19]), str(result[20]), str(result[21]), str(result[22]), str(result[23]), \
                            str(result[24]), str(result[25]), str(result[26]), str(result[27]), str(result[28]), \
                            str(result[29]), str(result[30]), str(result[31]), str(result[32]), str(result[33]), 0])

    for i in range(len(raw_data)):
        raw_data[i][3] = datetime.datetime.strptime(raw_data[i][3] + " " + raw_data[i][4], '%Y-%m-%d %H:%M:%S')

    return raw_data
