import time
import datetime

def last_month():

    [anoq, mesq, diaq, horaq, minq, secq, wdayq, ydayq, isdstq] = time.gmtime(time.time())

    if mesq == 1:
        anoq = anoq - 1
        mesq = 12
    else:
        mesq = mesq - 1

    return str(anoq) + "-" + str(mesq).zfill(2) + "-25 00:00:00"

def gmtime():

    [anoq, mesq, diaq, horaq, minq, secq, wdayq, ydayq, isdstq]=time.gmtime(time.time() + 3600 * 3)

    return str(anoq) + "-" + str(mesq).zfill(2) + "-" + str(diaq).zfill(2) + " " + str(horaq).zfill(2) + ":00:00"
