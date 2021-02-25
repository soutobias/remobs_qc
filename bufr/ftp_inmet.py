import os
import sys
import glob

home = os.environ['HOME']
sys.path.append(home)

import ftplib
from user_config import IP_FTP_INMET, user_ftp_inmet, psw_ftp_inmet


directory = '/boias'

ftp = ftplib.FTP(IP_FTP_INMET)
ftp.login(user=user_ftp_inmet, passwd=psw_ftp_inmet)
ftp.cwd(directory)

bufr_files = glob.glob('*.bufr')


for bufr_file in bufr_files:
    print(f"Sending {bufr_file} to INMET")
    ftp_file = open(bufr_file, 'rb')
    send_file = ftp.storbinary('STOR %s' % bufr_file, ftp_file, 1024)
    ftp_file.close()
    if send_file == "226 Transfer complete.":
        print("f{bufr_file} sent to INMET FTP.")
        os.remove(bufr_file)
        print(f"{bufr_file} removed!")
