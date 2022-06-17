import os
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import smtplib
import pandas as pd


home = os.environ['HOME']
sys.path.append(home)

from user_config import EMAIL_FROM, EMAIL_TO, EMAIL_BUOYS_SUBJECT, EMAIL_BUOYS_CONTENT, EMAIL_BUOYS_FILES, remo_mail, remo_password


# Getting some details of files:

###################################################
#### SPOTTER TRINDADE

#file_spotter_trindade = pd.read_csv(EMAIL_BUOYS_FILES[1])

#start_date_spotter_trindade = min(file_spotter_trindade['Datetime'])
#last_date_spotter_trindade = max(file_spotter_trindade['Datetime'])



###################################################
#### SPOTTER ABROLHOS

file_spotter_abrolhos = pd.read_csv(EMAIL_BUOYS_FILES[0])

start_date_spotter_abrolhos = min(file_spotter_abrolhos['Datetime'])
last_date_spotter_abrolhos = max(file_spotter_abrolhos['Datetime'])

###################################################
#### SPOTTER ALCATRAZES

file_spotter_alcatrazes = pd.read_csv(EMAIL_BUOYS_FILES[1])

start_date_spotter_alcatrazes = min(file_spotter_alcatrazes['Datetime'])
last_date_spotter_alcatrazes = max(file_spotter_alcatrazes['Datetime'])


###################################################
#### SPOTTER NORONHA

file_spotter_noronha = pd.read_csv(EMAIL_BUOYS_FILES[2])

start_date_spotter_noronha = min(file_spotter_noronha['Datetime'])
last_date_spotter_noronha = max(file_spotter_noronha['Datetime'])



###################################################
#### SPOTTER PINGUIM - OPERANTAR 

# file_spotter_pinguim = pd.read_csv(EMAIL_BUOYS_FILES[1])

# start_date_spotter_pinguim = min(file_spotter_pinguim['Datetime'])
# last_date_spotter_pinguim = max(file_spotter_pinguim['Datetime'])


###################################################
#### SPOTTER POTTER - OPERANTAR 

# file_spotter_potter = pd.read_csv(EMAIL_BUOYS_FILES[2])

# start_date_spotter_potter = min(file_spotter_potter['Datetime'])
# last_date_spotter_potter = max(file_spotter_potter['Datetime'])


###################################################
#### SPOTTER KELLER - OPERANTAR 

# file_spotter_keller = pd.read_csv(EMAIL_BUOYS_FILES[2])

# start_date_spotter_keller = min(file_spotter_keller['Datetime'])
# last_date_spotter_keller = max(file_spotter_keller['Datetime'])


###################################################
#### SPOTTER MEXILHAO

# file_spotter_mexilhao = pd.read_csv(EMAIL_BUOYS_FILES[1])

# start_date_spotter_mexilhao = min(file_spotter_mexilhao['Datetime'])
# last_date_spotter_mexilhao = max(file_spotter_mexilhao['Datetime'])



###################################################
#### BMO BR

# file_bmo = pd.read_csv('dados_bmo_bc1.txt')

# start_date_bmo = min(file_bmo['Datetime'])
# last_date_bmo = max(file_bmo['Datetime'])


CONTENT = EMAIL_BUOYS_CONTENT.format(#start_date_bmo = start_date_bmo,
                                     #end_date_bmo = last_date_bmo,
                                    #  start_date_spotter_trindade=start_date_spotter_trindade,
                                    #  end_date_spotter_trindade=last_date_spotter_trindade,
                                      start_date_spotter_abrolhos=start_date_spotter_abrolhos,
                                      end_date_spotter_abrolhos=last_date_spotter_abrolhos,
                                      start_date_spotter_alcatrazes=start_date_spotter_alcatrazes,
                                      end_date_spotter_alcatrazes=last_date_spotter_alcatrazes,
                                      start_date_spotter_noronha=start_date_spotter_noronha,
                                      end_date_spotter_noronha=last_date_spotter_noronha)
                                    #  start_date_spotter_pinguim=start_date_spotter_pinguim,
                                    #  end_date_spotter_pinguim=last_date_spotter_pinguim,
                                    #  start_date_spotter_potter=start_date_spotter_potter,
                                    #  end_date_spotter_potter=last_date_spotter_potter,                                     
                                    #  start_date_spotter_keller=start_date_spotter_keller,
                                    #  end_date_spotter_keller=last_date_spotter_keller)
                                    #  start_date_spotter_mexilhao=start_date_spotter_mexilhao,
                                    #  end_date_spotter_mexilhao=last_date_spotter_mexilhao)



msg = MIMEMultipart('alternative')
msg['Subject'] = EMAIL_BUOYS_SUBJECT
msg['From'] = EMAIL_FROM


msg['To'] = ", ".join(EMAIL_TO)

msg.attach(MIMEText(CONTENT))


for file in EMAIL_BUOYS_FILES:
    name_file = file.split('/')[-1]

    print("Attaching %s " % file)
    payload = MIMEBase('application', "octet-stream")
    payload.set_payload(open(file, "rb").read())
    encoders.encode_base64(payload)
    payload.add_header('Content-Disposition', 'attachment', filename = name_file)
    msg.attach(payload)
    print("%s attached." % name_file)



server = smtplib.SMTP('smtp.gmail.com',587)

server.starttls()
print("Login on Email...")
server.login(remo_mail, remo_password)
print("Sending email...")
server.sendmail(remo_mail, EMAIL_TO, msg.as_string())
print("Done! Email sent!")
server.quit()
print("Quit Server and Email.\n Script Finished!")
