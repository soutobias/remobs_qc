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
#### SPOTTER

#file_spotter = pd.read_csv(EMAIL_BUOYS_FILES[0])

#start_date_spotter = min(file_spotter['Datetime'])
#last_date_spotter = max(file_spotter['Datetime'])

###################################################
#### BMO BR

file_bmo = pd.read_csv(EMAIL_BUOYS_FILES[0])

start_date_bmo = min(file_bmo['Datetime'])
last_date_bmo = max(file_bmo['Datetime'])


CONTENT = EMAIL_BUOYS_CONTENT.format(start_date_bmo = start_date_bmo,
                                      end_date_bmo = last_date_bmo)




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
