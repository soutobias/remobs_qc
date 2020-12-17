import os 
import sys 

home_path = os.environ['HOME']
sys.path.append(home_path)

from user_config import EMAIL_FROM, EMAIL_TO, EMAIL_SUBJECT, EMAIL_CONTENT, EMAIL_FILES

import email_fun as gmail

import pandas as pd 

# Read File to check dates

file = pd.read_csv(EMAIL_FILES[0])

start_date = min(file['Datetime'])
last_date = max(file['Datetime'])




service = gmail.service_account_login()



CONTENT = EMAIL_CONTENT.format(start_date = start_date, end_date = last_date)

message_with_attachment = gmail.create_message_with_attachment(EMAIL_FROM, EMAIL_TO, EMAIL_SUBJECT, CONTENT, EMAIL_FILES)

sent = gmail.send_message(service,'me', message_with_attachment)

