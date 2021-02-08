import os
import sys

home_path = os.environ['HOME']
sys.path.append(home_path)

from user_config import EMAIL_FROM, EMAIL_TO, EMAIL_BUOYS_SUBJECT, EMAIL_BUOYS_CONTENT, EMAIL_BUOYS_FILES

import email_fun as gmail

import pandas as pd

###################################################
#### SPOTTER

file_spotter = pd.read_csv(EMAIL_BUOYS_FILES[0])

start_date_spotter = min(file_spotter['Datetime'])
last_date_spotter = max(file_spotter['Datetime'])

###################################################
#### BMO BR

file_bmo = pd.read_csv(EMAIL_BUOYS_FILES[1])

start_date_bmo = min(file_bmo['Datetime'])
last_date_bmo = max(file_bmo['Datetime'])



CONTENT = EMAIL_BUOYS_CONTENT.format(start_date_spotter = start_date_spotter,
                                       end_date_spotter = last_date_spotter,
                                       start_date_bmo = start_date_bmo,
                                       end_date_bmo = last_date_bmo)



service = gmail.service_account_login()
message_with_attachment = gmail.create_message_with_attachment(EMAIL_FROM, EMAIL_TO, EMAIL_BUOYS_SUBJECT, CONTENT, EMAIL_BUOYS_FILES)

sent = gmail.send_message(service,'me', message_with_attachment)
