
from datetime import datetime
import pandas as pd 
import numpy as np
import requests

import os, sys
sys.path.append(os.environ['HOME'])
home_path = os.environ['HOME']
from user_config import *
cwd_path = home_path + '/remobs_qc/boias/easywave/real_time/'
bd_path = home_path + '/remobs_qc/boias/easywave/bd/'
sys.path.append(cwd_path)
sys.path.append(bd_path)

import db_functions as db


def get_values_ew(ew_messages, cols_ew):

	final_data = dict(zip(cols_ew, ([] for _ in cols_ew)))


	for ew_message in ew_messages:

		ew_message = ew_message.split(";")

		final_data['buoy_name'].append(ew_message[0]) 
		final_data['month'].append(ew_message[1]) 
		final_data['day'].append(ew_message[2]) 
		final_data['year'].append(ew_message[3]) 
		final_data['hour'].append(ew_message[4]) 
		final_data['minute'].append(ew_message[5])
		final_data['battery_v'].append(ew_message[6]) 
		final_data['temp_datalogger'].append(ew_message[7]) 
		final_data['lat'].append(ew_message[8]) 
		final_data['lon'].append(ew_message[9]) 
		final_data['swvht'].append(ew_message[10])
		final_data['tp'].append(ew_message[11])
		final_data['wvdir'].append(ew_message[12])


	return pd.DataFrame.from_dict(final_data)


def get_datetime(ew_message):

	minute = ew_message['minute']
	hour = ew_message['hour']
	day = ew_message['day']
	month = ew_message['month']
	year = ew_message['year']

	date_string = year + "-" + month + "-" + day + " " + hour + ":" + minute

	date_time = pd.to_datetime(date_string, format = "%Y-%m-%d %H:%M")

	return date_time



url_ew = WEB_MESSEN


file = requests.get(url_ew)


message = file.text

# Separating messages..
messages = message.split("\r\n")

#Filter EasyWaves messages...
ew_messages = list(filter(lambda ew: 'EW1' in ew, messages))

ew_messages = ew_messages[:30] # getting the most actuals...

#to df
cols_ew = ['buoy_name', 'month', 'day', 'year', 'hour', 'minute',
	        'battery_v', 'temp_datalogger', 'lat', 'lon', 'swvht', 'tp', 'wvdir']



df_ew = get_values_ew(ew_messages, cols_ew)

date_time = get_datetime(df_ew)

df_ew.insert(6, 'date_time', date_time)


# Filtering to last date...

conn = db.remobs_db(host=HOST_RAW, db=DATABASE_RAW, usr=USER_RAW, pwd=PASSWORD_RAW)

last_data = conn.get_last_time(21)

# new_data...

final_df_ew = df_ew[df_ew['date_time'] > last_data]

if final_df_ew.empty:
	print("No new data to insert")
	print(f"Last data from {last_data}")
	print("Script finished.")
else:
	# inserting new data

	#check NAN 
	final_dw_ew =  final_df_ew.replace("NAN", np.NaN)

	# ordering by date 
	final_dw_ew = final_df_ew.sort_values(by='date_time')


	status = conn.insert_ew_data(final_dw_ew , 21)


	if status == 1:
		"EasyWave data inserted on database."
	elif status == 0:
		print("Some problem occurred. EasyWave data not inserted.")



