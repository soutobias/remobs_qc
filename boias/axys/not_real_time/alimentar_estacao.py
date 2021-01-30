

import pandas as pd
from datetime import datetime
import numpy as np
import re
from math import radians, cos, sin, asin, sqrt
import csv
import operator
from pandas.io import sql
import sqlalchemy


# usr = 'remobs'
# password = 'Marinha1@'
# local = 'remobs.postgres.uhserver.com'
# data_base = 'remobs'

# QC DATABASE

usr = 'remobs_qc'
password = 'Marinha1@'
local = 'remobs-qc.postgres.uhserver.com'
data_base = 'remobs_qc'

dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
x=1

df = pd.read_excel('pnboia_estacao.xls')

df = df.set_index('id')

from sqlalchemy import create_engine


con = create_engine(f'postgres+psycopg2://{usr}:{password}@{local}/{data_base}')


df.to_sql(con=con, name='buoys', if_exists='append')


