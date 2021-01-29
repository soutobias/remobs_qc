import pandas as pd
import time_codes
import numpy as np
from datetime import datetime
import time_codes

def rotate_data(df, flag, buoy):

    df['tmp_dec'] = (df.index.year - 2020) * float(buoy["var_mag_dec"]) + float(buoy["mag_dec"])

    df.loc[flag['cdir1'] == 0, "cdir1"] = df['cdir1'] - df['tmp_dec']
    df.loc[df["cdir1"] < 0, "cdir1"] = df["cdir1"] + 360

    df.loc[flag['cdir2'] == 0, "cdir2"] = df['cdir2'] - df['tmp_dec']
    df.loc[df["cdir2"] < 0, "cdir2"] = df["cdir2"] + 360

    df.loc[flag['cdir3'] == 0, "cdir3"] = df['cdir3'] - df['tmp_dec']
    df.loc[df["cdir3"] < 0, "cdir3"] = df["cdir3"] + 360

    df.loc[flag['wdir'] == 0, "wdir"] = df['wdir'] - df['tmp_dec']
    df.loc[df["wdir"] < 0, "wdir"] = df["wdir"] + 360

    df.loc[flag['wvdir'] == 0, "wvdir"] = df['wvdir'] - df['tmp_dec']
    df.loc[df["wvdir"] < 0, "wvdir"] = (df["wvdir"] + 360)

    del df['tmp_dec']

    return df
