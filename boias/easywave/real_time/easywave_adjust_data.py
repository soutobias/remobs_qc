
import numpy as np


def rotate_data(conn, df, flag, buoy_id):


    def get_declination(conn, buoy_id):
        import pandas as pd

        query = f"SELECT mag_dec, var_mag_dec FROM buoys WHERE buoy_id = {buoy_id};"

        df = pd.read_sql_query(query, conn._db)

        dec = df['mag_dec'][0]
        var_mag_dec = df['var_mag_dec'][0]

        return dec, var_mag_dec


    dec, var_dec = get_declination(conn, buoy_id)


# Adjusting different for Wvdir2 and Wdir.
# Data from these sensors already adjusted for Vitoria - ES.

# This adjustment is from SBG System
# Correcting for the actual position Addin 1.19 degrees

# Declination for Vitoria 23.80 W
# Declination for actual position 22.61 W

    #add_diff_dec_sbg = 1.19


    df['tmp_dec'] = (df.index.year - 2020) * float(var_dec) + float(dec)

    df.loc[flag['wvdir'] == 0, "wvdir"] = df['wvdir'] + df['tmp_dec']

    df.loc[df["wvdir"] < 0, "wvdir"] = df["wvdir"] + 360
    df.loc[df["wvdir"] > 360, "wvdir"] = df["wvdir"] - 360



    del df['tmp_dec']

    return df



def rename_flag_data(flag):
    import pandas as pd

    prefix = 'flag_'
    flag = flag.add_prefix(prefix)

    return flag





def adjust_ew_qc(ew_qc_data):
    import pandas as pd
    import numpy as np

    columns_data = ['buoy_id', 'id',
                       'date_time', 'lat', 'lon',
                        'battery', 'swvht', 'tp',
                       'wvdir']

    ew_qc_adjusted = pd.DataFrame(columns = columns_data)


    ew_qc_data = ew_qc_data.replace(np.nan, -9999)

    ew_qc_adjusted['buoy_id'] = ew_qc_data['buoy_id'].astype(int)
    ew_qc_adjusted['id'] = ew_qc_data['id'].astype(int)
    ew_qc_adjusted['date_time'] = ew_qc_data.index
    ew_qc_adjusted['lat'] = pd.to_numeric(ew_qc_data['lat'], errors = 'coerce').round(4)
    ew_qc_adjusted['lon'] = pd.to_numeric(ew_qc_data['lon'], errors = 'coerce').round(4)
    ew_qc_adjusted['battery'] = pd.to_numeric(ew_qc_data['battery_voltage'], errors = 'coerce').round(1)
    ew_qc_adjusted['swvht'] = pd.to_numeric(ew_qc_data['swvht'], errors = 'coerce').round(2)
    ew_qc_adjusted['tp'] = pd.to_numeric(ew_qc_data['tp'], errors = 'coerce').round(1)
    ew_qc_adjusted['wvdir'] = pd.to_numeric(ew_qc_data['wvdir'], errors='coerce',  downcast='signed').astype(int)



    # flags_columns

    flag_columns = [col for col in ew_qc_data if col.startswith('flag_')]

    for col in flag_columns:
        ew_qc_adjusted[col] = pd.to_numeric(ew_qc_data[col],
                                             errors='coerce',
                                             downcast='signed').astype(int)

    ew_qc_adjusted.set_index('date_time', inplace=True)


    return ew_qc_adjusted


def check_size_values(df):
    """ Check size values to input in database """
    """ Replacing very spurious data (>9999) with -9999 """


    data_cols =  ["swvht",
                    "tp",
                    "wvdir"]

    # Replace values:

    df[df[data_cols].ge(9999)] = -9999
    df[df[data_cols].eq(-9639)] = -9999

    return df


def zulu_time(df):
    from datetime import timedelta

    df.index = df.index + timedelta(hours=3)

    return df


def get_synoptic_data(df):

    zulu_hours = [0, 3, 6, 9, 12, 15, 18, 21]
    idx_zulu = df.index.hour.isin(zulu_hours)

    df = df[idx_zulu]

    return df
