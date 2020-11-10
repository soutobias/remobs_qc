

def adjust_bmo_general(raw_data):
    import pandas as pd


    columns_general = ['id_buoy', 'id',
                       'date_time', 'lat', 'lon',
                        'battery', 'wspd1', 'gust1',
                       'wdir1', 'wspd2', 'gust2',
                       'wdir2', 'atmp', 'rh', 'dewpt',
                       'pres', 'sst', 'compass',
                       'arad', 'cspd1', 'cdir1',
                       'cspd2', 'cdir2', 'cspd3',
                       'cdir3', 'swvht1', 'tp1',
                       'mxwvht1', 'wvdir1', 'wvspread1',
                       'swvht2', 'tp2', 'wvdir2']



    bmo_general_data = pd.DataFrame(columns = columns_general)

    raw_data = raw_data.replace('NAN', -9999)

    bmo_general_data['id_buoy'] = raw_data['id_buoy'].astype(int)
    bmo_general_data['id'] = raw_data['id'].astype(int)
    bmo_general_data['date_time'] = raw_data['date_time']
    bmo_general_data['lat'] = pd.to_numeric(raw_data['lat'], errors = 'coerce').round(4)
    bmo_general_data['lon'] = pd.to_numeric(raw_data['lon'], errors = 'coerce').round(4)
    bmo_general_data['battery'] = pd.to_numeric(raw_data['battery'], errors = 'coerce').round(1)
    bmo_general_data['wspd1'] = pd.to_numeric(raw_data['wspd1'], errors = 'coerce').round(2)
    bmo_general_data['gust1'] = pd.to_numeric(raw_data['gust1'], errors = 'coerce').round(2)
    bmo_general_data['wdir1'] = pd.to_numeric(raw_data['wdir1'], errors= 'coerce', downcast = 'signed').astype(int)
    bmo_general_data['wspd2'] = pd.to_numeric(raw_data['wspd2'], errors = 'coerce').round(2)
    bmo_general_data['gust2'] = pd.to_numeric(raw_data['gust2'], errors = 'coerce').round(2)
    bmo_general_data['wdir2'] = pd.to_numeric(raw_data['wdir2'], errors= 'coerce', downcast = 'signed').astype(int)
    bmo_general_data['atmp'] = pd.to_numeric(raw_data['atmp'], errors = 'coerce').round(2)
    bmo_general_data['rh'] = pd.to_numeric(raw_data['rh'], errors = 'coerce').round(2)
    bmo_general_data['dewpt'] = pd.to_numeric(raw_data['dewpt'], errors = 'coerce').round(2)
    bmo_general_data['pres'] = pd.to_numeric(raw_data['pres'], errors = 'coerce').round(1)
    bmo_general_data['sst'] = pd.to_numeric(raw_data['sst'], errors = 'coerce').round(2)
    bmo_general_data['compass'] = pd.to_numeric(raw_data['compass'], errors= 'coerce', downcast = 'signed').astype(int)
    bmo_general_data['arad'] = pd.to_numeric(raw_data['arad'], errors = 'coerce').round(2)
    bmo_general_data['cspd1'] = pd.to_numeric(raw_data['cspd1'],errors = 'coerce').round(1)
    bmo_general_data['cdir1'] = pd.to_numeric(raw_data['cdir1'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['cspd2'] = pd.to_numeric(raw_data['cspd2'], errors = 'coerce').round(1)
    bmo_general_data['cdir2'] = pd.to_numeric(raw_data['cdir2'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['cspd3'] = pd.to_numeric(raw_data['cspd3'], errors = 'coerce').round(1)
    bmo_general_data['cdir3'] = pd.to_numeric(raw_data['cdir3'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['swvht1'] = pd.to_numeric(raw_data['swvht1'], errors = 'coerce').round(2)
    bmo_general_data['tp1'] = pd.to_numeric(raw_data['tp1'], errors = 'coerce').round(1)
    bmo_general_data['mxwvht1'] = pd.to_numeric(raw_data['mxwvht1'], errors = 'coerce').round(2)
    bmo_general_data['wvdir1'] = pd.to_numeric(raw_data['wvdir1'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['wvspread1'] = pd.to_numeric(raw_data['wvspread1'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['swvht2'] = pd.to_numeric(raw_data['swvht2'], errors = 'coerce').round(2)
    bmo_general_data['tp2'] = pd.to_numeric(raw_data['tp2'], errors = 'coerce').round(1)
    bmo_general_data['wvdir2'] = pd.to_numeric(raw_data['wvdir2'], errors='coerce', downcast='signed').astype(int)

    bmo_general_data.set_index('date_time', inplace = True)
    return bmo_general_data




def adjust_bmo_current(raw_data):
    import pandas as pd


    columns_current = ['id_buoy', 'id',
                       'date_time', 'lat', 'lon','cspd1', 'cdir1',
                        'cspd2', 'cdir2', 'cspd3','cdir3', 'cspd4', 'cdir4',
                       'cspd5', 'cdir5', 'cspd6','cdir6', 'cspd7', 'cdir7',
                       'cspd8', 'cdir8', 'cspd9','cdir9', 'cspd10', 'cdir10',
                       'cspd11','cdir11', 'cspd12','cdir12', 'cspd13', 'cdir13',
                       'cspd14','cdir14', 'cspd15','cdir15', 'cspd16', 'cdir16',
                       'cspd17','cdir17', 'cspd18','cdir18']


    bmo_general_data = pd.DataFrame(columns = columns_current)

    raw_data = raw_data.replace('NAN', -9999)

    bmo_general_data['id_buoy'] = raw_data['id_buoy'].astype(int)
    bmo_general_data['id'] = raw_data['id'].astype(int)
    bmo_general_data['date_time'] = raw_data['date_time']
    bmo_general_data['lat'] = pd.to_numeric(raw_data['lat'], errors = 'coerce').round(4)
    bmo_general_data['lon'] = pd.to_numeric(raw_data['lon'], errors = 'coerce').round(4)
    bmo_general_data['cspd1'] = pd.to_numeric(raw_data['cspd1'],errors = 'coerce').round(1)
    bmo_general_data['cdir1'] = pd.to_numeric(raw_data['cdir1'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['cspd2'] = pd.to_numeric(raw_data['cspd2'], errors = 'coerce').round(1)
    bmo_general_data['cdir2'] = pd.to_numeric(raw_data['cdir2'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['cspd3'] = pd.to_numeric(raw_data['cspd3'], errors = 'coerce').round(1)
    bmo_general_data['cdir3'] = pd.to_numeric(raw_data['cdir3'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['cspd4'] = pd.to_numeric(raw_data['cspd1'],errors = 'coerce').round(1)
    bmo_general_data['cdir4'] = pd.to_numeric(raw_data['cdir1'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['cspd5'] = pd.to_numeric(raw_data['cspd2'], errors = 'coerce').round(1)
    bmo_general_data['cdir5'] = pd.to_numeric(raw_data['cdir2'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['cspd6'] = pd.to_numeric(raw_data['cspd3'], errors = 'coerce').round(1)
    bmo_general_data['cdir6'] = pd.to_numeric(raw_data['cdir3'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['cspd7'] = pd.to_numeric(raw_data['cspd1'],errors = 'coerce').round(1)
    bmo_general_data['cdir7'] = pd.to_numeric(raw_data['cdir1'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['cspd8'] = pd.to_numeric(raw_data['cspd2'], errors = 'coerce').round(1)
    bmo_general_data['cdir8'] = pd.to_numeric(raw_data['cdir2'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['cspd9'] = pd.to_numeric(raw_data['cspd3'], errors = 'coerce').round(1)
    bmo_general_data['cdir9'] = pd.to_numeric(raw_data['cdir3'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['cspd10'] = pd.to_numeric(raw_data['cspd1'],errors = 'coerce').round(1)
    bmo_general_data['cdir10'] = pd.to_numeric(raw_data['cdir1'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['cspd11'] = pd.to_numeric(raw_data['cspd2'], errors = 'coerce').round(1)
    bmo_general_data['cdir11'] = pd.to_numeric(raw_data['cdir2'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['cspd12'] = pd.to_numeric(raw_data['cspd3'], errors = 'coerce').round(1)
    bmo_general_data['cdir12'] = pd.to_numeric(raw_data['cdir3'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['cspd13'] = pd.to_numeric(raw_data['cspd1'],errors = 'coerce').round(1)
    bmo_general_data['cdir13'] = pd.to_numeric(raw_data['cdir1'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['cspd14'] = pd.to_numeric(raw_data['cspd2'], errors = 'coerce').round(1)
    bmo_general_data['cdir14'] = pd.to_numeric(raw_data['cdir2'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['cspd15'] = pd.to_numeric(raw_data['cspd3'], errors = 'coerce').round(1)
    bmo_general_data['cdir15'] = pd.to_numeric(raw_data['cdir3'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['cspd16'] = pd.to_numeric(raw_data['cspd1'],errors = 'coerce').round(1)
    bmo_general_data['cdir16'] = pd.to_numeric(raw_data['cdir1'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['cspd17'] = pd.to_numeric(raw_data['cspd2'], errors = 'coerce').round(1)
    bmo_general_data['cdir17'] = pd.to_numeric(raw_data['cdir2'], errors='coerce',  downcast='signed').astype(int)
    bmo_general_data['cspd18'] = pd.to_numeric(raw_data['cspd3'], errors = 'coerce').round(1)
    bmo_general_data['cdir18'] = pd.to_numeric(raw_data['cdir3'], errors='coerce',  downcast='signed').astype(int)

    bmo_general_data.set_index('date_time', inplace = True)


    return bmo_general_data



def rotate_data(conn, df, flag, id_buoy):


    def get_declination(conn, id_buoy):
        import pandas as pd

        query = f"SELECT mag_dec, var_mag_dec FROM buoys WHERE id_buoy = {id_buoy};"

        df = pd.read_sql_query(query, conn)

        dec = df['mag_dec'][0]
        var_mag_dec = df['var_mag_dec'][0]

        return dec, var_mag_dec


    dec, var_dec = get_declination(conn, id_buoy)



    df['tmp_dec'] = (df.index.year - 2020) * float(var_dec) + float(dec)

    df.loc[flag['cdir1'] == 0, "cdir1"] = df['cdir1'] - df['tmp_dec']
    df.loc[df["cdir1"] < 0, "cdir1"] = df["cdir1"] + 360

    df.loc[flag['cdir2'] == 0, "cdir2"] = df['cdir2'] - df['tmp_dec']
    df.loc[df["cdir2"] < 0, "cdir2"] = df["cdir2"] + 360

    df.loc[flag['cdir3'] == 0, "cdir3"] = df['cdir3'] - df['tmp_dec']
    df.loc[df["cdir3"] < 0, "cdir3"] = df["cdir3"] + 360

    df.loc[flag['wdir'] == 0, "wdir"] = df['wdir'] - df['tmp_dec']
    df.loc[df["wdir"] < 0, "wdir"] = df["wdir"] + 360

    df.loc[flag['wvdir1'] == 0, "wvdir1"] = df['wvdir1'] - df['tmp_dec']
    df.loc[df["wvdir1"] < 0, "wvdir1"] = df["wvdir1"] + 360

    df.loc[flag['wvdir2'] == 0, "wvdir2"] = df['wvdir2'] - df['tmp_dec']
    df.loc[df["wvdir2"] < 0, "wvdir2"] = df["wvdir2"] + 360

    del df['tmp_dec']

    return df



def rename_flag_data(flag):
    import pandas as pd

    prefix = 'flag_'
    flag = flag.add_prefix(prefix)

    return flag





def adjust_bmo_qc(bmo_qc_data):
    import pandas as pd
    import numpy as np

    columns_data = ['id_buoy', 'id',
                       'date_time', 'lat', 'lon',
                        'battery', 'wspd', 'gust',
                       'wdir', 'atmp', 'rh', 'dewpt',
                       'pres', 'sst', 'compass',
                       'arad', 'cspd1', 'cdir1',
                       'cspd2', 'cdir2', 'cspd3',
                       'cdir3', 'swvht1', 'tp1',
                       'mxwvht1', 'wvdir1', 'wvspread1',
                       'swvht2', 'tp2', 'wvdir2']

    bmo_qc_adjusted = pd.DataFrame(columns = columns_data)


    bmo_qc_adjusted['id_buoy'] = bmo_qc_data['id_buoy'].astype(int)
    bmo_qc_adjusted['id'] = bmo_qc_data['id'].astype(int)
    bmo_qc_adjusted['date_time'] = bmo_qc_data.index
    bmo_qc_adjusted['lat'] = pd.to_numeric(bmo_qc_data['lat'], errors = 'coerce').round(4)
    bmo_qc_adjusted['lon'] = pd.to_numeric(bmo_qc_data['lon'], errors = 'coerce').round(4)
    bmo_qc_adjusted['battery'] = pd.to_numeric(bmo_qc_data['battery'], errors = 'coerce').round(1)
    bmo_qc_adjusted['wspd'] = pd.to_numeric(bmo_qc_data['wspd'], errors = 'coerce').round(2)
    bmo_qc_adjusted['gust'] = pd.to_numeric(bmo_qc_data['gust'], errors = 'coerce').round(2)
    bmo_qc_adjusted['wdir'] = pd.to_numeric(bmo_qc_data['wdir'], errors= 'coerce', downcast = 'signed').astype(int)
    bmo_qc_adjusted['atmp'] = pd.to_numeric(bmo_qc_data['atmp'], errors = 'coerce').round(2)
    bmo_qc_adjusted['rh'] = pd.to_numeric(bmo_qc_data['rh'], errors = 'coerce').round(2)
    bmo_qc_adjusted['dewpt'] = pd.to_numeric(bmo_qc_data['dewpt'], errors = 'coerce').round(2)
    bmo_qc_adjusted['pres'] = pd.to_numeric(bmo_qc_data['pres'], errors = 'coerce').round(1)
    bmo_qc_adjusted['sst'] = pd.to_numeric(bmo_qc_data['sst'], errors = 'coerce').round(2)
    bmo_qc_adjusted['compass'] = pd.to_numeric(bmo_qc_data['compass'], errors= 'coerce', downcast = 'signed').astype(int)
    bmo_qc_adjusted['arad'] = pd.to_numeric(bmo_qc_data['arad'], errors = 'coerce').round(2)
    bmo_qc_adjusted['cspd1'] = pd.to_numeric(bmo_qc_data['cspd1'],errors = 'coerce').round(1)
    bmo_qc_adjusted['cdir1'] = pd.to_numeric(bmo_qc_data['cdir1'], errors='coerce',  downcast='signed').astype(int)
    bmo_qc_adjusted['cspd2'] = pd.to_numeric(bmo_qc_data['cspd2'], errors = 'coerce').round(1)
    bmo_qc_adjusted['cdir2'] = pd.to_numeric(bmo_qc_data['cdir2'], errors='coerce',  downcast='signed').astype(int)
    bmo_qc_adjusted['cspd3'] = pd.to_numeric(bmo_qc_data['cspd3'], errors = 'coerce').round(1)
    bmo_qc_adjusted['cdir3'] = pd.to_numeric(bmo_qc_data['cdir3'], errors='coerce',  downcast='signed').astype(int)
    bmo_qc_adjusted['swvht1'] = pd.to_numeric(bmo_qc_data['swvht1'], errors = 'coerce').round(2)
    bmo_qc_adjusted['tp1'] = pd.to_numeric(bmo_qc_data['tp1'], errors = 'coerce').round(1)
    bmo_qc_adjusted['mxwvht1'] = pd.to_numeric(bmo_qc_data['mxwvht1'], errors = 'coerce').round(2)
    bmo_qc_adjusted['wvdir1'] = pd.to_numeric(bmo_qc_data['wvdir1'], errors='coerce',  downcast='signed').astype(int)
    bmo_qc_adjusted['wvspread1'] = pd.to_numeric(bmo_qc_data['wvspread1'], errors='coerce',  downcast='signed').astype(int)
    bmo_qc_adjusted['swvht2'] = pd.to_numeric(bmo_qc_data['swvht2'], errors = 'coerce').round(2)
    bmo_qc_adjusted['tp2'] = pd.to_numeric(bmo_qc_data['tp2'], errors = 'coerce').round(1)
    bmo_qc_adjusted['wvdir2'] = pd.to_numeric(bmo_qc_data['wvdir2'], errors='coerce', downcast='signed').astype(int)





    # flags_columns

    flag_columns = [col for col in bmo_qc_data if col.startswith('flag_')]

    for col in flag_columns:
        bmo_qc_adjusted[col] = pd.to_numeric(bmo_qc_data[col],
                                             errors='coerce',
                                             downcast='signed').astype(int)

    bmo_qc_adjusted.set_index('date_time', inplace=True)


    return bmo_qc_adjusted
