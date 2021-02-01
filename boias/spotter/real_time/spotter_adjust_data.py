
def rotate_data(conn, df, flag, buoy_id):


    def get_declination(conn, buoy_id):
        import pandas as pd

        query = f"SELECT mag_dec, var_mag_dec FROM buoys WHERE buoy_id = {buoy_id};"

        df = pd.read_sql_query(query, conn)

        dec = df['mag_dec'][0]
        var_mag_dec = df['var_mag_dec'][0]

        return dec, var_mag_dec


    dec, var_dec = get_declination(conn, buoy_id)



    df['tmp_dec'] = (df.index.year - 2020) * float(var_dec) + float(dec)



    df.loc[flag['wdir'] == 0, "wdir"] = df['wdir'] + df['tmp_dec']
    df.loc[df["wdir"] < 0, "wdir"] = df["wdir"] + 360
     df.loc[df["wdir"] > 360, "wdir"] = df["wdir"] - 360

    df.loc[flag['pk_dir'] == 0, 'pk_dir'] = df['pk_dir'] + df['tmp_dec']
    df.loc[df['pk_dir'] < 0, 'pk_dir'] = df['pk_dir'] + 360
    df.loc[df['pk_dir'] > 360, 'pk_dir'] = df['pk_dir'] - 360

    df.loc[flag['wvdir'] == 0, 'wvdir'] = df['wvdir'] + df['tmp_dec']
    df.loc[df['wvdir'] < 0, 'wvdir'] = df['wvdir'] + 360
    df.loc[df['wvdir'] > 360, 'wvdir'] = df['wvdir'] - 360

    df.loc[flag['wvspread'] == 0, 'wvspread'] = df['wvspread'] + df['tmp_dec']
    df.loc[df['wvspread'] < 0, 'wvspread'] = df['wvspread'] + 360
    df.loc[df['wvspread'] > 360, 'wvspread'] = df['wvspread'] - 360

    df.loc[flag['pk_wvspread'] == 0, 'pk_wvspread'] = df['pk_wvspread'] + df['tmp_dec']
    df.loc[df['pk_wvspread'] < 0, 'pk_wvspread'] = df['pk_wvspread'] + 360
    df.loc[df['pk_wvspread'] > 360, 'pk_wvspread'] = df['pk_wvspread'] - 360





    del df['tmp_dec']

    return df



def rename_flag_data(flag):
    import pandas as pd

    prefix = 'flag_'
    flag = flag.add_prefix(prefix)

    return flag


