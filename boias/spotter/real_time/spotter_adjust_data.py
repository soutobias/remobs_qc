
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



    df.loc[flag['wdir'] == 0, "wdir"] = df['wdir'] - df['tmp_dec']
    df.loc[df["wdir"] < 0, "wdir"] = df["wdir"] + 360

    df.loc[flag['peak_dir'] == 0, 'peak_dir'] = df['peak_dir'] - df['tmp_dec']
    df.loc[df['peak_dir'] < 0, 'peak_dir'] = df['peak_dir'] + 360

    df.loc[flag['mean_dir'] == 0, 'mean_dir'] = df['mean_dir'] - df['tmp_dec']
    df.loc[df['mean_dir'] < 0, 'mean_dir'] = df['mean_dir'] + 360





    del df['tmp_dec']

    return df



def rename_flag_data(flag):
    import pandas as pd

    prefix = 'flag_'
    flag = flag.add_prefix(prefix)

    return flag


