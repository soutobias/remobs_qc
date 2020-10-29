range_limits = {
    "swvht1": [0.1, 19.9],
    "mxwvht1": [0.1, 19.9],
    "swvht2": [0.1, 19.9],
    "mxwvht2": [0.1, 19.9],
    "tp1": [1.7, 30],
    "tp2": [1.7, 30],
    "wvdir1": [0, 360],
    "wvdir2": [0, 360],
    "wspd1": [0.1, 59],
    "wdir1": [0, 360],
    "gust1": [0.1, 59],
    "wspd2": [0.1, 59],
    "wdir2": [0, 360],
    "gust2": [0.1, 59],
    "atmp": [-39, 59],
    "pres": [501, 1099],
    "dewpt": [-29, 39],
    "sst": [-3, 39],
    "rh": [25, 102],
    "cspd1": [-4990, 4990],
    "cdir1": [0, 360],
    "cspd2": [-4990, 4990],
    "cdir2": [0, 360],
    "cspd3": [-4990, 4990],
    "cdir3": [0, 360],
    }

sigma_limits = {
    "swvht1": 6,
    "swvht2": 6,
    "rh": 20,
    "pres": 21,
    "atmp": 11,
    "wspd": 25,
    "sst": 8.6,
    }

mis_value_limits = {
    "swvht1": -9999,
    "mxwvht1": -9999,
    "swvht2": -9999,
    "mxwvht2": -9999,
    "tp1": -9999,
    "tp2": -9999,
    "wvdir1": -9999,
    "wvdir2": -9999,
    "wspd1": -9999,
    "wdir1": -9999,
    "gust1": -9999,
    "wspd2": -9999,
    "wdir2": -9999,
    "gust2": -9999,
    "atmp": -9999,
    "pres": -9999,
    "dewpt": -9999,
    "sst": -9999,
    "rh": -9999,
    "cspd1": -9999,
    "cdir1": -9999,
    "cspd2": -9999,
    "cdir2": -9999,
    "cspd3": -9999,
    "cdir3": -9999
    }

climate_limits = {
    "swwht1": [0, 15],
    "swwht2": [0, 15],
    "mxwvht1": [0, 19],
    "tp1": [1.7, 20],
    "tp2": [1.7, 20],
    "wspd1": [0, 59],
    "gust1": [0, 59],
    "wspd2": [0, 59],
    "gust2": [0, 59],
    "atmp": [-8, 42],
    "atmp": [8, 48],
    "atmp": [15, 48],
    "pres": [950, 1050],
    "dewpt": [-29, 39],
    "sst": [-3, 39],
    "cspd1": [-2500, 2500],
    "cspd2": [-2500, 2500],
    "cspd3": [-2500, 2500],
    }


std_mean_values = {
    "swvht1": [0, 15],
    "swvht2": [0, 15],
    "mxwvht1": [0, 19],
    "tp": [1.7, 20],
    "wspd1": [0, 59],
    "gust1": [0, 59],
    "wspd2": [0, 59],
    "gust2": [0, 59],
    "atmp": [-8, 42],
    "atmp": [8, 48],
    "atmp": [15, 48],
    "pres": [950, 1050],
    "dewpt": [-29, 39],
    "sst": [-3, 39],
    "cspd1": [-2500, 2500],
    "cspd2": [-2500, 2500],
    "cspd3": [-2500, 2500],
    }

stuck_limits = 7

continuity_limits = 3