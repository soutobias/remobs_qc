import numpy as np

range_limits = {
    'wspd': [0,30],
    'wdir': [0,360],
    'swvht': [0,1],
    'sst': [0,30],
    'peak_tp': [0,20],
    'mean_tp': [0,15],
    'peak_dir': [0,360],
    'mean_dir': [0,360],
    }

mis_value_limits = {
    'wspd': -999,
    'wdir': -999,
    'sst': -999,
    'swvht': -999,
    'peak_tp': -999,
    'mean_tp': -999,
    'peak_dir': -999,
    'mean_dir': -999,
}

stuck_limits = 7

continuity_limits = 3