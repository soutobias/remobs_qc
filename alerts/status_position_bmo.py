import os
import sys

home_path = os.environ['HOME']
cwd_path = home_path + '/remobs_qc/alerts/'

sys.path.append(cwd_path)
sys.path.append(home_path)

from datetime import timedelta
import numpy as np
from bd_system import db_remo
from systems_buoys import *


conn = db_remo()


# BMO POINTS
# bmo_spot = buoys[['lat','lon']][buoys['buoy_id']==2]

bmo_now = conn.last_positions('BMO',2,1)
date_time_now = bmo_now['date_time'][0]

# ZULU TIME...
date_time_now = date_time_now + timedelta(hours=3)

bmo_now = [float(bmo_now['lon']), float(bmo_now['lat'])]



pts_bmo = conn.last_positions('BMO', 2, 2100)
pts_lat_bmo = (pts_bmo['lat'].values).astype(np.float)
pts_lon_bmo = (pts_bmo['lon'].values).astype(np.float)

coords_bmo = [[pts_lon_bmo[p],pts_lat_bmo[p]] for p in range(len(pts_lat_bmo))]

# Safe_Radius
# safe_radius = radius_buoy(2164, 2100,300,27.5,15)

#### to do: change the calculus of centroid, not in 100% in center...
# center_lon, center_lat = find_centroid(pts_lon_bmo, pts_lat_bmo)



# very close points to center...
center_lon = -42.73667
center_lat = -25.51108

bmo_spot = [center_lon, center_lat]

hav_bmo = haversine(bmo_spot, bmo_now)




watch_circle_radius = 1500

if hav_bmo['meters'] > watch_circle_radius:

    print("Boia fora de posição!")

    safe_circle, safe_range_bmo_lat, safe_range_bmo_lon = safe_range_circle(center_lon, center_lat, watch_circle_radius)


    # PLOT
    import cartopy.crs as ccrs
    import matplotlib.pyplot as plt
    import cartopy.feature as cfeature
    from matplotlib.offsetbox import AnchoredText





    ##### PLOT
    ### BMO PLOT
    plt.figure(figsize=(10, 10))
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.set(facecolor = "#5ACEFF")
    ax.add_feature(cfeature.LAND)
    ax.add_feature(cfeature.COASTLINE)

    ax.set_xlim(float(center_lon)-0.18, float(center_lon)+0.18)
    ax.set_ylim(float(center_lat)-0.18, float(center_lat)+0.18)
    bmo_points = ax.plot(pts_lon_bmo,pts_lat_bmo, c='r', marker='o' ,label = 'BMO', alpha=0.2)

    ## BORDER POINTS

    bmo_now_pt = ax.plot(bmo_now[0], bmo_now[1], c='b', marker = 'h', label = "Current Position BMO")
    bmo_distance = ax.plot([bmo_now[0],center_lon], [bmo_now[1],center_lat], c='g')
    bmo_text = ax.annotate(str(hav_bmo['meters']) + ' m', xy = ((bmo_now[0] + bmo_spot[0])/2, (bmo_now[1] + bmo_spot[1])/2),
                                    xytext=((bmo_now[0] + bmo_spot[0])/2, (bmo_now[1] + bmo_spot[1])/2),
                                    bbox=dict(boxstyle="round", fc=(0, 0, 0), ec="none"),c = 'w')

    ## Radius Safe BMO
    range_bmo = ax.plot(safe_range_bmo_lon, safe_range_bmo_lat,c='k', marker = '.', label = 'Watch Circle')

    fill_range = ax.fill(safe_range_bmo_lon, safe_range_bmo_lat, c='w', alpha=0.3)
    #Estimated center point:

    point_text = ax.annotate(str(abs(bmo_now[1])) + ' °S \n' + str(abs(bmo_now[0])) + ' °W', xy = (bmo_now[0]+0.0002, bmo_now[1]),
                                    xytext=(bmo_now[0]+0.001, bmo_now[1]),
                                    bbox=dict(boxstyle="round", fc=(0, 0, 0), ec="none"),c = 'w')




    center_bmo = ax.plot(center_lon, center_lat,c='k', marker = '.', label = 'center circle')


    at = AnchoredText("LAST DATA: \n"
                      "LAT: " + str(abs(bmo_now[1])) + " °S \n"
                      "LON: " + str(abs(bmo_now[0])) + " °W \n"
                      "DATETIME(Z): " + str(date_time_now) + " \n"
                      "DISTANCE: " + str(hav_bmo['meters']) + "meters",
                      prop=dict(size=11), frameon=True,
                      loc='upper right',
                      )
    at.patch.set_boxstyle("round,pad=0.,rounding_size=0.2")
    ax.add_artist(at)

    ax.legend(loc = 'upper left')
    gr = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
                      linewidth=0.5, color='k', linestyle='--')

    gr.top_labels = False
    gr.right_labels = False
    gr.xlabel_style = {'size': 10}
    gr.ylabel_style = {'size': 10}

    plt.savefig(home_path + '/bmo_last_position.png', dpi = 100)


# send email:
    from alert_email_bmo import send_alert_mail

    file_plot = home_path + '/bmo_last_position.png'

    send_alert_mail(bmo_now[1], bmo_now[0], date_time_now, hav_bmo['meters'], file_plot)



