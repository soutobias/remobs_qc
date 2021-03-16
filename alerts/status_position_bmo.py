import os
import sys

home_path = os.environ['HOME']
cwd_path = home_path + '/remobs_qc/alerts/'

sys.path.append(cwd_path)
sys.path.append(home_path)


import numpy as np
from bd_system import db_remo
from systems_buoys import *


conn = db_remo()
buoys = conn.active_buoys()

# BMO POINTS
bmo_spot = buoys[['lat','lon']][buoys['buoy_id']==2]

bmo_now = conn.last_positions('BMO',2,1)

bmo_spot = [float(bmo_spot['lon']), float(bmo_spot['lat'])]
bmo_now = [float(bmo_now['lon']), float(bmo_now['lat'])]


# TESTE OUT POSITION

#bmo_now = [-42.7335,-25.5323]




pts_bmo = conn.last_positions('BMO', 2, 2100)
pts_lat_bmo = (pts_bmo['lat'].values).astype(np.float)
pts_lon_bmo = (pts_bmo['lon'].values).astype(np.float)

coords_bmo = [[pts_lon_bmo[p],pts_lat_bmo[p]] for p in range(len(pts_lat_bmo))]

# Safe_Radius
safe_radius = radius_buoy(2164, 2100,300,27.5,15)
center_lon, center_lat = find_centroid(pts_lon_bmo, pts_lat_bmo)

#new_center_lon , new_center_lat = find_centroid(outer_points_lon, outer_points_lat)

#lon_in, lat_in = find_outer_points(pts_lon_bmo, pts_lat_bmo, center_lon, center_lat)

bmo_spot = [center_lon, center_lat]

hav_bmo = haversine(bmo_spot, bmo_now)

safe_circle, safe_range_bmo_lat, safe_range_bmo_lon = safe_range_circle(center_lon, center_lat, 1500)

#new_safe_circle, new_safe_range_bmo_lat, new_safe_range_bmo_lon = safe_range_circle(new_center_lon, new_center_lat, 1300)


# PLOT
import cartopy.crs as ccrs
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import cartopy.feature as cfeature




##### PLOT
### BMO PLOT
ax = plt.axes(projection=ccrs.PlateCarree())
ax.set(facecolor = "#5ACEFF")
ax.add_feature(cfeature.LAND)
ax.add_feature(cfeature.COASTLINE)

ax.set_xlim(float(center_lon)-0.15, float(center_lon)+0.15)
ax.set_ylim(float(center_lat)-0.15, float(center_lat)+0.15)
bmo_points = ax.plot(pts_lon_bmo,pts_lat_bmo, c='r', marker='o' ,label = 'BMO', alpha=0.2)

## BORDER POINTS
#axys_point = ax.plot(pts_lon_axys, pts_lat_axys, c = 'b', marker = 'o', label = 'AXYS')
#bmo_fund = ax.plot(bmo_spot[0], bmo_spot[1], c='k', marker = 'x', label = 'BMO_FUNDEIO')
#axys_fund = ax.plot(axys_spot[1], axys_spot[0], c='k', marker = 'x', label = 'AXYS_FUNDEIO')
bmo_now_pt = ax.plot(bmo_now[0], bmo_now[1], c='b', marker = 'h', label = "Current Position BMO")
bmo_distance = ax.plot([bmo_now[0],center_lon], [bmo_now[1],center_lat], c='g')
bmo_text = ax.annotate(str(hav_bmo['meters']) + ' m', xy = ((bmo_now[0] + bmo_spot[0])/2, (bmo_now[1] + bmo_spot[1])/2),
                                xytext=((bmo_now[0] + bmo_spot[0])/2, (bmo_now[1] + bmo_spot[1])/2),
                                bbox=dict(boxstyle="round", fc=(0, 0, 0), ec="none"),c = 'w')
#hull_plot = ax.plot(points[hull.vertices, 0], points[hull.vertices, 1], 'g.', lw=1)
## Radius Safe BMO
range_bmo = ax.plot(safe_range_bmo_lon, safe_range_bmo_lat,c='k', marker = '.', label = 'Safe Circle')
#new_range_bmo = ax.plot(new_safe_range_bmo_lon, new_safe_range_bmo_lat,c='w', marker = '.', label = 'NEW_Safe Circle')
fill_range = ax.fill(safe_range_bmo_lon, safe_range_bmo_lat, c='w', alpha=0.3)
#Estimated center point:

point_text = ax.annotate(str(abs(bmo_now[1])) + ' °S \n' + str(abs(bmo_now[0])) + ' °W', xy = (bmo_now[0]+0.0002, bmo_now[1]),
                                xytext=(bmo_now[0]+0.001, bmo_now[1]),
                                bbox=dict(boxstyle="round", fc=(0, 0, 0), ec="none"),c = 'w')
###
#far_points = incremental_farthest_search(safe_circle, 2)



center_bmo = ax.plot(center_lon, center_lat,c='k', marker = '.', label = 'CENTER_BMO')
#new_center_bmo = ax.plot(new_center_lon, new_center_lat,c='b', marker = '.', label = 'NEW_CENTER_BMO')



ax.legend(loc = 'upper left')
gr = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
                  linewidth=0.5, color='k', linestyle='--')

gr.top_labels = False
gr.right_labels = False
gr.xlabel_style = {'size': 10}
gr.ylabel_style = {'size': 10}
plt.show()





