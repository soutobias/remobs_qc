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
axys3m_spot = buoys[['lat','lon']][buoys['buoy_id']==1]


axys3m_now = conn.last_positions_by_tag(1,1)

last_time = axys3m_now['date_time'][0]
axys3m_spot = [float(axys3m_spot['lon']), float(axys3m_spot['lat'])]
axys3m_now = [float(axys3m_now['lon']), float(axys3m_now['lat'])]


# TESTE OUT POSITION

#bmo_now = [-42.7335,-25.5323]




pts_axys3m = conn.last_positions_by_tag(1,70)


pts_lat_axys3m = (pts_axys3m['lat'].values).astype(np.float)
pts_lon_axys3m = (pts_axys3m['lon'].values).astype(np.float)

coords_axys = [[pts_lon_axys3m[p],pts_lat_axys3m[p]] for p in range(len(pts_lat_axys3m))]




# Safe_Radius

safe_radius = radius_buoy(2164, 2100,300,27.5,15)
center_lon, center_lat = find_centroid(pts_lon_axys3m, pts_lat_axys3m)

#new_center_lon , new_center_lat = find_centroid(outer_points_lon, outer_points_lat)

#lon_in, lat_in = find_outer_points(pts_lon_bmo, pts_lat_bmo, center_lon, center_lat)



center_lon = -43.22003064516129
center_lat = -25.85692258064516

axys3m_spot = [center_lon, center_lat]


hav_axys3m = haversine(axys3m_spot, axys3m_now)



safe_circle, safe_range_axys3m_lat, safe_range_axys3m_lon = safe_range_circle(center_lon, center_lat, 3000)



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

ax.set_xlim(float(center_lon)-0.4, float(center_lon)+0.6)
ax.set_ylim(float(center_lat)-0.4, float(center_lat)+0.6)
axys3m_points = ax.plot(pts_lon_axys3m,pts_lat_axys3m, c='r', marker='o' ,label = 'AXYS-3M', alpha=0.2)

## BORDER POINTS
#axys_point = ax.plot(pts_lon_axys, pts_lat_axys, c = 'b', marker = 'o', label = 'AXYS')
#bmo_fund = ax.plot(bmo_spot[0], bmo_spot[1], c='k', marker = 'x', label = 'BMO_FUNDEIO')
#axys_fund = ax.plot(axys_spot[1], axys_spot[0], c='k', marker = 'x', label = 'AXYS_FUNDEIO')
axys3m_now_pt = ax.plot(axys3m_now[0], axys3m_now[1], c='b', marker = 'h', label = "Current Position AXYS-3M")
axys3m_distance = ax.plot([axys3m_now[0],center_lon], [axys3m_now[1],center_lat], c='g')
axys3m_text = ax.annotate(str(hav_axys3m['meters']) + ' m', xy = ((axys3m_now[0] + axys3m_spot[0])/2, (axys3m_now[1] + axys3m_spot[1])/2),
                                xytext=((axys3m_now[0] + axys3m_spot[0])/2, (axys3m_now[1] + axys3m_spot[1])/2),
                                bbox=dict(boxstyle="round", fc=(0, 0, 0), ec="none"),c = 'w')
#hull_plot = ax.plot(points[hull.vertices, 0], points[hull.vertices, 1], 'g.', lw=1)
## Radius Safe BMO
range_axys3m = ax.plot(safe_range_axys3m_lon, safe_range_axys3m_lat,c='k', marker = '.', label = 'Safe Circle')
#new_range_bmo = ax.plot(new_safe_range_bmo_lon, new_safe_range_bmo_lat,c='w', marker = '.', label = 'NEW_Safe Circle')
fill_range = ax.fill(safe_range_axys3m_lon, safe_range_axys3m_lat, c='w', alpha=0.3)
#Estimated center point:

point_text = ax.annotate(str(abs(axys3m_now[1])) + ' °S \n' + str(abs(axys3m_now[0])) + ' °W \n' + str(last_time), xy = (axys3m_now[0]+0.0004, axys3m_now[1]),
                                xytext=(axys3m_now[0]+0.005, axys3m_now[1]),
                                bbox=dict(boxstyle="round", fc=(0, 0, 0), ec="none"),c = 'w')
###
#far_points = incremental_farthest_search(safe_circle, 2)



center_axys3m = ax.plot(center_lon, center_lat,c='orange', marker = '.', label = 'CENTER_AXYS')
#new_center_bmo = ax.plot(new_center_lon, new_center_lat,c='b', marker = '.', label = 'NEW_CENTER_BMO')



ax.legend(loc = 'upper left')
gr = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
                  linewidth=0.5, color='k', linestyle='--')

gr.top_labels = False
gr.right_labels = False
gr.xlabel_style = {'size': 10}
gr.ylabel_style = {'size': 10}
plt.show()
