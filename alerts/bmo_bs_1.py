from bd_system import db_remo
from harvesine import *



conn = db_remo()
buoys = conn.active_buoys()
bmo_spot = buoys[['lat','lon']][buoys['buoy_id']==2]


bmo_local = conn.last_positions('BMO',2,1)
bmo_spot = [float(bmo_spot['lat']), float(bmo_spot['lon'])]
bmo_local = [float(bmo_local['lat']), float(bmo_local['lon'])]

hav_bmo = haversine(bmo_spot, bmo_local)

pts_bmo = conn.last_positions('BMO', 2, 1000)


import cartopy.crs as ccrs
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import cartopy.feature as cfeature
import numpy as np

# Define the radius_of_movment of buoy:

# depth = 2100 * to confirm.
# cable_1 = 2100
# cable_2 = 300
# tie_1 = 27.5
# tie_2 = 15
local_depth = 2100
cable_1 = 2100
cable_2 = 300
tie_1 = 27.5
tie_2 = 15

safe_radius = radius_buoy(local_depth, cable_1, cable_2, tie_1, tie_2)


# BMO
pts_lat_bmo = (pts_bmo['lat'].values).astype(np.float)
pts_lon_bmo = (pts_bmo['lon'].values).astype(np.float)

coords_bmo = [(pts_lon_bmo[p],pts_lat_bmo[p]) for p in range(len(pts_lat_bmo))]



safe_circle, safe_range_bmo_lat, safe_range_bmo_lon = safe_range_circle(float(bmo_spot[0]), float(bmo_spot[1]), 1000)

# Find Center
center_lon, center_lat = find_centroid(pts_lon_bmo, pts_lat_bmo)
# Second Center
lon_in, lat_in = find_outer_points(pts_lon_bmo, pts_lat_bmo, bmo_spot[1],bmo_spot[0])

center_lon2, center_lat2 = find_centroid(lon_farthest_point, lat_farthest_point)

safe_circle2, safe_range_bmo_lat2, safe_range_bmo_lon2 = safe_range_circle(center_lon2, center_lat2, 1500)







##### PLOT
### BMO PLOT
ax = plt.axes(projection=ccrs.PlateCarree())
ax.set(facecolor = "#5ACEFF")
ax.add_feature(cfeature.LAND)
ax.add_feature(cfeature.COASTLINE)

ax.set_xlim(float(pts_bmo['lon'][0])-0.03, float(pts_bmo['lon'][0])+0.03)
ax.set_ylim(float(pts_bmo['lat'][0])-0.03, float(pts_bmo['lat'][0])+0.03)
bmo_points = ax.scatter(pts_lon_bmo,pts_lat_bmo, c='r', marker='o' ,label = 'BMO')
#bmo_points_inside = ax.plot(lon_inside, lat_inside, c='r', marker='o' ,label = 'BMO IN')
## BORDER POINTS
#bmo_points_border = ax.plot(lon_in, lat_in, c='r', marker='o' ,label = 'BMO BORDER')
bmo_fund = ax.plot(bmo_spot[1], bmo_spot[0], c='k', marker = 'x', label = 'BMO_FUNDEIO')
bmo_now = ax.plot(bmo_local[1], bmo_local[0], c='b', marker = 'h', label = "Current Position BMO")
bmo_distance = ax.plot([bmo_local[1],bmo_spot[1]], [bmo_local[0],bmo_spot[0]], c='g')
bmo_text = ax.annotate(str(hav_bmo['meters']) + ' m', xy = ((bmo_local[1] + bmo_spot[1])/2, (bmo_local[0] + bmo_spot[0])/2),
                                xytext=((bmo_local[1] + bmo_spot[1])/2, (bmo_local[0] + bmo_spot[0])/2),
                                bbox=dict(boxstyle="round", fc=(0, 0, 0), ec="none"),c = 'w')

## Radius Safe BMO
range_bmo = ax.plot(safe_range_bmo_lon, safe_range_bmo_lat,c='k', marker = '.', label = 'Range_BMO')
fill_range = ax.fill(safe_range_bmo_lon, safe_range_bmo_lat, c='w', alpha=0.3)
#Estimated center point:


###
#far_points = incremental_farthest_search(safe_circle, 2)



center_bmo = ax.plot(center_lon, center_lat,c='k', marker = '.', label = 'CENTER_BMO')
#center_bmo2 = ax.plot(center_lon2, center_lat2,c='k', marker = '.', label = 'CENTER_BMO2')
#
#test_circle = ax.plot(circle_lon, circle_lat, c='g', marker='.', label = 'CircleFunc')

ax.legend(loc = 'upper left')
gr = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
                  linewidth=0.5, color='k', linestyle='--')

gr.top_labels = False
gr.right_labels = False
gr.xlabel_style = {'size': 10}
gr.ylabel_style = {'size': 10}
plt.show()

