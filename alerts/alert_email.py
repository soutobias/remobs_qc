
import numpy as np
from bd_system import db_remo
from harvesine import *



## BMO

conn = db_remo()
buoys = conn.active_buoys()

# buoys
axys_spot = buoys[['lat','lon']][buoys['buoy_id']==1]
bmo_spot = buoys[['lat','lon']][buoys['buoy_id']==2]
spotter_spot = buoys[['lat','lon']][buoys['buoy_id']==3]

## AXYS

axys_local = conn.last_positions('AXYS','1',1)

axys_spot = [float(axys_spot['lat']), float(axys_spot['lon'])]

# Converting LON to -180,180

axys_local_lon = (float(axys_local['lon'])+180)%360 - 180
axys_local = [float(axys_local['lat']), axys_local_lon]

#hav_axys = haversine(axys_spot, axys_local)




bmo_local = conn.last_positions('BMO',2,1)
bmo_spot = [float(bmo_spot['lat']), float(bmo_spot['lon'])]
bmo_local = [float(bmo_local['lat']), float(bmo_local['lon'])]

hav_bmo = haversine(bmo_spot, bmo_local)


import plotly.express as px
import plotly.graph_objects as go

pts_bmo = conn.last_positions('BMO', 2, 1000)
#pts_axys = conn.last_positions('AXYS', 1, 200)


import cartopy.crs as ccrs
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import cartopy.feature as cfeature
import numpy as np


# BMO
pts_lat_bmo = (pts_bmo['lat'].values).astype(np.float)
pts_lon_bmo = (pts_bmo['lon'].values).astype(np.float)

coords_bmo = [(pts_lon_bmo[p],pts_lat_bmo[p]) for p in range(len(pts_lat_bmo))]
#
# # AXYS
# pts_lat_axys = (pts_axys['lat'].values).astype(np.float)
# pts_lon_axys = (pts_axys['lon'].values).astype(np.float)

#pts_lon_axys = (pts_lon_axys+180)%360 - 180


safe_circle, safe_range_bmo_lat, safe_range_bmo_lon = safe_range_circle(float(bmo_spot[0]), float(bmo_spot[1]), 1000)

# Find Center
center_lon, center_lat = find_centroid(pts_lon_bmo, pts_lat_bmo)
# Second Center
center_lon2, center_lat2 = find_centroid(lon_farthest_point, lat_farthest_point)

safe_circle2, safe_range_bmo_lat2, safe_range_bmo_lon2 = safe_range_circle(center_lon2, center_lat2, 1500)
lon_in, lat_in = find_outer_points(pts_lon_bmo, pts_lat_bmo, bmo_spot[1],bmo_spot[0])








##### PLOT
### BMO PLOT
ax = plt.axes(projection=ccrs.PlateCarree())
ax.set(facecolor = "#5ACEFF")
ax.add_feature(cfeature.LAND)
ax.add_feature(cfeature.COASTLINE)

ax.set_xlim(float(pts_bmo['lon'][0])-0.08, float(pts_bmo['lon'][0])+0.08)
ax.set_ylim(float(pts_bmo['lat'][0])-0.08, float(pts_bmo['lat'][0])+0.08)
#bmo_points = ax.plot(pts_lon_bmo,pts_lat_bmo, c='r', marker='o' ,label = 'BMO')
#bmo_points_inside = ax.plot(lon_inside, lat_inside, c='r', marker='o' ,label = 'BMO IN')
## BORDER POINTS
bmo_points_border = ax.plot(lon_farthest_point, lat_farthest_point, c='r', marker='o' ,label = 'BMO BORDER')
#axys_point = ax.plot(pts_lon_axys, pts_lat_axys, c = 'b', marker = 'o', label = 'AXYS')
bmo_fund = ax.plot(bmo_spot[1], bmo_spot[0], c='k', marker = 'x', label = 'BMO_FUNDEIO')
#axys_fund = ax.plot(axys_spot[1], axys_spot[0], c='k', marker = 'x', label = 'AXYS_FUNDEIO')
bmo_now = ax.plot(bmo_local[1], bmo_local[0], c='b', marker = 'h', label = "Current Position BMO")
bmo_distance = ax.plot([bmo_local[1],bmo_spot[1]], [bmo_local[0],bmo_spot[0]], c='g')
bmo_text = ax.annotate(str(hav_bmo['meters']) + ' m', xy = ((bmo_local[1] + bmo_spot[1])/2, (bmo_local[0] + bmo_spot[0])/2),
                                xytext=((bmo_local[1] + bmo_spot[1])/2, (bmo_local[0] + bmo_spot[0])/2),
                                bbox=dict(boxstyle="round", fc=(0, 0, 0), ec="none"),c = 'w')

## Radius Safe BMO
range_bmo = ax.plot(safe_range_bmo_lon2, safe_range_bmo_lat2,c='k', marker = '.', label = 'Range_BMO')
fill_range = ax.fill(safe_range_bmo_lon, safe_range_bmo_lat, c='w', alpha=0.3)
#Estimated center point:


###
#far_points = incremental_farthest_search(safe_circle, 2)



center_bmo = ax.plot(center_lon, center_lat,c='k', marker = '.', label = 'CENTER_BMO')
center_bmo2 = ax.plot(center_lon2, center_lat2,c='k', marker = '.', label = 'CENTER_BMO2')
#
test_circle = ax.plot(circle_lon, circle_lat, c='g', marker='.', label = 'CircleFunc')

ax.legend(loc = 'upper left')
gr = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
                  linewidth=0.5, color='k', linestyle='--')

gr.top_labels = False
gr.right_labels = False
gr.xlabel_style = {'size': 10}
gr.ylabel_style = {'size': 10}
plt.show()







        ax.add_feature(cfeature.LAND)
        ax.add_feature(cfeature.COASTLINE)
        # ax.add_feature(states_provinces, edgecolor='gray')
        # ax.add_feature(cfeature.NaturalEarthFeature('physical', 'land', '50m', edgecolor='face', facecolor='0.8'))
        ax.set_xlim(metareas['Lon_1'][area] , metareas['Lon_2'][area])
        ax.set_ylim(metareas['Lat_1'][area] , metareas['Lat_2'][area])


        # Colocando a zona
        gr = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
                     linewidth=0.5, color='gray', alpha=0.6, linestyle='--')

        gr.top_labels = False
        gr.right_labels = False
        gr.xlabel_style = {'size':6}
        gr.ylabel_style = {'size':6}

        # Lines zones

        for zona in range(len(lats)):
            ax.plot(lons[zona],lats[zona],linewidth=0.5,color='k')



        ax.set_title("Área %s" % metareas['Area'][area])
        ax.set_box_aspect(1)



        # Colocando os ponto
        ax.scatter(dados_sst['lon'],dados_sst['lat'], c = dados_sst['sst'], s = 1.6,cmap = 'Reds')


        # Plotando temp em cada ponto :
        ## Condicional em Golf para mudar a posição dos valores
        for pos in range(len(dados_sst)):
            if metareas['Area'][area] == 'Golf':
                if dados_sst['lat'][pos] > metareas['Lat_1'][area] and dados_sst['lat'][pos] < metareas['Lat_2'][area]:
                    ax.annotate(str(round(dados_sst['sst'][pos],1)),xy=(dados_sst['lon'][pos]+0.15,dados_sst['lat'][pos]+0.15),color='k',fontsize=4, fontweight = 'bold')
            else:
                if dados_sst['lat'][pos] > metareas['Lat_1'][area] and dados_sst['lat'][pos] < metareas['Lat_2'][area]:
                    ax.annotate(str(round(dados_sst['sst'][pos],1)),xy=(dados_sst['lon'][pos]+0.15,dados_sst['lat'][pos]),color='k',fontsize=4, fontweight = 'bold')



        # set a margin around the data


        ax.margins(0.005,0.005)



fig.subplots_adjust(top=0.9, bottom=0.08, hspace = 0.05)
fig.suptitle("TSM - %s" % data_plot, size = 16, y = 0.95)
plt.savefig("tsm_ultimo_dado.jpg", dpi = 300)



####
####
#### Wave Dir BMO ####

conn = db_remo()
bmo_wv = conn.get_data("SELECT date_time, wvdir1, wvdir2 from bmo_br where date_time >= '2020-12-10' and date_time < '2020-12-17' order by date_time;")
conn_qc = db_remo_qc()
qc_data = conn_qc.get_data("SELECT date_time, wvdir1, wvdir2 from data_buoys where buoy_id = 2 and date_time >= '2020-12-10' and date_time < '2020-12-17' order by date_time;")


norte_verdadeiro_zero_correcao = -22.61
norte_verdadeiro_vitoria = -23.80
norte_verdadeiro_fundeio_vitoria = norte_verdadeiro_zero_correcao - norte_verdadeiro_vitoria



wv_axys = bmo_wv.wvdir1
wv_sbg = bmo_wv.wvdir2
wv_sbg = wv_sbg.mask(wv_sbg > 400)
wv_sbg = wv_sbg.mask(wv_sbg < -1)


wv_axys = qc_data['wvdir1']
wv_sbg = qc_data['wvdir2']




# SEM CORRECAO
plt.cla()
#plt.plot(new_time_spotter, spotter_df['wvht'], label='Spotter Buoy - Observed', marker = 'o', linewidth=0.6, markersize=0.7, alpha = 0.5)
plt.plot(bmo_wv.date_time, wv_axys, label='Axys Sensor', linewidth=1.2, color = 'red',marker = 'o')
#plt.plot(time_model, model_remo['swvht'], label = 'WaveWatch | GFS + ICON - Model', marker = 'h',linewidth=0.6, markersize = 0.5, alpha=0.5)
plt.plot(bmo_wv.date_time, wv_sbg, label = 'SBG Sensor',linewidth=1.2, color = 'navy',marker = 'o')
plt.xlabel("DATE TIME")
plt.ylabel("Dir")
plt.legend()
plt.title("Wave Direction")
plt.grid(color='black', linestyle='-', linewidth=0.1)


# Dados Qualificados


# SEM CORRECAO


plt.cla()
#plt.plot(new_time_spotter, spotter_df['wvht'], label='Spotter Buoy - Observed', marker = 'o', linewidth=0.6, markersize=0.7, alpha = 0.5)
plt.plot(qc_data.date_time, wv_axys, label='Axys Sensor', linewidth=1.2, color = 'black',marker = 'o')
#plt.plot(time_model, model_remo['swvht'], label = 'WaveWatch | GFS + ICON - Model', marker = 'h',linewidth=0.6, markersize = 0.5, alpha=0.5)
plt.plot(qc_data.date_time, wv_sbg, label = 'SBG Sensor',linewidth=1.2, color = 'red',marker = 'o')
plt.xlabel("DATE TIME")
plt.ylabel("Dir")
plt.legend()
plt.title("Wave Direction")
plt.grid(color='black', linestyle='-', linewidth=0.1)



# Corrigindo: (MODO IGUAL MARCELO)
wv_axys = bmo_wv.wvdir1
wv_sbg = bmo_wv.wvdir2
wv_axys = wv_axys + norte_verdadeiro_zero_correcao
wv_sbg = wv_sbg - norte_verdadeiro_fundeio_vitoria
wv_sbg = wv_sbg.mask(wv_sbg > 400)
wv_sbg = wv_sbg.mask(wv_sbg < -1)


# Plot1

plt.cla()
#plt.plot(new_time_spotter, spotter_df['wvht'], label='Spotter Buoy - Observed', marker = 'o', linewidth=0.6, markersize=0.7, alpha = 0.5)
plt.plot(bmo_wv.date_time, wv_axys, label='Axys Sensor', linewidth=1.2, color = 'black',marker = 'o')
#plt.plot(time_model, model_remo['swvht'], label = 'WaveWatch | GFS + ICON - Model', marker = 'h',linewidth=0.6, markersize = 0.5, alpha=0.5)
plt.plot(bmo_wv.date_time, wv_sbg, label = 'SBG Sensor',linewidth=1.2, color = 'red',marker = 'o')
plt.xlabel("DATE TIME")
plt.ylabel("Dir")
plt.legend()
plt.title("Wave Direction")
plt.grid(color='black', linestyle='-', linewidth=0.1)



###########################################

# Corrigindo: (MODO 2)
wv_axys = bmo_wv.wvdir1
wv_sbg = bmo_wv.wvdir2
wv_axys = wv_axys - norte_verdadeiro_zero_correcao
wv_sbg = (wv_sbg + 23.80) - norte_verdadeiro_zero_correcao

wv_sbg = wv_sbg.mask(wv_sbg > 400)
wv_sbg = wv_sbg.mask(wv_sbg < -1)
# Plot1

plt.cla()
#plt.plot(new_time_spotter, spotter_df['wvht'], label='Spotter Buoy - Observed', marker = 'o', linewidth=0.6, markersize=0.7, alpha = 0.5)
plt.plot(bmo_wv.date_time, wv_axys, label='Axys Sensor', linewidth=1.2, color = 'black',marker = 'o')
#plt.plot(time_model, model_remo['swvht'], label = 'WaveWatch | GFS + ICON - Model', marker = 'h',linewidth=0.6, markersize = 0.5, alpha=0.5)
plt.plot(bmo_wv.date_time, wv_sbg, label = 'SBG Sensor',linewidth=1.2, color = 'red',marker = 'o')
plt.xlabel("DATE TIME")
plt.ylabel("Dir")
plt.legend()
plt.title("Wave Direction")
plt.grid(color='black', linestyle='-', linewidth=0.1)



# Corrigindo: (MODO 3)
wv_axys = wv_axys - norte_verdadeiro_zero_correcao
wv_sbg = wv_sbg - norte_verdadeiro_fundeio_vitoria

wv_sbg = wv_sbg.mask(wv_sbg > 400)
wv_sbg = wv_sbg.mask(wv_sbg < -1)
# Plot1

plt.cla()
#plt.plot(new_time_spotter, spotter_df['wvht'], label='Spotter Buoy - Observed', marker = 'o', linewidth=0.6, markersize=0.7, alpha = 0.5)
plt.plot(bmo_wv.date_time, wv_axys, label='Axys Sensor', linewidth=1.2, color = 'black',marker = 'o')
#plt.plot(time_model, model_remo['swvht'], label = 'WaveWatch | GFS + ICON - Model', marker = 'h',linewidth=0.6, markersize = 0.5, alpha=0.5)
plt.plot(bmo_wv.date_time, wv_sbg, label = 'SBG Sensor',linewidth=1.2, color = 'red',marker = 'o')
plt.xlabel("DATE TIME")
plt.ylabel("Dir")
plt.legend()
plt.title("Wave Direction")
plt.grid(color='black', linestyle='-', linewidth=0.1)

