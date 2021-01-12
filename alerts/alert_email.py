



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

hav_axys = haversine(axys_spot, axys_local)




bmo_local = conn.last_positions('BMO',2,1)
bmo_spot = [float(bmo_spot['lat']), float(bmo_spot['lon'])]
bmo_local = [float(bmo_local['lat']), float(bmo_local['lon'])]

hav_bmo = haversine(bmo_spot, bmo_local)


import plotly.express as px
import plotly.graph_objects as go

pts_bmo = conn.last_positions('BMO', 2, 800)
pts_axys = conn.last_positions('AXYS', 1, 300)


import cartopy.crs as ccrs
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import cartopy.feature as cfeature
import numpy as np


# BMO
pts_lat_bmo = (pts_bmo['lat'].values).astype(np.float)
pts_lon_bmo = (pts_bmo['lon'].values).astype(np.float)

# AXYS
pts_lat_axys = (pts_axys['lat'].values).astype(np.float)
pts_lon_axys = (pts_axys['lon'].values).astype(np.float)

pts_lon_axys = (pts_lon_axys+180)%360 - 180


ax = plt.axes(projection=ccrs.PlateCarree())
ax.add_feature(cfeature.LAND)
ax.add_feature(cfeature.COASTLINE)
ax.set_xlim(float(pts_bmo['lon'][0])-3, float(pts_bmo['lon'][0])+3)
ax.set_ylim(float(pts_bmo['lat'][0])-3, float(pts_bmo['lat'][0])+3)
bmo_points = ax.plot(pts_lon_bmo,pts_lat_bmo, c='r', marker='o' ,label = 'BMO')
axys_point = ax.plot(pts_lon_axys, pts_lat_axys, c = 'b', marker = 'o', label = 'AXYS')
ax.legend(loc = 'upper left')
gr = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
                  linewidth=0.5, color='gray', alpha=0.6, linestyle='--')

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


