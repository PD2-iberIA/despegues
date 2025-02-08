import folium
import preprocess.airport_constants as ac
from folium.plugins import HeatMapWithTime
from folium.plugins import MarkerCluster

class Maps:
    """Clase encargada de generar mapas"""

    @staticmethod
    def getRadarMarker():

        icon = folium.CustomIcon("./custom_icons/radar_icon.png", icon_size=(30,30))
        return folium.Marker(
                location=[ac.RADAR_POSITION[0], ac.RADAR_POSITION[1]],
                popup="RADAR",
                icon=icon,
            )
    
    @staticmethod
    def getRunwayMarkers():
        runways = [ac.RUNWAY_1, ac.RUNWAY_2, ac.RUNWAY_3, ac.RUNWAY_4]

        
        icons = [folium.CustomIcon("./visualization/custom_icons/runway_icon.png", icon_size=(40,40)) for i in range(len(runways))]
        
        return [
            folium.Marker(
                location=[rw["position"][0], rw["position"][1]],
                popup=f"RUNWAY {i + 1}",
                icon=icons[i],
            ) for i, rw in enumerate(runways)]
    
    @staticmethod
    def positionsHeatMap(df, freq=5):
        """
        Genera un mapa de calor animado en función de una frecuencia dada.
    
        Parámetros:
        df (pandas.DataFrame): Dataframe con las siguientes columnas: "Timestamp (date)", "lat", "lon"
        freq (int): Frecuencia en minutos
    
        Retorna:
        folium.map: Mapa de calor
        """
    
        center = [ac.RADAR_POSITION[0], ac.RADAR_POSITION[1]]
        
        # Creamos el mapa
        m = folium.Map(location=center, tiles="Cartodb Positron", zoom_start=8)
    
        # Marcadores
        Maps.getRadarMarker().add_to(m) # radar

        # Agruparemos los puntos 
        time_tolerance = pd.Timedelta(f"{freq/2}min") 
        time_steps = pd.date_range(df['Timestamp (date)'].min(), df['Timestamp (date)'].max(), freq=f"{freq}min")
        data = []

        # Generamos las nubes de puntos
        for time in time_steps:
            time_data = df[(df['Timestamp (date)'] >= time - time_tolerance) & (df['Timestamp (date)'] <= time + time_tolerance)][['lat', 'lon']].values.tolist()
            data.append(time_data)
        
        # Creamos el mapa
        heatmap = HeatMapWithTime(data, index=[str(time) for time in time_steps], auto_play=True)
        heatmap.add_to(m)

        return m

    @staticmethod
    def positionsScatterMap(df):
        """
        Genera un Scatter Map.
    
        Parámetros:
        df (pandas.DataFrame): Dataframe con las siguientes columnas: "ICAO", "lat", "lon", "Flight status"
    
        Retorna:
        folium.map: Scatter Map
        """

        center = [ac.RADAR_POSITION[0], ac.RADAR_POSITION[1]]
        
        # Creamos el mapa
        m = folium.Map(location=center, tiles="Cartodb Positron", zoom_start=13)

        # Grupos de capas
        group1 = MarkerCluster(
            name="Locations"
        ).add_to(m)
        group2 = folium.FeatureGroup("On-ground points").add_to(m)
        group3 = folium.FeatureGroup("Airborne points").add_to(m)
        
        # Marcadores
        Maps.getRadarMarker().add_to(group1)
        rwMarkers = Maps.getRunwayMarkers()
        for mk in rwMarkers:
            mk.add_to(group1)
        
        # Colores para estados de vuelo
        colores = {
            'airborne': 'DarkSlateBlue',
            'on-ground': 'MediumSeaGreen'
        }
        
        # Añadimos los puntos
        for i, row in df.iterrows():
            folium.Circle(
                location=[row['lat'], row['lon']],
                radius=7,
                tooltip=row['ICAO'],
                color=colores.get(row['Flight status'], 'black'),
                fill=True,
                fill_color=colores.get(row['Flight status'], 'black'),
                opacity=0.5
            ).add_to(group2 if row['Flight status'] == "on-ground" else group3)

        folium.LayerControl().add_to(m)

        # - Leyenda -
        legend_html = '''
        <div style="position: fixed; 
                    bottom: 50px; right: 50px; width: 150px; height: 100px; 
                    background-color: black; opacity: 0.7; z-index: 9999; 
                    border-radius: 5px; padding: 10px; font-size: 12px; color: white;">
            <strong>Flight Status</strong><br>
            <i style="background: MediumSeaGreen; width: 20px; height: 20px; display: inline-block; margin-right: 5px;"></i> On-ground<br>
            <i style="background: DarkSlateBlue; width: 20px; height: 20px; display: inline-block; margin-right: 5px;"></i> Airborne
        </div>
        '''
        m.get_root().html.add_child(folium.Element(legend_html))
        
        return m