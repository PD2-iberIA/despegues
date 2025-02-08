import folium
import preprocess.airport_constants as ac
from folium.plugins import HeatMapWithTime

class Maps:
    """Clase encargada de generar mapas"""

    @staticmethod
    def positionsHeatMap(df, freq=5):
        """
        Genera un mapa de calor animado en función de una.
    
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
    def getRadarMarker():
        return folium.Marker(
                location=[ac.RADAR_POSITION[0], ac.RADAR_POSITION[1]],
                popup="RADAR",
                icon=folium.Icon(icon='star'),
            )