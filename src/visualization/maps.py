import folium
import preprocess.airport_constants as ac
from folium.plugins import HeatMapWithTime
from folium.plugins import MarkerCluster
import movingpandas as mpd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import geopy.distance
import re
import pandas as pd
from branca.colormap import linear

class Maps:
    """Clase encargada de generar mapas."""

    CATEGORY_COLORS = {
        'Reserved': 'grey',
        'No category information': 'lightgrey',
        'Surface emergency vehicle': 'red',
        'Surface service vehicle': 'blue',
        'Ground obstruction': 'orange',
        'Glider, sailplane': 'green',
        'Lighter-than-air': 'purple',
        'Parachutist, skydiver': 'yellow',
        'Ultralight, hang-glider, paraglider': 'pink',
        'Unmanned aerial vehicle': 'cyan',
        'Space or transatmospheric vehicle': 'brown',
        'Light (less than 7000 kg)': 'lightgreen',
        'Medium 1 (between 7000 kg and 34000 kg)': 'lightblue',
        'Medium 2 (between 34000 kg to 136000 kg)': 'lime',
        'High vortex aircraft': 'magenta',
        'Heavy (larger than 136000 kg)': 'black',
        'High performance (>5 g acceleration) and high speed (>400 kt)': 'darkblue',
        'Rotorcraft': 'darkgreen',
    }

    @staticmethod
    def getTitleHTML(title):
        """
        Genera el título del mapa en html.

        Parámetros:
            title (str): Nombre del mapa.
        """
        return f'''
        <div style="position: fixed; 
                    bottom: 50px; left: 50%; transform: translateX(-50%); width: auto; 
                    background-color: black; opacity: 0.7; z-index: 9999; 
                    border-radius: 5px; padding: 10px; font-size: 14px; color: white; text-align: center;">
            <strong>{title}</strong>
        </div>
        '''

    @staticmethod
    def getRadarMarker():
        """
        Genera el icono del radar para que se muestre en el mapa.
        
        Devuelve:
            folium.Marker: Un marker de folium correspondiente al radar.
        """
        icon = folium.CustomIcon("./visualization/custom_icons/radar_icon.png", icon_size=(30,30))
        return folium.Marker(
                location=[ac.RADAR_POSITION[0], ac.RADAR_POSITION[1]],
                popup="RADAR",
                icon=icon,
            )
    
    @staticmethod
    def getRunwayMarkers():
        """
        Genera los iconos de las pistas del aeropuerto.
        
        Devuelve:
            folium.Marker (list): Lista con los marcadores.
        """
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
    def positionsScatterMap(df, title="Flight Status Scatter Map"):
        """
        Genera un Scatter Map.
    
        Parámetros:
            df (pandas.DataFrame): Dataframe con las siguientes columnas: "ICAO", "lat", "lon", "Flight status"
            title (string): Título del mapa
        
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
            # Añadimos un hover con la información de cada punto
            tooltip_text = f"""
                ICAO: {row['ICAO']}<br>
                Flight status: {row['Flight status']}<br>
                Latitud: {row['lat']:.5f}<br>
                Longitud: {row['lon']:.5f}
            """

            folium.Circle(
                location=[row['lat'], row['lon']],
                radius=7,
                tooltip=folium.Tooltip(tooltip_text, sticky=True),
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

        # - Título -
        m.get_root().html.add_child(folium.Element(Maps.getTitleHTML(title)))
        
        return m


    @staticmethod
    def trajectoriesMap(df):
        """
        Genera un mapa con las trayectorias clasificadas por avión y tipo de vuelo.
    
        Parámetros:
            df (pandas.DataFrame): Dataframe con las siguientes columnas: "Timestamp (date)", "lat", "lon", "ICAO", "Callsign", "TurbulenceCategory".
    
        Retorna:
            folium.map: Mapa con las trayectorias.
        """
        
        center = [ac.RADAR_POSITION[0], ac.RADAR_POSITION[1]]
    
        # Creamos el mapa
        m = folium.Map(location=center, tiles="Cartodb Positron", zoom_start=13)
    
        # Grupos de capas
        group1 = MarkerCluster(name="Locations").add_to(m)
        group2 = folium.FeatureGroup("Take-offs").add_to(m)
        group3 = folium.FeatureGroup("Landings").add_to(m)
        group4 = folium.FeatureGroup("Other flights").add_to(m)
        
        # Marcadores
        Maps.getRadarMarker().add_to(group1)
        rwMarkers = Maps.getRunwayMarkers()
        for mk in rwMarkers:
            mk.add_to(group1)

        # Generamos las trayectorias
        trajs = mpd.TrajectoryCollection(
            df,
            traj_id_col="Callsign",
            obj_id_col="ICAO",
            t="Timestamp (date)",
            x="lon",
            y="lat"
        )

        # Generalizamos las trayectorias (submuestreo)
        TOLERANCE = 3 # cuanto más grande menos puntos hay
        generalized_trajs = mpd.TopDownTimeRatioGeneralizer(trajs).generalize(tolerance=TOLERANCE)

        # Umbral de proximidad a la antena (para clasificar despegues y aterrizajes)
        PROXIMITY_THRESHOLD = 10 # en km
        
        # Recorremos las trayectorias
        for traj in generalized_trajs:

            # Primer y último punto
            first_point = traj.df.iloc[0]
            last_point = traj.df.iloc[-1]

            # Primera y última posición
            lon_first, lat_first = first_point['geometry'].x, first_point['geometry'].y
            lon_last, lat_last = last_point['geometry'].x, last_point['geometry'].y

            dist_first = geopy.distance.distance(ac.RADAR_POSITION, (lat_first, lon_first)).km
            dist_last = geopy.distance.distance(ac.RADAR_POSITION, (lat_last, lon_last)).km
            
            # Dibujamos la trayectoria y la clasificamos
            if dist_first < PROXIMITY_THRESHOLD:  
                group = group2
            elif dist_last < PROXIMITY_THRESHOLD:
                group = group3
            else:
                group = group4
            
            # Hover personalizado para cada trayectoria
            hover_text = (
                f"<strong>ICAO:</strong> {first_point['ICAO']}<br>"
                f"<strong>Callsign:</strong> {first_point['Callsign']}<br>"
                f"<strong>Turbulence Category:</strong> {first_point['TurbulenceCategory']}<br>"
                f"<strong>Flight Status:</strong> {'Take-off' if dist_first < PROXIMITY_THRESHOLD else ('Landing' if dist_last < PROXIMITY_THRESHOLD else 'In flight')}"
            )

            # Coordenadas de la trayectoria
            trajectory_points = [[row['geometry'].y, row['geometry'].x] for _, row in traj.df.iterrows()]

            # Trayectoria con hover
            folium.PolyLine(
                locations=trajectory_points,
                color=Maps.CATEGORY_COLORS[first_point["TurbulenceCategory"]],
                weight=3,
                opacity=0.8,
                tooltip=folium.Tooltip(hover_text, sticky=True, direction="top")
            ).add_to(group)

            # Dibujamos un círculo al final de la trayectoria para indicar sentido
            folium.Circle(
                location=[lat_last, lon_last],
                radius=120,
                color="black",
                fill=True,
                fill_color=Maps.CATEGORY_COLORS[first_point["TurbulenceCategory"]],
                opacity=0.5
            ).add_to(group)


        folium.LayerControl().add_to(m)

        # - Leyenda -
        
        legend_html = '''
            <div style="position: fixed; 
                        bottom: 50px; right: 50px; width: 225px; height: 370px; 
                        background-color: black; opacity: 0.7; z-index: 9999; 
                        border-radius: 5px; padding: 10px; font-size: 12px; color: white;">
                <strong>Aircraft Categories</strong><br>
        '''
                
        def remove_parentheses(text):
            return re.sub(r'\(.*?\)', '', text).strip()

        for category, color in Maps.CATEGORY_COLORS.items():
            clean_category = remove_parentheses(category) 
            legend_html += f'<i style="background-color: {color}; width: 10px; height: 10px; display: inline-block; border-radius: 50%; margin-top: 3px;"></i> {clean_category}<br>'
        
        legend_html += '</div>'

        m.get_root().html.add_child(folium.Element(legend_html))
            
        return m
    
    @staticmethod
    def altitudesMap(df):
        """
        Genera un mapa con las trayectorias coloreadas por altura.

        Parámetros:
            df (pandas.DataFrame): obtenido a partir de la función `getAltitudes` del módulo dataframe_processor. Columnas: "Timestamp (date)", "lat", "lon", "ICAO", 
            "Callsign", "TurbulenceCategory", "Altitude (ft)".

        Retorna:
            folium.Map: Mapa con trayectorias degradadas por altura.
        """

        # Creamos un colormap para la altura
        min_alt, max_alt = df["Altitude (ft)"].min(), df["Altitude (ft)"].max()
        colormap = linear.Set2_04.scale(min_alt, max_alt)
        colormap.caption = 'Altitude (ft)'

        center = [ac.RADAR_POSITION[0], ac.RADAR_POSITION[1]]
        m = folium.Map(location=center, tiles="Cartodb Positron", zoom_start=13)

        # Grupos de capas
        group1 = MarkerCluster(name="Locations").add_to(m)
        group4 = folium.FeatureGroup("Flights by Altitude").add_to(m)

        # Marcadores
        Maps.getRadarMarker().add_to(group1)
        for mk in Maps.getRunwayMarkers():
            mk.add_to(group1)

        # Generamos las trayectorias
        trajs = mpd.TrajectoryCollection(
            df,
            traj_id_col="Callsign",
            obj_id_col="ICAO",
            t="Timestamp (date)",
            x="lon",
            y="lat"
        )

        # Dibujamos cada trayectoria con degradado por altura
        for traj in trajs:
            traj_df = traj.df.sort_values("Timestamp (date)")

            # Calculamos la altitud media de la trayectoria para usarla en el tooltip
            avg_altitude = traj_df["Altitude (ft)"].mean()
            # Tooltip para la trayectoria completa
            trajectory_tooltip = f"ICAO: {traj.df['ICAO'].iloc[0]}<br>Callsign: {traj.df['Callsign'].iloc[0]}<br>Altitud media: {avg_altitude:.0f} pies"

            for i in range(len(traj_df) - 1):
                p1 = traj_df.iloc[i]
                p2 = traj_df.iloc[i + 1]
                
                # Color basado en la altura promedio del segmento
                avg_alt = (p1["Altitude (ft)"] + p2["Altitude (ft)"]) / 2
                color = colormap(avg_alt)

                folium.PolyLine(
                    locations=[
                        [p1['geometry'].y, p1['geometry'].x],
                        [p2['geometry'].y, p2['geometry'].x]
                    ],
                    color=color,
                    weight=3,
                    opacity=0.9,
                    tooltip=f"ICAO: {traj.df['ICAO'].iloc[0]}<br>Callsign: {traj.df['Callsign'].iloc[0]}<br>Altitude: {avg_alt:.0f} ft"
                ).add_to(group4)

            # Círculo final para indicar el sentido
            last_point = traj_df.iloc[-1]
            folium.Circle(
                location=[last_point['geometry'].y, last_point['geometry'].x],
                radius=120,
                color="black",
                fill=True,
                fill_color=color,
                opacity=0.7,
                tooltip=trajectory_tooltip
            ).add_to(group4)

        # Añadimos leyenda y controles
        colormap.add_to(m)
        folium.LayerControl().add_to(m)

        return m
