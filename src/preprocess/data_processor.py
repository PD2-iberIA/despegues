import preprocess.airport_constants as ac
import math
import plotly.express as px
from preprocess.utilities import stringToNan
import preprocess.utilities as ut
from preprocess.dataframe_processor import DataframeProcessor
import pandas as pd
import glob

# Radio de la Tierra (km)
EARTH_RADIUS = 6378

# 1 milla náutica (NM) = 1.852 km
NM_KM_EQUIVALENCE = 1852
MAXIMUM_RANGE = 180 * NM_KM_EQUIVALENCE

class DataProcessor:
    """Clase que incluye funciones para procesar los datos una vez han sido codificados."""

    @staticmethod
    def airborne_position_is_valid(lat, lon, h):
        """
        Determina si la posición decodificada de un avión en el aire es válida.
        
        Parámetros:
            lat, lon: coordenadas del avión
            h: altura del avión

        Devuelve:
            boolean: indica si la posición es válida (True) o no (False)
        """
        inside_range = True

        # La distancia entre el avión y el radar ha de ser menor de 180 NM
        distance = DataProcessor.radar_aircraft_distance(lat, lon)
        if (distance > MAXIMUM_RANGE):
            inside_range = False

        # La posición del avión deberá estar dentro del rango máximo del radar
        # El rango máximo depende de la altura del avión
        radar_range = DataProcessor.radar_maximun_range(h)
        if (distance > radar_range):
            inside_range = False

        return inside_range
    
    @staticmethod
    def radar_maximun_range(h_t, h_r=0):
        """
        Calcula el rango máximo de alcance del radar.

        Parámetros:
            h_t (float): altura del avión
            h_r (float): altura del radar
        
        Devuelve:
            float: rango máximo del radar (km)
        """
        R = EARTH_RADIUS
        alpha_r = math.acos(R / (R + h_r))
        alpha_t = math.acos(R / (R + h_t))
        d = (alpha_r + alpha_t) * R

        return d
    
    @staticmethod
    def radar_aircraft_distance(lat, lon):
        """
        Calcula la distancia entre una aeronave y el radar. Para ello utiliza la fórmula de
        Haversine, que mide la distancia entre dos puntos en la superficie de la Tierra.
        Para validar si el avión está dentro de 180 NM, la distancia Haversine por sí sola será 
        suficiente porque la altitud tiene un impacto muy pequeño en la distancia total.

        Parámetros:
            lat, lon: coordenadas del avión

        Devuelve:
            float: distancia entre el radar y el avión (km)
        """
        # Coordenadas de referencia (radar)
        lat_ref, lon_ref = ac.RADAR_POSITION

        # Convertir grados a radianes
        lat_ref, lon_ref, lat, lon = map(math.radians, [lat_ref, lon_ref, lat, lon])

        dlat = lat - lat_ref
        dlon = lon - lon_ref

        # Fórmula de Haversine
        a = math.sin(dlat / 2)**2 + math.cos(lat_ref) * math.cos(lat) * math.sin(dlon / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = EARTH_RADIUS * c

        return distance

    @staticmethod
    def readWithColumnsFilter(file_pattern, selected_columns):
        """
        Lee múltiples archivos Parquet, selecciona columnas específicas y procesa los datos.

        Parámetros:
            file_pattern (str): Patrón de búsqueda para encontrar los archivos Parquet.
            selected_columns (list): Lista de columnas a seleccionar de los archivos.

        Devuelve:
            pd.DataFrame: DataFrame consolidado con los datos filtrados y procesados.
        """
        file_list = sorted(glob.glob(file_pattern))
        df_list = [stringToNan(pd.read_parquet(file)[selected_columns]) for file in file_list]
        df = pd.concat(df_list, ignore_index=True)

        df['Timestamp (date)'] = pd.to_datetime(df['Timestamp (date)'])
        df['hour'] = df['Timestamp (date)'].dt.floor('h')
        df['day_of_week'] = df['Timestamp (date)'].dt.strftime('%a')

        return df

    @staticmethod
    def get_dff(df):
        """
        Fusiona los datos de velocidad y estado de vuelo en un solo DataFrame basado en la proximidad temporal.

        Parámetros:
            df (pd.DataFrame): DataFrame con los datos de vuelo sin procesar.

        Devuelve:
            pd.DataFrame: DataFrame combinado con información de velocidad y estado de vuelo.
        """
        df1 = DataframeProcessor.getVelocities(df)
        df2 = DataframeProcessor.getFlights(df)

        df1_s = df1.sort_values(["Timestamp (date)", "ICAO"])
        df2_s = df2.sort_values(["Timestamp (date)", "ICAO"])

        t = pd.Timedelta('10 minute')
        dff = pd.merge_asof(df1_s, df2_s, on="Timestamp (date)", by="ICAO", direction="nearest", tolerance=t)

        dff['Timestamp (date)'] = pd.to_datetime(dff['Timestamp (date)'])
        dff = ut.extractHour(dff)
        dff = ut.extractDaysOfTheWeek(dff)

        return dff

    @staticmethod
    def get_status(df):
        """
        Calcula el número de vuelos por estado de vuelo y hora del día.

        Parámetros:
            df (pd.DataFrame): DataFrame con los datos de vuelos.

        Devuelve:
            pd.DataFrame: DataFrame con la cantidad de vuelos agrupados por hora y estado de vuelo.
        """
        df_status = df.groupby(['hour', 'Flight status', 'Callsign']).size().unstack(fill_value=0)
        df_status['count_nonzero'] = (df_status.ne(0)).sum(axis=1)
        df_status = df_status.reset_index()
        df_status = df_status.groupby(['hour', 'Flight status'])['count_nonzero'].sum().reset_index()

        return df_status

    @staticmethod
    def get_wait_times(dff):
        """
        Calcula los tiempos de espera en tierra antes del despegue para cada vuelo.

        Parámetros:
            dff (pd.DataFrame): DataFrame con datos de vuelos combinados.

        Devuelve:
            pd.DataFrame: DataFrame con los tiempos de espera en tierra y otras métricas asociadas.
        """
        on_ground = dff[(dff["Flight status"] == "on-ground") & (dff["Speed"] == 0)].groupby("Callsign")[
            "Timestamp (date)"].min()
        airborne = dff[dff["Flight status"] == "airborne"].groupby("Callsign")["Timestamp (date)"].min()

        on_ground = pd.DataFrame(on_ground)
        on_ground.columns = ["ts ground"]
        airborne = pd.DataFrame(airborne)
        airborne.columns = ["ts airborne"]

        df_wait_times = on_ground.merge(airborne, how="inner", on="Callsign")
        df_wait_times = df_wait_times[df_wait_times["ts airborne"] > df_wait_times["ts ground"]]
        df_wait_times["Wait time"] = df_wait_times["ts airborne"] - df_wait_times["ts ground"]
        df_wait_times["Wait time (s)"] = df_wait_times["Wait time"].dt.total_seconds()
        df_wait_times['day_of_week'] = df_wait_times['ts ground'].dt.strftime('%a')

        return df_wait_times


