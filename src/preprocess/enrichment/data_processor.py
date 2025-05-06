import preprocess.airport_constants as ac
import math
import preprocess.utilities as ut
import pyspark.sql.functions as f

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
            df (pyspark.sql.DataFrame): DataFrame consolidado con los datos filtrados y procesados.
        """
        # Leemos todos los archivos Parquet coincidentes con el patrón
        df = spark.read.parquet(file_pattern).select(*selected_columns)

        df = df.withColumn("Timestamp (date)", f.to_timestamp(f.col("Timestamp (date)")))
        df = df.withColumn("hour", f.date_format(f.col("Timestamp (date)"), "yyyy-MM-dd HH:00:00"))
        df = df.withColumn("day_of_week", f.date_format(f.col("Timestamp (date)"), "E"))

        return df

    @staticmethod
    def get_status(df):
        """
        Calcula el número de vuelos únicos por estado de vuelo y hora del día.

        Parámetros:
            df (pyspark.sql.DataFrame): DataFrame con los datos de vuelos.

        Devuelve:
            df_status (pyspark.sql.DataFrame): DataFrame con la cantidad de vuelos agrupados por hora y estado de vuelo.
        """
        df_status = df.groupBy("hour", "Flight status").agg(f.countDistinct("Callsign").alias("count_nonzero"))

        return df_status

    @staticmethod
    def get_wait_times(dff):
        """
        Calcula los tiempos de espera en tierra antes del despegue para cada vuelo.

        Parámetros:
            dff (pyspark.sql.DataFrame): DataFrame con datos de vuelos combinados.

        Devuelve:
            df_wait_times (pyspark.sql.DataFrame): DataFrame con los tiempos de espera en tierra y otras métricas asociadas.
        """
        # Filtra vuelos en tierra (on-ground, velocidad = 0) y obtener el primer timestamp por Callsign
        on_ground = (
            dff.filter((f.col("Flight status") == "on-ground") & (f.col("Speed") == 0))
            .groupBy("Callsign")
            .agg(min("Timestamp (date)").alias("ts_ground"))
        )

        # Filtra vuelos en el aire (airborne) y obtener el primer timestamp por Callsign
        airborne = (
            dff.filter(f.col("Flight status") == "airborne")
            .groupBy("Callsign")
            .agg(min("Timestamp (date)").alias("ts_airborne"))
        )

        # Une ambas tablas por Callsign
        df_wait_times = on_ground.join(airborne, "Callsign", "inner")

        # Filtra solo los casos donde el vuelo realmente despegó después de estar en tierra
        df_wait_times = df_wait_times.filter(f.col("ts_airborne") > f.col("ts_ground"))

        # Calcula tiempo de espera en segundos
        df_wait_times = df_wait_times.withColumn(
            "Wait time (s)", f.unix_timestamp("ts_airborne") - f.unix_timestamp("ts_ground")
        )

        # Extrae el día de la semana
        df_wait_times = df_wait_times.withColumn("day_of_week", f.date_format(f.col("ts_ground"), "E"))

        return df_wait_times

    @staticmethod
    def is_in_holding_point(lat, lon):
        """Indica si un avión se encuentra en un punto de espera del aeropuerto.
        
        Args:
            lat (float): Latitud del avión.
            lon (float): Longitud del avión.
        Returns:
            (bool): True si el avión está en un punto de espera, False en caso contrario.
        """
        TOLERANCE_RADIUS = 0.01 # km (10 metros)

        for point in ac.HOLDING_POINTS:

            # Coordenadas del punto de espera
            point_lat = point[1]
            point_lon = point[0]

            # Distancia entre el punto de espera y el avión
            distancia = ut.haversine(point_lat, point_lon, lat, lon)

            if distancia is None:
                return False
            if distancia <= TOLERANCE_RADIUS:
                #print(f"El avión en posición ({lat}, {lon}) está en el punto de espera ({point_lat}, {point_lon}) a una distancia de: {distancia:.2f} km)")
                return True
            
        return False
