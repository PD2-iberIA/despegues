# pipeline.py

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, radians, sin, cos, acos, col, count, when, udf, min, unix_timestamp, from_unixtime, round, date_trunc, last, row_number, hour
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, StructType, StructField, BooleanType
import math
from pyspark import StorageLevel



class Pipeline:
    """Clase que contiene el pipeline de enriquecimiento de datos para el análisis de vuelos en el aeropuerto de Barajas (MAD)."""
    AIRPORT_CENTER_LAT = 40.49291
    AIRPORT_CENTER_LON = -3.56974 
    RADIUS_AIRPORT = 5000 # 5 km

    # Diccionario de puntos de espera
    HOLDING_POINTS = [
        ("Z1", "36L/18R", -3.573093114831562, 40.490653160130186),
        ("KA6", "32R/14L", -3.537524367869231, 40.472076292010001),
        ("KA8", "32R/14L", -3.536653485337274, 40.466622566754253),
        ("K3", "32R/14L", -3.558959606954449, 40.494122669084419),
        ("K2", "32R/14L", -3.559326044131887, 40.4945961819448),
        ("K1", "32R/14L", -3.560411408421098, 40.495554592925956),
        ("Y1", "36R/18L", -3.560656492186808, 40.499431092287409),
        ("Y2", "36R/18L", -3.560645785166937, 40.500298406173002),
        ("Y3", "36R/18L", -3.560660061193443, 40.501183565039504),
        ("Y7", "36R/18L", -3.560800449906033, 40.533391949745102),
        ("Z6", "36L/18R", -3.576129307304151, 40.495184843931881),
        ("Z4", "36L/18R", -3.576034129003182, 40.492555539298088),
        ("Z2", "36L/18R", -3.575903257941006, 40.491865496230126),
        ("Z3", "36L/18R", -3.57319305240692, 40.491819096186241),
        ("LF", "32L/14R", -3.572566658955927, 40.479721203031424),
        ("L1", "32L/14R", -3.57652786733783, 40.483565816902733),
        ("LA", "32L/14R", -3.577181028787666, 40.484251101106899),
        ("LB", "32L/14R", -3.577553413710587, 40.484873329796898),
        ("LC", "32L/14R", -3.575750378154376, 40.486690643924192),
        ("LD", "32L/14R", -3.575150753600524, 40.486522892072891),
        ("LE", "32L/14R", -3.574915186586964, 40.485580625293494)
    ]
    RUNWAYS = [
        { "type": "Feature", "properties": { "Runway": "36R/18L" }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -3.560314698993372, 40.536214953564219 ], [ -3.560115619892884, 40.499859371026062 ], [ -3.558758924220036, 40.499866800139706 ], [ -3.558958003320524, 40.536222382677863 ], [ -3.560314698993372, 40.536214953564219 ] ] ] } },
        { "type": "Feature", "properties": { "Runway": "32R/14L" }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -3.560616443164626, 40.49648882920043 ], [ -3.530233771171787, 40.466560555412769 ], [ -3.529390530746063, 40.467416598679677 ], [ -3.559773202738903, 40.49734487246733 ], [ -3.560616443164626, 40.49648882920043 ] ] ] } },
        { "type": "Feature", "properties": { "Runway": "36L/18R" }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -3.575312220900675, 40.533035679421005 ], [ -3.575065318943644, 40.492052937902109 ], [ -3.574163865545885, 40.492058368739578 ], [ -3.574410767502916, 40.533041110258473 ], [ -3.575312220900675, 40.533035679421005 ] ] ] } },
        { "type": "Feature", "properties": { "Runway": "32L/14R" }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -3.545264836113798, 40.455701584425526 ], [ -3.576990231488946, 40.486626436878893 ], [ -3.577924321488386, 40.485668166355438 ], [ -3.546198926113238, 40.454743313902071 ], [ -3.545264836113798, 40.455701584425526 ] ] ] } }
    ]
    RUNWAY_POLYGONS = {
        r["properties"]["Runway"]: r["geometry"]["coordinates"][0]
        for r in RUNWAYS
    }
    RUNWAY_NAMES = [
        r["properties"]["Runway"]
        for r in RUNWAYS
    ]
    HP_NAMES = [
        hp[0]
        for hp in HOLDING_POINTS
    ]
    # Lista en json de festivos en España y en la Comunidad de Madrid extraída en local (no está instalada la librería 'holidays' en Zepelin)
    HOLIDAY_LIST_JSON = [
    {
        "date": "2024-01-01",
        "name": "A\u00f1o Nuevo"
    },
    {
        "date": "2024-01-06",
        "name": "Epifan\u00eda del Se\u00f1or"
    },
    {
        "date": "2024-03-29",
        "name": "Viernes Santo"
    },
    {
        "date": "2024-05-01",
        "name": "Fiesta del Trabajo"
    },
    {
        "date": "2024-08-15",
        "name": "Asunci\u00f3n de la Virgen"
    },
    {
        "date": "2024-10-12",
        "name": "Fiesta Nacional de Espa\u00f1a"
    },
    {
        "date": "2024-11-01",
        "name": "Todos los Santos"
    },
    {
        "date": "2024-12-06",
        "name": "D\u00eda de la Constituci\u00f3n Espa\u00f1ola"
    },
    {
        "date": "2024-12-25",
        "name": "Natividad del Se\u00f1or"
    },
    {
        "date": "2024-03-28",
        "name": "Jueves Santo"
    },
    {
        "date": "2024-05-02",
        "name": "Fiesta de la Comunidad de Madrid"
    },
    {
        "date": "2024-07-25",
        "name": "Santiago Ap\u00f3stol"
    },
    {
        "date": "2025-01-01",
        "name": "A\u00f1o Nuevo"
    },
    {
        "date": "2025-01-06",
        "name": "Epifan\u00eda del Se\u00f1or"
    },
    {
        "date": "2025-04-18",
        "name": "Viernes Santo"
    },
    {
        "date": "2025-05-01",
        "name": "Fiesta del Trabajo"
    },
    {
        "date": "2025-08-15",
        "name": "Asunci\u00f3n de la Virgen"
    },
    {
        "date": "2025-11-01",
        "name": "Todos los Santos"
    },
    {
        "date": "2025-12-06",
        "name": "D\u00eda de la Constituci\u00f3n Espa\u00f1ola"
    },
    {
        "date": "2025-12-08",
        "name": "Inmaculada Concepci\u00f3n"
    },
    {
        "date": "2025-12-25",
        "name": "Natividad del Se\u00f1or"
    },
    {
        "date": "2025-04-17",
        "name": "Jueves Santo"
    },
    {
        "date": "2025-05-02",
        "name": "Fiesta de la Comunidad de Madrid"
    },
    {
        "date": "2025-07-25",
        "name": "Santiago Ap\u00f3stol"
    }
    ]
    # Crear conjunto de fechas festivas (como strings "yyyy-MM-dd")
    HOLIDAY_DATES = set(item["date"] for item in HOLIDAY_LIST_JSON)

    def __init__(self):
        """Inicializa el pipeline de enriquecimiento de datos."""

        # Esquema para la UDF
        self.schema = StructType([
            StructField("Designator", StringType(), True),
            StructField("Runway", StringType(), True)
        ])

        # Registrar la UDF con el tipo de retorno correcto (STRUCT)
        self.assign_udf = udf(lambda lat, lon: Pipeline.assign_designator_runway(lat, lon, Pipeline.HOLDING_POINTS), self.schema)
        self.get_runway_udf = udf(lambda lat, lon: Pipeline.get_runway_for_point(lat, lon, Pipeline.RUNWAY_POLYGONS), StringType())
        
        # UDF para marcar si una fecha es festiva
        self.is_holiday_udf = udf(lambda d: d.strftime("%Y-%m-%d") in Pipeline.HOLIDAY_DATES, BooleanType())

    # Funciones estáticas
    # Función para calcular la distancia de Haversine
    @staticmethod
    def haversine_distance(lat1, lon1, lat2, lon2):
        """Calcula la distancia entre dos puntos en la superficie de la Tierra usando la fórmula de Haversine."""
        R = 6371000  # Radio de la Tierra en metros
        lat1_rad = math.radians(float(lat1))
        lon1_rad = math.radians(float(lon1))
        lat2_rad = math.radians(float(lat2))
        lon2_rad = math.radians(float(lon2))
        
        return R * math.acos(
            math.sin(lat1_rad) * math.sin(lat2_rad) +
            math.cos(lat1_rad) * math.cos(lat2_rad) * math.cos(lon1_rad - lon2_rad)
        )

    # UDF para asignar Designator y Runway basado en la distancia
    @staticmethod
    def assign_designator_runway(lat, lon, holding_points):
        """Asigna un Designator y Runway a un punto basado en la distancia a los puntos de espera."""
        # Inicializamos el valor de retorno como None
        
        DIST_THRESHOLD = 20
        
        closest_designator = None
        closest_runway = None
        min_distance = float("inf")  # Inicializamos con un valor grande
        
        # Iteramos sobre los puntos de espera para encontrar el más cercano
        for hold in holding_points:
            designator, runway, lon_p, lat_p = hold
            dist = Pipeline.haversine_distance(lat, lon, lat_p, lon_p)
            
            # Si la distancia es menor a 20 metros, devolvemos el primer punto
            if dist < DIST_THRESHOLD:
                closest_designator = designator
                closest_runway = runway
                break  # Detenemos la búsqueda una vez encontramos el primer punto cercano
        
        return (closest_designator, closest_runway)

    #### 12. Con dataframe A, creamos otro dataframe que indique cada 10s qué pistas y puntos de espera están ocupados → dataframe C
    @staticmethod
    def point_in_polygon(lat, lon, polygon):
        """
        Algoritmo que calcula si un punto está dentro de un polígono.
        
        lat, lon: coordenadas del punto
        polygon: lista de coordenadas [(lon1, lat1), (lon2, lat2), ...] del polígono (ojo al orden lon/lat)
        """
        num = len(polygon)
        j = num - 1
        inside = False
        for i in range(num):
            xi, yi = polygon[i]
            xj, yj = polygon[j]
            if ((yi > lat) != (yj > lat)) and \
            (lon < (xj - xi) * (lat - yi) / (yj - yi + 1e-9) + xi):
                inside = not inside
            j = i
        return inside

    # Obtenemos las pistas ocupadas (geométricamente)
    @staticmethod
    def get_runway_for_point(lat, lon, rpolygons):
        for runway, polygon in rpolygons.items():
            if Pipeline.point_in_polygon(lat, lon, polygon):
                return runway
        return None  # si no está en ninguna pista

    
    #-----------------------------------------------------------------

    #### Separamos dataframes de posiciones, callsigns, velocidades y categorías de turbulencia. Decodificamos mensajes y convertimos en dataframe
    def getPositions(self, df):
        """
        Genera un DataFrame con los datos necesarios para visualizar las posiciones.
        
        Parámetros:
            df: DataFrame de datos
        Devuelve:
            DataFrame con las siguientes columnas: "Timestamp (date)", "ICAO", "Flight status", "lat", "lon".
        """
        df_pos = df.filter((F.col("Typecode") >= 5) & (F.col("Typecode") <= 22) & (F.col("Typecode") != 19))\
               .select("Timestamp (date)", "ICAO", "Flight status", "lat", "lon")\
               .distinct()
        
        return df_pos

    def getAirplaneCategories(self, df):
        """
        Genera un DataFrame con la categoría de cada aeronave.
        
        Parámetros:
            df: DataFrame de datos.
        Devuelve:
            DataFrame con las siguientes columnas: "ICAO", "TurbulenceCategory".
        """
        # Seleccionamos mensajes ADS-B
        df_filtered = df.filter(F.col("Downlink Format").isin([17, 18]))
        # Nos quedamos con los ICAOs y su tipo de avión
        df_filtered = df_filtered.filter(F.col("TurbulenceCategory").isNotNull() & (F.col("TurbulenceCategory") != "None") & (~F.isnan(F.col("TurbulenceCategory"))))
        df_result = df_filtered.select("ICAO", "TurbulenceCategory").dropDuplicates()
        return df_result
    
    def getFlights(self, df):
        """
        Genera un DataFrame con los datos de vuelo.
        
        Parámetros:
            df: DataFrame de datos.
        Devuelve:
            DataFrame con las siguientes columnas: "Timestamp (date)", "ICAO", "Callsign".
        """
        NULL_CALLSIGN = "########"  # valor de nulo de la columna
        # Seleccionamos las filas que contengan información relativa al identificador de vuelo
        df_flights = df.filter((F.col("Callsign").isNotNull()) & 
                            (F.col("Callsign") != NULL_CALLSIGN) & 
                            (~F.isnan(F.col("Callsign"))))\
                .select("Timestamp (date)", "ICAO", "Callsign")\
                .distinct()  # Para evitar duplicados si es necesario
        return df_flights
    
    def getAltitudes(self, df):
        """
        Genera un Dataframe con los datos necesarios para visualizar las altitudes.
        
        Parámetros:
            df: DataFrame de datos.
        Devuelve:
            DataFrame con las siguientes columnas: "Timestamp (date)", "ICAO", "Callsign", "Flight status", "Altitude (ft)", "lat", "lon".
        """
        # DataFrame filtrando las filas que contienen una altitud no nula
        df_alt = df.filter(F.col("Altitude (ft)").isNotNull()  & (~F.isnan(F.col("Speed"))))\
               .select("Timestamp (date)", "ICAO", "Callsign", "Flight status", "Altitude (ft)", "lat", "lon")
        return df_alt
        
    def getVelocities(self, df):
        """
        Genera un dataframe con los mensajes relativos a la velocidad.
        Parámetros:
            df: DataFrame de datos.
        Devuelve:
            DataFrame con las siguientes columnas: "Timestamp (date)", "ICAO", "Flight status", "Speed", "lat", "lon".
        """
        # Filtramos las filas donde la velocidad no es nula
        df_vel = df.filter(F.col("Speed").isNotNull() & (~F.isnan(F.col("Speed"))))\
            .select("Timestamp (date)", "ICAO", "Flight status", "Speed")
        
        return df_vel
    
    #### 3. Filtramos para posiciones cercanas al aeropuerto (radio de 5km)
    def filter_positions_within_radius(self, df, lat_col="lat", lon_col="lon", lat_ref= AIRPORT_CENTER_LAT, lon_ref=AIRPORT_CENTER_LON, radius_m=RADIUS_AIRPORT):
        """
        Filtra un DataFrame de PySpark para conservar solo los registros que están dentro de un radio específico
        desde un punto de referencia, usando la fórmula de Haversine.
        :param df: DataFrame de entrada con columnas de latitud y longitud
        :param lat_col: Nombre de la columna de latitud en el DataFrame
        :param lon_col: Nombre de la columna de longitud en el DataFrame
        :param lat_ref: Latitud del punto de referencia
        :param lon_ref: Longitud del punto de referencia
        :param radius_m: Radio en metros para el filtrado (por defecto 5000 m)
        :return: DataFrame filtrado con una columna adicional 'distance_ref'
        """
        R = 6371000  # Radio de la Tierra en metros
        # Calcular distancia
        distance_expr = R * acos(
            sin(radians(lit(lat_ref))) * sin(radians(col(lat_col))) +
            cos(radians(lit(lat_ref))) * cos(radians(col(lat_col))) *
            cos(radians(lit(lon_ref)) - radians(col(lon_col)))
        )
        # Calculamos la distancia con respecto al punto de referencia
        df_with_dist = df.withColumn("distance_ref", distance_expr)
        # Nos quedamos solo con las posiciones que no superan la distancia radius_m
        filtered_df = df_with_dist.filter(col("distance_ref") <= radius_m)
        # Eliminamos la columna 'distance_ref'
        return filtered_df.drop("distance_ref")
    
    #### 4. Combinar posiciones con callsigns
    # Merge de posiciones y estado de vuelo
    def combinePsitionsFlights(self, df_pos_airport, df_flights):
        """Combina los DataFrames de posiciones y vuelos para asignar un callsign a cada posición."""
        
        TOLERANCE_CALLSIGN_ASSIGNMENT = 600  # 10 minutos en segundos
        
        df_pos_airport = df_pos_airport.withColumn("Timestamp_sec", F.unix_timestamp("Timestamp (date)"))
        df_flights = df_flights.withColumn("Timestamp_sec", F.unix_timestamp("Timestamp (date)"))
        
        df_pos_airport = df_pos_airport.withColumnRenamed("Timestamp_sec", "Timestamp_sec_pos") \
                                    .withColumnRenamed("Timestamp (date)", "Timestamp")
        
        df_flights = df_flights.withColumnRenamed("Timestamp_sec", "Timestamp_sec_flight") \
                            .withColumnRenamed("Timestamp (date)", "Timestamp_flight")
        
        df_pos_callsign = df_pos_airport.join(df_flights, on="ICAO", how="inner").filter(
            F.abs(df_pos_airport["Timestamp_sec_pos"] - df_flights["Timestamp_sec_flight"]) <= TOLERANCE_CALLSIGN_ASSIGNMENT
        )
        
        df_pos_callsign = df_pos_callsign.drop("Timestamp_sec_flight", "Timestamp_sec_pos", "Timestamp_flight")
        df_pos_callsign = df_pos_callsign.dropDuplicates(["ICAO", "Timestamp"])
        
        return df_pos_callsign
    
    #### 5. Detección de aeronaves situados en puntos de espera
    
    def assignHoldingPoint(self, df_pos_callsign):
        """Asigna un Designator y Runway a cada posición de vuelo en función de su latitud y longitud."""
        # Asignamos puntos de espera
        df_with_hp = df_pos_callsign.withColumn(
            "Designator_Runway", 
            self.assign_udf("lat", "lon")
        )
        
        # Separamos en dos columnas el resultado
        df_with_hp = df_with_hp.withColumn(
            "Designator", F.col("Designator_Runway.Designator")
        ).withColumn(
            "Runway", F.col("Designator_Runway.Runway")
        )
        df_with_hp = df_with_hp.drop("Designator_Runway")
        
        return df_with_hp
    
    #### 6. Combinamos con las categorías de turbulencia
    #### 7. Eliminamos todos los vuelos que no se les ha detectado en un punto de espera

    def filterFlights(self, df_with_hp_tc):
        """Filtra los vuelos que no han pasado por un punto de espera y asigna la categoría de turbulencia."""

        # Filtramos solo los vuelos que alguna vez tuvieron un Designator no nulo
        df_holding_count = df_with_hp_tc.groupBy("Callsign").agg(
            count(when(col("Designator").isNotNull(), True)).alias("holding_count")
        ).filter(col("holding_count") > 0)
        
        # Nos quedamos solo con los vuelos válidos (que pasaron por un holding point)
        df_valid_callsigns = df_holding_count.select("Callsign")
        
        # Unimos para mantener solo las filas de esos vuelos
        df_valid_flights = df_with_hp_tc.join(df_valid_callsigns, on="Callsign", how="inner")
        
        return df_valid_flights
    
    #### 8. Nos quedamos con las posiciones desde que se le detecta en un punto de espera hasta la primera vez que se le detecta en el aire

    def filterPositions(self, df_valid_flights):
        """Filtra las posiciones desde que se detecta en un punto de espera hasta la primera vez que se detecta en el aire."""
        # Ventana por Callsign
        window_spec = Window.partitionBy("Callsign")
        
        # Añadimos la primera vez que el vuelo fue detectado en tierra
        df_with_first_on_ground = df_valid_flights.withColumn(
            "first_on_ground_time",
            min(when(col("Flight status") == "on-ground", col("Timestamp"))).over(window_spec)
        )
        
        # Encontramos el primer timestamp en que el Designator no es nulo
        df_with_first_holding = df_with_first_on_ground.withColumn(
            "first_holding_time",
            min(when(col("Designator").isNotNull(), col("Timestamp"))).over(window_spec)
        )
        
        # Filtramos solo las filas en o después del primer punto de espera
        df_from_hp = df_with_first_holding.filter(col("Timestamp") >= col("first_holding_time"))
        
        # Encontramos el primer timestamp en que FlightStatus es 'airborne'
        df_with_first_airborne = df_from_hp.withColumn(
            "first_airborne_time",
            F.min(F.when(F.col("Flight status") == "airborne", F.col("Timestamp"))).over(window_spec)
        )
        
        # Filtramos solo las filas en o después del primer estado airborne
        df_takeoff_segment = df_with_first_airborne.filter(F.col("Timestamp") <= F.col("first_airborne_time"))
        
        return df_takeoff_segment
    
    #### 9. Combinamos con los mensajes de velocidad

    # Merge de posiciones y velocidades
    def mergePositionsVelocities(self, df_takeoff_segment, df_speed):
        """Combina los DataFrames de posiciones y velocidades para asignar la velocidad a cada posición."""
        TOLERANCE_VELOCITY_ASSIGNMENT = 1  # 1 segundo
        
        df_takeoff_segment = df_takeoff_segment.withColumn("Timestamp_sec", F.unix_timestamp("Timestamp"))
        df_speed = df_speed.withColumn("Timestamp_sec", F.unix_timestamp("Timestamp (date)"))
        
        df_speed = df_speed.withColumnRenamed("Timestamp_sec", "Timestamp_sec_speed") \
                            .withColumnRenamed("Timestamp (date)", "Timestamp_speed") \
                            .withColumnRenamed("Flight status", "Flight status Speed")
        
        df_with_velocities = df_takeoff_segment.join(df_speed, on="ICAO", how="inner").filter(
            F.abs(df_takeoff_segment["Timestamp_sec"] - df_speed["Timestamp_sec_speed"]) <= TOLERANCE_VELOCITY_ASSIGNMENT
        )
        
        df_with_velocities = df_with_velocities.drop("Timestamp_sec", "Timestamp_sec_speed", "Timestamp_speed", "Flight status Speed")
        df_with_velocities = df_with_velocities.dropDuplicates(["ICAO", "Timestamp"])
        
        return df_with_velocities
    
    #### 10. Filtramos para quedarnos solo con las aeronaves que en algún momento se paran en un punto de espera y recalculamos primer timestamp

    def importantTakeoffs(self, df_with_velocities):
        """Filtra los vuelos que tienen velocidad 0 y un Designator no nulo."""
        # Filtrar los callsigns con velocidad 0 y punto de espera no nulo

        df_callsigns_zero_speed = df_with_velocities.filter(
            (col("Speed") == 0) & 
            (col("Designator").isNotNull())
        ).select("ICAO").distinct()


        # Nos quedamos solo con estos vuelos
        df_important_takeoffs = df_with_velocities.join(
            df_callsigns_zero_speed,
            on="ICAO",
            how="inner" 
        )
        
        # Ventana por Callsign
        window_spec = Window.partitionBy("Callsign")
        
        # Encontramos el primer timestamp en que el Designator no es nulo
        df_important_takeoffs = df_important_takeoffs.withColumn(
            "first_holding_time",
            min(when(col("Designator").isNotNull() & (F.col("Speed") == 0), col("Timestamp"))).over(window_spec)
        )
        
        # Filtramos solo las filas en o después del primer punto de espera
        df_important_takeoffs = df_important_takeoffs.filter(col("Timestamp") >= col("first_holding_time"))
        
        return df_important_takeoffs
    
    #### 11. Calculamos tiempos de espera
    def calculateHoldingTime(self, df_important_takeoffs):
        """Calcula los tiempos de espera en los puntos de espera."""
        df_holding = df_important_takeoffs.filter(
            (col("Speed") == 0) & 
            (col("Designator").isNotNull())
        )
        
        dfB = df_holding.withColumn("takeoff time", 
                        (F.unix_timestamp("first_airborne_time") - F.unix_timestamp("Timestamp"))
                        .cast("double"))
                        
        dfB = dfB.withColumn("time_before_holding_point", 
                        (F.unix_timestamp("first_holding_time") - F.unix_timestamp("first_on_ground_time"))
                        .cast("double"))
                        
        dfB = dfB.withColumn("time_at_holding_point", 
                        (F.unix_timestamp("Timestamp") - F.unix_timestamp("first_holding_time"))
                        .cast("double"))
        
        return dfB
    
    #### 12. Creamos un dataframe que indique los puntos de espera ocupados y las pistas ocupadas por cada intervalo de 10 segundos
    

    
    def occupaidEach10s(self, dfA):
        """Genera un DataFrame que indica los puntos de espera ocupados y las pistas ocupadas por cada intervalo de 10 segundos."""
        # Redondear el timestamp a bloques de 10 segundos
        # La columna time_10s va a contener timestamps redondeados a bloques de 10 segundos en formato UNIX

        dfA = dfA.withColumn("timestamp_unix", unix_timestamp("Timestamp"))
        dfA = dfA.withColumn("time_10s", F.ceil(F.col("timestamp_unix") / 10) * 10)
        dfA = dfA.withColumn("time_10s", F.to_timestamp(from_unixtime("time_10s")))
        dfA = dfA.drop("timestamp_unix")
        
        # Obtenemos los puntos de espera ocupados en cada intervalo
        # collect_set(): Agrupa todos los valores únicos de una columna (en este caso "Designator") dentro de cada grupo definido por un groupBy() o window y devuelve una lista sin duplicados
        dfA_holding_points = dfA.groupBy("time_10s")\
            .agg(F.collect_set("Designator").alias("occupied_holding_points"))
        
        dfA = dfA.withColumn("RunwayFromPosition", self.get_runway_udf("lat", "lon"))
        # Pistas ocupadas por cada intervalo
        dfA_runways = dfA.groupBy("time_10s")\
            .agg(F.collect_set("RunwayFromPosition").alias("occupied_runways"))
        
        status_by_interval = dfA_holding_points.join(dfA_runways, on="time_10s")
        
        # Generamos las columnas booleanas para puntos de espera
        for hp in self.HP_NAMES:
            status_by_interval = status_by_interval.withColumn(f"{hp}", F.array_contains(F.col("occupied_holding_points"), hp))
            
        # Y para las pistas
        for rw in self.RUNWAY_NAMES:
            safe_rw = rw.replace("/", "_")  # Evita caracteres problemáticos en nombres de columna
            status_by_interval = status_by_interval.withColumn(f"{safe_rw}", F.array_contains(F.col("occupied_runways"), rw))
        
        status_by_interval_final = status_by_interval.drop("occupied_holding_points", "occupied_runways")
        return status_by_interval_final, dfA
    
    #### 13. Con dataframe A, creamos otro dataframe que indique todos los despegues / aterrizajes por pista, debe incluir el timestamp y la categoría de turbulencia del avión

    def eventsByRunway(self, dfA):
        """Genera un DataFrame que indica todos los despegues y aterrizajes por pista, incluyendo el timestamp y la categoría de turbulencia del avión."""
        window_spec = Window.partitionBy("ICAO").orderBy("Timestamp")
        
        dfA = dfA.withColumn("Runway", F.coalesce("Runway", "RunwayFromPosition"))
        dfA = dfA.withColumn("prev_status", F.lag("Flight status").over(window_spec))
        
        dfD = dfA.filter(
            ((F.col("Flight status") == "airborne") & (F.col("prev_status") == "on-ground")) |
            ((F.col("Flight status") == "on-ground") & (F.col("prev_status") == "airborne"))
        ).withColumn(
            "Event",
            F.when(F.col("Flight status") == "airborne", F.lit("takeoff"))
            .when(F.col("Flight status") == "on-ground", F.lit("landing"))
        )
        
        
        dfD = dfD.select("Runway", "Timestamp", "TurbulenceCategory", "Event")
        
        dfD = dfD.filter(F.col("Runway").isNotNull())
        
        return dfD
    
    #### 14. Agrupamos D por minuto para conseguir tasa de despegues y de aterrizajes por minuto

    def eventsMinuteRate(self, dfD):
        """Agrupa el DataFrame de eventos por minuto para calcular la tasa de despegues y aterrizajes por minuto."""
        dfE = dfD.withColumn(
            "Minute", 
            F.from_unixtime(
                F.ceil(F.unix_timestamp(F.col("Timestamp")) / 60) * 60
            ).cast("timestamp")
        ) \
        .groupBy("Minute") \
        .agg(
            count(when(F.col("Event") == "takeoff", True)).alias("last min takeoffs"),
            count(when(F.col("Event") == "landing", True)).alias("last min landings")
        )
        
        return dfE
    
    #### 15. Combinamos B con C, D y E
    def combineBCDE(self, dfB, dfC, dfD, dfE):
        """Combina los DataFrames B, C, D y E para obtener el DataFrame final con la información enriquecida."""
        # Redondeamos a múltiplos de 10 segundos
        dfB = dfB.withColumn(
            "time_10s",
            from_unixtime((unix_timestamp("Timestamp") / 10).cast("long") * 10)
        )
        
        # Paso 2: Join con dfC
        dfF = dfB.join(dfC, on="time_10s", how="inner")
        
        # Paso 1: Redondear a minuto
        dfF = dfF.withColumn(
            "Minute",
            date_trunc("minute", col("Timestamp"))
        )
        
        # Paso 2: Join con dfE
        dfF = dfF.join(dfE, on="Minute", how="inner")
        
        # Paso 1: Creamos la window
        w = Window.partitionBy("Runway").orderBy("Timestamp").rangeBetween(Window.unboundedPreceding, Window.currentRow)
        
        # Paso 2: Renombramos Timestamp para evitar conflictos en el join
        dfD = dfD.withColumnRenamed("Timestamp", "event_timestamp")
        dfD = dfD.withColumnRenamed("TurbulenceCategory", "last_event_turb_cat")
        dfD = dfD.withColumnRenamed("Event", "last_event")
        
        # Paso 3: Join de los dataframes en base a Runway y evento previo en el tiempo
        df_combined = dfF.join(dfD, on="Runway", how="left") \
            .filter(col("event_timestamp") < col("Timestamp"))
        
        # Paso 4: Usamos Window para quedarnos con la fila más reciente antes de `first_holding_time`
        w2 = Window.partitionBy("Callsign", "Timestamp", "Runway").orderBy(col("event_timestamp").desc())
        
        df_final = df_combined.withColumn("rank", row_number().over(w2)) \
            .filter(col("rank") == 1) \
            .drop("rank")
        
        df_final = df_final.withColumn(
            "time_since_last_event_seconds",
            unix_timestamp("Timestamp") - unix_timestamp("event_timestamp")
        )
        
        return df_final
    
    #### 16. Extraemos la hora, día de la semana y si es festivo o no
    def dateColumns(self, df_final):
        """Extrae la hora, día de la semana y si es festivo o no del DataFrame final."""
        # Añadir columnas a dfE
        df_final = df_final.withColumn("Hour", hour("Minute")) \
                .withColumn("Weekday", F.date_format("Minute", "E")) \
                .withColumn("Date", F.to_date("Minute")) \
                .withColumn("IsHoliday", self.is_holiday_udf("Date")) \
                .drop("Date")
                
        return df_final
    
    #### 17. Extraemos la aerolínea
    #### 18. Renombramos y reordenamos columnas
    def cleanColumns(self, df_final):
        """Renombra y reordena las columnas del DataFrame final."""
        # Suponiendo que df es tu DataFrame original
        df_final_clean = df_final.select(
            F.col('takeoff time').alias('takeoff_time'),
            F.col('Timestamp').alias('timestamp'),
            F.col('ICAO').alias('icao'),
            F.col('Callsign').alias('callsign'),
            F.col('Designator').alias('holding_point'),
            F.col('Runway').alias('runway'),
            F.col('operator'),
            F.col('TurbulenceCategory').alias('turbulence_category'),
            F.col('lat'),
            F.col('lon'),
            F.col('last min takeoffs').alias('last_min_takeoffs'),
            F.col('last_event'),
            F.col('last min landings').alias('last_min_landings'),
            F.col('last_event_turb_cat').alias('last_event_turb_cat'),
            F.col('time_since_last_event_seconds'),
            F.col('time_before_holding_point'),
            F.col('time_at_holding_point'),
            F.col('Hour').alias('hour'),
            F.col('Weekday').alias('weekday'),
            F.col('IsHoliday').alias('is_holiday'),
            F.col('event_timestamp'),
            F.col('first_holding_time'),
            F.col('first_airborne_time'),
            F.col('first_on_ground_time'),
            'Z1', 'KA6', 'KA8', 'K3', 'K2', 'K1', 'Y1', 'Y2', 'Y3', 'Y7', 'Z6', 'Z4', 'Z2', 
            'Z3', 'LF', 'L1', 'LA', 'LB', 'LC', 'LD', 'LE', '36R_18L', '32R_14L', '36L_18R', '32L_14R'   
        )
    
        return df_final_clean
    
    # Aplicamos el pipeline
    def apply_pipeline(self, df):
        """
        Aplica el pipeline de enriquecimiento de datos a un DataFrame de PySpark."""
        # 1.
        # posiciones
        df_pos = self.getPositions(df)
        # vuelos
        df_flights = self.getFlights(df)
        # categorías de turbulencia
        df_types = self.getAirplaneCategories(df)
        # velocidades
        df_speed = self.getVelocities(df)

        # altitudes
        # df_alt = self.getAltitudes(self.df)
    

        # 3. 
        df_pos_airport = self.filter_positions_within_radius(df_pos)
    
        # 4.
        df_pos_callsign = self.combinePsitionsFlights(df_pos_airport, df_flights)

        # 5.
        df_with_hp = self.assignHoldingPoint(df_pos_callsign)
        df_with_hp.show()

        # 6.
        df_with_hp_tc = df_with_hp.join(df_types, on="ICAO", how="inner")
        dfA = df_with_hp_tc
    
        # 7.
        df_valid_flights = self.filterFlights(df_with_hp_tc)
       
        # 8.
        df_takeoff_segment = self.filterPositions(df_valid_flights)
    
        # 9.
        df_with_velocities = self.mergePositionsVelocities(df_takeoff_segment, df_speed)
    
        # 10. 
        df_important_takeoffs = self.importantTakeoffs(df_with_velocities)
    
        # 11.
        dfB = self.calculateHoldingTime(df_important_takeoffs)

        # 12.
        status_by_interval_final, dfA = self.occupaidEach10s(dfA)
        dfC = status_by_interval_final

        # 13.
        dfD = self.eventsByRunway(dfA)
    
        # 14.
        dfE = self.eventsMinuteRate(dfD)

        print(dfA.count())
        print(dfB.count())
        print(dfC.count())
        print(dfD.count())
        print(dfE.count())

        # 15.
        dfA.persist(StorageLevel.MEMORY_AND_DISK)
        dfB.persist(StorageLevel.MEMORY_AND_DISK)
        dfC.persist(StorageLevel.MEMORY_AND_DISK)
        dfD.persist(StorageLevel.MEMORY_AND_DISK)
        dfE.persist(StorageLevel.MEMORY_AND_DISK)
        df_final = self.combineBCDE(dfB, dfC, dfD, dfE)
        print("after 15")
        df_final.show()
        
        # 16.
        df_final = self.dateColumns(df_final)
        print("after 16")
        df_final.show()
 
        # 17.
        df_final = df_final.withColumn("operator", F.substring("Callsign", 1, 3))
        print("after 17")
        df_final.show()
    
        # 18.
        df_final_clean = self.cleanColumns(df_final)
        print("after 18")
        df_final_clean.show()


        """
        # Guardamos el archivo procesado en el directorio de salida
        df_final_clean.persist(StorageLevel.MEMORY_AND_DISK)
        df_final_clean.show()
        df_final_clean.coalesce(1).write.parquet(output_file_path)
        print(f"Archivo procesado: {file} -> {output_file}")
        """
        # Liberamos memoria
        dfA.unpersist()
        dfB.unpersist()
        dfC.unpersist()
        dfD.unpersist()
        dfE.unpersist()
        #df_final_clean.unpersist()

        return df_final_clean


    
    
    