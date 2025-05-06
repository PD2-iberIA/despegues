from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import preprocess.utilities as ut
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from preprocess.data_processor import DataProcessor

class DataframeProcessor:
    """Clase que permite realizar operaciones de procesamiento y análisis de datos con los dataframes de Pandas."""

    @staticmethod
    def getFlightStatus(df):
        """Crea el DataFrame para el diagrama de barras de aviones aterrizados vs en vuelo.

        Parámetros:
            df: DataFrame de datos.

        Devuelve:
            df_status: DataFrame transformado.
        """
        # Conseguimos el df con todos los datos necesarios -> flight status en todos los callsign
        dff = DataProcessor.get_dff(df)

        return DataProcessor.get_status(dff)
    
    @staticmethod
    def getWaitTimes(df):
        """Crea el DataFrame para las gráficas de tiempo de espera.
        
        Parámetros:
            df: DataFrame de datos.

        Devuelve:
            df_wait_times: DataFrame transformado.
        """
        df1 = DataframeProcessor.getVelocities(df)
        df2 = DataframeProcessor.getFlights(df)

        window_spec = Window.partitionBy("ICAO").orderBy("Timestamp (date)")
        df1_s = df1.withColumn("row_num", F.row_number().over(window_spec))
        df2_s = df2.withColumn("row_num", F.row_number().over(window_spec))

        t = 600  # 10 minutos en segundos
        df1_s = df1_s.withColumn("Timestamp_sec", F.unix_timestamp("Timestamp (date)"))
        df2_s = df2_s.withColumn("Timestamp_sec", F.unix_timestamp("Timestamp (date)"))

        dff = df1_s.join(df2_s, on="ICAO", how="inner").filter(
            F.abs(df1_s["Timestamp_sec"] - df2_s["Timestamp_sec"]) <= t
        )
        # Definimos las pistas
        RUNWAYS = [
            {"name": "1", "position": (40.463, -3.554)},
            {"name": "2", "position": (40.473, -3.536)},
            {"name": "3", "position": (40.507, -3.574)},
            {"name": "4", "position": (40.507, -3.559)}
        ]

        def find_nearest_runway(lat, lon):
            return min(RUNWAYS, key=lambda r: (r["position"][0] - lat) ** 2 + (r["position"][1] - lon) ** 2)["name"]
        find_nearest_runway_udf = F.udf(find_nearest_runway, StringType())

        # Separamos on-ground y airborne
        on_ground = dff.filter((dff["Flight status"] == "on-ground") & (dff["Speed"] == 0))\
            .groupBy("Callsign", "ICAO")\
            .agg(F.min("Timestamp (date)").alias("ts_ground"))

        airborne = dff.filter(dff["Flight status"] == "airborne")\
            .groupBy("Callsign", "ICAO")\
            .agg(F.min("Timestamp (date)").alias("ts_airborne"),
                 F.first("lat").alias("lat"),
                 F.first("lon").alias("lon"))\
            .withColumn("runway", find_nearest_runway_udf("lat", "lon"))

        # Creamos las columnas de tiempos de espera
        df_wait_times = on_ground.join(airborne, on=["Callsign", "ICAO"], how="inner")\
            .filter(F.col("ts_airborne") > F.col("ts_ground"))\
            .withColumn("Wait time", F.col("ts_airborne").cast("long") - F.col("ts_ground").cast("long"))\
            .withColumn("Wait time (s)", F.col("Wait time").cast("double"))

        df_wait_times = ut.extractDaysOfTheWeek(df_wait_times, "ts_airborne")

        return df_wait_times

    @staticmethod
    def getAirplaneCategories(df):
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
        df_filtered = df_filtered.filter(F.col("TurbulenceCategory").isNotNull())

        df_result = df_filtered.select("ICAO", "TurbulenceCategory").dropDuplicates()

        return df_result

    @staticmethod
    def getFlights(df):
        """
        Genera un DataFrame con los datos de vuelo.
        
        Parámetros:
            df: DataFrame de datos.

        Devuelve:
            DataFrame con las siguientes columnas: "Timestamp (date)", "ICAO", "Callsign".
        """
        NULL_CALLSIGN = "########"  # valor de nulo de la columna
        flightColumns = ["Timestamp (date)", "ICAO", "Callsign"] # columnas de la proyección

        # Seleccionamos las filas que contengan información relativa al identificador de vuelo
        df_flights = df.filter((F.col("Callsign").isNotNull()) & (F.col("Callsign") != NULL_CALLSIGN))\
                   .select("Timestamp (date)", "ICAO", "Callsign")\
                   .distinct()  # Para evitar duplicados si es necesario


        return df_flights
    
    @staticmethod
    def getPositions(df):
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
    
    @staticmethod
    def getVelocities(df):
        """
        Genera un dataframe con los datos necesarios para visualizar las velocidades.

        Parámetros:
            df: DataFrame de datos.

        Devuelve:
            DataFrame con las siguientes columnas: "Timestamp (date)", "ICAO", "Flight status", "Speed", "lat", "lon".
        """
        # Filtramos las filas donde la velocidad no es nula
        df_vel = df.filter(F.col("Speed").isNotNull())\
               .select("Timestamp (date)", "ICAO", "Flight status", "Speed", "lat", "lon")

        # Dividimos en 2 dataframe según si los vuelos están en tierra o en aire
        df_vel_ground = df_vel.filter(F.col("Flight status") == "on-ground")
        df_vel_air = df_vel.filter(F.col("Flight status") == "airborne")


        df_pos = DataframeProcessor.getPositions(df)
        window_spec = Window.partitionBy("ICAO").orderBy("Timestamp (date)")
        df_pos = df_pos.withColumn("row_num", F.row_number().over(window_spec))
        df_vel_air = df_vel_air.withColumn("row_num", F.row_number().over(window_spec))
        # df_pos = df_pos.compute().sort_values(by="Timestamp (date)")
        # df_vel_air = df_vel_air.compute().sort_values(by="Timestamp (date)")

        # Juntamos posiciones y velocidades de los vuelos en el aire según el timestamp
        tolerance = 1  # en segundos
        df_pos = df_pos.withColumn("Timestamp_sec", F.unix_timestamp("Timestamp (date)"))
        df_vel_air = df_vel_air.withColumn("Timestamp_sec", F.unix_timestamp("Timestamp (date)"))

        df_vel_air_pos = df_pos.join(df_vel_air, on="ICAO", how="inner").filter(
            F.abs(df_pos["Timestamp_sec"] - df_vel_air["Timestamp_sec"]) <= tolerance
        )

        # Eliminamos columnas redundantes
        df_vel_air_pos = df_vel_air_pos.withColumnRenamed("lat_x", "lat")\
                                   .withColumnRenamed("lon_x", "lon")\
                                   .withColumnRenamed("Flight status_x", "Flight status")\
                                   .select("Timestamp (date)", "ICAO", "Flight status", "Speed", "lat", "lon")

        # El df que buscamos con esto tiene: velocidades de aviones en tierra y velocidades+posiciones de aviones en el aire
        df_vel_final = df_vel_ground.union(df_vel_air_pos)
        df_vel_final = df_vel_final.withColumn("Speed", ut.knots_to_kmh(F.col("Speed")))

        return df_vel_final
    
    @staticmethod
    def getFlightsInfo(df):
        """
        Genera un dataframe con los datos necesarios para visualizar toda la información sobre un vuelo.

        Parámetros:
            df: DataFrame de datos.

        Devuelve:
            DataFrame con las siguientes columnas: "Timestamp (date)", "ICAO", "Flight status", "lat", "lon",
            "Callsign", "TurbulenceCategory", "Speed", "Altitude (ft).
        """
        df_pos = DataframeProcessor.getPositions(df)
        df_flights = DataframeProcessor.getFlights(df)
        df_types = DataframeProcessor.getAirplaneCategories(df)
        df_speed = DataframeProcessor.getVelocities(df)
        df_alt = DataframeProcessor.getAltitudes(df)

        df_pos = df_pos.sort_values(["Timestamp (date)", "ICAO"])
        df_flights = df_flights.sort_values(["Timestamp (date)", "ICAO"])
        df_speed = df_speed.sort_values(["Timestamp (date)", "ICAO"])

        # Merge de posiciones y estado de vuelo
        tolerance = 600  # 10 minutos en segundos
        df_pos = df_pos.withColumn("Timestamp_sec", F.unix_timestamp("Timestamp (date)"))
        df_flights = df_flights.withColumn("Timestamp_sec", F.unix_timestamp("Timestamp (date)"))

        df = df_pos.join(df_flights, on="ICAO", how="inner").filter(
            F.abs(df_pos["Timestamp_sec"] - df_flights["Timestamp_sec"]) <= tolerance
        )
        # Merge con velocidades
        tolerance_speed = 60  
        df_speed = df_speed.withColumn("Timestamp_sec", F.unix_timestamp("Timestamp (date)"))
        df = df.join(df_speed, on="ICAO", how="inner").filter(
            F.abs(df["Timestamp_sec"] - df_speed["Timestamp_sec"]) <= tolerance_speed
        )
        # Limpiar columnas duplicadas y renombrar
        df = df.withColumnRenamed("lat_x", "lat")\
           .withColumnRenamed("lon_x", "lon")\
           .withColumnRenamed("Flight status_x", "Flight status")\
           .select("Timestamp (date)", "ICAO", "Flight status", "lat", "lon", "Callsign")

        # Filtrar vuelos válidos
        df = df.filter(F.col("Callsign").isNotNull()).filter(F.col("Flight status") == "airborne")

        # Unimos con categorías de turbulencia y altitudes
        df = df.join(df_types, on="ICAO", how="left")
        df = df.join(df_alt, on=["ICAO", "Callsign", "Timestamp (date)"], how="left")

        return df
    
    @staticmethod
    def getAltitudes(df):
        """
        Genera un Dataframe con los datos necesarios para visualizar las altitudes.
        
        Parámetros:
            df: DataFrame de datos.

        Devuelve:
            DataFrame con las siguientes columnas: "Timestamp (date)", "ICAO", "Callsign", "Flight status", "Altitude (ft)", "lat", "lon".
        """
        # DataFrame filtrando las filas que contienen una altitud no nula
        df_alt = df.filter(F.col("Altitude (ft)").isNotNull())\
               .select("Timestamp (date)", "ICAO", "Callsign", "Flight status", "Altitude (ft)", "lat", "lon")

        return df_alt
    
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

        tolerance = 600  # 10 minutos en segundos
        df1 = df1.withColumn("Timestamp_sec", F.unix_timestamp("Timestamp (date)"))
        df2 = df2.withColumn("Timestamp_sec", F.unix_timestamp("Timestamp (date)"))

        dff = df1.join(df2, on="ICAO", how="inner").filter(
            F.abs(df1["Timestamp_sec"] - df2["Timestamp_sec"]) <= tolerance
        )
        dff = dff.withColumn("Timestamp (date)", F.to_timestamp("Timestamp (date)"))
        dff = ut.extractHour(dff)
        dff = ut.extractDaysOfTheWeek(dff)

        return dff

    @staticmethod
    def removeOutlierFlights(df):
        """
        Elimina vuelos considerados outliers de un DataFrame. Consideramos como outlier un vuelo si recorre una distancia
        mayor a 200km en menos de 10 minutos. Para que esta función se aplique correctamente es necesario que el DataFrame
        de entrada haya sido procesado con la función `getFlightsInfo`.

        Parámetros:
            df: DataFrame de datos.

        Devuelve:
            df_filtered: DataFrame con los datos limpios.
        """
        df = df.filter(F.col("lat").isNotNull())
        # Calcula la distancia y tiempo entre filas consecutivas por ICAO
        window_spec = Window.partitionBy("ICAO", "Callsign").orderBy("Timestamp (date)")

        df = df.withColumn("prev_lat", F.lag("lat").over(window_spec))
        df = df.withColumn("prev_lon", F.lag("lon").over(window_spec))
        df = df.withColumn("prev_time", F.lag("Timestamp (date)").over(window_spec))

        # Calcular distancia y diferencia de tiempo
        df = df.withColumn("distance", ut.haversine_spark("lat", "lon", "prev_lat", "prev_lon"))
        df = df.withColumn("time_diff", F.unix_timestamp("Timestamp (date)") - F.unix_timestamp("prev_time"))

        # Filtrar outliers
        DISTANCE_THRESHOLD = 200  # km
        MINUTES_THRESHOLD = 600 

        df_outliers = df.filter((F.col("distance") > DISTANCE_THRESHOLD) & (F.col("time_diff") < MINUTES_THRESHOLD))

        # Obtener ICAOs outliers
        outliers_icao = df_outliers.select("ICAO").distinct()

        # Eliminar outliers
        df_filtered = df.join(outliers_icao, on="ICAO", how="left_anti")

        return df_filtered

