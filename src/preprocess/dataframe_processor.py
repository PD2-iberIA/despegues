import pandas as pd
from datetime import timedelta
import preprocess.utilities as ut
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

        df1_s = df1.sort_values(["Timestamp (date)", "ICAO"])
        df2_s = df2.sort_values(["Timestamp (date)", "ICAO"])

        t = pd.Timedelta('10 minute')
        dff = pd.merge_asof(df1_s, df2_s, on="Timestamp (date)", by="ICAO", direction="nearest", tolerance=t)

        # Ordenamos los datos por callsign y fecha
        dff = dff.sort_values(by=["Callsign", "Timestamp (date)"])

        # Definimos las pistas
        RUNWAYS = [
            {"name": "1", "position": (40.463, -3.554)},
            {"name": "2", "position": (40.473, -3.536)},
            {"name": "3", "position": (40.507, -3.574)},
            {"name": "4", "position": (40.507, -3.559)}
        ]

        def find_nearest_runway(lat, lon):
            return min(RUNWAYS, key=lambda r: (r["position"][0] - lat) ** 2 + (r["position"][1] - lon) ** 2)["name"]

        # Separamos on-ground y airborne
        on_ground = dff[(dff["Flight status"] == "on-ground") & (dff["Speed"]==0)].groupby(["Callsign", "ICAO"])["Timestamp (date)"].min()
        on_ground = pd.DataFrame(on_ground).reset_index()
        on_ground.columns = ["Callsign", "ICAO", "ts ground"]

        airborne = dff[dff["Flight status"] == "airborne"].groupby(["Callsign", "ICAO"]).agg({
            "Timestamp (date)": "min",
            "lat": "first",
            "lon": "first"
        }).reset_index()
        airborne.columns = ["Callsign", "ICAO", "ts airborne", "lat", "lon"]
        airborne["runway"] = airborne.apply(lambda row: find_nearest_runway(row["lat"], row["lon"]), axis=1)

        # Creamos las columnas de tiempos de espera
        df_wait_times = on_ground.merge(airborne, how="inner", on=["Callsign", "ICAO"])
        df_wait_times = df_wait_times[df_wait_times["ts airborne"] > df_wait_times["ts ground"]]
        df_wait_times["Wait time"] = df_wait_times["ts airborne"] - df_wait_times["ts ground"]
        df_wait_times["Wait time (s)"] = df_wait_times["Wait time"].dt.total_seconds()
        df_wait_times = ut.extractDaysOfTheWeek(df_wait_times, "ts airborne")

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
        df = df[df["Downlink Format"].isin([17, 18])]

        # Nos quedamos con los ICAOs y su tipo de avión
        df = df[df["TurbulenceCategory"].notna()]
        df = df[["ICAO", "TurbulenceCategory"]].drop_duplicates().reset_index(drop=True)

        return df

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
        df_flights = df[df["Callsign"].notna() & (df["Callsign"] != NULL_CALLSIGN)]
        df_flights = df_flights[flightColumns].reset_index(drop=True)

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
        columnasPosiciones = ["Timestamp (date)", "ICAO", "Flight status", "lat", "lon"]

        # Typecodes que indican posición
        df_pos = df[((5.0 <= df["Typecode"]) & (df["Typecode"]  <= 22.0)) & (df["Typecode"] != 19.0)]
        df_pos = df_pos[columnasPosiciones].reset_index(drop=True)
        
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
        df_vel = df[df["Speed"].notna()] # con pandas
        # df_vel = df[df["Speed"].isnull() == False]  # Dask-friendly filtering 
        df_vel = df_vel[["Timestamp (date)", "ICAO", "Flight status", "Speed", "lat", "lon"]]

        # Dividimos en 2 dataframe según si los vuelos están en tierra o en aire
        df_vel_ground = df_vel[df_vel["Flight status"] == "on-ground"]
        df_vel_air = df_vel[df_vel["Flight status"] == "airborne"]

        df_pos = DataframeProcessor.getPositions(df)
        df_pos = df_pos.sort_values(by="Timestamp (date)")
        df_vel_air = df_vel_air.sort_values(by="Timestamp (date)")
        # df_pos = df_pos.compute().sort_values(by="Timestamp (date)")
        # df_vel_air = df_vel_air.compute().sort_values(by="Timestamp (date)")

        # Juntamos posiciones y velocidades de los vuelos en el aire según el timestamp
        tolerance = pd.Timedelta('1 second') # tolerancia de 1 segundo
        df_vel_air_pos = pd.merge_asof(df_pos, df_vel_air, on="Timestamp (date)", by="ICAO", direction="nearest", tolerance=tolerance)

        df_vel_air_pos = df_vel_air_pos[df_vel_air_pos["Speed"].notna()]

        # Eliminamos columnas redundantes
        df_vel_air_pos = df_vel_air_pos.drop(columns=['lat_y', 'lon_y', 'Flight status_y'])
        df_vel_air_pos = df_vel_air_pos.rename(columns={'lat_x': 'lat', 'lon_x': 'lon', 'Flight status_x': 'Flight status'})

        df_vel_air_pos = df_vel_air_pos[["Timestamp (date)", "ICAO", "Flight status", "Speed", "lat", "lon"]]

        # El df que buscamos con esto tiene: velocidades de aviones en tierra y velocidades+posiciones de aviones en el aire
        df_vel_final = pd.concat([df_vel_ground, df_vel_air_pos])
        df_vel_final["Speed"] = df_vel_final["Speed"].apply(ut.knots_to_kmh)

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
        tolerance = pd.Timedelta('10 minute') # tolerancia
        df = pd.merge_asof(df_pos, df_flights, on="Timestamp (date)", by="ICAO", direction="nearest", tolerance=tolerance)

        # Merge con velocidades
        tolerance_speed = pd.Timedelta('1 minute')
        df = pd.merge_asof(df, df_speed, on="Timestamp (date)", by="ICAO", direction="nearest", tolerance=tolerance_speed)
        
        # Limpiar columnas duplicadas y renombrar
        df = df.drop(columns=['lat_y', 'lon_y', 'Flight status_y'])
        df = df.rename(columns={'lat_x': 'lat', 'lon_x': 'lon', 'Flight status_x': 'Flight status'})

        # Filtrar vuelos válidos
        df = df[df["Callsign"].notna()]
        df = df[df["Flight status"] == "airborne"]

        # Añadimos categoría de turbulencia
        df = df.merge(df_types, on="ICAO")

        # Hacemos merge con las altitudes y ordenamos
        df_merged = pd.merge(df, df_alt, on=["ICAO", "Callsign", "Timestamp (date)"])
        df_merged.sort_values(by=["ICAO", "Callsign", "Timestamp (date)"], inplace=True)

        # Filtramos las columnas de x (las de df) y las renombramos
        df_filtered = df_merged.filter(like='_x')
        df_filtered.columns = df_filtered.columns.str.replace('_x', '')
        
        # Unimos las columnas filtradas con el resto de columnas necesarias
        df_final = pd.concat([df_filtered, df_merged[["Timestamp (date)", "ICAO", "Callsign", "TurbulenceCategory", "Speed", "Altitude (ft)"]]], axis=1)

        return df_final
    
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
        df_alt = df[df["Altitude (ft)"].notna()]
        df_alt = df_alt[["Timestamp (date)", "ICAO", "Callsign", "Flight status", "Altitude (ft)", "lat", "lon"]]

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

        df1_s = df1.sort_values(["Timestamp (date)", "ICAO"])
        df2_s = df2.sort_values(["Timestamp (date)", "ICAO"])

        t = pd.Timedelta('10 minute')
        dff = pd.merge_asof(df1_s, df2_s, on="Timestamp (date)", by="ICAO", direction="nearest", tolerance=t)

        dff['Timestamp (date)'] = pd.to_datetime(dff['Timestamp (date)'])
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
        df_limpio = df[~df['lat'].isna()]
    
        # Calcula la distancia y tiempo entre filas consecutivas por ICAO
        df_limpio['prev_lat'] = df_limpio.groupby(['ICAO', 'Callsign'])['lat'].shift(1)
        df_limpio['prev_lon'] = df_limpio.groupby(['ICAO', 'Callsign'])['lon'].shift(1)
        df_limpio['distance'] = df_limpio.apply(lambda row: ut.haversine(row['lat'], row['lon'], row['prev_lat'], row['prev_lon']) 
                            if not pd.isna(row['prev_lat']) else 0, axis=1)
        df_limpio['prev_time'] = df_limpio.groupby(['ICAO', 'Callsign'])['Timestamp (date)'].shift(1)
        df_limpio['time_diff'] = df_limpio['Timestamp (date)'] - df_limpio['prev_time']

        # Límites de tiempo y distancia
        DISTANCE_THRESHOLD = 200 
        MINUTES_THRESHOLD = 10
        
        # Filtros
        filtro_distancia = df_limpio['distance'] > DISTANCE_THRESHOLD
        filtro_tiempo = df_limpio['time_diff'] < timedelta(minutes=MINUTES_THRESHOLD)

        # Obtenemos los ICAO de los outliers
        outliers_icao = df_limpio[filtro_distancia & filtro_tiempo]['ICAO'].unique()
        print(f"Se han eliminado un total de {len(outliers_icao)} aeronaves: {outliers_icao}")

        # Eliminamos los outliers (y las columnas auxiliares para el cálculo)
        df_filtered = df[~df['ICAO'].isin(outliers_icao)]

        return df_filtered
