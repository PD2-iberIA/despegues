import numpy as np
import pandas as pd
import preprocess.utilities as ut

class DataframeProcessor:
    """Clase que permite realizar operaciones de procesamiento y análisis de datos con los dataframes de Pandas"""

    @staticmethod
    def getFlightStatus(df):
        """ Creamos el df para el diagrama de barras de aviones aterrizados vs en vuelo"""
        df_status = df.groupby(['hour', 'Flight status']).size().unstack(fill_value=0)
        df_status = df_status.reset_index()
        df_status = df_status.melt(id_vars=['hour'], var_name='Flight Status', value_name='Count')
        return df_status
    
    @staticmethod
    def getWaitTimes(df):
        """Creamos el df para las gráficas de tiempo de espera"""
        df1 = DataframeProcessor.getVelocities(df)
        df2 = DataframeProcessor.getFlights(df)

        df1_s = df1.sort_values(["Timestamp (date)", "ICAO"])
        df2_s = df2.sort_values(["Timestamp (date)", "ICAO"])

        t = pd.Timedelta('10 minute')
        dff = pd.merge_asof(df1_s, df2_s, on="Timestamp (date)", by="ICAO", direction="nearest", tolerance=t)

        # Ensure data is sorted by Flight ID and timestamp
        dff = dff.sort_values(by=["Callsign", "Timestamp (date)"])

        # Separate on-ground and airborne events
        on_ground = dff[(dff["Flight status"] == "on-ground") & (dff["Speed"]==0)].groupby("Callsign")["Timestamp (date)"].min()
        airborne = dff[dff["Flight status"] == "airborne"].groupby("Callsign")["Timestamp (date)"].min()

        # Convertimos las series en df de Pandas
        on_ground = pd.DataFrame(on_ground)
        on_ground.columns = ["ts ground"]

        airborne = pd.DataFrame(airborne)
        airborne.columns = ["ts airborne"]

        # Creamos las columnas de tiempos de espera
        df_wait_times = on_ground.merge(airborne, how="inner", on="Callsign")
        df_wait_times = df_wait_times[df_wait_times["ts airborne"] > df_wait_times["ts ground"]]
        df_wait_times["Wait time"] = df_wait_times["ts airborne"] - df_wait_times["ts ground"]
        df_wait_times["Wait time (s)"] = df_wait_times["Wait time"].dt.total_seconds()
        df_wait_times = ut.extractDaysOfTheWeek(df_wait_times)

        return df_wait_times

    @staticmethod
    def getAirplaneCategories(df):
        """
        Genera un DataFrame solo con los datos de la categoría de los aviones.
        
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
        df_vel = df[df["Speed"].notna()]
        df_vel = df_vel[["Timestamp (date)", "ICAO", "Flight status", "Speed", "lat", "lon"]]

        # Dividimos en 2 dataframes según si los vuelos están en tierra o en aire
        df_vel_ground = df_vel[df_vel["Flight status"] == "on-ground"]
        df_vel_air = df_vel[df_vel["Flight status"] == "airborne"]

        df_pos = DataframeProcessor.getPositions(df)
        df_pos = df_pos.sort_values(by="Timestamp (date)")
        df_vel_air = df_vel_air.sort_values(by="Timestamp (date)")

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

        return df_vel_final
    
    @staticmethod
    def getFlightsInfo(df):
        """
        Genera un dataframe con los datos necesarios para visualizar la  información de vuelo.

        Parámetros:
            df: DataFrame de datos.

        Devuelve:
            DataFrame con las siguientes columnas: "Timestamp (date)", "ICAO", "Flight status", "lat", "lon", "Callsign", "TurbulenceCategory".
        """

        df_pos = DataframeProcessor.getPositions(df)
        df_flights = DataframeProcessor.getFlights(df)
        df_types = DataframeProcessor.getAirplaneCategories(df)

        df_pos = df_pos.sort_values(["Timestamp (date)", "ICAO"])
        df_flights = df_flights.sort_values(["Timestamp (date)", "ICAO"])

        tolerance = pd.Timedelta('10 minute') # tolerancia
        df = pd.merge_asof(df_pos, df_flights, on="Timestamp (date)", by="ICAO", direction="nearest", tolerance=tolerance)

        df = df[df["Callsign"].notna()]
        df = df[df["Flight status"] == "airborne"]
        df = df.merge(df_types, on="ICAO")

        return df
    
    @staticmethod
    def getAltitudes(df):
        """
        Genera un Dataframe con los datos necesarios para visualizar las altitudes por trayectoria.
        
        Parámetros:
            df: DataFrame de datos.

        Devuelve:
            DataFrame con las siguientes columnas: "Timestamp (date)", "ICAO", "Flight status", "lat", "lon", "Callsign", "TurbulenceCategory".
        """
        # DataFrame filtrando las filas que contienen una altitud no nula
        df_alt = df[df["Altitude (ft)"].notna()]
        df_alt = df_alt[["Timestamp (date)", "ICAO", "Callsign", "Flight status", "Altitude (ft)", "lat", "lon"]]

        # DataFrame para las trayectorias
        df_traj = DataframeProcessor.getFlightsInfo(df)

        # Hacemos merge y ordenamos
        df_merged = pd.merge(df_traj, df_alt, on=["ICAO", "Callsign", "Timestamp (date)"])
        df_merged.sort_values(by=["ICAO", "Callsign", "Timestamp (date)"], inplace=True)

        # Filtramos las columnas de x (las de df_traj) y las renombramos
        df_filtered = df_merged.filter(like='_x')
        df_filtered.columns = df_filtered.columns.str.replace('_x', '')
        
        # Unimos las columnas filtradas con las demás columnas necesarias
        df_final = pd.concat([df_filtered, df_merged[['ICAO', 'Callsign', 'Timestamp (date)', 'Altitude (ft)']]], axis=1)

        return df_final

    @staticmethod
    def removeOutlierFlights(df):
        """
        Elimina vuelos considerados outliers de un DataFrame.

        Parámetros:
            df: DataFrame de datos.

        Devuelve:
            DataFrame con los datos limpios.
        """

        # Calcula la distancia entre dos puntos geográficos
        def haversine(lat1, lon1, lat2, lon2):
            R = 6371  # Radio de la Tierra en km
            phi1, phi2 = np.radians(lat1), np.radians(lat2)
            delta_phi = np.radians(lat2 - lat1)
            delta_lambda = np.radians(lon2 - lon1)

            a = np.sin(delta_phi / 2)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2)**2
            c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
            return R * c  # Distancia en km

        # Calcula la distancia entre filas consecutivas por ICAO y Callsign
        df['prev_lat'] = df.groupby(['ICAO', 'Callsign'])['lat'].shift(1)
        df['prev_lon'] = df.groupby(['ICAO', 'Callsign'])['lon'].shift(1)
        df['distance'] = df.apply(lambda row: haversine(row['lat'], row['lon'], row['prev_lat'], row['prev_lon']) 
                                if not pd.isna(row['prev_lat']) else 0, axis=1)

        DISTANCE_THRESHOLD = 200 

        # Outlier si la distancia supera los 200km
        removed_planes = df[df['distance'] > DISTANCE_THRESHOLD]['ICAO'].unique()

        # Eliminamos los outliers (y las columnas auxiliares para el cálculo)
        df_filtered = df[~df['ICAO'].isin(removed_planes)]
        df_filtered = df_filtered.drop(columns=['prev_lat', 'prev_lon', 'distance'])

        return df_filtered
