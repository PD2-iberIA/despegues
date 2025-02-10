import numpy as np
import pandas as pd

class DataframeProcessor:
    """Clase que permite realizar operaciones con los dataframes de Pandas"""

    @staticmethod
    def getAirplaneCategories(df):

        # Seleccionamos mensajes ADS-B
        df = df[df["Downlink Format"].isin([17, 18])]

        # Nos quedamos con los ICAOs y su tipo de avión
        df = df[df["TurbulenceCategory"].notna()]
        df = df[["ICAO", "TurbulenceCategory"]].drop_duplicates().reset_index(drop=True)
        return df

    @staticmethod
    def getFlights(df):

        NULL_CALLSIGN = "########"  # valor de nulo de la columna
        flightColumns = ["Timestamp (date)", "ICAO", "Callsign"] # columnas de la proyección

        # Seleccionamos las filas que contengan información relativa al identificador de vuelo
        df_flights = df[df["Callsign"].notna() & (df["Callsign"] != NULL_CALLSIGN)]
        df_flights = df_flights[flightColumns].reset_index(drop=True)
        return df_flights
    
    @staticmethod
    def getPositions(df):

        columnasPosiciones = ["Timestamp (date)", "ICAO", "Flight status", "lat", "lon"]

        # Typecodes que indican posición
        df_pos = df[((5.0 <= df["Typecode"]) & (df["Typecode"]  <= 22.0)) & (df["Typecode"] != 19.0)]
        df_pos = df_pos[columnasPosiciones].reset_index(drop=True)
        
        df_pos
    
    @staticmethod
    def getVelocities(df):

        df_vel = df[df["Speed"].notna()]
        df_vel = df_vel[["Timestamp (date)", "ICAO", "Flight status", "Speed", "lat", "lon"]]

        df_vel_ground = df_vel[df_vel["Flight status"] == "on-ground"]
        df_vel_air = df_vel[df_vel["Flight status"] == "airborne"]

        df_pos = DataframeProcessor.getPositions(df)
        df_pos = df_pos.sort_values(by="Timestamp (date)")
        df_vel_air = df_vel_air.sort_values(by="Timestamp (date)")

        tolerance = pd.Timedelta('1 second')
        df_vel_air_pos = pd.merge_asof(df_pos, df_vel_air, on="Timestamp (date)", by="ICAO", direction="nearest", tolerance=tolerance)

        df_vel_air_pos = df_vel_air_pos[df_vel_air_pos["Speed"].notna()]

        df_vel_air_pos = df_vel_air_pos.drop(columns=['lat_y', 'lon_y', 'Flight status_y'])
        df_vel_air_pos = df_vel_air_pos.rename(columns={'lat_x': 'lat', 'lon_x': 'lon', 'Flight status_x': 'Flight status'})

        df_vel_air_pos = df_vel_air_pos[["Timestamp (date)", "ICAO", "Flight status", "Speed", "lat", "lon"]]

        df_vel_final = pd.concat([df_vel_ground, df_vel_air_pos])

        return df_vel_final
    

    def getFlightsInfo(df):

        df_pos = DataframeProcessor.getPositions(df)
        df_flights = DataframeProcessor.getFlights(df)
        df_types = DataframeProcessor.getAirplaneCategories(df)

        df_pos = df_pos.sort_values(["Timestamp (date)", "ICAO"])
        df_flights = df_flights.sort_values(["Timestamp (date)", "ICAO"])

        tolerance = pd.Timedelta('10 minute')
        df = pd.merge_asof(df_pos, df_flights, on="Timestamp (date)", by="ICAO", direction="nearest", tolerance=tolerance)

        df = df[df["Callsign"].notna()]
        df = df[df["Flight status"] == "airborne"]
        df = df.merge(df_types, on="ICAO")

        return df

    def removeOutlierFlights(df):

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

        removed_planes = df[df['distance'] > DISTANCE_THRESHOLD]['ICAO'].unique()

        df_filtered = df[~df['ICAO'].isin(removed_planes)]
        df_filtered = df_filtered.drop(columns=['prev_lat', 'prev_lon', 'distance'])

        df_filtered


