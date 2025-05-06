import pandas as pd
import re

class Depurador:
    """
    Clase para depurar datos de un DataFrame tras aplicar el pipeline de preprocesado y añadir los datos meteorologicos.
    """

    def __init__(self):
        pass

    @staticmethod
    def apply_basic(df):
        # Modificaciones estéticas
        df["turbulence_category"] = df["turbulence_category"].apply(lambda x: re.sub(r"\(.*\)", "", str(x)).strip())
        df["last_event_turb_cat"] = df["last_event_turb_cat"].apply(lambda x: re.sub(r"\(.*\)", "", str(x)).strip())

        # Eliminamos la columna con mayor número de nulos 
        df = df.drop(columns=["snow_depth (m)"], axis=1)

        # Selección de columnas
        eliminar_no_varianza = [
            "last_event", 
        ]

        eliminar_correlacion = [
            'apparent_temperature (°C)', 'rain (mm)', 'pressure_msl (hPa)', 'wind_speed_100m (km/h)', 'wind_gusts_10m (km/h)', 'soil_temperature_0_to_7cm (°C)', 'soil_temperature_7_to_28cm (°C)', 'soil_temperature_28_to_100cm (°C)', 'soil_moisture_7_to_28cm (m³/m³)', 'soil_moisture_28_to_100cm (m³/m³)', 'vapour_pressure_deficit (kPa)' 
        ]

        eliminar_otras = [
            'lat', 'lon', 'event_timestamp', 'first_holding_time', 'first_airborne_time', 'first_on_ground_time', 'time'
        ]

        variables_eliminar = eliminar_no_varianza + eliminar_correlacion + eliminar_otras

        df = df.drop(columns = variables_eliminar, axis=1)

        # Convertimos la columna 'timestamp' a tipo datetime (si aún no lo está)
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        return df


    @staticmethod
    def remove_bounds_callsigns(df, columna, limite_inferior, limite_superior):
        """
        Elimina todas las filas que no cumplen con la condición especificada en la columna
        (por debajo del límite inferior o por encima del límite superior),
        y luego elimina todas las filas con el mismo callsign que esas filas no válidas
        **solo del mismo día**.

        Parámetros:
            df (pd.DataFrame): DataFrame original.
            columna (str): Nombre de la columna en la cual aplicar la condición.
            limite_inferior (float): Valor mínimo para que la fila sea válida.
            limite_superior (float): Valor máximo para que la fila sea válida.

        Retorna:
            pd.DataFrame: DataFrame filtrado sin las filas no válidas y sin los callsign asociados.
            int: Número de filas eliminadas.
        """
        # Filtramos el DataFrame para obtener las filas válidas
        df_valido = df[(df[columna] > limite_inferior) & (df[columna] < limite_superior)]
        
        # Identificamos las filas no válidas
        filas_invalidas = df[(df[columna] <= limite_inferior) | (df[columna] >= limite_superior)]
        
        # Extraemos la fecha del timestamp (sin la parte de la hora)
        filas_invalidas['fecha'] = pd.to_datetime(filas_invalidas['timestamp']).dt.date
        
        # Identificamos los callsign de las filas no válidas y sus fechas correspondientes
        callsign_invalidos = filas_invalidas[['callsign', 'fecha']].drop_duplicates()
        
        # Para cada callsign, eliminamos las filas del mismo día que contienen ese callsign
        df_final = df.copy()
        for callsign, fecha in zip(callsign_invalidos['callsign'], callsign_invalidos['fecha']):
            df_final = df_final[~((df_final['callsign'] == callsign) & (pd.to_datetime(df_final['timestamp']).dt.date == fecha))]
        
        # Contamos el número de filas eliminadas
        filas_eliminadas = len(df) - len(df_final)
        
        return df_final, filas_eliminadas, callsign_invalidos




