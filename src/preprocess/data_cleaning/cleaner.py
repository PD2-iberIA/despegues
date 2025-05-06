import pandas as pd
import re

class Cleaner:
    """
    Clase para depurar datos de un DataFrame tras aplicar el pipeline de preprocesado 
    y añadir los datos meteorológicos.
    """

    def __init__(self):
        pass

    @staticmethod
    def apply_basic(df):
        """
        Aplica una limpieza básica al DataFrame, eliminando columnas irrelevantes, 
        renombrando categorías y formateando fechas.

        :param df: DataFrame original con las columnas meteorológicas y de eventos.
        :type df: pd.DataFrame
        :return: DataFrame depurado.
        :rtype: pd.DataFrame
        """

        # Modificaciones estéticas: limpiamos las categorías de turbulencia
        df["turbulence_category"] = df["turbulence_category"].apply(lambda x: re.sub(r"\(.*\)", "", str(x)).strip())
        df["last_event_turb_cat"] = df["last_event_turb_cat"].apply(lambda x: re.sub(r"\(.*\)", "", str(x)).strip())

        # Eliminamos la columna con mayor número de nulos
        df = df.drop(columns=["snow_depth (m)"], axis=1)

        # Columnas sin varianza
        eliminar_no_varianza = [
            "last_event", 
        ]

        # Columnas altamente correlacionadas
        eliminar_correlacion = [
            'apparent_temperature (°C)', 'rain (mm)', 'pressure_msl (hPa)', 'wind_speed_100m (km/h)',
            'wind_gusts_10m (km/h)', 'soil_temperature_0_to_7cm (°C)', 'soil_temperature_7_to_28cm (°C)',
            'soil_temperature_28_to_100cm (°C)', 'soil_moisture_7_to_28cm (m³/m³)', 
            'soil_moisture_28_to_100cm (m³/m³)', 'vapour_pressure_deficit (kPa)' 
        ]

        # Otras columnas no necesarias
        eliminar_otras = [
            'lat', 'lon', 'event_timestamp', 'first_holding_time', 'first_airborne_time',
            'first_on_ground_time', 'time'
        ]

        # Eliminamos todas las columnas marcadas
        variables_eliminar = eliminar_no_varianza + eliminar_correlacion + eliminar_otras
        df = df.drop(columns=variables_eliminar, axis=1)

        # Convertimos 'timestamp' a tipo datetime si aún no lo es
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        return df

    @staticmethod
    def remove_bounds_callsigns(df, columna, limite_inferior, limite_superior):
        """
        Elimina todas las filas que no cumplen con los límites establecidos en una columna, 
        y luego elimina todas las filas que tienen el mismo 'callsign' en la misma fecha.

        :param df: DataFrame original.
        :type df: pd.DataFrame
        :param columna: Nombre de la columna a verificar contra los límites.
        :type columna: str
        :param limite_inferior: Límite inferior válido para la columna.
        :type limite_inferior: float
        :param limite_superior: Límite superior válido para la columna.
        :type limite_superior: float
        :return: Una tupla con el DataFrame filtrado, número de filas eliminadas, y los 'callsigns' eliminados por día.
        :rtype: tuple[pd.DataFrame, int, pd.DataFrame]
        """

        # Filtramos las filas válidas según los límites
        df_valido = df[(df[columna] > limite_inferior) & (df[columna] < limite_superior)]

        # Filtramos las filas no válidas
        filas_invalidas = df[(df[columna] <= limite_inferior) | (df[columna] >= limite_superior)]

        # Añadimos columna con la fecha sin la hora
        filas_invalidas['fecha'] = pd.to_datetime(filas_invalidas['timestamp']).dt.date

        # Extraemos las combinaciones únicas de callsign y fecha
        callsign_invalidos = filas_invalidas[['callsign', 'fecha']].drop_duplicates()

        # Eliminamos del DataFrame original cualquier fila que tenga los mismos callsign y fecha que los inválidos
        df_final = df.copy()
        for callsign, fecha in zip(callsign_invalidos['callsign'], callsign_invalidos['fecha']):
            df_final = df_final[~((df_final['callsign'] == callsign) & 
                                  (pd.to_datetime(df_final['timestamp']).dt.date == fecha))]

        # Número de filas eliminadas
        filas_eliminadas = len(df) - len(df_final)

        return df_final, filas_eliminadas, callsign_invalidos
