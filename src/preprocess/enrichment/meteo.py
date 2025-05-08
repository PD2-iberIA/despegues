import pandas as pd

class MeteoDataset:
    """
    Clase para cargar datos meteorológicos desde un archivo parquet y fusionarlos con otro DataFrame
    basado en la columna de tiempo.
    """
    def __init__(self, meteo_path=None):
        """
        Inicializa la clase con la ruta del archivo de datos meteorológicos.
        Si se proporciona una ruta, carga los datos.
        """
        self.meteo_df = self._load_meteo_data(meteo_path) if meteo_path else None

    def _load_meteo_data(self, path):
        """
        Carga los datos meteorológicos desde un archivo parquet.
        Convierte la columna 'time' a tipo datetime.
        Maneja errores si el archivo no se encuentra o hay otros problemas.
        """
        try:
            meteo_df = pd.read_csv("../src/open-meteo-40.53N3.56W602m-2.csv", skiprows=2)
            meteo_df['time'] = pd.to_datetime(meteo_df['time'])
            return meteo_df
        except FileNotFoundError:
            print(f"Error: El archivo de meteorología en '{path}' no se encontró.")
            return None

    def merge(self, df, on='time', how='inner'):
        """
        Fusiona el DataFrame proporcionado con el DataFrame meteorológico usando la columna de tiempo.
        Convierte 'timestamp' a datetime, lo trunca a la hora y luego hace el merge.
        Ordena el resultado por timestamp y reinicia el índice.
        """
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['time'] = df['timestamp'].dt.floor('H')  # Truncar a la hora
        
        if self.meteo_df is None:
            print("Advertencia: El DataFrame de meteorología no ha sido cargado.")
            return df

        df_merged = pd.merge(df, self.meteo_df, on=on, how=how)
        if df_merged.empty:
            print("Advertencia: No se encontraron coincidencias entre los DataFrames.")
            return None
        # Ordenar por timestamp y reiniciar el índice
        df_merged = df_merged.sort_values(by='timestamp').reset_index(drop=True)
        return df_merged
