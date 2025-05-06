import pandas as pd

class MeteoDataset:
    def __init__(self, meteo_path=None):
        self.meteo_df = self._load_meteo_data(meteo_path) if meteo_path else None

    def _load_meteo_data(self, path):
        try:
            meteo_df = pd.read_parquet(path)
            meteo_df['time'] = pd.to_datetime(meteo_df['time'])
            return meteo_df
        except FileNotFoundError:
            print(f"Error: El archivo de meteorología en '{path}' no se encontró.")
            return None
        except Exception as e:
            print(f"Error al cargar el archivo de meteorología: {e}")
            return None

    def merge(self, df, on='time', how='inner'):
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



if data_df is not None:
    merged_df = meteo_dataset.merge(data_df)
    if merged_df is not None:
        print(merged_df.head())