
import pandas as pd
import os

# 1. Cargar y combinar los archivos .parquet
folder_path = 'datatest'
parquet_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.parquet')]
df = pd.concat([pd.read_parquet(file) for file in parquet_files], ignore_index=True)

# 2. Cargar los datos meteorológicos
df_meteo = pd.read_csv("open-meteo-40.53N3.56W602m-2.csv", skiprows=2)

# 3. Asegurarse de que las columnas 'timestamp' y 'time' son datetime
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['time'] = df['timestamp'].dt.floor('H')  # Truncar a la hora

df_meteo['time'] = pd.to_datetime(df_meteo['time'])

# 4. Hacer el merge por la columna 'time'
df_merged = pd.merge(df, df_meteo, on='time', how='inner')

# 5. Ordenar por timestamp y reiniciar el índice
df_merged = df_merged.sort_values(by='timestamp').reset_index(drop=True)

# 6. Guardar si se desea
df_merged.to_parquet("combinado_meteo.parquet")

df_merged
