import tarfile
import pandas as pd
import multiprocessing as mp
import concurrent.futures
import gc
from decoder import Decoder
import os
import pyarrow.parquet as pq

# Configuración
CHUNK_SIZE = 2_000_000 # tamaño de cada chunk
SAVE_EVERY = 10 # guardamos cada 10 chunks (~5M filas por Parquet)
START_ROW = 0 # fila de inicio de lectura

import numpy as np
import os
import pandas as pd
import importlib
from visualization.maps import Maps
import preprocess.utilities as ut
import preprocess.dataframe_processor as dp
from preprocess.dataframe_processor import DataframeProcessor

importlib.reload(dp)

def procesar_y_guardar_particiones(df, transformacion, groupby_columns, output_dir, name):
    """
    Procesa por particiones de un DataFrame aplicando una transformación específica,
    permitiendo definir las columnas para la agrupación.

    Parámetros:
    df (pd.DataFrame): DataFrame con los datos a procesar.
    transformacion (function): Función a aplicar a cada partición del DataFrame.
    groupby_columns (list): Lista de columnas por las cuales se agruparán los datos.
    output_dir (str): Nombre del directorio donde se guardarán las particiones. 
    name (str): Nombre del archivo final combinado.

    Return:
    str: Ruta del archivo Parquet consolidado tras aplicar la transformación.

    Funcionalidad:
    - Convierte valores de cadena a NaN en el DataFrame.
    - Crea una carpeta auxiliar `output_dir` si no existe.
    - Agrupa por las columnas definidas en `groupby_columns` y asigna particiones basadas en el módulo 10.
    - Aplica la función `transformacion` a cada partición.
    - Guarda cada partición como un archivo Parquet.
    - Combina todas las particiones en un solo archivo Parquet final (`name.parquet`).
    """

    # Procesamos los datos de la forma adecuada
    df = ut.stringToNan(df)

    # Crear la carpeta auxiliar si no existe
    os.makedirs(output_dir, exist_ok=True)

    # Agrupar por las columnas especificadas y asignar número de partición
    df['partition'] = (df.groupby(groupby_columns).ngroup() % 10)

    # Guardar cada partición en un archivo Parquet aplicando la función de transformación
    for partition_num in range(10):
        partition_df = df[df['partition'] == partition_num]
        print(partition_df.shape)
        
        # Aplicar la transformación recibida como parámetro
        processed_df = transformacion(partition_df)
        print(processed_df.shape)
        
        # Eliminar la columna 'partition'
        if 'partition' in processed_df.columns:
            processed_df = processed_df.drop(columns=['partition'])
        
        # Definir la ruta del archivo Parquet
        partition_file_path = os.path.join(output_dir, f'partition_{partition_num}.parquet')
        
        # Guardar el DataFrame procesado en formato Parquet
        processed_df.to_parquet(partition_file_path, index=False)

    print(f"Particiones procesadas y guardadas en formato Parquet en la carpeta {output_dir}.")

    # Inicializar una lista para almacenar los DataFrames
    dataframes = []

    # Recorrer las particiones y cargar cada archivo Parquet
    for partition_num in range(10):
        partition_file_path = os.path.join(output_dir, f'partition_{partition_num}.parquet')
        
        if os.path.exists(partition_file_path):
            partition_df = pd.read_parquet(partition_file_path)
            dataframes.append(partition_df)

    # Combinar todos los DataFrames en uno solo
    combined_df = pd.concat(dataframes, ignore_index=True)

    # Guardar el DataFrame combinado en formato Parquet
    combined_file_path = f'{name}.parquet'
    combined_df.to_parquet(combined_file_path, index=False)

    print(f"Todas las particiones han sido combinadas y guardadas en '{name}.parquet'.")

    return combined_file_path


def consolidar_parquet(directory):
    """
    Consolida múltiples archivos Parquet de subcarpetas en un único archivo Parquet.

    Parámetros:
    directory (str): Ruta de la carpeta principal que contiene las subcarpetas con archivos Parquet.

    Retorna:
    str: Ruta del archivo Parquet consolidado.

    Excepciones:
    - FileNotFoundError: Si el directorio base no existe.
    - ValueError: Si no se encuentran archivos Parquet en las subcarpetas.s

    Funcionalidad:
    - Recorre todas las subcarpetas dentro de `directory`.
    - Busca y lee los archivos Parquet dentro de cada subcarpeta.
    - Concatena todos los DataFrames en un único DataFrame.
    - Guarda el DataFrame consolidado en un archivo Parquet en `directory.parquet`.
    """
    if not os.path.exists(directory):
        raise FileNotFoundError(f"El directorio '{directory}' no existe.")

    # Definir la ruta de salida
    output_file = f"{directory}.parquet"

    # Lista para almacenar DataFrames
    dataframes = []

    # Recorrer las subcarpetas dentro de la carpeta principal
    for subfolder in os.listdir(directory):
        subfolder_path = os.path.join(directory, subfolder)

        if os.path.isdir(subfolder_path):  # Verificar que sea una carpeta
            parquet_files = [f for f in os.listdir(subfolder_path) if f.endswith(".parquet")]

            for parquet_file in parquet_files:
                parquet_path = os.path.join(subfolder_path, parquet_file)
                df = pd.read_parquet(parquet_path)
                dataframes.append(df)

    if not dataframes:
        raise ValueError("No se encontraron archivos Parquet en las subcarpetas.")

    # Concatenar todos los DataFrames
    df_final = pd.concat(dataframes, ignore_index=True)

    # Guardar el resultado en un único archivo Parquet
    df_final.to_parquet(output_file, index=False)

    print(f"Parquet consolidado guardado en: {output_file}")

    return output_file


def apply_parallel(df, func, num_workers=mp.cpu_count()):
    """Divide el DataFrame y aplica la función para procesamiento en paralelo usando ProcessPoolExecutor.
    
    Args:
        df (DataFrame): Datos.
        func: Función a ejecutar.
        num_workers: Número de CPUs.
    Returns:
        (DataFrame)
    """
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        chunks = [df.iloc[i::num_workers, :].copy() for i in range(num_workers)]
        results = executor.map(func, chunks)
    return pd.concat(results, ignore_index=True)

def process_chunk(chunk):
    """Procesa un solo chunk en paralelo.
    
    Args:
        chunk: Chunk de datos leído del archivo csv.
    Returns:
        (dict): Diccionario resultante del procesamiento del chunk.
    """
    return chunk.apply(lambda x: safe_process_message(x["message"], x["ts_kafka"]), axis=1).apply(pd.Series)

def read_data(tar_path, file_name):
    """Lee el archivo TAR y procesa los datos en chunks con paralelismo.
    
    Args:
        tar_path (str): Ruta al archivo .tar que contiene los datos comprimidos.
        file_path (str): Ruta al archivo .csv que está contenido en el .tar.
    """
    dfs = []
    num_chunks = 0
    part = 0

    with tarfile.open(tar_path, "r") as tar:
        csv_file = tar.extractfile(file_name)

        for chunk in pd.read_csv(csv_file, chunksize=CHUNK_SIZE, sep=';', skiprows=range(1, START_ROW + 1), header=0):
            try:
                chunk.index += START_ROW 
                num_chunks += 1
                print(f"🔹 Procesando chunk {num_chunks}...")

                # Eliminar columna no deseada
                if "Unnamed: 2" in chunk.columns:
                    chunk = chunk.drop(columns=['Unnamed: 2'])

                # Conversión de tipos para ahorro de memoria
                conversiones = {
                    "columna_int": "int32",
                    "columna_float": "float32"
                }
                for col, dtype in conversiones.items():
                    if col in chunk.columns:
                        chunk[col] = chunk[col].astype(dtype)

                # Procesamiento en paralelo
                df = apply_parallel(chunk, process_chunk)
                dfs.append(df)

                # Liberar memoria del chunk procesado
                del chunk
                gc.collect()

                # Guardar cada SAVE_EVERY chunks
                if num_chunks % SAVE_EVERY == 0:
                    part += 1
                    save_parquet(dfs, part)
                    dfs = []  # Vaciar lista después de guardar

            except Exception as e:
                print(f"Error procesando el chunk {num_chunks}: {e}")

        # Guardar el último bloque pendiente
        if dfs:
            part += 1
            save_parquet(dfs, part)

def save_parquet(dfs, part):
    """Guarda los datos en un archivo Parquet, manejando errores de conversión.
    
    Args:
        dfs (list): Lista de DataFrames.
        part (int): Número de partición.
    """
    try:
        final_df = pd.concat(dfs, ignore_index=True)

        # Manejo de errores en tipos de datos al guardar
        for col in final_df.columns:
            if final_df[col].dtype == "object":
                final_df[col] = final_df[col].astype(str)  # Evita errores con None y tuplas
            elif final_df[col].dtype in ["float32", "float64"]:
                final_df[col] = pd.to_numeric(final_df[col].replace({None: float("nan")}), errors="coerce").fillna(0)

        final_df.to_parquet(f"data/part_{part}.parquet")
        print(f"Guardado: data/part_{part}.parquet")

        # Liberar memoria
        del final_df
        gc.collect()

    except Exception as e:
        print(f"Error guardando Parquet (parte {part}): {e}")


def safe_process_message(message, ts_kafka):
    """Envuelve Decoder.processMessage en un try-except para manejar errores.
    
    Args:
        message: Mensaje enviado por una aeronave.
        ts_kafka: Timestamp del mensaje.
    Returns:
        (dict): Diccionario con el mensaje codificado o en caso de error un diccionario vacío.
    """
    try:
        return Decoder.processMessage(message, ts_kafka)
    except Exception as e:
        print(f"Error en processMessage: {e}")
        return {}  # Devuelve un diccionario vacío en caso de error
    
