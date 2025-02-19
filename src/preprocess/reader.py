import tarfile
import pandas as pd
import multiprocessing as mp
import concurrent.futures
import gc
from decoder import Decoder

# Configuración

TAR_PATH = "C:/Users/bryan/PycharmProjects/despegues/src/202412010000_202412072359.tar"
FILE_NAME = "202412010000_202412072359.csv"

CHUNK_SIZE = 2_000_000
SAVE_EVERY = 10 # Guardar cada 10 chunks (~5M filas por Parquet)
START_ROW = 0

# Procesamiento en paralelo
def apply_parallel(df, func, num_workers=mp.cpu_count()):
    """Divide el DataFrame y aplica la función en paralelo usando ProcessPoolExecutor."""
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        chunks = [df.iloc[i::num_workers, :].copy() for i in range(num_workers)]
        results = executor.map(func, chunks)
    return pd.concat(results, ignore_index=True)


def process_chunk(chunk):
    """Procesa un solo chunk en paralelo."""
    return chunk.apply(lambda x: safe_process_message(x["message"], x["ts_kafka"]), axis=1).apply(pd.Series)


def read_data():
    """Lee el archivo TAR y procesa los datos en chunks con paralelismo."""
    dfs = []
    num_chunks = 0
    part = 0

    with tarfile.open(TAR_PATH, "r") as tar:
        csv_file = tar.extractfile(FILE_NAME)

        for chunk in pd.read_csv(csv_file, chunksize=CHUNK_SIZE, sep=';', skiprows=range(1, START_ROW + 1), header=0):
            try:
                chunk.index += START_ROW  # Ajustar índices manualmente
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
                print(f"❌ Error procesando el chunk {num_chunks}: {e}")

        # Guardar el último bloque pendiente
        if dfs:
            part += 1
            save_parquet(dfs, part)


def save_parquet(dfs, part):
    """Guarda los datos en un archivo Parquet, manejando errores de conversión."""
    try:
        final_df = pd.concat(dfs, ignore_index=True)

        # Manejo de errores en tipos de datos al guardar
        for col in final_df.columns:
            if final_df[col].dtype == "object":
                final_df[col] = final_df[col].astype(str)  # Evita errores con None y tuplas
            elif final_df[col].dtype in ["float32", "float64"]:
                final_df[col] = pd.to_numeric(final_df[col].replace({None: float("nan")}), errors="coerce").fillna(0)

        final_df.to_parquet(f"data/part_{part}.parquet")
        print(f"✅ Guardado: data/part_{part}.parquet")

        # Liberar memoria
        del final_df
        gc.collect()

    except Exception as e:
        print(f"❌ Error guardando Parquet (parte {part}): {e}")


def safe_process_message(message, ts_kafka):
    """Envuelve Decoder.processMessage en un try-except para manejar errores."""
    try:
        return Decoder.processMessage(message, ts_kafka)
    except Exception as e:
        print(f"⚠️ Error en processMessage: {e}")
        return {}  # Devuelve un diccionario vacío en caso de error


if __name__ == "__main__":
    read_data()
