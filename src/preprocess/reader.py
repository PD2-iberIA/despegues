import tarfile
import pandas as pd
from preprocess.decoder import Decoder
import gc

class Reader:

    TAR_PATH = "202412010000_202412072359.tar"
    FILE_NAME = "202412010000_202412072359.csv"

    CHUNK_SIZE = 500_000
    SAVE_EVERY = 10 # guardamos cada 10 chunks (~5M filas)

    @staticmethod
    def read_data():

        # Lista para almacenar los dataframes de cada chunk
        dfs = []

        num_chunks = 0
        part = 0

        with tarfile.open(Reader.TAR_PATH, "r") as tar:
            csv_file = tar.extractfile(Reader.FILE_NAME)

            for chunk in pd.read_csv(csv_file, chunksize=Reader.CHUNK_SIZE, sep=';'):
                try:
                    print(f"Procesando chunk {num_chunks}")
                    num_chunks += 1

                    # Eliminamos la columna no deseada
                    if "Unnamed: 2" in chunk.columns:
                        chunk = chunk.drop(columns=['Unnamed: 2'])

                    # Aplicamos la función de decodificación
                    df = chunk.apply(
                        lambda x: Reader.safe_process_message(x["message"], x["ts_kafka"]), axis=1
                    ).apply(pd.Series)

                    dfs.append(df)

                    # Guardamos cada SAVE_EVERY chunks en un solo archivo Parquet
                    if num_chunks % Reader.SAVE_EVERY == 0:
                        part += 1

                        final_df = pd.concat(dfs, ignore_index=True)
                        final_df.to_parquet(f"data/part_{part}.parquet")

                        # Liberamos la memoria RAM
                        del final_df, dfs[:]
                        gc.collect()

                except Exception as e:
                    print(f"Error procesando el chunk {num_chunks-1}: {e}")

            # Guardamos el último bloque de datos si hay algo pendiente
            if dfs:
                part += 1
                final_df = pd.concat(dfs, ignore_index=True)
                final_df.to_parquet(f"data/part_{part}.parquet")
                del final_df, dfs[:]
                gc.collect()

    @staticmethod
    def safe_process_message(message, ts_kafka):
        """Envuelve Decoder.processMessage en un try-except para manejar errores."""
        try:
            return Decoder.processMessage(message, ts_kafka)
        except Exception as e:
            print(f"Error en processMessage: {e}")
            # Devuelve un diccionario vacío en caso de error
            return {}     
