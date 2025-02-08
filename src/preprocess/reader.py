import tarfile
import pandas as pd
from decoder import Decoder

class Reader:

    TAR_PATH = "202412010000_202412072359.tar"
    FILE_NAME = "202412010000_202412072359.csv"

    CHUNK_SIZE = 500_000

    @staticmethod
    def read_data():

        # Lista para almacenar los dataframes de cada chunk
        dfs = []

        with tarfile.open(Reader.TAR_PATH, "r") as tar:
            csv_file = tar.extractfile(Reader.FILE_NAME)
            num_chunks = 0

            for chunk in pd.read_csv(csv_file, chunksize=Reader.CHUNK_SIZE):
                try:
                    # Eliminamos la columna no deseada
                    chunk = chunk.drop(columns=['Unnamed: 2'])

                    # Aplicamos la función de decodificación
                    df = chunk.apply(
                        lambda x: Reader.safe_process_message(x["message"], x["ts_kafka"]), axis=1
                    ).apply(pd.Series)

                    print(chunk.head())

                    dfs.append(df)
                    num_chunks += 1
                except Exception as e:
                    print(f"Error procesando el chunk {num_chunks}: {e}")
            
            # Concatenamos los chunks y guardamos en parquet
            final_df = pd.concat(dfs, ignore_index=True)
            final_df.to_parquet("final_output.parquet")

    @staticmethod
    def safe_process_message(message, ts_kafka):
        """Envuelve Decoder.processMessage en un try-except para manejar errores."""
        try:
            return Decoder.processMessage(message, ts_kafka)
        except Exception as e:
            print(f"Error en processMessage: {e}")
            # Devuelve un diccionario vacío en caso de error
            return {}     
