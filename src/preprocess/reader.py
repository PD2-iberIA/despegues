import tarfile
import pandas as pd
from preprocess.decoder import Decoder

class Reader:

    TAR_PATH = "202412010000_202412072359.tar"
    FILE_NAME = "202412010000_202412072359.csv"

    CHUNK_SIZE = 500_000 # número de filas por chunk

    @staticmethod
    def read_data():
        with tarfile.open(Reader.TAR_PATH, "r") as tar:
            csv_file = tar.extractfile(Reader.FILE_NAME)

            num_chunks = 0
            for chunk in pd.read_csv(csv_file, chunksize=Reader.CHUNK_SIZE):
                # Aquí habría que procesar cada chunk
                #df = chunk.apply(lambda x: Decoder.processMessage(x["message"], x["ts_kafka"]), axis=1).apply(pd.Series)
                print(chunk.head()) # de momento mostramos el head de cada uno
                num_chunks += 1
