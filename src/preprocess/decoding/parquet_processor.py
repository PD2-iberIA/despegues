import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, date_format, to_timestamp, lit, concat_ws, when

class ParquetProcessor:
    def __init__(self, filepaths, output_folder="processed_data"):
        """
        Inicializa el procesador con una lista de rutas a archivos Parquet.

        :param filepaths: Lista de rutas de archivos Parquet.
        :param output_folder: Carpeta donde se guardarán los archivos procesados.
        """
        self.spark = SparkSession.builder.appName("ParquetProcessor").getOrCreate()
        self.filepaths = filepaths
        self.output_folder = output_folder

    def process_file(self, file):
        """
        Carga un archivo Parquet, lo divide por fecha y hora y guarda los resultados,
        fusionando datos si ya existe un archivo de la misma hora.

        :param file: Nombre del archivo Parquet.
        """
        try:
            df = self.spark.read.parquet(file)

            # Convertir la columna "Static air temperature (C)" a string si existe
            if "Static air temperature (C)" in df.columns:
                df = df.withColumn("Static air temperature (C)", col("Static air temperature (C)").cast("string"))

            # Asegurar que la columna Timestamp está en formato timestamp
            df = df.withColumn("Timestamp (date)", to_timestamp("Timestamp (date)"))

            # Filtrar valores nulos en "Timestamp (date)"
            df = df.na.drop(subset=["Timestamp (date)"])

            # Extraer fecha y hora
            df = df.withColumn("date", date_format("Timestamp (date)", "yyyy-MM-dd"))
            df = df.withColumn("hour", date_format("Timestamp (date)", "HH"))

            # Guardar por día y hora
            for date, hour in df.select("date", "hour").distinct().collect():
                folder_path = os.path.join(self.output_folder, date, hour)
                os.makedirs(folder_path, exist_ok=True)
                output_filepath = os.path.join(folder_path, f"data_{date}_{hour}.parquet")

                # Filtrar datos de esa fecha y hora
                df_filtered = df.filter((col("date") == date) & (col("hour") == hour))

                # Si existe, fusionar datos
                if os.path.exists(output_filepath):
                    existing_df = self.spark.read.parquet(output_filepath)
                    if "Static air temperature (C)" in existing_df.columns:
                        existing_df = existing_df.withColumn("Static air temperature (C)", col("Static air temperature (C)").cast("string"))
                    df_filtered = existing_df.union(df_filtered)

                df_filtered.write.mode("overwrite").parquet(output_filepath)
                print(f"Actualizado {output_filepath}")

        except Exception as e:
            print(f"Error al procesar {file}: {e}")

    def process(self):
        """
        Procesa cada archivo de la lista sin concatenarlos en memoria.
        """
        for file in self.filepaths:
            print(f"Procesando {file}...")
            self.process_file(file)

    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Limpia los datos reemplazando valores nulos y no convertibles.

        :param df: DataFrame de Spark.
        :return: DataFrame limpio.
        """
        return df.replace(["null", "None", "", "nan"], None)

    def convert_data_types(self, df: DataFrame) -> DataFrame:
        """
        Convierte las columnas del df a los tipos de datos correctos.

        :param df: DataFrame de Spark.
        :return: DataFrame con los tipos convertidos.
        """
        dtype_mapping = {
            "Timestamp (kafka)": "bigint",
            "Timestamp (date)": "timestamp",
            "Message (base64)": "string",
            "Message (hex)": "string",
            "ICAO": "string",
            "Downlink Format": "int",
            "Flight status": "string",
            "BDS": "string",
            "Roll angle (deg)": "double",
            "True track angle (deg)": "double",
            "Ground speed (kt)": "double",
            "Track angle rate (deg/sec)": "double",
            "True airspeed (kt)": "double",
            "Altitude (ft)": "double",
            "Typecode": "double",
            "TurbulenceCategory": "string",
            "Position with ref (RADAR)": "string",
            "lat": "double",
            "lon": "double",
            "Speed": "double",
            "Angle": "double",
            "Vertical rate": "double",
            "Speed type": "string",
            "Callsign": "string",
            "MCP/FCU selected altitude (ft)": "double",
            "FMS selected altitude (ft)": "double",
            "Barometric pressure (mb)": "double",
            "Speed heading": "string",
            "Magnetic heading (deg)": "double",
            "Indicated airspeed (kt)": "double",
            "Mach number (-)": "double",
            "Barometric altitude rate (ft/min)": "double",
            "Inertial vertical speed (ft/min)": "double",
            "Squawk code": "string",
            "GICB capability": "string",
            "Turbulence level (0-3)": "double",
            "Wind shear level (0-3)": "double",
            "Microburst level (0-3)": "double",
            "Icing level (0-3)": "double",
            "Wake vortex level (0-3)": "double",
            "Static air temperature (C)": "double",
            "Average static pressure (hPa)": "double",
            "Radio height (ft)": "double",
            "Overlay capability": "double",
            "Wind speed (kt) and direction (true) (deg)": "string",
            "Humidity (%)": "double"
        }

        for col_name, dtype in dtype_mapping.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, col(col_name).cast(dtype))

        return df
