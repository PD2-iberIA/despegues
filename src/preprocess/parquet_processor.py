import os
import pandas as pd
import numpy as np

class ParquetProcessor:
    def __init__(self, filepaths, output_folder="processed_data"):
        """
        Inicializa el procesador con una lista de rutas a archivos Parquet.
        
        :param filepaths: Lista de rutas de archivos Parquet.
        :param output_folder: Carpeta donde se guardarán los archivos procesados.
        """
        self.filepaths = filepaths
        self.output_folder = output_folder
        self.df = None

    def load_data(self):
        """
        Carga múltiples archivos Parquet en un solo DataFrame.
        """
        dfs = []
        for file in self.filepaths:
            try:
                df = pd.read_parquet(file, engine="pyarrow")
                dfs.append(df)
            except Exception as e:
                print(f"Error al cargar {file}: {e}")

        if dfs:
            self.df = pd.concat(dfs, ignore_index=True)
        else:
            print("No se pudieron cargar archivos.")

    def clean_data(self):
        """
        Limpia los datos reemplazando valores nulos y no convertibles.
        """
        if self.df is None:
            print("No hay datos cargados para limpiar.")
            return
        
        # Reemplazar valores que deberían ser NaN
        self.df.replace(["null", "None", "", "nan"], np.nan, inplace=True)

    def convert_data_types(self):
        """
        Convierte las columnas del df a los tipos de datos correctos.
        """
        dtypes_correctos = {
            "Timestamp (kafka)": "int64",
            "Timestamp (date)": "datetime64[ns]",
            "Message (base64)": "object",
            "Message (hex)": "object",
            "ICAO": "object",
            "Downlink Format": "int64",
            "Flight status": "object",
            "BDS": "object",
            "Roll angle (deg)": "float64",
            "True track angle (deg)": "float64",
            "Ground speed (kt)": "float64",
            "Track angle rate (deg/sec)": "float64",
            "True airspeed (kt)": "float64",
            "Altitude (ft)": "float64",
            "Typecode": "float64",
            "TurbulenceCategory": "object",
            "Position with ref (RADAR)": "object",
            "lat": "float64",
            "lon": "float64",
            "Speed": "float64",
            "Angle": "float64",
            "Vertical rate": "float64",
            "Speed type": "object",
            "Callsign": "object",
            "MCP/FCU selected altitude (ft)": "float64",
            "FMS selected altitude (ft)": "float64",
            "Barometric pressure (mb)": "float64",
            "Speed heading": "object",
            "Magnetic heading (deg)": "float64",
            "Indicated airspeed (kt)": "float64",
            "Mach number (-)": "float64",
            "Barometric altitude rate (ft/min)": "float64",
            "Inertial vertical speed (ft/min)": "float64",
            "Squawk code": "object",
            "GICB capability": "object",
            "Turbulence level (0-3)": "float64",
            "Wind shear level (0-3)": "float64",
            "Microburst level (0-3)": "float64",
            "Icing level (0-3)": "float64",
            "Wake vortex level (0-3)": "float64",
            "Static air temperature (C)": "float64",
            "Average static pressure (hPa)": "float64",
            "Radio height (ft)": "float64",
            "Overlay capability": "float64",
            "Wind speed (kt) and direction (true) (deg)": "object",
            "Humidity (%)": "float64"
        }

        for col, dtype in dtypes_correctos.items():
            if dtype == "datetime64[ns]":
                self.df[col] = pd.to_datetime(self.df[col], errors="coerce")  # Convierte a fecha
                self.df[col] = pd.to_datetime(self.df['Timestamp (kafka)'] // 1000, unit='s', errors='coerce')
            elif dtype == "float64":
                self.df[col] = pd.to_numeric(self.df[col], errors="coerce")  # Convierte a float, poniendo NaN si falla
            else:
                self.df[col] = self.df[col].astype(dtype)  # Convierte a int64 u object sin ignorar errores

    def save_by_date_hour(self):
        """
        Divide el df por día y hora y guarda los archivos en la estructura de carpetas adecuada.
        """

        self.df.dropna(subset=["Timestamp (date)"], inplace=True)

        # Extraemos la fecha y hora en el formato correcto
        self.df["date"] = self.df["Timestamp (date)"].dt.strftime("%Y-%m-%d")
        self.df["hour"] = self.df["Timestamp (date)"].dt.strftime("%H")

        for (date, hour), group in self.df.groupby(["date", "hour"]):
            folder_path = os.path.join(self.output_folder, date, hour)
            os.makedirs(folder_path, exist_ok=True)

            output_filepath = os.path.join(folder_path, f"data_{date}_{hour}.parquet")
            group.to_parquet(output_filepath, engine="pyarrow")
            print(f"Guardado en {output_filepath}")

    def process(self):
        """
        Ejecuta el guardado.
        """
        self.save_by_date_hour()
