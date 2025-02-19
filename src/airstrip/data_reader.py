import pandas as pd
from preprocess.decoder import Decoder

# Los datos crudos nos llegan en un archivo CSV, por lo que necesitamos una función que lea estos datos y los convierta en un diccionario de Python.
# Por el momento, emplearemos pandas para leer el archivo CSV y convertirlo en un DataFrame, que gardara los datos a guardar y tranformar en un diccionario.

class DataReader:
    def __init__(self, path):
        self.data = pd.read_csv(path, sep=';') # self.data es un dataframe

    def cleanData(self):
        # Elimina las columnas que no se van a utilizar
        self.data = self.data.drop(columns=['Unnamed: 2'])
        # Elimina duplicados en los datos
        self.data = self.data.drop_duplicates()

    def dfToDict(self, msg, ts_kafka):
        # Convierte un elemento de tipo DataFrame a un diccionario de Python
        return Decoder.processMessage(msg, ts_kafka)
    
    def transformData(self):
        # Transforma el DataFrame a un conjunto de diccionarios aplicando la función dfToDict a cada fila
        # pero, tras estructurar mejor los datos, se convierten de vuelta a un dataframe
        self.data = self.data.apply(lambda x: self.dfToDict(x['Message'], x['Timestamp']), axis=1).apply(pd.Series)

    def getAirplaneMessages(self, icao):
        # Devuelve los datos de un avión concreto
        return self.data[self.data['ICAO'] == icao]

