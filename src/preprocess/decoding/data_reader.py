import pandas as pd
from preprocess.decoder import Decoder

class DataReader:
    """
    Clase que gestiona la lectura, limpieza y transformación de datos provenientes de un archivo CSV.

    Atributos:
        data (DataFrame): DataFrame de pandas que almacena los datos leídos desde el archivo CSV.

    Métodos:
        cleanData():
            Elimina columnas innecesarias y elimina duplicados en los datos.
        dfToDict(msg, ts_kafka):
            Convierte una fila de datos en un diccionario de Python utilizando un procesador de mensajes.
        transformData():
            Transforma el DataFrame en un conjunto de diccionarios y los convierte de nuevo en un DataFrame estructurado.
        getAirplaneMessages(icao):
            Filtra los datos del DataFrame y devuelve las filas correspondientes a un avión específico.
    """

    def __init__(self, path):
        """
        Inicializa un objeto DataReader y carga los datos desde el archivo CSV.

        Args:
            path (str): Ruta al archivo CSV que contiene los datos crudos.
        """
        self.data = pd.read_csv(path, sep=';')  # self.data es un DataFrame

    def cleanData(self):
        """
        Limpia los datos eliminando columnas innecesarias y duplicados.

        Elimina la columna 'Unnamed: 2' y cualquier fila duplicada en el DataFrame.
        """
        # Elimina las columnas que no se van a utilizar
        self.data = self.data.drop(columns=['Unnamed: 2'])
        # Elimina duplicados en los datos
        self.data = self.data.drop_duplicates()

    def dfToDict(self, msg, ts_kafka):
        """
        Convierte un mensaje y su marca de tiempo a un diccionario de Python.

        Args:
            msg (str): El mensaje crudo a procesar.
            ts_kafka (datetime): La marca de tiempo asociada al mensaje.

        Returns:
            dict: El mensaje procesado como un diccionario.
        """
        return Decoder.processMessage(msg, ts_kafka)
    
    def transformData(self):
        """
        Transforma el DataFrame original en un conjunto de diccionarios.

        Aplica la función `dfToDict` a cada fila del DataFrame, transformando los datos en diccionarios
        y luego convierte estos diccionarios de vuelta en un DataFrame estructurado.
        """
        self.data = self.data.apply(lambda x: self.dfToDict(x['Message'], x['Timestamp']), axis=1).apply(pd.Series)

    def getAirplaneMessages(self, icao):
        """
        Filtra y devuelve los datos correspondientes a un avión específico.

        Args:
            icao (str): Código ICAO del avión.

        Returns:
            DataFrame: Un DataFrame filtrado que contiene los datos del avión especificado.
        """
        return self.data[self.data['ICAO'] == icao]
