import numpy as np
import pandas as pd

def separateCoordinates(coord):
    """Obtiene la latitud y longitud a partir de la tupla de posición."""
    return {"lat": coord[0], "lon": coord[1]}

def separateVelocity(velocity):
    """Separa los elementos de la tupla devuelta por la función `pms.adsb.velocity()`"""
    if velocity is None:
        return {'Speed': None, 'Angle': None, 'Vertical rate': None, 'Speed type':None}
    else:
        return {'Speed': velocity[0], 'Angle': velocity[1], 'Vertical rate': velocity[2], 'Speed type': velocity[3]}

def processStaticAirTemperature(temperature):
    """Si la temperatura obtenida es una tupla de ceros se establece como nula ya que se trata de un error de inconsistencia.

    Fuente: https://mode-s.org/pymodes/api/pyModeS.decoder.bds.bds44.html#pyModeS.decoder.bds.bds44.temp44"""
    if temperature == (0.0, 0.0):
        temperature = np.nan
    return temperature

def stringToNan(df):
    """Transforma todos los strings 'nan' o 'None' de un dataframe a nulos de Numpy."""
    df.replace("nan", np.nan, inplace=True)
    df.replace("None", np.nan, inplace=True)
    return df

def extractDaysOfTheWeek(df, col='Timestamp (date)'):
    """ Crea una nueva columna 'day_of_week' con las tres primeras letras del día de la semana"""
    df['day_of_week'] = df[col].dt.strftime('%a')
    return df

def extractHour(df):
    """ Asegura q el df está en formato timestamp y extrae una columna 'hour' con la hora a partir de la fecha"""
    df['Timestamp (date)'] = pd.to_datetime(df['Timestamp (date)'], format='mixed', errors='coerce')
    df['hour'] = df['Timestamp (date)'].dt.floor('H')
    return df

def haversine(lat1, lon1, lat2, lon2):
    """Calcula la distancia entre dos puntos. Para ello utilizamos la fórmula de Haversine.
    
    Parámetros:
        lat1, lon1: coordenadas del punto 1
        lat2, lon2: coordenadas del punto 2

    Devuelve:
        float: distancia entre los dos puntos (km)"""
    EARTH_RADIUS = 6378 # radio de la Tierra (km)

    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    delta_phi = np.radians(lat2 - lat1)
    delta_lambda = np.radians(lon2 - lon1)

    a = np.sin(delta_phi / 2)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    distance = EARTH_RADIUS * c # km
    return distance

def knots_to_kmh(speed_knots):
    """Transforma una velocidad de nudos (knots) a km/h

    Parámetros:
        speed_knots: la velocidad en nudos
    
    Devuelve:
        float: la velocidad en km/h
    """
    return speed_knots * 1.852 if pd.notna(speed_knots) else None
