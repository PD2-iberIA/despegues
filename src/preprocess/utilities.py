import numpy as np

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
