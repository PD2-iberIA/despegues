import airport_constants as ac
import math

# Radio de la Tierra (km)
EARTH_RADIUS = 6378

# 1 milla náutica (NM) = 1.852 km
NM_KM_EQUIVALENCE = 1852
MAXIMUM_RANGE = 180 * NM_KM_EQUIVALENCE

class DataProcessor:

    @staticmethod
    def airborne_position_is_valid(lat, lon, h):
        """
        Determina si la posición decodificada de un avión en el aire es válida.
        
        Parámetros:
        lat, lon: coordenadas del avión
        h: altura del avión

        Devuelve:
        boolean: indica si la posición es válida (True) o no (False)
        """
        inside_range = True

        # La distancia entre el avión y el radar ha de ser menor de 180 NM
        distance = DataProcessor.radar_aircraft_distance(lat, lon)
        if (distance > MAXIMUM_RANGE):
            inside_range = False

        # La posición del avión deberá estar dentro del rango máximo del radar
        # El rango máximo depende de la altura del avión
        radar_range = DataProcessor.radar_maximun_range(h)
        if (distance > radar_range):
            inside_range = False

        return inside_range
    
    @staticmethod
    def radar_maximun_range(h_t, h_r=0):
        """
        Calcula el rango máximo de alcance del radar.

        Parámetros:
        h_t (float): altura del avión
        h_r (float): altura del radar
        
        Devuelve:
        float: rango máximo del radar (km)
        """
        R = EARTH_RADIUS
        alpha_r = math.acos(R / (R + h_r))
        alpha_t = math.acos(R / (R + h_t))
        d = (alpha_r + alpha_t) * R

        return d
    
    @staticmethod
    def radar_aircraft_distance(lat, lon):
        """
        Calcula la distancia entre una aeronave y el radar. Para ello utiliza la fórmula de
        Haversine, que mide la distancia entre dos puntos en la superficie de la Tierra.
        Para validar si el avión está dentro de 180 NM, la distancia Haversine por sí sola será 
        suficiente porque la altitud tiene un impacto muy pequeño en la distancia total.

        Parámetros:
        lat, lon: coordenadas del avión

        Devuelve:
        float: distancia entre el radar y el avión (km)
        """
        # Coordenadas de referencia (radar)
        lat_ref, lon_ref = ac.RADAR_POSITION

        # Convertir grados a radianes
        lat_ref, lon_ref, lat, lon = map(math.radians, [lat_ref, lon_ref, lat, lon])

        dlat = lat - lat_ref
        dlon = lon - lon_ref

        # Fórmula de Haversine
        a = math.sin(dlat / 2)**2 + math.cos(lat_ref) * math.cos(lat) * math.sin(dlon / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = EARTH_RADIUS * c

        return distance
