import json
import os

# Posición del radar (lat, lon)
RADAR_POSITION = (40.51, -3.53)

# Pistas con sus orientaciones (lat, lon, orient)
RUNWAY_1 = {
    'position': (40.463, -3.554),
    'orientation': '32L/14R'
}
RUNWAY_2 = {
    'position': (40.473, -3.536),
    'orientation': '32R/14L'
}
RUNWAY_3 = {
    'position': (40.507, -3.574),
    'orientation': '36L/18R'
}
RUNWAY_4 = {
    'position': (40.507, -3.559),
    'orientation': '36R/18L'
}

ruta_base = os.path.dirname(os.path.abspath(__file__))
ruta_geojson = os.path.join(ruta_base, "..", "puntosespera", "holding_points.geojson")

with open(ruta_geojson, "r") as file:
    geojson_data = json.load(file)

# Coordenadas de los puntos de espera
HOLDING_POINTS = [
    feature["geometry"]["coordinates"] for feature in geojson_data["features"]
]