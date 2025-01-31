from preprocess.decoder import Decoder
from preprocess.decoder import MessageType

class Airplane:

    def __init__(self, icao):
        self.icao = icao
        self.messages = []
        self.timestamps = []
        self.total_airborne_time = 0  # Tiempo total en aire (segundos)
        self.total_ground_time = 0    # Tiempo total en tierra (segundos)
        self.last_status = None
        self.last_timestamp = None

    def add_message(self, decoded_msg):
        self.messages.append(decoded_msg)
        current_timestamp = decoded_msg["Timestamp (date)"]
        self.timestamps.append(current_timestamp)

        # Obtener msgType correctamente
        msgType = Decoder.MAP_DF.get(decoded_msg["Downlink Format"], [MessageType.NONE])
        status = Decoder.getFlightStatus(decoded_msg["Message (hex)"], msgType)

        # Ignorar mensajes sin información de estado
        if status in ["airborne", "on-ground"]:
            self.update_flight_status(status, current_timestamp)

    def update_flight_status(self, status, current_timestamp):
        # Si es el primer mensaje con estado conocido
        if self.last_status is None:
            self.last_status = status
            self.last_timestamp = current_timestamp
            return

        # Calcular el tiempo transcurrido desde el último mensaje con estado conocido
        time_diff = (current_timestamp - self.last_timestamp).total_seconds()

        # Acumular tiempo según el estado anterior
        if self.last_status == "airborne":
            self.total_airborne_time += time_diff
        elif self.last_status == "on-ground":
            self.total_ground_time += time_diff

        # Actualizar el estado y timestamp para el próximo cálculo
        self.last_status = status
        self.last_timestamp = current_timestamp

    def finalize(self, final_timestamp):
        # Calcular el tiempo desde el último mensaje hasta el final del periodo
        if self.last_status is not None:
            time_diff = (final_timestamp - self.last_timestamp).total_seconds()
            if self.last_status == "airborne":
                self.total_airborne_time += time_diff
            elif self.last_status == "on-ground":
                self.total_ground_time += time_diff
