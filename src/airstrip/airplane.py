class Airplane:
    def __init__(self, icao):
        self.icao = icao
        self.total_airborne_time = 0  # Tiempo total en aire (segundos)
        self.total_ground_time = 0    # Tiempo total en tierra (segundos)
        self.last_status = None
        self.last_timestamp = None

    def update_flight_status(self, status, current_timestamp):
        # Si es el primer mensaje con estado conocido, lo guardamos y salimos
        if self.last_status is None:
            self.last_status = status
            self.last_timestamp = current_timestamp
            return

        # Aseguramos que los timestamps estén en orden
        if current_timestamp < self.last_timestamp:
            return

        # Calculamos el tiempo transcurrido desde el último mensaje con estado conocido
        time_diff = (current_timestamp - self.last_timestamp).total_seconds()
        if time_diff < 0:
            time_diff = 0  # Evitar tiempos negativos

        # Acumulamos tiempo según el estado anterior
        if self.last_status == "airborne":
            self.total_airborne_time += time_diff
        elif self.last_status == "on-ground":
            self.total_ground_time += time_diff

        # Actualizamos el estado y timestamp para el próximo cálculo
        self.last_status = status
        self.last_timestamp = current_timestamp

    def finalize(self, final_timestamp):
        if self.last_status is not None:
            time_diff = (final_timestamp - self.last_timestamp).total_seconds()
            if time_diff < 0:
                time_diff = 0  # Evitar tiempos negativos
            if self.last_status == "airborne":
                self.total_airborne_time += time_diff
            elif self.last_status == "on-ground":
                self.total_ground_time += time_diff