from collections import defaultdict
from datetime import datetime
import pyModeS as pms
import base64
from enum import Enum
from preprocess.utilities import separateCoordinates, separateVelocity, processStaticAirTemperature
import preprocess.airport_constants as ac

class MessageType(Enum):
    """
    Enumerado para los diferentes tipos de mensajes que pueden ser procesados.

    Los tipos de mensaje incluyen:
        ALTITUDE: Mensajes relacionados con la altitud.
        IDENTITY: Mensajes de identidad.
        ADS_B: Mensajes ADS-B.
        MODE_S: Mensajes de modo S.
        NONE: Tipo de mensaje no reconocido.
    """
    ALTITUDE = "ALTITUDE"
    IDENTITY = "IDENTITY"
    ADS_B = "ADS_B"
    MODE_S = "MODE_S"
    NONE = "NONE"


class Decoder:
    """
    Clase para procesar mensajes y extraer información relevante de los mismos.

    Los métodos de esta clase son responsables de decodificar los mensajes, identificar su tipo,
    y extraer información como la altitud, el código de identidad, la turbulencia, entre otros.
    """

    ON_GROUND = "on-ground"
    AIRBORNE = "airborne"
    
    # Diccionarios de mapeo para diferentes tipos de mensajes y estados

    # Identifica qué tipo de mensaje se recibe
    MAP_DF = defaultdict(lambda: [MessageType.NONE], {  
        4: [MessageType.ALTITUDE],
        5: [MessageType.IDENTITY],
        17: [MessageType.ADS_B],
        18: [MessageType.ADS_B],
        20: [MessageType.MODE_S, MessageType.ALTITUDE],
        21: [MessageType.MODE_S, MessageType.IDENTITY]
    })

    # Estado de vuelo del avión
    MAP_CA = defaultdict(lambda: float('nan'), {
        4: ON_GROUND,
        5: AIRBORNE
    })
    
    MAP_FS = defaultdict(lambda: float('nan'), {
        0: AIRBORNE,
        1: ON_GROUND,
        2: AIRBORNE,
        3: ON_GROUND
    })

    # Categorías de turbulencias
    MAP_WTC = {
        (4, 1): "Light",
        (4, 2): "Medium 1",
        (4, 3): "Medium 2",
        (4, 5): "Heavy",
    }

    # Categorías de las aeronaves
    MAP_AIRCRAFT_CATEGORY = {
        (1, 0): 'Reserved',
        (0, 0): 'No category information',
        (2, 1): 'Surface emergency vehicle',
        (2, 3): 'Surface service vehicle',
        (2, 4): 'Ground obstruction',
        (2, 5): 'Ground obstruction',
        (2, 6): 'Ground obstruction',
        (2, 7): 'Ground obstruction',
        (3, 1): 'Glider, sailplane',
        (3, 2): 'Lighter-than-air',
        (3, 3): 'Parachutist, skydiver',
        (3, 4): 'Ultralight, hang-glider, paraglider',
        (3, 5): 'Reserved',
        (3, 6): 'Unmanned aerial vehicle',
        (3, 7): 'Space or transatmospheric vehicle',
        (4, 1): 'Light (less than 7000 kg)',
        (4, 2): 'Medium 1 (between 7000 kg and 34000 kg)',
        (4, 3): 'Medium 2 (between 34000 kg to 136000 kg)',
        (4, 4): 'High vortex aircraft',
        (4, 5): 'Heavy (larger than 136000 kg)',
        (4, 6): 'High performance (>5 g acceleration) and high speed (>400 kt)',
        (4, 7): 'Rotorcraft',
    }

    @staticmethod
    def processMessage(msg, tsKafka):
        """
        Procesa un mensaje y extrae la información relevante.

        Args:
            msg (str): El mensaje en formato base64 a procesar.
            tsKafka (int): La marca de tiempo de Kafka asociada al mensaje.

        Returns:
            dict: Un diccionario con la información extraída del mensaje.
        """
        data = {}

        # Procesa el timestamp
        data["Timestamp (kafka)"] = tsKafka
        data["Timestamp (date)"] = Decoder.kafkaToDate(tsKafka) 
        
        # Transforma el mensaje a hexadecimal
        msgHex = Decoder.base64toHex(msg)
        
        data["Message (base64)"] = msg
        data["Message (hex)"] = msgHex
        
        # Información básica
        data["ICAO"] = Decoder.getICAO(msgHex)
        downlinkFormat = Decoder.getDF(msgHex)
        data["Downlink Format"] = downlinkFormat
        
        # Información dependiente del DF
        msgType = Decoder.MAP_DF[data["Downlink Format"]]
        data["Flight status"] = Decoder.getFlightStatus(msgHex, msgType)

        # Procesa según el tipo de mensaje
        if Decoder.isADS_B(msgType):
            data.update(Decoder.processADS_B(msgHex))
        if Decoder.isMODE_S(msgType):
            data.update(Decoder.processMODE_S(msgHex))
        if Decoder.isALTITUDE(msgType):
            data.update(Decoder.processALTITUDE(msgHex))
        if Decoder.isIDENTITY(msgType):
            data.update(Decoder.processIDENTITY(msgHex))
            
        return data
    
    @staticmethod
    def kafkaToDate(tsKafka):
        """
        Convierte un timestamp de Kafka a una fecha legible.

        Args:
            tsKafka (int): El timestamp de Kafka en milisegundos.

        Returns:
            datetime: El timestamp convertido a un objeto datetime.
        """
        return datetime.fromtimestamp(tsKafka / 1000)

        
    @staticmethod
    def base64toHex(msg):
        """
        Convierte un mensaje en base64 a formato hexadecimal.

        Args:
            msg (str): El mensaje en formato base64.

        Returns:
            str: El mensaje convertido a hexadecimal.
        """
        return base64.b64decode(msg).hex().upper()

    
    @staticmethod
    def getICAO(msg):
        """
        Extrae el código ICAO del mensaje.

        Args:
            msg (str): El mensaje en formato hexadecimal.

        Returns:
            str: El código ICAO extraído del mensaje.
        """
        return pms.icao(msg)
    
    @staticmethod
    def getDF(msg):
        """
        Obtiene el formato de downlink del mensaje.

        Args:
            msg (str): El mensaje en formato hexadecimal.

        Returns:
            int: El formato de downlink del mensaje.
        """
        return pms.df(msg)
    
    @staticmethod
    def isIDENTITY(msgType):
        """
        Verifica si el tipo de mensaje es de tipo IDENTITY.

        Args:
            msgType (list): Lista de tipos de mensajes a verificar.

        Returns:
            bool: True si el mensaje es de tipo IDENTITY, False de lo contrario.
        """
        return MessageType.IDENTITY in msgType
    
    @staticmethod
    def isALTITUDE(msgType):
        """
        Verifica si el tipo de mensaje es de tipo ALTITUDE.

        Args:
            msgType (list): Lista de tipos de mensajes a verificar.

        Returns:
            bool: True si el mensaje es de tipo ALTITUDE, False de lo contrario.
        """
        return MessageType.ALTITUDE in msgType
    
    @staticmethod
    def isADS_B(msgType):
        """
        Verifica si el tipo de mensaje es de tipo ADS-B.

        Args:
            msgType (list): Lista de tipos de mensajes a verificar.

        Returns:
            bool: True si el mensaje es de tipo ADS-B, False de lo contrario.
        """
        return MessageType.ADS_B in msgType
    
    @staticmethod
    def isMODE_S(msgType):
        """
        Verifica si el tipo de mensaje es de tipo MODE-S.

        Args:
            msgType (list): Lista de tipos de mensajes a verificar.

        Returns:
            bool: True si el mensaje es de tipo MODE-S, False de lo contrario.
        """
        return MessageType.MODE_S in msgType
    
    @staticmethod
    def processALTITUDE(msg):
        """
        Procesa un mensaje de tipo ALTITUDE y extrae la altitud.

        Args:
            msg (str): El mensaje en formato hexadecimal.

        Returns:
            dict: Un diccionario con la altitud extraída.
        """
        return {"Altitude (ft)": pms.common.altcode(msg)}
    
    @staticmethod
    def processIDENTITY(msg):
        """
        Procesa un mensaje de tipo IDENTITY y extrae el código de identificación.

        Args:
            msg (str): El mensaje en formato hexadecimal.

        Returns:
            dict: Un diccionario con el código de identificación extraído.
        """
        return {"Squawk code": pms.common.idcode(msg)}
    
    @staticmethod
    def getFlightStatus(msgHex, msgType):
        """
        Obtiene el estado del vuelo desde el mensaje.

        Args:
            msgHex (str): El mensaje en formato hexadecimal.
            msgType (list): El tipo de mensaje.

        Returns:
            str: El estado del vuelo (AIRBORNE o ON_GROUND).
        """
        byteData = bytes.fromhex(msgHex)
        status_byte = byteData[4]

        if Decoder.isIDENTITY(msgType) or Decoder.isALTITUDE(msgType) or Decoder.isMODE_S(msgType):
            fs = (status_byte >> 5) & 0b111  # bits 5-7
            return Decoder.MAP_FS[fs]
        
        elif Decoder.isADS_B(msgType):
            ca = (status_byte >> 5) & 0b111  # bits 5-7
            return Decoder.MAP_CA[ca]
        
        return None
    
    @staticmethod
    def getWakeTurbulenceCategory(msg):
        """
        Determina la categoría de turbulencia del mensaje.

        Args:
            msg (str): El mensaje en formato base64.

        Returns:
            str: La categoría de turbulencia (Light, Medium, Heavy, etc.).
        """
        msgHex = Decoder.base64toHex(msg)
        byteData = bytes.fromhex(msgHex)  

        typecode = pms.adsb.typecode(msg)
        status_byte = byteData[4]
        ca = (status_byte >> 5) & 0b111  # Bits 6-8

        if typecode == 1:
            ca = 0

        return Decoder.MAP_AIRCRAFT_CATEGORY.get((typecode, ca))
    
    @staticmethod
    def processADS_B(msg):
        """
        Procesa un mensaje de tipo ADS-B y extrae la información relevante.

        Args:
            msg (str): El mensaje en formato hexadecimal.

        Returns:
            dict: Un diccionario con la información de ADS-B extraída.
        """
        data = {}
        
        typecode = pms.adsb.typecode(msg)
        
        if not typecode:
            return {}
        
        data["Typecode"] = typecode
        data['TurbulenceCategory'] = Decoder.getWakeTurbulenceCategory(msg)

        if typecode <= 4:
            data["Callsign"] = pms.adsb.callsign(msg)

        elif typecode == 19:
            velocity = pms.adsb.velocity(msg) # maneja mensajes de tipo surface y airborne
            data.update(separateVelocity(velocity))    
            data["Speed heading"] = pms.adsb.speed_heading(msg)
            data["Flight status"] = Decoder.AIRBORNE
        
        elif 5 <= typecode <= 22:
            lat_ref, lon_ref = ac.RADAR_POSITION

            posRef = pms.adsb.position_with_ref(msg, lat_ref, lon_ref) # maneja mensajes de tipo surface y airborne
            data["Position with ref (RADAR)"] = posRef
            data.update(separateCoordinates(posRef))
        
            # Surface - Typecode 5-8
            if 5 <= typecode <= 8:
                velocity = pms.adsb.velocity(msg)
                data.update(separateVelocity(velocity))

                if pms.adsb.surface_position_with_ref(msg, lat_ref, lon_ref):
                    data["Flight status"] = Decoder.ON_GROUND

            # Airborne
            elif pms.adsb.airborne_position_with_ref(msg, lat_ref, lon_ref):
                data["Flight status"] = Decoder.AIRBORNE

            data["Altitude (ft)"] = pms.adsb.altitude(msg)

        return data

    @staticmethod
    def processMODE_S(msg):
        """
        Procesa un mensaje de tipo MODE-S y extrae la información relevante.

        Args:
            msg (str): El mensaje en formato hexadecimal.

        Returns:
            dict: Un diccionario con la información de MODE-S extraída.
        """
        data = {}
        
        bds = pms.bds.infer(msg, mrar=True)
        data["BDS"] = bds
        
        # BDS 1,0
        if pms.bds.bds10.is10(msg):
            data["Overlay capability"] = pms.commb.ovc10(msg)
            
        # BDS 1,7
        if pms.bds.bds17.is17(msg):
            data["GICB capability"] = pms.commb.cap17(msg)
            
        # BDS 2,0
        if pms.bds.bds20.is20(msg):
            data["Callsign"] = pms.commb.cs20(msg)
            
        # BDS 4,0
        if pms.bds.bds40.is40(msg):
            data["MCP/FCU selected altitude (ft)"] = pms.commb.selalt40mcp(msg)
            data["FMS selected altitude (ft)"] = pms.commb.selalt40fms(msg)
            data["Barometric pressure (mb)"] = pms.commb.p40baro(msg)
        
        # BDS 4,4
        if pms.bds.bds44.is44(msg):
            data["Wind speed (kt) and direction (true) (deg)"] = pms.commb.wind44(msg)
            sat = pms.commb.temp44(msg)
            data["Static air temperature (C)"] = processStaticAirTemperature(sat)
            data["Average static pressure (hPa)"] = pms.commb.p44(msg)
            data["Humidity (%)"] = pms.commb.hum44(msg)
        
        # BDS 4,5
        if pms.bds.bds45.is45(msg):
            data["Turbulence level (0-3)"] = pms.commb.turb45(msg)
            data["Wind shear level (0-3)"] = pms.commb.ws45(msg)
            data["Microburst level (0-3)"] = pms.commb.mb45(msg)
            data["Icing level (0-3)"] = pms.commb.ic45(msg)
            data["Wake vortex level (0-3)"] = pms.commb.wv45(msg)
            sat = pms.commb.temp45(msg)
            data["Static air temperature (C)"] = processStaticAirTemperature(sat)
            data["Average static pressure (hPa)"] = pms.commb.p45(msg)
            data["Radio height (ft)"] = pms.commb.rh45(msg)

        # BDS 5,0
        if pms.bds.bds50.is50(msg):
            data["Roll angle (deg)"] = pms.commb.roll50(msg)
            data["True track angle (deg)"] = pms.commb.trk50(msg)
            data["Ground speed (kt)"] = pms.commb.gs50(msg)
            data["Track angle rate (deg/sec)"] = pms.commb.rtrk50(msg)
            data["True airspeed (kt)"] = pms.commb.tas50(msg)

        # BDS 6,0
        if pms.bds.bds60.is60(msg):
            data["Magnetic heading (deg)"] = pms.commb.hdg60(msg)
            data["Indicated airspeed (kt)"] = pms.commb.ias60(msg)
            data["Mach number (-)"] = pms.commb.mach60(msg)
            data["Barometric altitude rate (ft/min)"] = pms.commb.vr60baro(msg)   
            data["Inertial vertical speed (ft/min)"] = pms.commb.vr60ins(msg)
        
        return data
