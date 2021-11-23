'''
Created on May 24, 2020

@author: oirraza
'''
from abc import ABC, abstractmethod
import logging


class LecturaMedidor(ABC):
    '''
    classdocs
    '''
    __id = None
    __timestamp = None


    def __init__(self, idLecturaMedidor, timestamp):
        '''
        Constructor
        '''
        self.id = idLecturaMedidor
        self.timestamp = timestamp
    
    @property
    def id(self):
        return self.__id
    
    @id.setter
    def id(self, value):
        self.__id = value

    @property
    def timestamp(self):
        return self.__timestamp
    
    @timestamp.setter
    def timestamp(self, value):
        self.__timestamp = value

    @property
    def fecha(self):
        return self.timestamp.date()
    
    @property
    def hora(self):
        return self.timestamp.time()

    @abstractmethod
    def parseLineaArchivoLectura(self, linea):
        logging.debug(f"Parseando la línea {linea}")
        pass


class LecturaMedidorTanque(LecturaMedidor):
    '''Lectura medidor tipo Tanque'''
    
    @abstractmethod
    def parseLineaArchivoLectura(self, linea):
        super().parseLineaArchivoLectura(self, linea)
        
    

class LecturaMedidorGeneral(LecturaMedidor):
    '''
    Lectura medidor tipo:
        Desplazamiento
        Coriolis
        Báscula
        Placa Orificio
        Ultrasónico
    '''
    
    
    

class LecturaMedidorFactory():
    
    @staticmethod    
    def getLecturaMedidor(tipoMedidor):
        if (tipoMedidor == 'TANQUE'):
            return LecturaMedidorTanque
        elif (tipoMedidor in ['DESPLAZAMIENTO', 'CORIOLIS', 'BASCULA', 'PLACA ORIFICIO', 'ULTRASONICO']):
            return LecturaMedidorGeneral
        else:
            raise ValueError(f"Tipo de medidor invlálido (valor={tipoMedidor}")
         


