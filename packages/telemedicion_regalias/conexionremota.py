'''
Created on May 23, 2020

@author: oirraza
'''

from abc import ABC, abstractmethod
from enum import Enum
import logging
from ftplib import FTP, FTP_TLS
from ssl import SSLSocket
import paramiko



class Protocolos(Enum):
    '''
    Enumeración de los protocolos implementados
    '''
    FTP = 'FTP'
    FTPS = 'FTPS'   #Implicit FTPS
    FTPES = 'FTPES'  #Explicit FTPS
    SFTP = 'SFTP'
    
    @staticmethod
    def list():
        return list(map(lambda p: p.value, Protocolos))




class ReusedSSLSocket(SSLSocket):
    """
    Subclase de SSLSocket que deja sin efecto la llamada unwrap
    """
    def unwrap(self):
        pass




class ImplicitFTP_TLS(FTP_TLS):
    """
    Subclase de FTP_TLS que automáticamente envuelve (wrapea) el socket en SSL para soportar FTPS implícito
    Nota: este método de conexión ya casi no se utiliza, en su lugar se utiliza FTPES (FTPS explícito)
    Solución basada en Anders Tornkvist (https://stackoverflow.com/questions/46633536/getting-a-oserror-when-trying-to-list-ftp-directories-in-python)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sock = None

    @property
    def sock(self):
        """Devuelve el socket"""
        return self._sock

    @sock.setter
    def sock(self, value):
        """Al modificar el socket, asegurar que esta envuelto (wrapped) en SSL"""
        if value is not None and not isinstance(value, SSLSocket):
            value = self.context.wrap_socket(value)
        self._sock = value

    def ntransfercmd(self, cmd, rest=None):
        """ Sobreescribir ntransfercmd reutilizando la sesión TLS sin cerrar el socket SSL cuando termina la transferencia """
        conn, size = FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:
            conn = self.context.wrap_socket(conn,
                                            server_hostname=self.host,
                                            session=self.sock.session)  # Reutilizar la sesión TLS            
            conn.__class__ = ReusedSSLSocket  # No cerrar el socket SSL cuando termina la transferencia del archivo
        return conn, size





class ConexionRemota(ABC):
    '''
    Clase abstracta para modelar distintos tipos de conexiones
    '''
    __host = ""
    __port = ""
    #Eventos
    __onBeforeGetFile = None
    __onAfterGetFile = None


    def __init__(self, host, port):
        '''Constructor'''
        self.host = host
        self.port = port
    
    @property
    def host(self):
        return self.__host
    
    @host.setter
    def host(self, value):
        if (value == ''):
            raise ValueError('Host no puede ser vacío')
        self.__host = value
    
    @property
    def port(self):
        return self.__port
    
    @port.setter
    def port(self, value):
        self.__port = value

    @property
    def onBeforeGetFile(self):
        return self.__onBeforeGetFile

    @onBeforeGetFile.setter
    def onBeforeGetFile(self, value):
        self.__onBeforeGetFile = value

    @property
    def onAfterGetFile(self):
        return self.__onAfterGetFile

    @onAfterGetFile.setter
    def onAfterGetFile(self, value):
        self.__onAfterGetFile = value

    @property
    @abstractmethod
    def connected(self):
        pass
    
    def checkConnected(self):
        if (not self.connected):
            raise ConnectionError("La conexión no está activa")
        return True
    
    @abstractmethod
    def connect(self, user, password):
        pass
    
    @abstractmethod
    def disconnect(self):
        pass
    
    @abstractmethod
    def changeDir(self, directory):
        logging.debug(f"Cambiando al directorio {directory}")
        self.checkConnected()

    @abstractmethod
    def _doGetFile(self, remoteFilename, localFilename):
        pass

    def getFile(self, remoteFilename, localFilename):
        logging.debug(f"Descargando archivo {remoteFilename} en {localFilename}")
        self.checkConnected()
        #Lanzar el evento OnBeforeGetFile
        if (self.__onBeforeGetFile is not None):
            logging.debug("Ejecutando handler evento BeforeGetFile")
            self.__onBeforeGetFile(self, remoteFilename, localFilename)
        #Descargar el archivo 
        self._doGetFile(remoteFilename, localFilename)
        #Lanzar el evento OnBeforeGetFile
        if (self.__onAfterGetFile is not None):
            logging.debug("Ejecutando handler evento AfterGetFile")
            self.__onAfterGetFile(self, remoteFilename, localFilename) 




class ConexionFTP(ConexionRemota):
    '''
    Subclase de ConexionRemota que implementa una conexión FTP
    '''
    _ftp = None
    _labelProtocol = ''


    def __init__(self, host, port=21):
        super().__init__(host, port)
        self._initFTP()
        
    def _initFTP(self):
        self._ftp = FTP()
        self._labelProtocol = 'FTP'
        
    @property
    def connected(self):
        try:
            self._ftp.voidcmd("NOOP")
            return True
        except:
            return False    
    
    def connect(self, user, password):
#         try:
            self._ftp.connect(self.host, self.port)
            logging.debug(f"Estableciendo conexión {self._labelProtocol} (host:{self.host}, port:{self.port}, user:{user}, pass:********)")
            self._ftp.login(user, password)
            logging.debug(f"Conexión {self._labelProtocol} establecida")
#         except Exception as e:
#             raise ConnectionError(f"Error al establecer la conexión {self._labelProtocol} (error: {e})")
        
    def disconnect(self):
        #Primero intentar una desconexión cortés, si no funciona se 
        #genera una excepción, entonces forzar la desconexión
        try:
            self._ftp.quit()
            logging.debug(f"Conexión {self._labelProtocol} cerrada")
        except (Exception) as e:
            logging.debug(f"Error al intentar cerrar la conexión {self._labelProtocol} bilateralmente (error: {e})")
            self._ftp.close()
            logging.debug(f"Conexión {self._labelProtocol} cerrada unilateralmente")
    
    def changeDir(self, directory):
        super().changeDir(directory)
        try:
            self._ftp.chdir(directory)
        except Exception as e:
            raise Exception(f"Error al cambiar al directorio {directory} (error: {e})")

    def _doGetFile(self, remoteFilename, localFilename):
        super()._doGetFile(remoteFilename, localFilename)
        #TODO: implementar getFile con FTP
        raise NotImplementedError("Error: Método aún no implementado")
    
    def setMode(self, mode):
        '''Configura el modo de transferencia ASCII (ASC) o Binario (BIN)'''
        #Validar el modo de transferencia
        if (mode.upper() == 'ASC'):
            cmdMode = 'TYPE A'
        elif (mode.upper() == 'BIN'):
            cmdMode = 'TYPE I'
        else:    
            raise ValueError(f"Modo de transferencia {mode} inválido, valores válidos 'A' y 'B'")
        logging.debug(f"Configurando el modo de transferencia {mode}")
        #Activar el modo de transferencia
        try:
            self._ftp.sendcmd(cmdMode)
        except Exception as e:
            raise Exception(f"Error al configurar el modo de transferencia {mode} (error: {e})")
    
    
    
    
class ConexionFTPS(ConexionFTP):
    '''
    Subclase de ConexionFTP que implementa una conexión FTPS (Implicit FTPS)
    '''
    
    def __init__(self, host, port=990):
        super().__init__(host, port)
 
    def _initFTP(self):
        self._ftp = ImplicitFTP_TLS()
#         self._ftp._sock = self._ftp.context.wrap_socket()
        self._labelProtocol = 'FTPS'

    def connect(self, user, password):
        super().connect(user, password)
        #Activar la encriptación de todas las transferencias
        try:
            self._ftp.prot_p()
        except Exception as e:
            raise ConnectionError(f"Error al activar la encriptación de las transferencias (error: {e})")




class ConexionFTPES(ConexionFTP):
    '''
    Subclase de ConexionFTP que implementa una conexión FTPES (Explicit FTPS)
    '''
 
    def _initFTP(self):
        self._ftp = FTP_TLS()
        self._labelProtocol = 'FTPES'

    def connect(self, user, password):
        super().connect(user, password)
        #Activar la encriptación de todas las transferencias
        try:
            self._ftp.prot_p()
        except Exception as e:
            raise ConnectionError(f"Error al activar la encriptación de las transferencias (error: {e})")
    
    
    
    
class ConexionSFTP(ConexionRemota):  
    '''
    Subclase de ConexionRemota que implementa una conexión SFTP
    '''
    _sftp = None
    _transport = None


    def __init__(self, host, port=22):
        '''Constructor ConexionSFTP, configura el puerto por defecto en 22 
        ''' 
        super().__init__(host, port)
    
    @property
    def connected(self):
        try:
            return self._transport.is_authenticated()
        except:
            return False    
            
    def connect(self, user, password):
        try:
            logging.debug(f"Estableciendo conexión SFTP (host:{self.host}, port:{self.port}, user:{user}, pass:********)")
            self._transport = paramiko.Transport(self.host, self.port)
            self._transport.connect(None, user, password, None)
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
            logging.debug(f"Conexión SFTP establecida")
        except Exception as e:
            raise ConnectionError(f"Error al establecer la conexión SFTP (error: {e})")

    def disconnect(self):
        if self._sftp is not None:
            try:
                self._sftp.close()
                logging.debug(f"Conexión SFTP cerrada")
            except Exception as e:
                logging.debug(f"Error al intentar cerrar la conexión SFTP (error: {e})")
        if self._transport is not None:
            self._transport.close()

    def changeDir(self, directory):
        super().changeDir(directory)
        try:
            self._sftp.chdir(directory)
        except IOError as e:
            raise Exception(f"Error al cambiar al directorio {directory} (error: {e})")

    def _doGetFile(self, remoteFilename, localFilename):
        super()._doGetFile(remoteFilename, localFilename)
        try:
            self._sftp.get(remoteFilename, localFilename)
            #TODO: calcular un hash para ver si se descargo correctamente
        except Exception as e:
            raise Exception(f"Error al descargar el archivo {remoteFilename} (error: {e})")
    



class ConexionRemotaFactory():
    
    @staticmethod    
    def getConexionRemota(protocolo, host, port=0):
        if (Protocolos(protocolo) == Protocolos.SFTP):
            return ConexionSFTP(host, port)
        elif (Protocolos(protocolo) == Protocolos.FTP):
            return ConexionFTP(host, port)
        elif (Protocolos(protocolo) == Protocolos.FTPS):
            return ConexionFTPS(host, port)
        elif (Protocolos(protocolo) == Protocolos.FTPES):
            return ConexionFTPES(host, port)        
        else:
            raise ValueError(f"Protocolo inválido (protocolo={protocolo})")   
