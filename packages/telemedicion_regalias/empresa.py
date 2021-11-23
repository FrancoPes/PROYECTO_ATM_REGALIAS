'''
Created on May 23, 2020

@author: oirraza
'''

from sqlalchemy import Column, DateTime, ForeignKey
from sqlalchemy.dialects.oracle import CHAR, NUMBER, VARCHAR
from sqlalchemy.orm import relationship
from sqlalchemy import orm
from sqlalchemy.ext.hybrid import hybrid_property

from .conexionremota import ConexionRemotaFactory, Protocolos
#from .medidor_fiscal import MedidorFiscal  # @UnusedImport

from .base import Base

import json

class Empresa(Base):
    '''
    classdocs
    '''
    __tablename__ = 'treempresa'
    __table_args__ = {'schema': 'regalias'}
    #Campos
    id = Column('reempid', NUMBER(4, 0, False), primary_key=True)
    cuit = Column('reempcuit', VARCHAR(13), nullable=False, unique=True)
    codigo = Column('reempcod', CHAR(8), nullable=False)
    nombre = Column('reempnom', CHAR(80), nullable=False)
    retipempid = Column('retipempid', NUMBER(4, 0, False), index=True)
    telefono = Column('reemptel', NUMBER(10, 0, False))
    reempcartel = Column('reempcartel', NUMBER(5, 0, False))
    mail = Column('reempmai', VARCHAR(40))
    website = Column('reempsit', VARCHAR(40))
    observaciones = Column('reempobs', VARCHAR(1000))
    fecha_alta = Column('reempfchalt', DateTime, nullable=False)
    usuario_alta = Column('reempusualt', VARCHAR(30), nullable=False)
    fecha_ult_mod = Column('reempfchmod', DateTime)
    usuario_ult_mod = Column('reempusumod', VARCHAR(13))
    fecha_baja = Column('reempfchbaj', DateTime)
    usuario_baja = Column('reempusubaj', VARCHAR(13))
    reempultlin = Column('reempultlin', NUMBER(4, 0, False))
    reempvighas = Column('reempvighas', DateTime)
    reempvigdes = Column('reempvigdes', DateTime)
    reempdespag = Column('reempdespag', NUMBER(1, 0, False))
    #Relaciones
    conexion = relationship("Conexion_Empresa", uselist=False, back_populates="empresa")
    medidores = relationship("MedidorFiscal", back_populates="empresa")



class Conexion_Empresa(Base):
    __tablename__ = 'tlm_conexiones_empresas'
    __table_args__ = {'schema': 'regalias'}
    #Campos
    id = Column('cem_id', NUMBER(9, 0, False), primary_key=True)
    _empresa_id = Column('cem_reempid', ForeignKey('regalias.treempresa.reempid'), nullable=False, index=True)
    _protocolo = Column('cem_protocolo', VARCHAR(5))
    _host = Column('cem_host', VARCHAR(255))
    _port = Column('cem_port', NUMBER(9, 0, False))
    _usuario = Column('cem_usuario', VARCHAR(255))
    _password = Column('cem_password', VARCHAR(255))
    _prefijo_archivos = Column('cem_prefijo_archivos', VARCHAR(20), nullable=False)
    _directorio_remoto = Column('cem_directorio_remoto', VARCHAR(255))
    _filtros = Column('cem_filtros', VARCHAR(255))
    usuario_alta = Column('cem_usuario_alta', VARCHAR(20), nullable=False)
    fecha_alta = Column('cem_fecha_alta', DateTime, nullable=False)
    usuario_ult_mod = Column('cem_usuario_ult_mod', VARCHAR(20), nullable=False)
    fecha_ult_mod = Column('cem_fecha_ult_mod', DateTime, nullable=False)
    usuario_baja = Column('cem_usuario_baja', VARCHAR(20))
    fecha_baja = Column('cem_fecha_baja', DateTime)
    #Relaciones
    empresa = relationship('Empresa', back_populates="conexion")


    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__conexionRemota = None
    
    @orm.reconstructor
    def __init_on_load(self):
        self.__conexionRemota = None


    @hybrid_property   
    def protocolo(self):
        return self._protocolo
    
    @protocolo.setter         
    def protocolo(self, value):
        if (value not in Protocolos.list()):
            raise ValueError(f"Protocolo '{value}' no válido")             
        self._protocolo = value    


    @hybrid_property   
    def host(self):
        return self._host
    
    @host.setter         
    def host(self, value):
        if (value == ""):
            raise ValueError(f"Host no puede ser vacío")             
        self._host = value    


    @hybrid_property   
    def port(self):
        return self._port
    
    @port.setter         
    def port(self, value):
        if (value < 0):
            raise ValueError(f"Port no puede ser un valor negativo (port: {value})")             
        self._port = value    

        
    @hybrid_property   
    def usuario(self):
        return self._usuario
    
    @usuario.setter         
    def usuario(self, value):
        self._usuario = value    


    @hybrid_property   
    def password(self):
        raise AttributeError('El atributo password es de sólo escritura, no se puede leer') 
    
    @password.setter         
    def password(self, value):
        self._password = value    


    @hybrid_property   
    def prefijo_archivos(self):
        return self._prefijo_archivos
    
    @prefijo_archivos.setter         
    def prefijo_archivos(self, value):
        if (not value.isalnum()):
            raise ValueError(f"Prefijo_archivo solo puede contener valores alfanuméricos (prefijo_archivos: {value})")             
        self._prefijo_archivos = value    
    
    
    @hybrid_property   
    def directorio_remoto(self):
        return self._directorio_remoto
    
    @directorio_remoto.setter         
    def directorio_remoto(self, value):
        self._directorio_remoto = value    


    @hybrid_property   
    def filtros(self):
        return self._filtros
    
    @filtros.setter         
    def filtros(self, value):
        self._filtros = value    
        

    def filtros2Dict(self):
        """Retorna los filtros transformados en un diccionario"""
        #TODO: probar los formatos de fecha y hora
        dict_filtros = None
        filtros = self._filtros
        if (filtros) and (filtros != ""):
            try:
                dict_filtros = json.loads(filtros)
            except:
                raise Exception(f"El filtro no tiene un formato JSON válido (filtro: {filtros})")    
        return dict_filtros

    
    @property   
    def conexionRemota(self):
        if (self.__conexionRemota is None):
            self.__conexionRemota = ConexionRemotaFactory.getConexionRemota(self.protocolo, self.host, self.port)
        return self.__conexionRemota

    def connectServer(self):
        self.conexionRemota.connect(self.usuario, self._password)
        #Si los archivos no se encuentran en el directorio home del usuario,
        #cambiar al directorio indicado
        if (self.directorio_remoto is not None) and (self.directorio_remoto != '/') and (self.directorio_remoto != ''):
            self.conexionRemota.changeDir(self.directorio_remoto)
        
    def disconnectServer(self):
        if (self.connectedServer()):
            self.conexionRemota.disconnect()
        
    def connectedServer(self):
        """Retorna True si existe una conexión activa al servidor remoto"""
        return self.conexionRemota.connected
    