'''
Created on May 25, 2020

@author: oirraza
'''

import logging


from datetime import datetime, date, timedelta
from pathlib import Path
import json
from jsonschema import validate

from sqlalchemy import Column, DateTime, ForeignKey, VARCHAR, func, inspect
from sqlalchemy.dialects.oracle import NUMBER
from sqlalchemy.orm import relationship
# from sqlalchemy import orm
from sqlalchemy.ext.hybrid import hybrid_property

from .base import Base
from .cuenca import Cuenca  # @UnusedImport
from .lectura_res11 import ArchivoLecturaRes11, LecturaMedidorRes11, SCHEMA_LECTURA_MEDIDOR_RES11
from sqlalchemy.orm.exc import NoResultFound




class TipoFluido(Base):
    __tablename__ = 'tlm_par_tipo_fluido'
    __table_args__ = {'schema': 'regalias'}

    id = Column('ptf_id', NUMBER(9, 0, False), primary_key=True)
    _descripcion = Column('ptf_descripcion', VARCHAR(80), nullable=False)
    usuario_alta = Column('ptf_usuario_alta', VARCHAR(128), nullable=False)
    fecha_alta = Column('ptf_fecha_alta', DateTime, nullable=False)
    usuario_ult_mod = Column('ptf_usuario_ult_mod', VARCHAR(128), nullable=False)
    fecha_ult_mod = Column('ptf_fecha_ult_mod', DateTime, nullable=False)
    usuario_baja = Column('ptf_usuario_baja', VARCHAR(128))
    fecha_baja = Column('ptf_fecha_baja', DateTime)

    @hybrid_property   
    def descripcion(self):
        return self._descripcion
    
    @descripcion.setter         
    def descripcion(self, value):
        if (value == ""):
            raise ValueError(f"La descripción del tipo de fluido no puede ser vacía")             
        self._descripcion = value    

    





class TipoMedidor(Base):
    __tablename__ = 'tlm_par_tipo_medidor'
    __table_args__ = {'schema': 'regalias'}

    id = Column('ptm_id', NUMBER(9, 0, False), primary_key=True)
    _descripcion = Column('ptm_descripcion', VARCHAR(80), nullable=False)
    _campos_lectura = Column('ptm_campos_lectura', VARCHAR(2048), nullable=False)
    usuario_alta = Column('ptm_usuario_alta', VARCHAR(20), nullable=False)
    fecha_alta = Column('ptm_fecha_alta', DateTime, nullable=False)
    usuario_ult_mod = Column('ptm_usuario_ult_mod', VARCHAR(20), nullable=False)
    fecha_ult_mod = Column('ptm_fecha_ult_mod', DateTime, nullable=False)
    usuario_baja = Column('ptm_usuario_baja', VARCHAR(20))
    fecha_baja = Column('ptm_fecha_baja', DateTime)


    @hybrid_property   
    def descripcion(self):
        return self._descripcion
    
    @descripcion.setter         
    def descripcion(self, value):
        if (value == ""):
            raise ValueError(f"La descripción del tipo de medidor no puede ser vacía")             
        self._descripcion = value    

    @property
    def campos_lectura(self):
        return json.loads(self._campos_lectura)
    
    def validateCamposLectura(self):
        validate(instance = self.campos_lectura, schema = SCHEMA_LECTURA_MEDIDOR_RES11)
    
        

#Constantes de filtros de Medidor
FM_FORMATO_FECHA = "formatoFecha"
FM_FORMATO_HORA = "formatoHora"

DEFAULT_FORMATO_FECHA = "%d/%m/%Y"
DEFAULT_FORMATO_HORA = "%H:%M:%S"



class MedidorBaaa(Base):
    '''
    classdocs
    '''
    __tablename__ = 'tlm_medidores'
    __table_args__ = {'schema': 'regalias'}

    id = Column('mds_id', NUMBER(9, 0, False), primary_key=True)
    empresa_id = Column('mds_reempid', ForeignKey('regalias.treempresa.reempid'), nullable=False, index=True, comment='FK A tre_empresas')
    _cuenca_id = Column('mds_recueid', ForeignKey('regalias.trecuenca.recueid'), nullable=False, index=True)
    _tipo_fluido_id = Column('mds_ptf_id', ForeignKey('regalias.tlm_par_tipo_fluido.ptf_id'), nullable=False, index=True)
    _tipo_medidor_id = Column('mds_ptm_id', ForeignKey('regalias.tlm_par_tipo_medidor.ptm_id'), nullable=False, index=True)
    _instalacion = Column('mds_instalacion', VARCHAR(20), nullable=False, index=True)
    _descripcion_instalacion = Column('mds_descripcion_instalacion', VARCHAR(80), nullable=False)
    _longitud = Column('mds_longitud', VARCHAR(255))
    _latidud = Column('mds_latidud', VARCHAR(255))
    _cota = Column('mds_cota', VARCHAR(255))
    _cant_ramales = Column('mds_cant_ramales', NUMBER(9, 0, False))
    usuario_alta = Column('mds_usuario_alta', VARCHAR(128), nullable=False)
    fecha_alta = Column('mds_fecha_alta', DateTime, nullable=False)
    usuario_ult_mod = Column('mds_usuario_ult_mod', VARCHAR(128), nullable=False)
    fecha_ult_mod = Column('mds_fecha_ult_mod', DateTime, nullable=False)
    usuario_baja = Column('mds_usuario_baja', VARCHAR(128))
    fecha_baja = Column('mds_fecha_baja', DateTime)
    #Relaciones
    tipoFluido = relationship('TipoFluido')
    tipoMedidor = relationship('TipoMedidor')
    cuenca = relationship('Cuenca', back_populates="medidores")
    empresa = relationship('Empresa', back_populates="medidores")


    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.dirDescargas = ''   #Directorio donde se descargarán los nuevos archivos. '' significa el directorio actual
        self._formatoFecha = DEFAULT_FORMATO_FECHA
        self._formatoHora = DEFAULT_FORMATO_HORA
    
#     @orm.reconstructor
#     def __init_on_load(self):
#         self.dirDescargas = ''
#         self._formatoFecha = DEFAULT_FORMATO_FECHA
#         self._formatoHora = DEFAULT_FORMATO_HORA

    @hybrid_property
    def instalacion(self):
        return self._instalacion
    
    @instalacion.setter
    def instalacion(self, value):
        if (value == ""):
            raise ValueError("La instalación no puede ser vacía")
        self._instalacion = value
        
    @hybrid_property
    def descripcion_instalacion(self):
        return self._descripcion_instalacion

    @descripcion_instalacion.setter
    def descripcion_instalacion(self, value):
        if (value == ""):
            raise ValueError("La descripción de la instalación no puede ser vacía")
        self._descripcion_instalacion = value

    @hybrid_property
    def latitud(self):
        return self._latitud
    
    @latitud.setter
    def latitud(self, value):
        #TODO: Validar latitud
        self._latitud = value

    @hybrid_property
    def longitud(self):
        return self._longitud
    
    @longitud.setter
    def longitud(self, value):
        #TODO: Validar longitud
        self._longitud = value

    @hybrid_property
    def cota(self):
        return self._cota
    
    @cota.setter
    def cota(self, value):
        #TODO: Validar cota
        self._cota = value


    @hybrid_property
    def cant_ramales(self):
        return self._cant_ramales
    
    @cant_ramales.setter
    def cant_ramales(self, value):
        if (value <= 0):
            raise ValueError(f"La cantidad de medidores debe ser mayor o igual a 0 (valor={value})")
        self._cant_ramales = value


    @property
    def formatoFecha(self):
        """Recupera el formato de la fecha de la conexión asociada a la empresa del medidor"""
        formatoFecha = DEFAULT_FORMATO_FECHA
        dictFormatos = self.empresa.conexion.filtros2Dict()
        if (dictFormatos):
            formatoFecha = dictFormatos.get(FM_FORMATO_FECHA, DEFAULT_FORMATO_FECHA) 
            if (formatoFecha == ''):
                formatoFecha = DEFAULT_FORMATO_FECHA
        return formatoFecha    
#         return self._formatoFecha
        
#     @formatoFecha.setter
#     def formatoFecha(self, value):
#         if (value != ''):
#             self._formatoFecha = value
#         else:
#             self._formatoFecha = DEFAULT_FORMATO_FECHA

    
    @property
    def formatoHora(self):
        """Recupera el formato de la hora de la conexión asociada a la empresa del medidor"""
        formatoHora = DEFAULT_FORMATO_HORA
        dictFormatos = self.empresa.conexion.filtros2Dict()
        if (dictFormatos):
            formatoHora = dictFormatos.get(FM_FORMATO_HORA, DEFAULT_FORMATO_HORA) 
            if formatoHora == '':
                formatoHora = DEFAULT_FORMATO_HORA
        return formatoHora    

    
#     @formatoHora.setter
#     def formatoHora(self, value):
#         if (value != ''):
#             self._formatoHora = value
#         else:
#             self._formatoHora = DEFAULT_FORMATO_HORA
#     
#     
#     def setFormatosFromDict(self, dictFormatos):
#         if (dictFormatos):
#             if FM_FORMATO_FECHA in dictFormatos:
#                 self.formatoFecha = dictFormatos[FM_FORMATO_FECHA]
#             if FM_FORMATO_HORA in dictFormatos:
#                 self.formatoHora = dictFormatos[FM_FORMATO_HORA]
    
        
    def getNombreArchivoLecturasRes11(self, ramal: int, fecha: datetime) -> str:
        """Devuelve el nombre del archivo de lecturas correspondiente a un ramal y fecha"""
        return f"{self.empresa.conexion.prefijo_archivos}_{self.instalacion.lower()}_{ramal}_{fecha.strftime('%d%m%Y')}_res11_dir_regalias.txt"


    def getUltimoArchivoRamal(self, ramal: int):
        """Devuelve el último archivo que se procesado almacenado en la DB
        Si no existe ningún archivo se tomará la fecha de alta del medidor
        """
        session = inspect(self).session
        ultimaFechaCreacion, = session.query(func.max(ArchivoLecturaRes11.fecha_creacion)).filter(ArchivoLecturaRes11.medidor_id == self.id,
                                                                                                  ArchivoLecturaRes11.ramal == ramal,
                                                                                                  ArchivoLecturaRes11.fecha_baja == None
                                            ).one_or_none()
        if (ultimaFechaCreacion):
            ultimoArchivo = session.query(ArchivoLecturaRes11).filter_by(medidor_id = self.id, 
                                                                         ramal = ramal,
                                                                         fecha_creacion = ultimaFechaCreacion, 
                                                                         fecha_baja = None).one()
        else:
            ultimoArchivo = None
        return ultimoArchivo
    
    @staticmethod
    def getCantidadArchivosAProcesar(session):
        """Devuelve la cantidad de achivos que debe procesar en función de la cantidad de ramales de cada Medidor
        """
        return session.query(func.sum(Medidor._cant_ramales)).filter(Medidor.fecha_baja == None).scalar()


    def getFechaHoraUltimaLecturaRamal(self, ramal: int) -> datetime:
        #TODO: revisar getFechaHoraUltimaLecturaRamal() cuando haya datos en la tabla de lecturas
        session = inspect(self).session
        ultimaLecturaMedidor = session.query(func.max(LecturaMedidorRes11.fecha_hora)) \
                                            .select_from(LecturaMedidorRes11).join(ArchivoLecturaRes11) \
                                            .filter(ArchivoLecturaRes11.medidor_id == self.id,
                                                    ArchivoLecturaRes11.ramal == ramal,
                                                    ArchivoLecturaRes11.fecha_baja == None,
                                                    LecturaMedidorRes11.fecha_baja == None) \
                                            .scalar()
        #Si no existe ninguna lectura válida significa que el medidor es nuevo o todo lo que se ha leído es erróneo. 
        #En ambos casos es necesario establecer una fecha de inicio a partir de la cual se harán las lecturas.
        #De acuerdo a lo conversado con Graciela decidimos tomar como fecha de inicio la fecha de alta del medidor.
        #Se le resta media hora para que procese el día completo
        if (not ultimaLecturaMedidor):
            ultimaLecturaMedidor = datetime.date(self.fecha_alta) - timedelta(minutes=30)
        return ultimaLecturaMedidor

        
    def cargarNuevasLecturas(self) -> None:
        """Carga en la tabla de Lecturas las nuevas lecturas de todos los ramales"""
        for ramal in range(1, self.cant_ramales + 1):
            logging.info(f"Procesando Ramal #{ramal}")
            self.cargarNuevasLecturasXRamal(ramal)


    def cargarNuevasLecturasXRamal(self, ramal: int, dirDescargas:str = '') -> None:
        """Carga en la tabla de Lecturas todas las nuevas lecturas del ramal indicado"""
        ultimaLectura = self.getFechaHoraUltimaLecturaRamal(ramal)
        #Obtener un array con todos los días entre la fecha de hoy y la de la ultima lectura,
        #el rango de fechas a descargar debe arrancar una hora después de la última lectura
        fechasParaDescargar = [date.fromordinal(i) for i in range((ultimaLectura + timedelta(hours=1)).toordinal(), 
                                                                  (date.today() + timedelta(days=1)).toordinal())]
        print(fechasParaDescargar)
        session = inspect(self).session
        for fecha in fechasParaDescargar:
            nombreArchivo = self.getNombreArchivoLecturasRes11(ramal, fecha)
            try:
                huboError = False
                #Si ya existe un archivo cargado para la fecha a procesar recuperarlo, sino crear uno nuevo
                try:
                    currArchivo = session.query(ArchivoLecturaRes11).filter_by(medidor_id = self.id, 
                                                                               nombre = nombreArchivo, 
                                                                               fecha_baja = None).one()
                    logging.debug('El archivo ya existe en la DB')
                except NoResultFound:
                    #Crear un nuevo archivo
                    currArchivo = ArchivoLecturaRes11(medidor_id = self.id,
                                                      nombre = nombreArchivo,
                                                      tamanio = 0,                
                                                      fecha_creacion = fecha,         
                                                      cantidad_registros = 0,    
                                                      cantidad_registros_ok = 0, 
                                                      cantidad_registros_err = 0,
                                                      hash = None)               
                    logging.debug('El archivo no existe en la DB, insertando un nuevo registro')
                    session.add(currArchivo)  
                remoteFilename = nombreArchivo
                localFilename = Path(self.dirDescargas, remoteFilename)
                #Descargar el archivo
                self.empresa.conexion.conexionRemota.getFile(remoteFilename, localFilename)
                #TODO: cuando se deben guardar tamaño, fecha_creacion y hash
                
                #Procesar el archivo descargado
                logging.debug(f"Procesando el archivo {localFilename}")
                currArchivo.importarLecturas(localFilename)
                session.commit()
            except Exception as e:
                session.rollback()
                huboError = True
                logging.error(e)
            finally:
                #Borrar el archivo descargado
                if (localFilename.exists() and localFilename.is_file()):
                    #En el caso de que haya ocurrido un error no mostrar el mensaje Borrando.... porque ya se mostró el error
                    if (not huboError):
                        logging.debug(f"Borrando el archivo {localFilename}")
                    localFilename.unlink()
            #FIXME: Borrar el siguiente break
            break