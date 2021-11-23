
import logging

from typing import Optional, Any

from sqlalchemy import Column, DateTime, ForeignKey, Index, text, inspect, func, Integer, Boolean, Sequence, FetchedValue
from sqlalchemy.dialects.oracle import NUMBER, VARCHAR
from sqlalchemy.orm import relationship

from .base import Base
from sqlalchemy.ext.hybrid import hybrid_property

from datetime import datetime
import re
from csv import DictReader

#from abc import abstractstaticmethod
#from telemedicion_regalias.medidor_back import TipoFluido




class ArchivoLecturaRes11(Base):
    __tablename__ = 'tlm_archivos_lectura_res11_cab'
    __table_args__ = (
        Index('un_nom_fec_baj', 'alc_nombre', 'alc_fecha_baja', unique=True),
        {'schema': 'regalias'}
    )

    id = Column('alc_id', Integer, Sequence('seq_tlm_archivos_cab', schema='regalias'), primary_key=True)
    medidor_id = Column('alc_mds_id', ForeignKey('regalias.tremedfiscal.remedfisid'), comment='ID del medidor que genero el archivo')
    _nombre = Column('alc_nombre', VARCHAR(80), nullable=False, comment='Nombre del archivo')
    tamanio = Column('alc_tamanio', NUMBER(15, 2, True), server_default=text("0"), comment='Tamaño del archivo en bytes')
    fecha_creacion = Column('alc_fecha_creacion', DateTime, comment='Fecha de creación del archivo por el medidor')
    cantidad_registros = Column('alc_cantidad_registros', NUMBER(9, 0, False), server_default=text("0"), comment='Cantidad total de registros que contiene el archivo')
    cantidad_registros_ok = Column('alc_cantidad_registros_ok', NUMBER(9, 0, False), server_default=text("0"), comment='Cantidad de registros procesados OK')
    cantidad_registros_err = Column('alc_cantidad_registros_err', NUMBER(9, 0, False), server_default=text("0"), comment='Cantidad de registros procesados con ERROR')
    hash = Column('alc_hash', VARCHAR(32), comment='Hash MD5 para verificar la integridad del archivo')
    usuario_alta = Column('alc_usuario_alta', VARCHAR(128), nullable=False, server_default=FetchedValue())
    fecha_alta = Column('alc_fecha_alta', DateTime, nullable=False, server_default=FetchedValue())
    usuario_ult_mod = Column('alc_usuario_ult_mod', VARCHAR(128), nullable=False, server_default=FetchedValue(), server_onupdate=FetchedValue())
    fecha_ult_mod = Column('alc_fecha_ult_mod', DateTime, nullable=False, server_default=FetchedValue(), server_onupdate=FetchedValue())
    usuario_baja = Column('alc_usuario_baja', VARCHAR(128))
    fecha_baja = Column('alc_fecha_baja', DateTime)
    #Relaciones
    medidor = relationship('MedidorFiscal')
    Lecturas = relationship('LecturaMedidorRes11', back_populates='archivo', order_by='LecturaMedidorRes11.fecha_hora')


    def __repr__(self):
        return f"LineItems(id={self.id}, " \
               f"_medidor_id={self._medidor_id}, " \
               f"_nombre={self._nombre}, " \
               f"_tamanio={self._tamanio})"

   
    @hybrid_property
    def nombre(self) -> str:
        return self._nombre

    @nombre.setter
    def nombre(self, value: str) -> None:
        #Formato del nombre del archivo:
        #   {prefijo}_{instalación}_{ramal}_{ddmmyyyy}_res11_dir_regalias.txt
        if (value == ""):
            raise ValueError("El nombre del archivo no puede ser vacío")
        self._nombre = value

   
    @hybrid_property   
    def ramal(self) -> Optional[int]:
        """Retorna el ramal al que pertenece el archivo obteniéndolo de su nombre, para operaciones a nivel de instancia"""
        ramal = None
        if (self._nombre):
            #La expresión regular parte el nombre en tres grupos, el segundo es el ramal
            matchRamal = re.match(r"(.+_)([0-9]+)(_[0-9]{8}_res11_dir_regalias.txt)", self.nombre, re.IGNORECASE)
            if (matchRamal):
                ramal = matchRamal.group(2)
        return ramal
        
    @ramal.expression
    def ramal(cls) -> Optional[int]:  # @NoSelf
        """Retorna el ramal al que pertenece el archivo obteniéndolo de su nombre, para operaciones a nivel de clase"""
        return func.regexp_substr(cls.nombre, '(.+_)([0-9]+)(_[0-9]{8}_res11_dir_regalias.txt)', 1, 1, 'i', 2)
        
    @property
    def estructuraCampos(self):
        self.medidor.tipoMedidor.validateCamposLectura()
        return self.medidor.tipoMedidor.campos_lectura

    def getUltimaLectura(self) -> Optional[datetime]:
        """Retorna la fecha y hora de la mayor lectura para de este archivo, si no hay ninguna devuelve None"""
        #TODO: Revisar la recuperación de la última fecha desde la tabla de lecturas
        session = inspect(self).session
        ultimaLectura = session.query(func.max(LecturaMedidorRes11.fecha_hora)) \
                                     .filter(LecturaMedidorRes11.archivo_id == self.id,
                                             LecturaMedidorRes11.fecha_baja == None) \
                                     .scalar()
        return ultimaLectura


    def importarLecturas(self, archivo) -> None:
        """Carga en la DB las nuevas lecturas desde el archivo indicado"""
        session = inspect(self).session   
        fechaHoraUltimaLectura = self.getUltimaLectura()
#       with open(localFilename, mode='r', encoding='iso-8859-1') as f:
        with archivo.open(mode='r', errors='replace') as csvFile:
            #Saltear la cabecera
            next(csvFile)
            #Inicializar el parser
            lecturasReader = DictReader(csvFile, fieldnames=list(self.estructuraCampos.keys()), 
                                        restval='', delimiter=';')
            #La variable siguenMayores indica que a partir de que se encontró un valor posterior, todo lo que sigue debería ser posterior
            siguenMayores = False
            nroLinea = 1
            #Procesar el archivo línea x línea
            for lineaLectura in lecturasReader:
                try:
                    fechaHoraLineaLectura = datetime.strptime(f"{lineaLectura['fecha']} {lineaLectura['hora']}", 
                                                              f"{self.medidor.formatoFecha} {self.medidor.formatoHora}")
                    #Si no existe ultimaLectura (None) para este archivo significa que es la primera y debe ser insertada.
                    #O si la fecha y hora de la línea que se esta procesando es posterior a la de la última lectura, 
                    #también debe ser insertada  
                    if (not fechaHoraUltimaLectura) or (fechaHoraLineaLectura > fechaHoraUltimaLectura):
                        #A parir de ahora todas las lecturas deberían ser posteriores a la última lectura
                        siguenMayores = True
                        logging.debug(f"Parseando lectura {lineaLectura}...")
                        #Instanciar la clase LecturaMedidor según el tipo de medidor y fluido
#                         nuevaLecturaRes11 = LecturaFactory.getLectura(self.medidor.tipoMedidor.descripcion, 
#                                                                       self.medidor.tipoFluido.descripcion)
                        nuevaLecturaRes11 = LecturaMedidorRes11(self, nroLinea)
                        nuevaLecturaRes11.rellenarCamposFromLineaArchivo(lineaLectura)
                        #Actualizar la cantidad de registros del archivo
                        if nuevaLecturaRes11.tiene_errores:
                            self.cantidad_registros_err += 1
                        else:
                            self.cantidad_registros_ok += 1
                        self.cantidad_registros += 1
                        #Insertar en la DB        
                        logging.debug(f"Insertando lectura en la DB...")
                        session.add(nuevaLecturaRes11)
                        #FIXME: la inserción del error no debe ir aqui, esto debe ir dentro de la clase lectura
                        if nuevaLecturaRes11._error:
                            session.add(nuevaLecturaRes11._error)
                        #FIXME: sacar este commit
                        session.commit()
                        fechaHoraUltimaLectura = nuevaLecturaRes11.fecha_hora
                    else:
                        #TODO: Que se debe hacer en este caso
                        #El manejo de este error hay que hacerlo dentro de nuevaLecturaRes11.error
                        if siguenMayores:
                            raise Exception(f"La fecha y hora de la línea {nroLinea} debería ser posterior a las de la línea anterior")
                    
                except Exception as e:
                    logging.error(e)
                    #TODO: Grabar la línea en la tabla de errores
                finally:
                    nroLinea += 1
                    
                    
                    

#-------------------------------------------------------------------------------
# Excepciones definidas para las lecturas
#-------------------------------------------------------------------------------

class CampoLecturaRes11Exception(Exception):
    """Excepción genérica para manejar errores en los campos de lectura.

    Atributos:
        campo: campo en el cual ocurrió el error
        message: explicación del error
    """
    
    def __init__(self, campo: str, mensaje: str) -> None:
        self.campo = campo
        self.message = mensaje


class CampoRequeridoException(CampoLecturaRes11Exception):
    """Excepción lanzada cuando un campo definido como requerido no tiene un valor en el archivo de lecturas."""

class ValidacionCampoException(CampoLecturaRes11Exception):
    """Excepción lanzada por las funciones validadoras de los campos de los archivos de lecturas."""


#-------------------------------------------------------------------------------
# Constantes de tipo de obligatoriedad
#-------------------------------------------------------------------------------

TO_REQUERIDO = "requerido"
TO_OPCIONAK = "opcional"


#-------------------------------------------------------------------------------
# Schema Estructura Campos Lectura
#-------------------------------------------------------------------------------

SCHEMA_LECTURA_MEDIDOR_RES11 = {
    "type": "object",
    "properties": {
        "fecha": {"type": "boolean"},
        "hora": {"type": "boolean"},
        "instalacion": {"type": "boolean"},
        "medidor": {"type": "boolean"},
        "temperatura": {"type": "boolean"},
        "presion": {"type": "boolean"},
        "caudal_instantaneo_gross": {"type": "boolean"},
        "acumulador_gross_no_reseteable": {"type": "boolean"},
        "acumulador_pulsos_brutos_no_reseteable": {"type": "boolean"},
        "factor_k_del_medidor": {"type": "boolean"},
        "altura_liquida": {"type": "boolean"},
        "acumulador_masa_no_reseteable": {"type": "boolean"},
        "volumen_acumulado_24_hs": {"type": "boolean"},
        "volumen_acumulado_hoy": {"type": "boolean"},
        "sh2": {"type": "boolean"},
        "n2": {"type": "boolean"},
        "c6_mas": {"type": "boolean"},
        "nc5": {"type": "boolean"},
        "densidad_relativa": {"type": "boolean"},
        "co2": {"type": "boolean"},
        "caudal_instantaneo_a_9300": {"type": "boolean"}, 
        "c1": {"type": "boolean"}, 
        "c2": {"type": "boolean"},
        "c3": {"type": "boolean"},
        "ic4": {"type": "boolean"},
        "nc4": {"type": "boolean"},
        "ic5": {"type": "boolean"},
        "poder_calorifico": {"type": "boolean"}
    },
    "required": ["fecha", "hora", "instalacion", "medidor"],
    "additionalProperties": False
}



class LecturaMedidorRes11(Base):
    """Clase base para las lectura de todos los medidores"""
    
    __tablename__ = 'tlm_archivos_lectura_res11_det'
    __table_args__ = (
        Index('un_alc_fecha_hora_fec_baj', 'ald_alc_id', 'ald_fecha_hora', 'ald_fecha_baja', unique=True),
        Index('un_inst_med_fecha_hora_fec_baj', 'ald_instalacion', 'ald_medidor', 'ald_fecha_hora', 'ald_fecha_baja', unique=True),
        {'schema': 'regalias'}
    )

    id = Column('ald_id', Integer, Sequence('seq_tlm_archivos_det', schema='regalias'),primary_key=True)
    archivo_id = Column('ald_alc_id', ForeignKey('regalias.tlm_archivos_lectura_res11_cab.alc_id'), nullable=False)
    nro_linea = Column('ald_nro_linea', NUMBER(9, 0, False), nullable=False)
    fecha_hora = Column('ald_fecha_hora', DateTime, nullable=False)
    instalacion = Column('ald_instalacion', VARCHAR(20), nullable=False, comment='Verificar la existencia en la tabla TLM_MEDIDORES_FISCALES')
    medidor = Column('ald_medidor', VARCHAR(10), nullable=False)
    tiene_errores = Column('ald_tiene_errores', Boolean, nullable=False, default=False, comment='0 Registro valido - 1 Registro con errores')
    usuario_alta = Column('ald_usuario_alta', VARCHAR(128), nullable=False, server_default=FetchedValue())
    fecha_alta = Column('ald_fecha_alta', DateTime, nullable=False, server_default=FetchedValue())
    usuario_ult_mod = Column('ald_usuario_ult_mod', VARCHAR(128), nullable=False, server_default=FetchedValue(), server_onupdate=FetchedValue())
    fecha_ult_mod = Column('ald_fecha_ult_mod', DateTime, nullable=False, server_default=FetchedValue(), server_onupdate=FetchedValue())
    usuario_baja = Column('ald_usuario_baja', VARCHAR(128))
    fecha_baja = Column('ald_fecha_baja', DateTime)
    #Medidor Líquido
    temperatura = Column('ald_temperatura', NUMBER(9, 2, True))
    presion = Column('ald_presion', NUMBER(9, 2, True))
    caudal_instantaneo_gross = Column('ald_caudal_instantaneo_gross', NUMBER(9, 0, False))
    acumulador_gross_no_reseteable = Column('ald_acumulador_gross_no_resete', NUMBER(9, 0, False))
    acumulador_pulsos_brutos_no_reseteable = Column('ald_acumulador_pusos_brutos_no', NUMBER(9, 0, False))
    factor_k_del_medidor = Column('ald_factor_k_del_medidor', NUMBER(9, 0, False))
    #Tanque
    altura_liquida = Column('ald_altura_liquida', NUMBER(9, 2, True))
    acumulador_masa_no_reseteable = Column('ald_acumulador_masa_no_resete', NUMBER(9, 0, False))
    #Medidor Gaseoso
    volumen_acumulado_24_hs = Column('ald_volumen_acumulado_24_hs', NUMBER(9, 3, True))
    volumen_acumulado_hoy = Column('ald_volumen_acumulado_hoy', NUMBER(9, 3, True))
    sh2 = Column('ald_sh2', NUMBER(9, 2, True))
    n2 = Column('ald_n2', NUMBER(9, 2, True))
    c6_mas = Column('ald_c6_mas', NUMBER(9, 2, True))
    nc5 = Column('ald_nc5', NUMBER(9, 2, True))
    densidad_relativa = Column('ald_densidad_relativa', NUMBER(12, 5, True))
    co2 = Column('ald_co2', NUMBER(9, 2, True))
    caudal_instantaneo_a_9300 = Column('ald_caudal_instantaneo_a_9300', NUMBER(9, 0, False))
    c1 = Column('ald_c1', NUMBER(9, 2, True))
    c2 = Column('ald_c2', NUMBER(9, 2, True))
    c3 = Column('ald_c3', NUMBER(9, 2, True))
    ic4 = Column('ald_ic4', NUMBER(9, 2, True))
    nc4 = Column('ald_nc4', NUMBER(9, 2, True))
    ic5 = Column('ald_ic5', NUMBER(9, 0, False))
    poder_calorifico = Column('ald_poder_calorifico', NUMBER(9, 0, False))
    #Relaciones
    archivo = relationship('ArchivoLecturaRes11', back_populates='Lecturas')
    error = relationship('ErrorLecturaRes11', enable_typechecks=False, uselist=False, back_populates='lectura')

    def __init__(self, archivo, nroLinea: int, **kwargs) -> None:
        super().__init__(**kwargs)
        self.archivo = archivo
        self.nro_linea = nroLinea
        self._setEstructuraCampos(self.archivo.estructuraCampos)
        self._error = None
        self.onError = None
        

    def _setEstructuraCampos(self, estructuraCampos) -> None:
        if not estructuraCampos:
            raise AttributeError('No se inicializado la estructura de campos de la lectura')
        
        self._estructuraCampos = estructuraCampos
        
                
    def rellenarCamposFromLineaArchivo(self, dicLinea) -> None:
        for campo, obligatoriedad in self._estructuraCampos.items():
            obligatorio = (obligatoriedad.lower() == TO_REQUERIDO)
            if (campo == 'fecha'):
                fecha_hora = datetime.strptime(f"{dicLinea['fecha']} {dicLinea['hora']}", 
                                               f"{self.archivo.medidor.formatoFecha} {self.archivo.medidor.formatoHora}")
                self._setValorCampo('fecha_hora', fecha_hora, obligatorio)
            elif (campo == 'hora'):
                #El campo hora se ignora ya que fue procesado junto con el campo fecha
                continue
            else:
                self._setValorCampo(campo, dicLinea[campo], obligatorio)
                
                    
    def _str2Float(self, strNum: str) -> Optional[float]:
        """Convierte un string a float, haciendo trim y reemplazando ',' por '.'. 
        Si el string es None retorna None
        """
        if (strNum) and (strNum.strip() != ''):
            return float(strNum.replace(",", ".").strip())
        else:
            return None    

    
    def _str2Integer(self, strNum: str) -> Optional[int]:
        """Convierte un string a integer, haciendo trim y eliminando los decimales. 
        Si el string es None retorna None
        """
        if (strNum) and (strNum.strip() != ''):
            return int(float(strNum.replace(",", ".").strip()))
        else:
            return None    
        
        
    def _getColumnDataType(self, nombreCampo: str) -> Any:
        """Devuelve el tipo de dato del campo especificado. 
        Parámetros:
            nombreCampo: puede ser el nombre del campo de la tabla o su alias definido en el mapper
            
        Genera una excepción si el campo no existe en la tabla o el alias no existe en el mapper
        """
        tipoColumna = None
        #Buscar primero el campo en las columnas de la tabla, sino en los alias del mapper,
        #sino generar una excepción
        if (nombreCampo in self.__table__.columns):
            tipoColumna = self.__table__.columns[nombreCampo].type
        elif (nombreCampo in self.__mapper__._props):
            tipoColumna = self.__mapper__._props[nombreCampo].columns[0].type
        else:
            raise Exception(f"Nombre de campo no válido (nombreCampo: {nombreCampo})")
        return tipoColumna
        
        
    def _convert2DBType(self, nombreCampo: str, valorOriginal: Any) -> Any:
        """Retorna el valor convertido al formato de la columna de la base de datos"""
        #Obtener el data type de la columna.
        tipoOriginal = type(valorOriginal) 
        tipoColumna = self._getColumnDataType(nombreCampo)
        #Convertir a números enteros y flotantes
        if isinstance(tipoColumna, NUMBER):
            if (tipoColumna.scale == 0):
                #Numeros enteros
                if (tipoOriginal is str):
                    valorDB = self._str2Integer(valorOriginal)
                elif (tipoOriginal in (int, float)):
                    valorDB = int(valorOriginal)
                else:
                    raise ValueError(f"Tipo de valor inválido para el campo {nombreCampo} (tipo: {tipoOriginal})")
            else:
                #Números flotantes
                if (tipoOriginal is str):
                    valorDB = self._str2Float(valorOriginal)
                elif (tipoOriginal in (int, float)):
                    valorDB = float(valorOriginal)
                else:
                    raise ValueError(f"Tipo de valor inválido para el campo {nombreCampo} (tipo: {tipoOriginal})")
        else:
            #El resto de los campos se asignan directamente
            valorDB = valorOriginal
        return valorDB


    def _setValorCampo(self, nombreCampo: str, valorEnArchivo: Any, obligatorio: bool) -> None:
        """Asigna el un nuevo valor a un campo de la DB, previa validación.
        Si ocurre un error guarda el valor erróneo en la tabla de errores y pone en verdadero la marca que 
        indica que hay valores erróneos en la lectura.
        
        Generará una excepción AttributeError si el nombre del campo es erróneo (no existe en la tabla)
        """ 
        logging.debug(f"{nombreCampo}: {valorEnArchivo}")
        try:
            #Verificar que exista el campo
            if hasattr(self, nombreCampo):
                #Verificar que si el campo es obligatorio tenga un valor 
                if ((valorEnArchivo == '') and obligatorio):
                    raise CampoRequeridoException(f"El campo {nombreCampo} no tiene valor y es obligatorio")
                #Convertir el valor al formato de la DB
                valor = self._convert2DBType(nombreCampo, valorEnArchivo)
                #Si hay un validador definido para el campo, ejecutarlo
                validador = f"_validar_{nombreCampo}"
                if hasattr(self, validador):
                    funcValidador = getattr(self, validador)
                    funcValidador(valor)
                setattr(self, nombreCampo, valor)
            else:
                logging.error(f"Nombre de campo inválido (campo={nombreCampo})")
                raise AttributeError(f"Nombre de campo inválido (campo={nombreCampo})")    
        except AttributeError:
            #Si el error es que no existe el campo, pasarlo para arriba porque es un error de programación
            raise
        except Exception as e:
            logging.error(e)
            #Si tiene un error handler asignado, ejecutarlo 
            if self.onError:
                #TODO: definir la interfaz del error handler de las lecturas
                self.onError(e)
            #Marcar la lectura como que contiene errores
            self.tiene_errores = True
            #Si no existe la instancia de error crearla
            if not self._error:
                self._error = ErrorLecturaRes11(self)
#                 inspect(self).session.add(self._error)   
            setattr(self._error, nombreCampo, valorEnArchivo)
            

        


class LecturaMedidorLiquido(LecturaMedidorRes11):
#     temperatura = Column('ald_temperatura', NUMBER(9, 2, True))
#     presion = Column('ald_presion', NUMBER(9, 2, True))
#     caudal_instantaneo_gross = Column('ald_caudal_instantaneo_gross', NUMBER(9, 0, False))
#     acumulador_gross_no_reseteable = Column('ald_acumulador_gross_no_resete', NUMBER(9, 0, False))
#     acumulador_pulsos_brutos_no_reseteable = Column('ald_acumulador_pusos_brutos_no', NUMBER(9, 0, False))
#     factor_k_del_medidor = Column('ald_factor_k_del_medidor', NUMBER(9, 0, False))


    def rellenarCamposFromLineaArchivo(self, dicLinea):
        super().rellenarCamposFromLineaArchivo(dicLinea)
#         self._setValorCampo('temperatura', dicLinea['temperatura'], True)
#         self._setValorCampo('presion', dicLinea['presion'], True) 
#         self._setValorCampo('caudal_instantaneo_gross', dicLinea['caudal_instantaneo_gross'], True)
#         self._setValorCampo('acumulador_gross_no_reseteable', dicLinea['acumulador_gross_no_reseteable'], True)
#         self._setValorCampo('acumulador_pulsos_brutos_no_reseteable', dicLinea['acumulador_pulsos_brutos_no_reseteable'], True)
#         self._setValorCampo('factor_k_del_medidor', dicLinea['factor_k_del_medidor'], True)

    
    """
    Definición de validadores de campos
    
    def self._validar_<nombre_campo>(valor)
    siendo:
        nombre_campo: el nombre del campo a validar, es sensible a mayúsculas 
                      y minúsculas
        valor: el valor del campo a validar
        
    La función retorna None (no devuelve resultado alguno) y si el valor es 
    inválido debe generar una excepción del tipo ValidacionCampoException con 
    el mensaje indicando claramente el motivo del error en la validación
    """
    def _validar_temperatura(self, valor: float) -> None:
        """Validador del campo temperatura"""
        #FIXME: borrar esta rango de validacion de prueba
        if not (0 > valor >= 25):
            raise ValidacionCampoException('temperatura', 
                                           f"Valor del campo temperatura fuera de rango (0,25] (temperatura={valor})")





class LecturaMedidorTanque(LecturaMedidorRes11):
#     altura_liquida = Column('ald_altura_liquida', NUMBER(9, 2, True))
#     acumulador_masa_no_reseteable = Column('ald_acumulador_masa_no_resete', NUMBER(9, 0, False))


    def rellenarCamposFromLineaArchivo(self, dicLinea):
        super().rellenarCamposFromLineaArchivo(dicLinea)
#         self._setValorCampo('altura_liquida', self._str2Float(dicLinea['altura_liquida']))
#         self._setValorCampo('acumulador_masa_no_reseteable', self._str2Integer(dicLinea['acumulador_masa_no_reseteable']))
        
        




class LecturaMedidorGas(LecturaMedidorRes11):
#    temperatura = Column('ald_temperatura', NUMBER(9, 2, True))
#    presion = Column('ald_presion', NUMBER(9, 2, True))

#     volumen_acumulado_24_hs = Column('ald_volumen_acumulado_24_hs', NUMBER(9, 3, True))
#     volumen_acumulado_hoy = Column('ald_volumen_acumulado_hoy', NUMBER(9, 3, True))
#     sh2 = Column('ald_sh2', NUMBER(9, 2, True))
#     n2 = Column('ald_n2', NUMBER(9, 2, True))
#     c6_mas = Column('ald_c6_mas', NUMBER(9, 2, True))
#     nc5 = Column('ald_nc5', NUMBER(9, 2, True))
#     densidad_relativa = Column('ald_densidad_relativa', NUMBER(12, 5, True))
#     co2 = Column('ald_co2', NUMBER(9, 2, True))
#     caudal_instantaneo_a_9300 = Column('ald_caudal_instantaneo_a_9300', NUMBER(9, 0, False))
#     c1 = Column('ald_c1', NUMBER(9, 2, True))
#     c2 = Column('ald_c2', NUMBER(9, 2, True))
#     c3 = Column('ald_c3', NUMBER(9, 2, True))
#     ic4 = Column('ald_ic4', NUMBER(9, 2, True))
#     nc4 = Column('ald_nc4', NUMBER(9, 2, True))
#     ic5 = Column('ald_ic5', NUMBER(9, 0, False))
#     poder_calorifico = Column('ald_poder_calorifico', NUMBER(9, 0, False))


    def rellenarCamposFromLineaArchivo(self, dicLinea):
        super().rellenarCamposFromLineaArchivo(dicLinea)
#         self._setValorCampo('volumen_acumulado_24_hs', self._str2Float(dicLinea['volumen_acumulado_24_hs']))
#         self._setValorCampo('volumen_acumulado_hoy', self._str2Float(dicLinea['volumen_acumulado_hoy']))
#         self._setValorCampo('sh2', self._str2Float(dicLinea['sh2']))
#         self._setValorCampo('n2', self._str2Float(dicLinea['n2']))
#         self._setValorCampo('c6_mas', self._str2Float(dicLinea['c6_mas']))
#         self._setValorCampo('nc5', self._str2Float(dicLinea['nc5']))
#         self._setValorCampo('densidad_relativa', self._str2Float(dicLinea['densidad_relativa']))
#         self._setValorCampo('co2', self._str2Float(dicLinea['co2']))
#         self._setValorCampo('caudal_instantaneo_a_9300', self._str2Integer(dicLinea['caudal_instantaneo_a_9300']))
#         self._setValorCampo('c1', self._str2Float(dicLinea['c1']))
#         self._setValorCampo('c2', self._str2Float(dicLinea['c2']))
#         self._setValorCampo('c3', self._str2Float(dicLinea['c3']))
#         self._setValorCampo('ic4', self._str2Float(dicLinea['ic4']))
#         self._setValorCampo('nc4', self._str2Float(dicLinea['nc4']))
#         self._setValorCampo('ic5', self._str2Integer(dicLinea['ic5']))
#         self._setValorCampo('poder_calorifico', self._str2Integer(dicLinea['poder_calorifico']))

    


class LecturaFactory():
    
    @staticmethod    
    def getLectura(tipoMedidor, tipoFluido):
        if (tipoFluido == 'LIQ'):
            if (tipoMedidor in ('DESPLAZAMIENTO', 'CORIOLIS', 'BASCULA')):
                return LecturaMedidorLiquido()
            elif (tipoMedidor == 'TANQUE'):
                return LecturaMedidorTanque()
            else:
                raise ValueError(f"Tipo de medidor líquido inválido (tipoMedidor={tipoMedidor})")
        elif (tipoFluido == 'GAS'):
            if (tipoMedidor in ('PLACA ORIFICIO', 'ULTRASONICO')):
                return LecturaMedidorGas()
            else:
                raise ValueError(f"Tipo de medidor gas inválido (tipoMedidor={tipoMedidor})")
        else:
            raise ValueError(f"Tipo de fluido inválido (tipoFluido={tipoFluido})")   







class ErrorLecturaRes11(Base):
    __tablename__ = 'tlm_archivos_lectura_res11_err'
    __table_args__ = {'schema': 'regalias'}

    id = Column('ale_id', NUMBER(9, 0, False), Sequence('seq_tlm_archivos_err', schema='regalias'), primary_key=True)
    lectura_id  = Column('ale_ald_id', ForeignKey('regalias.tlm_archivos_lectura_res11_det.ald_id'), nullable=False)
    temperatura = Column('ale_temperatura', VARCHAR(50))
    presion = Column('ale_presion', VARCHAR(50))
    caudal_instantaneo_gross = Column('ale_caudal_instantaneo_gross', VARCHAR(50))
    acumulador_gross_no_reseteable = Column('ale_acumulador_gross_no_resete', VARCHAR(50))
    acumulador_pulsos_brutos_no_reseteable = Column('ale_acumulador_pusos_brutos_no', VARCHAR(50))
    factor_k_del_medidor = Column('ale_factor_k_del_medidor', VARCHAR(50))
    altura_liquida = Column('ale_altura_liquida', VARCHAR(50))
    acumulador_masa_no_reseteable = Column('ale_acumulador_masa_no_resete', VARCHAR(50))
    volumen_acumulado_24_hs = Column('ale_volumen_acumulado_24_hs', VARCHAR(50))
    volumen_acumulado_hoy = Column('ale_volumen_acumulado_hoy', VARCHAR(50))
    sh2 = Column('ale_sh2', VARCHAR(50))
    n2 = Column('ale_n2', VARCHAR(50))
    c6_mas = Column('ale_c6_mas', VARCHAR(50))
    nc5 = Column('ale_nc5', VARCHAR(50))
    densidad_relativa = Column('ale_densidad_relativa', VARCHAR(50))
    co2 = Column('ale_co2', VARCHAR(50))
    caudal_instantaneo_a_9300 = Column('ale_caudal_instantaneo_a_9300', VARCHAR(50))
    c1 = Column('ale_c1', VARCHAR(50))
    c2 = Column('ale_c2', VARCHAR(50))
    c3 = Column('ale_c3', VARCHAR(50))
    ic4 = Column('ale_ic4', VARCHAR(50))
    nc4 = Column('ale_nc4', VARCHAR(50))
    ic5 = Column('ale_ic5', VARCHAR(50))
    poder_calorifico = Column('ale_poder_calorifico', VARCHAR(50))
    usuario_alta = Column('ale_usuario_alta', VARCHAR(128), nullable=False, server_default=FetchedValue())
    fecha_alta = Column('ale_fecha_alta', DateTime, nullable=False, server_default=FetchedValue())
    usuario_ult_mod = Column('ale_usuario_ult_mod', VARCHAR(128), nullable=False, server_default=FetchedValue(), server_onupdate=FetchedValue())
    fecha_ult_mod = Column('ale_fecha_ult_mod', DateTime, nullable=False, server_default=FetchedValue(), server_onupdate=FetchedValue())
    usuario_baja = Column('ale_usuario_baja', VARCHAR(128))
    fecha_baja = Column('ale_fecha_baja', DateTime)
    #Relaciones
    lectura = relationship('LecturaMedidorRes11', enable_typechecks=False, back_populates='error')

    def __init__(self, lectura, **kwargs):
            super().__init__(**kwargs)
            self.lectura = lectura

