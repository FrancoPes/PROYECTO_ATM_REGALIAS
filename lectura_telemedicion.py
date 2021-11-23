#!/usr/bin/python3
# encoding: utf-8
'''
lectura_telemedicion -- Script de lectura de valores de medidores de Regalías

lectura_telemedicion is a description

It defines classes_and_methods

@author:     Orlando Irrazabal

@copyright:  2020 Administración Tributaria Mendoza. All rights reserved.

@license:    license

@contact:    oirraza@mendoza.gov.ar
@deffield    updated: Updated
'''

import sys
import os

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

import logging

from telemedicion_regalias.empresa import Empresa
from telemedicion_regalias.medidor_fiscal import MedidorFiscal
from telemedicion_regalias import base
import lectura_telemedicion_config as config


# os.environ["NLS_LANG"] = ".WE8ISO8859P15"

__all__ = []
__version__ = 0.1
__date__ = '2020-05-17'
__updated__ = '2021-11-16'

DEBUG = 1
DEVRUN = 1
TESTRUN = 0
PROFILE = 0


dryRun = False


class CLIError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super(CLIError).__init__(type(self))
        self.msg = f"E: {msg}"
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg




           
def initLogging(logFilename, logLevel):
    if logFilename: 
        logging.basicConfig(level = logLevel,
                            format = '%(asctime)s %(levelname)-8s %(message)s',
                            datefmt = '%d/%m/%y %H:%M', 
                            filename = logFilename,
                            filemode = 'a')
    else:
        #Si no se indica el archivo de log, eliminar el logger por defecto
        logger = logging.getLogger()
        logger.setLevel(logLevel)
        for hnd in logger.handlers:
            if isinstance(hnd, logging.FileHandler):
                logger.removeHandler(hnd)
    #Configurar el logging de Paramiko            
    logging.getLogger("paramiko").setLevel(logging.WARNING)
    
    #logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)
    
def consoleLogging(logLevel):
    #Definir un logger para la consola
    console = logging.StreamHandler()
    console.setLevel(logLevel)
#    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter('%(message)s'))
    logging.getLogger().addHandler(console)


DEBUG_LEVELS=dict(critical=logging.CRITICAL, error=logging.ERROR, warning=logging.WARNING, 
              info=logging.INFO, debug=logging.DEBUG)


def main(argv=None): # IGNORE:C0111
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    program_name = os.path.basename(sys.argv[0])
    program_version = "v%s" % __version__
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]

    try:
        # Setup argument parser
        parser = ArgumentParser(description=f'{program_shortdesc}\n\nUSO', 
                                formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument("-b", "--database", 
                            dest="db", 
                            help="base de datos a utilizar [default: %(default)s]")
        parser.add_argument("-l", "--logfile", 
                            dest="logFilename", 
                            help="archivo de log (path completo) [default: %(default)s]", 
                            metavar="FILE")
        parser.add_argument("-d" , "--debug-level", 
                            dest="debugLevel", 
                            choices=["warning","info","debug"], 
                            help="nivel de debug. Valores validos: %(choices)s [default: %(default)s]")
        parser.add_argument("-q", "--silencioso", 
                            dest="quiet", 
                            action="store_true",
                            help="modo silencioso, sin salida por consola [default: %(default)s]")
        parser.add_argument("-s", "--simular", 
                            dest="dryRun", 
                            action="store_true",
                            help="ejecutar en modo simulación")

        parser.set_defaults(db="TAXWEBD", 
                            logFilename=f"{os.path.splitext(os.path.basename(sys.argv[0]))[0]}.log",
                            debugLevel=logging.INFO,
                            quiet=False, 
                            dryRun=False)
        # Process arguments
        args = parser.parse_args()

        db = args.db
        logFilename = args.logFilename
        debugLevel = getattr(logging, args.debugLevel.upper())
        quiet = args.quiet
        dryRun = args.dryRun

        initLogging(logFilename, debugLevel)
        
        if not quiet:
            consoleLogging(debugLevel)
         
        logging.info("Proceso de lecturas del Sistema de Telemedición")

        #Validar que la db exista en el archivo de configuración
        if (db in config.DB_CONFIG):
            #Configurar los parámetros para la conexión a SQLAlchemy
            dbConfig =  config.DB_CONFIG[db]
            #Formato URL: 'oracle+cx_oracle://user:pass@host:port/sid'
            urlSQLAlchemy = f"{dbConfig['dialect']}://{dbConfig['user']}:{dbConfig['pass']}@{dbConfig['host']}:{dbConfig['port']}/{dbConfig['sid']}"
            argsConexion = {}
            if "encode" in dbConfig:
                argsConexion['encoding'] = dbConfig['encode']
                argsConexion['nencoding'] = dbConfig['encode']
        else:
            logging.error(f"No se encontró la definición de la conexión {db}")
            raise Exception(f"No se encontró la definición de la conexión {db}")

        
        base.initSQLAlchemy(urlSQLAlchemy, connect_args=argsConexion)

        #Procesar sólo las empresas que tienen medidores
        #FIXME: ¿Que hago con las empresas que tienen medidores pero no tienen configurada una conexión?
        
        #Mostrando cantidad de archivos que se deben procesar 
        cantArchivos = MedidorFiscal.getCantidadArchivosAProcesar(base.session)
        logging.info(f"Cantidad de archivos a procesar : {cantArchivos} del dia")
        
        for empresa in base.session.query(Empresa).filter(Empresa.medidores!=None).order_by(Empresa.nombre):
            logging.info("------------------------------------------------------------------------------------------")
            logging.info(f"Procesando Empresa {empresa.id}-{empresa.nombre}")
            if (empresa.conexion):
                logging.debug(f"Empresa con conexion")
                try:
                    if (empresa.conexion.protocolo in ('FTPES','FTPS')):
                        logging.debug(f"Se descarta ya que el protocolo es: {empresa.conexion.protocolo}")
                        continue
                    if (empresa.id == 29):
                        logging.debug('skipping')
                        continue
                    empresa.conexion.connectServer()
                    try:
                        if empresa.conexion.connectedServer():
                            logging.info("Conectado al servidor de la empresa")
                            #Procesar cada medidor
                            for medidor in empresa.medidores:
                                try:
                                    logging.info(f"Procesando medidor: {medidor.descripcion}")
                                    medidor.dirDescargas = config.DIR_DESCARGAS
                                    #Setear los formatos de fecha, hora, etc que están definidos en la conexión
#                                     medidor.setFormatosFromDict(empresa.conexion.filtros2Dict())
                                    medidor.cargarNuevasLecturas()
                                except Exception as e:
                                    logging.error(f"Error al procesar el medidor (error={e})")
                                    if (DEBUG or TESTRUN):
                                        raise(e)
                    finally:
                        empresa.conexion.disconnectServer()            
                except Exception as e:
                    #El mensaje de la excepción ya viene formateado "msg (error=e)"
                    logging.error(e)
                    if (DEBUG or TESTRUN):
                        raise(e)
            else:
                logging.warning("No existe conexión definida")
                    
        return 0
    
    except Exception as e:
        if (DEBUG or TESTRUN):
            raise(e)
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2



if __name__ == "__main__":
    if DEBUG:
#        sys.argv.append("--logfile=./lectura_telemedicion.log")
#        sys.argv.append("-q")
#        sys.argv.append("-h")
        sys.argv.append("--debug-level=debug")
        pass
    if DEVRUN:
        sys.argv.append("--simular")
    if TESTRUN:
        import doctest
        doctest.testmod()
    if PROFILE:
        import cProfile
        import pstats
        profile_filename = 'hhhhhhh_profile.txt'
        cProfile.run('main()', profile_filename)
        statsfile = open("profile_stats.txt", "wb")
        p = pstats.Stats(profile_filename, stream=statsfile)
        stats = p.strip_dirs().sort_stats('cumulative')
        stats.print_stats()
        statsfile.close()
        sys.exit(0)
    sys.exit(main())