'''
Created on May 29, 2020

@author: oirraza
'''

from pathlib import Path
            
DIR_BASE = '/home/dbattezzati/eclipse-workspace/lectura_telemedicion_sqlalchemy/data'
DIR_DESCARGAS = Path(DIR_BASE, 'descargas')

#Usuario que debe ejecutar el sistema 
RUN_USER = 'oirraza'

DB_CONFIG = {'TAXWEB':   {'dialect': 'oracle+cx_oracle', 
                          'host': 'atmovdb.mendoza.gov.ar', 
                          'port': 1521,
                          'sid': 'TAXWEB',
                          'user': '',
                          'pass': '',
                          'encode': 'ISO-8859-15'}, 
             'TAXWEBTS': {'dialect': 'oracle+cx_oracle', 
                          'host': 'atmovdbtst.mendoza.gov.ar', 
                          'port': 1522,
                          'sid': 'TAXWEBTS',
                          'user': 'oirraza',
                          'pass': 'oirraza',
                          'encode': 'ISO-8859-15'},
             'TAXWEBD':  {'dialect': 'oracle+cx_oracle', 
                          'host': 'atmxibility1.mendoza.gov.ar', 
                          'port': 1521,
                          'sid': 'taxwebd',
                          'user': 'regalias',
                          'pass': 'manejoint',
                          'encode': 'ISO-8859-15'}
            }
