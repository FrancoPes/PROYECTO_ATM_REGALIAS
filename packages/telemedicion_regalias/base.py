'''
Created on May 30, 2020

@author: oirraza
'''

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

#Definir variables globales de SQLAlchemy
engine = None
session = None

Base = declarative_base()

 
def initSQLAlchemy(engineURL, **Kwargs):
    global engine
    global session
    
    engine = create_engine(engineURL, **Kwargs) 
#                            connect_args={
#                                "encoding": "ISO-8859-15",
#                                "nencoding": "ISO-8859-15"
#                            })

    Session = sessionmaker(bind=engine, autoflush=False)
    session = Session()


