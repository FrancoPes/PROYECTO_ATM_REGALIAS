'''
Created on Jun 2, 2020

@author: oirraza
'''

from sqlalchemy import Column, DateTime
from sqlalchemy.dialects.oracle import NUMBER, CHAR, VARCHAR
from sqlalchemy.orm import relationship

#from .medidor import Medidor

from .base import Base


class Cuenca(Base):
    '''
    classdocs
    '''
    __tablename__ = 'trecuenca'
    __table_args__ = {'schema': 'regalias'}

    id = Column('recueid', NUMBER(4, 0, False), primary_key=True)
    codigo = Column('recuecod', CHAR(8), nullable=False)
    nombre = Column('recuenom', CHAR(80), nullable=False)
    observaciones = Column('recueobs', VARCHAR(1000))
    usuario_alta = Column('recueusualt', VARCHAR(128), nullable=False)
    fecha_alta = Column('recuefchalt', DateTime, nullable=False)
    usuario_ult_mod = Column('recueusumod', VARCHAR(128), nullable=False)
    fecha_ult_mod = Column('recuefchmod', DateTime, nullable=False)
    usuario_baja = Column('recueusubaj', VARCHAR(128))
    fecha_baja = Column('recuefchbaj', DateTime)
    recueultlin = Column(NUMBER(4, 0, False))
    recuevighas = Column(DateTime)
    recuevigdes = Column(DateTime)
    #Relaciones
    #medidores = relationship("MedidorFiscal", back_populates="cuenca")



        