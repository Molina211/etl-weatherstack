from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime

Base = declarative_base()

class Ciudad(Base):
    __tablename__ = "ciudades"

    id = Column(Integer, primary_key=True)
    nombre = Column(String, nullable=False)
    pais = Column(String)
    latitud = Column(Float)
    longitud = Column(Float)

    registros = relationship("RegistroClima", back_populates="ciudad")


class RegistroClima(Base):
    __tablename__ = "registros_clima"

    id = Column(Integer, primary_key=True)
    ciudad_id = Column(Integer, ForeignKey("ciudades.id"))
    temperatura = Column(Float)
    sensacion_termica = Column(Float)
    humedad = Column(Float)
    velocidad_viento = Column(Float)
    descripcion = Column(String)
    codigo_tiempo = Column(Integer)
    fecha_extraccion = Column(DateTime, default=datetime.now)

    ciudad = relationship("Ciudad", back_populates="registros")


class MetricasETL(Base):
    __tablename__ = "metricas_etl"

    id = Column(Integer, primary_key=True)
    fecha_ejecucion = Column(DateTime, default=datetime.now)
    registros_extraidos = Column(Integer)
    registros_guardados = Column(Integer)
    registros_fallidos = Column(Integer)
    tiempo_ejecucion_segundos = Column(Float)
    estado = Column(String)