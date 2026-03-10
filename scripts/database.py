#!/usr/bin/env python3
import logging
import os
import tempfile
from pathlib import Path

import pandas as pd

from dotenv import load_dotenv
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

logger = logging.getLogger(__name__)

# Configuración de la conexión
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

Base = declarative_base()
SKIP_SQLITE_POPULATION = os.getenv("SKIP_SQLITE_POPULATION", "0").lower() in ("1", "true", "yes")

try:
    from scripts import models  # noqa: F401
    from scripts.models import Ciudad, RegistroClima
except Exception as exc:
    logger.warning("No se pudieron importar los modelos al iniciar la DB: %s", exc)
    Ciudad = RegistroClima = None


def _postgres_url():
    if not all((DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)):
        logger.info("PostgreSQL env vars incompletas, se omite la conexión remota.")
        return None
    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def _create_sqlite_engine():
    base_dir = Path(os.getenv("SQLITE_DB_DIR") or tempfile.gettempdir()).expanduser()
    base_dir.mkdir(parents=True, exist_ok=True)
    fallback_db = base_dir / "weatherstack.db"
    engine = create_engine(f"sqlite:///{fallback_db}", echo=False, future=True)
    logger.info("Base SQLite local lista en %s", fallback_db)
    return engine, f"sqlite:///{fallback_db}"


def _init_engine():
    postgres_url = _postgres_url()
    if postgres_url:
        try:
            engine = create_engine(postgres_url, echo=False, future=True)
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            logger.info("Conectado a PostgreSQL en %s", postgres_url)
            return engine, postgres_url, False
        except OperationalError as exc:
            logger.warning(
                "No se pudo conectar a PostgreSQL (%s), se usará SQLite local: %s",
                postgres_url,
                exc,
            )
    engine, sqlite_url = _create_sqlite_engine()
    Base.metadata.create_all(bind=engine)
    return engine, sqlite_url, True


engine, DATABASE_URL, USING_SQLITE_FALLBACK = _init_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
metadata = MetaData()
try:
    metadata.reflect(bind=engine)
except Exception as exc:
    logger.warning("No se pudo reflejar la metadata del motor: %s", exc)


def _get_ciudades_from_csv():
    data_file = Path(__file__).resolve().parents[1] / "data" / "clima.csv"
    if not data_file.exists():
        return []
    try:
        import pandas as pd
        df = pd.read_csv(data_file, usecols=["ciudad"])
        return [row["ciudad"] for _, row in df.dropna(subset=["ciudad"]).iterrows()]
    except Exception:
        return []


def _populate_local_db():
    if SKIP_SQLITE_POPULATION:
        logger.info("SKIP_SQLITE_POPULATION set, omitiendo creación/población de la base local.")
        return

    data_file = Path(__file__).resolve().parents[1] / "data" / "clima.csv"
    if not data_file.exists():
        logger.info("No hay data/clima.csv para poblar la base local.")
        return

    df = pd.read_csv(data_file, parse_dates=["fecha_extraccion"])
    if df.empty:
        logger.info("El archivo data/clima.csv está vacío.")
        return

    Base.metadata.create_all(bind=engine)
    session = SessionLocal()
    try:
        if session.query(Ciudad).first():
            logger.info("La base local ya contiene datos; se omite la importación.")
            return

        def _to_float(value):
            if value is None or pd.isna(value):
                return None
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        def _to_int(value):
            if value is None or pd.isna(value):
                return 0
            try:
                return int(value)
            except (TypeError, ValueError):
                return 0

        city_cache = {}
        for row in df.to_dict("records"):
            nombre = (row.get("ciudad") or "").strip()
            if not nombre:
                continue

            ciudad_obj = city_cache.get(nombre)
            if ciudad_obj is None:
                ciudad_obj = Ciudad(
                    nombre=nombre,
                    pais=row.get("pais") or "Desconocido",
                    latitud=_to_float(row.get("latitud")) or 0.0,
                    longitud=_to_float(row.get("longitud")) or 0.0,
                )
                session.add(ciudad_obj)
                session.flush()
                city_cache[nombre] = ciudad_obj

            registro = RegistroClima(
                ciudad_id=ciudad_obj.id,
                temperatura=_to_float(row.get("temperatura")) or 0.0,
                sensacion_termica=_to_float(row.get("sensacion_termica")),
                humedad=_to_float(row.get("humedad")) or 0.0,
                velocidad_viento=_to_float(row.get("velocidad_viento")) or 0.0,
                descripcion=row.get("descripcion") or "",
                codigo_tiempo=_to_int(row.get("codigo_tiempo")),
                fecha_extraccion=row.get("fecha_extraccion"),
            )
            session.add(registro)

        session.commit()
        logger.info("Se importaron %d registros desde data/clima.csv.", len(df))
    except Exception as exc:
        session.rollback()
        logger.error("Error al poblar la base local: %s", exc)
    finally:
        session.close()


def _ensure_minimum_cities_exist():
    if Ciudad is None:
        return
    session = SessionLocal()
    try:
        if session.query(Ciudad).count() == 0:
            nombres = [c.strip() for c in os.getenv("CIUDADES", "").split(",") if c.strip()]
            if not nombres:
                nombres = _get_ciudades_from_csv() or ["Bogota", "Cali", "Medellin", "Barranquilla", "Cartagena"]
            for nombre in nombres:
                ciudad = Ciudad(nombre=nombre, pais="Colombia", latitud=0.0, longitud=0.0)
                session.add(ciudad)
            session.commit()
            logger.info("Se insertaron ciudades base desde CIUDADES para evitar el fallo inicial.")
    except Exception as exc:
        session.rollback()
        logger.warning("No se pudieron crear ciudades base: %s", exc)
    finally:
        session.close()


if USING_SQLITE_FALLBACK:
    _populate_local_db()
    _ensure_minimum_cities_exist()


def get_db():
    """Obtiene una sesión de base de datos"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def test_connection():
    """Prueba la conexión a la base de datos"""
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logger.info("✅ Conexión a la base de datos exitosa (%s)", DATABASE_URL)
        return True
    except Exception as e:
        logger.error("❌ Error conectando a la base de datos: %s", str(e))
        return False


def create_all_tables():
    """Crea todas las tablas definidas en los modelos"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Tablas creadas exitosamente")
    except Exception as e:
        logger.error("❌ Error creando tablas: %s", str(e))
