import sys
sys.path.insert(0, '.')

from scripts.extractor import WeatherstackExtractor
from scripts.database import SessionLocal
from scripts.models import Ciudad, RegistroClima
from datetime import datetime

def guardar_en_bd(datos):
    db = SessionLocal()
    try:
        for dato in datos:
            # Buscar o crear ciudad
            ciudad = db.query(Ciudad).filter_by(nombre=dato['ciudad']).first()
            if not ciudad:
                ciudad = Ciudad(
                    nombre=dato['ciudad'],
                    pais=dato['pais'],
                    latitud=float(dato['latitud']) if dato['latitud'] else None,
                    longitud=float(dato['longitud']) if dato['longitud'] else None
                )
                db.add(ciudad)
                db.flush()

            # Crear registro de clima
            registro = RegistroClima(
                ciudad_id=ciudad.id,
                temperatura=dato['temperatura'],
                sensacion_termica=dato['sensacion_termica'],
                humedad=dato['humedad'],
                velocidad_viento=dato['velocidad_viento'],
                descripcion=dato['descripcion'],
                codigo_tiempo=dato['codigo_tiempo'],
                fecha_extraccion=datetime.now()
            )
            db.add(registro)

        db.commit()
        print(f"✅ {len(datos)} registros guardados en BD")
    except Exception as e:
        db.rollback()
        print(f"❌ Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    extractor = WeatherstackExtractor()
    datos = extractor.ejecutar_extraccion()
    guardar_en_bd(datos)