# contenido_pro.py

import json
import os
from datetime import datetime, date
from fastapi import HTTPException

# Archivo local donde se guardan las publicaciones
RUTA_DB = "contenido_pro.json"


def cargar_db():
    """
    Carga la base de datos de contenido PRO.
    Si no existe, devuelve una lista vacía.
    """
    if not os.path.exists(RUTA_DB):
        return []
    try:
        with open(RUTA_DB, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def guardar_db(data):
    """
    Guarda la base de datos completa en formato JSON.
    """
    with open(RUTA_DB, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def agregar_contenido(titulo, texto, imagen_url=None, fecha=None):
    """
    Agrega una nueva publicación al JSON.
    - titulo y texto son obligatorios.
    - imagen_url es opcional.
    - fecha es opcional: si viene vacía, usa la fecha de hoy.
    """
    if not titulo or not texto:
        raise HTTPException(status_code=400, detail="Título y texto son obligatorios.")

    # Manejo de fecha
    if not fecha or not str(fecha).strip():
        fecha_str = date.today().strftime("%Y-%m-%d")
    else:
        fecha_str = str(fecha).strip()
        try:
            datetime.strptime(fecha_str, "%Y-%m-%d")
        except Exception:
            raise HTTPException(status_code=400, detail="Formato de fecha inválido. Use YYYY-MM-DD")

    data = cargar_db()

    nuevo = {
        "titulo": titulo,
        "texto": texto,
        "imagen_url": imagen_url,
        "fecha": fecha_str,
    }

    data.append(nuevo)
    guardar_db(data)

    return {"status": "ok", "mensaje": "Contenido agregado correctamente"}


def obtener_contenido():
    """
    Devuelve todas las publicaciones, ordenadas por fecha descendente.
    """
    data = cargar_db()

    try:
        data = sorted(data, key=lambda x: x.get("fecha", ""), reverse=True)
    except Exception:
        pass

    return data
