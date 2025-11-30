from fastapi import FastAPI, HTTPException, Body, Form
from fastapi.middleware.cors import CORSMiddleware

from calculadora import calcular_todo, curva_AL, curva_GD
from curvas_opciones import analyze_ticker_for_api, LISTA_TICKERS
from contenido_pro import agregar_contenido, obtener_contenido

app = FastAPI()

# ... (todo lo demás igual: CORS, bonos, opciones, etc.)

# ============================
# NUEVO MÓDULO PRO: CONTENIDO DEL ASESOR
# ============================

@app.post("/pro/contenido")
def cargar_contenido(
    titulo: str = Form(...),
    texto: str = Form(...),
    imagen_url: str | None = Form(None),
    fecha: str | None = Form(None),
):
    """
    Carga una nueva publicación del asesor.
    Usado por el panel de administración o por Hoppscotch via form-data.
    """
    try:
        return agregar_contenido(titulo, texto, imagen_url, fecha)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/pro/contenido")
def listar_contenido():
    """
    Devuelve todas las publicaciones del asesor.
    Usado en la pantalla PRO consumidores.
    """
    try:
        return obtener_contenido()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
