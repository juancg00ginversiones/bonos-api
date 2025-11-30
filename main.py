from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# === TUS IMPORTS NORMALES ===
from calculadora import calcular_todo, curva_AL, curva_GD
from curvas_opciones import analyze_ticker_for_api, LISTA_TICKERS
from contenido_pro import agregar_contenido_pro, obtener_contenido_pro


# ================================
# APP
# ================================
app = FastAPI()

# ================================
# CORS (VERSIÓN ESTABLE)
# ================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],   # <- permite todo, incluido OPTIONS
    allow_headers=["*"],
)


# ================================
# MODELO Pydantic PARA POST PRO
# ================================
class ContenidoPRO(BaseModel):
    titulo: str
    texto: str
    imagen_url: str | None = None
    fecha: str | None = None


# ================================
# HOME
# ================================
@app.get("/")
def home():
    return {"status": "API funcionando"}


# ================================
# BONOS — VERSION ORIGINAL
# ================================
@app.get("/bonos")
def bonos():
    return calcular_todo()


@app.get("/curva/al")
def curva_al():
    return curva_AL()


@app.get("/curva/gd")
def curva_gd():
    return curva_GD()


# ================================
# OPCIONES — LISTA TICKERS
# ================================
@app.get("/tickers")
def tickers():
    return {"tickers": LISTA_TICKERS}


# ================================
# CURVAS OPCIONES (YA FUNCIONABA)
# ================================
@app.get("/curvas/opciones")
def curvas_opciones(ticker: str):
    t = ticker.upper().strip()

    if t not in LISTA_TICKERS:
        raise HTTPException(
            status_code=400,
            detail=f"Ticker '{t}' no permitido. Use uno de: {', '.join(LISTA_TICKERS)}"
        )

    try:
        return analyze_ticker_for_api(t)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")


# ================================
# PRO - GET CONTENIDO
# ================================
@app.get("/pro/contenido")
def contenido_pro_listado():
    return obtener_contenido_pro()


# ================================
# PRO - POST CONTENIDO
# ================================
@app.post("/pro/contenido")
def contenido_pro_post(payload: ContenidoPRO):
    try:
        agregar_contenido_pro(
            payload.titulo,
            payload.texto,
            payload.imagen_url,
            payload.fecha
        )
        return {"status": "ok", "mensaje": "Contenido agregado correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")


# ================================
# OPCIONES: MANEJO DE PRE-FLIGHT
# ================================
@app.options("/pro/contenido")
def contenido_pro_options():
    """
    Permite que Chrome acepte el preflight OPTIONS sin tirar 405.
    """
    return JSONResponse(
        content={"status": "ok"},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "*",
        }
    )
