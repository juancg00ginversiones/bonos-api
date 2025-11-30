from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware

# Módulos existentes
from calculadora import calcular_todo, curva_AL, curva_GD
from curvas_opciones import analyze_ticker_for_api, LISTA_TICKERS

# Nuevo módulo de contenido PRO
from contenido_pro import agregar_contenido, obtener_contenido


app = FastAPI()

# ============================
# HABILITAR CORS
# ============================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================
# HOME
# ============================
@app.get("/")
def home():
    return {"status": "API funcionando"}


# ============================
# BONOS
# ============================
@app.get("/bonos")
def bonos():
    return calcular_todo()


@app.get("/curva/al")
def curva_al():
    return curva_AL()


@app.get("/curva/gd")
def curva_gd():
    return curva_GD()


# ============================
# OPCIONES (PRO / PREMIUM)
# ============================
@app.get("/curvas/opciones/lista")
def lista_opciones():
    return {"tickers": LISTA_TICKERS}


@app.get("/curvas/opciones")
def curvas_opciones(ticker: str):
    t = ticker.upper().strip()

    if t not in LISTA_TICKERS:
        raise HTTPException(
            status_code=400,
            detail=f"Ticker '{t}' no permitido. Use uno de: {', '.join(LISTA_TICKERS)}"
        )

    try:
        result = analyze_ticker_for_api(t)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")


# ============================
# NUEVO MÓDULO PRO: CONTENIDO DEL ASESOR
# ============================

@app.post("/pro/contenido")
def cargar_contenido(
    titulo: str = Body(...),
    texto: str = Body(...),
    imagen_url: str | None = Body(None),
    fecha: str = Body(...)
):
    """
    Carga una nueva publicación del asesor.
    Usado por el panel de administración.
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
