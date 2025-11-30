from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# Tus módulos internos
from calculadora import calcular_todo, curva_AL, curva_GD
from curvas_opciones import analyze_ticker_for_api, LISTA_TICKERS
from contenido_pro import agregar_contenido_pro, obtener_contenido_pro

# ============================================
# INICIAR APP
# ============================================
app = FastAPI()

# ============================================
# CONFIGURACIÓN CORS (versión estable)
# Permite:
#   - llamadas desde frontend
#   - archivos locales "file://"
#   - origen "null" (HTML local)
# ============================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "*",        # cualquier origen
        "file://",  # archivos locales
        "null"      # HTML local
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================
# ENDPOINT PRINCIPAL
# ============================================
@app.get("/")
def home():
    return {"status": "API funcionando"}

# ============================================
# ENDPOINTS BONOS
# ============================================
@app.get("/bonos")
def bonos():
    return calcular_todo()

@app.get("/curva/al")
def curva_al():
    return curva_AL()

@app.get("/curva/gd")
def curva_gd():
    return curva_GD()

# ============================================
# CURVAS DE OPCIONES: lista de tickers
# ============================================
@app.get("/tickers")
def tickers():
    return {"tickers": LISTA_TICKERS}

# ============================================
# CURVAS DE OPCIONES: análisis por ticker
# ============================================
@app.get("/curvas/opciones")
def curvas_opciones(ticker: str = Query(..., description="Ticker entre los activos permitidos")):
    t = ticker.upper().strip()

    if t not in LISTA_TICKERS:
        raise HTTPException(
            status_code=400,
            detail=f"Ticker '{t}' no permitido. Use uno de: {', '.join(LISTA_TICKERS)}"
        )

    try:
        result = analyze_ticker_for_api(t)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

# ============================================
# CONTENIDO PRO: OBTENER PUBLICACIONES
# ============================================
@app.get("/pro/contenido")
def contenido_pro_listado():
    try:
        return obtener_contenido_pro()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al leer contenido PRO: {str(e)}")

# ============================================
# CONTENIDO PRO: AGREGAR PUBLICACIÓN
# ============================================
@app.post("/pro/contenido")
def contenido_pro_agregar(
    titulo: str,
    texto: str,
    imagen_url: str = None,
    fecha: str = None
):
    try:
        agregar_contenido_pro(titulo, texto, imagen_url, fecha)
        return {"status": "ok", "mensaje": "Contenido agregado correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al agregar contenido PRO: {str(e)}")
