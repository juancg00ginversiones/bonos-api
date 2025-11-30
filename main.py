from fastapi import FastAPI, HTTPException, Query, Form
from fastapi.middleware.cors import CORSMiddleware
from calculadora import calcular_todo, curva_AL, curva_GD
from curvas_opciones import analyze_ticker_for_api, LISTA_TICKERS

# ============================================================
# CONTENEDOR EN MEMORIA PARA CONTENIDO PRO
# ============================================================
CONTENIDO_PRO = []

# ============================================================
# FASTAPI BASE
# ============================================================
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# ENDPOINTS ORIGINALES – FUNCIONAN PERFECTO
# ============================================================

@app.get("/")
def home():
    return {"status": "API funcionando"}

@app.get("/bonos")
def bonos():
    return calcular_todo()

@app.get("/curva/al")
def curva_al():
    return curva_AL()

@app.get("/curva/gd")
def curva_gd():
    return curva_GD()

@app.get("/curvas/opciones/lista")
def lista_opciones():
    return {"tickers": LISTA_TICKERS}

@app.get("/curvas/opciones")
def curvas_opciones(ticker: str = Query(...)):
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
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")

# ============================================================
# CONTENIDO PRO – VERSIÓN SIMPLE (TEXTO + LINK)
# ============================================================

@app.get("/pro/contenido")
def leer_contenido():
    return CONTENIDO_PRO

@app.post("/pro/contenido")
def agregar_contenido(
    texto: str = Form(...),
    link: str = Form(None)
):
    from datetime import date

    nuevo = {
        "texto": texto,
        "link": link,
        "fecha": date.today().isoformat()
    }

    CONTENIDO_PRO.append(nuevo)
    return {"status": "ok", "added": nuevo}

