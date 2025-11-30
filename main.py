from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from calculadora import calcular_todo, curva_AL, curva_GD
from curvas_opciones import analyze_ticker_for_api, LISTA_TICKERS

app = FastAPI()

# =====================================================
# CORS ORIGINAL
# =====================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =====================================================
# ENDPOINTS ORIGINALES
# =====================================================
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

# =====================================================
# TICKERS OPCIONES
# =====================================================
@app.get("/tickers")
def lista_opciones():
    return {"tickers": LISTA_TICKERS}

# =====================================================
# CURVAS DE OPCIONES
# =====================================================
@app.get("/curvas/opciones")
def curvas_opciones(ticker: str = Query(..., description="Ticker permitido")):
    t = ticker.upper().strip()

    if t not in LISTA_TICKERS:
        raise HTTPException(
            status_code=400,
            detail=f"Ticker '{t}' no permitido. Use uno de: {', '.join(LISTA_TICKERS)}"
        )

    try:
        return analyze_ticker_for_api(t)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        raise HTTPException(status_code=500, detail="Error interno en el análisis de opciones")

