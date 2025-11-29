from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from curvas_opciones import analyze_ticker_for_api, LISTA_TICKERS

app = FastAPI(title="Curvas Opciones API")

# Permitir acceso desde cualquier frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"message": "API de Curvas de Opciones funcionando"}

@app.get("/tickers")
def tickers():
    """Lista oficial de tickers que acepta la API."""
    return {"tickers": LISTA_TICKERS}

@app.get("/curvas/opciones")
def curvas_opciones(ticker: str):
    """
    Endpoint principal.
    Devuelve:
      - forward curve
      - expected move
      - bandas
      - análisis
    """
    ticker = ticker.upper()

    if ticker not in LISTA_TICKERS:
        raise HTTPException(status_code=400, detail="Ticker no permitido")

    try:
        data = analyze_ticker_for_api(ticker)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

    return data

