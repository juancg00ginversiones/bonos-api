from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from jobs.scheduler import start_scheduler, stop_scheduler
from services.cache import cache_get, cache_set, CACHE_KEYS

app = FastAPI(title="INGECAPITAL Data API", version="1.0.0")

# ============================
# CORS (permitimos todo)
# ============================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================
# CONFIG DOCTA (PEGAR ACA)
# ============================
# ⚠️ Reemplazá por tus credenciales reales
DOCTA_CLIENT_ID = "docta-api-5431ff99-juancg00ginversiones"
DOCTA_CLIENT_SECRET = "e1ggoBcbMkl1_j698Yuiu2LIx45uH8dPbaK_R5C-Ju0"
DOCTA_SCOPE = "bonds:read cedears:read stocks:read"

# Le pasamos config al scheduler a través del cache (simple y directo)
cache_set(CACHE_KEYS.DOCTA_CONFIG, {
    "client_id": DOCTA_CLIENT_ID,
    "client_secret": DOCTA_CLIENT_SECRET,
    "scope": DOCTA_SCOPE
}, ttl_seconds=365 * 24 * 3600)

@app.on_event("startup")
async def _startup():
    await start_scheduler()

@app.on_event("shutdown")
async def _shutdown():
    await stop_scheduler()

@app.get("/")
def home():
    return {"status": "ok", "service": "ingecapital-data-api"}
@app.get("/test")
def test_endpoint():
    return {
        "ok": True,
        "message": "Endpoint /test funcionando correctamente",
        "service": "ingecapital-data-api"
    }

# ============================
# ENDPOINTS PARA HORIZONS
# ============================

@app.get("/v1/market/summary")
def market_summary():
    """
    Devuelve instrumentos de Data912 (notes/corp/bonds) + clasificación.
    Cache: 2 min
    """
    return cache_get(CACHE_KEYS.MARKET_SUMMARY) or {"status": "warming_up"}

@app.get("/v1/bonds/yields")
def bonds_yields():
    """
    Devuelve yields intraday (último dato) por ticker, para TODOS los tickers que detectamos.
    Cache: 10 min
    """
    return cache_get(CACHE_KEYS.DOCTA_YIELDS) or {"status": "warming_up"}

@app.get("/v1/bonds/cashflows")
def bonds_cashflows():
    """
    Devuelve cashflows por ticker (para calendario de pagos).
    Cache: 24 hs
    """
    return cache_get(CACHE_KEYS.DOCTA_CASHFLOWS) or {"status": "warming_up"}

@app.get("/v1/bonds/historical")
def bonds_historical():
    """
    Devuelve histórico (rango fijo) por ticker.
    Cache: 24 hs
    """
    return cache_get(CACHE_KEYS.DOCTA_HISTORICAL) or {"status": "warming_up"}

@app.get("/v1/bonds/pricer_scenarios")
def bonds_pricer_scenarios():
    """
    Devuelve escenarios prefijados (variaciones de precio) por ticker.
    Cache: 24 hs
    """
    return cache_get(CACHE_KEYS.DOCTA_PRICER) or {"status": "warming_up"}

@app.get("/v1/all")
def all_in_one():
    """
    “Todo en uno” por si Horizons prefiere 1 sola llamada.
    """
    return {
        "market": cache_get(CACHE_KEYS.MARKET_SUMMARY) or {"status": "warming_up"},
        "yields": cache_get(CACHE_KEYS.DOCTA_YIELDS) or {"status": "warming_up"},
        "cashflows": cache_get(CACHE_KEYS.DOCTA_CASHFLOWS) or {"status": "warming_up"},
        "historical": cache_get(CACHE_KEYS.DOCTA_HISTORICAL) or {"status": "warming_up"},
        "pricer_scenarios": cache_get(CACHE_KEYS.DOCTA_PRICER) or {"status": "warming_up"},
    }



