from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="IngeCapital Data API",
    version="1.0.0"
)

# ============================
# CORS (abierto para Horizon)
# ============================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================
# ROOT / HEALTHCHECK
# ============================
@app.get("/")
def home():
    return {
        "status": "ok",
        "service": "ingecapital-data-api"
    }

# ============================
# TEST ENDPOINT (CLAVE)
# ============================
@app.get("/test")
def test_endpoint():
    return {
        "ok": True,
        "message": "Endpoint /test funcionando correctamente",
        "service": "ingecapital-data-api"
    }



