from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from calculadora import calcular_todo, curva_AL, curva_GD

app = FastAPI()

# ============================
# HABILITAR CORS
# ============================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # permite llamadas desde cualquier página web
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
