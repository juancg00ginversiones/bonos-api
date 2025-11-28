import pandas as pd
import numpy as np
import requests
from datetime import date, datetime
import math


# ============================================================
# SANITIZADOR PARA JSON (evita NaN, inf, None)
# ============================================================
def safe(x):
    """Convierte NaN, inf, -inf en None para que JSON no falle."""
    if x is None:
        return None
    if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
        return None
    return x


# ============================================================
# 1) CARGAR EXCEL Y ARMAR FLUJOS POR TICKER
# ============================================================
def cargar_excel():
    RUTA_EXCEL = "BONOS Y ONS data.xlsx"

    df = pd.read_excel(RUTA_EXCEL)
    df["payment_date"] = pd.to_datetime(df["payment_date"])

    flujos = {}

    for _, row in df.iterrows():
        ticker = str(row["ticker"]).strip()

        if ticker not in flujos:
            flujos[ticker] = []

        flujos[ticker].append({
            "fecha": row["payment_date"].date(),
            "flujo": float(row["cash_flow"])
        })

    # Ordenar flujos por fecha
    for t in flujos:
        flujos[t] = sorted(flujos[t], key=lambda x: x["fecha"])

    return df, flujos


# ============================================================
# 2) CONSULTAR API 912
# ============================================================
def cargar_api():
    URL_BONOS = "https://data912.com/live/arg_bonds"
    URL_ONS   = "https://data912.com/live/arg_corp"

    precios = {}

    def leer(url):
        try:
            data = requests.get(url).json()
            for x in data:
                sym = x.get("symbol")
                price = x.get("c")
                pct = x.get("pct_change") if x.get("pct_change") is not None else x.get("dp")

                if sym and price is not None:
                    precios[sym] = {
                        "precio": float(price),
                        "pct_change": float(pct) if pct is not None else 0.0
                    }
        except:
            pass

    leer(URL_BONOS)
    leer(URL_ONS)

    return precios


# ============================================================
# 3) XIRR / NPV / DURATION
# ============================================================
def xnpv(rate, cashflows, dates):
    if rate <= -1:
        return np.nan

    t0 = dates[0]
    total = 0

    for cf, d in zip(cashflows, dates):
        t = (d - t0).days / 365
        total += cf / ((1 + rate) ** t)

    return total


def xirr(cfs, dates, lo=-0.99, hi=5.0, tol=1e-8):
    def f(r): return xnpv(r, cfs, dates)

    f_lo = f(lo)
    f_hi = f(hi)

    if f_lo * f_hi > 0:
        return None

    for _ in range(200):
        mid = (lo + hi) / 2
        fm = f(mid)

        if abs(fm) < tol:
            return mid

        if f_lo * fm > 0:
            lo = mid
            f_lo = fm
        else:
            hi = mid
            f_hi = fm

    return mid


def duration_macaulay(r, cf_fut, dates_fut, T0):
    if r is None:
        return None

    pv_total = 0
    acc = 0

    for cf, d in zip(cf_fut, dates_fut):
        t = (d - T0).days / 365
        pv = cf / ((1 + r)**t)
        pv_total += pv
        acc += t * pv

    if pv_total == 0:
        return None

    return acc / pv_total


def duration_modificada(d_mac, r):
    if d_mac is None or r is None:
        return None
    return d_mac / (1 + r)


# ============================================================
# 4) FUNCIÓN PRINCIPAL — CALCULAR TODOS LOS BONOS
# ============================================================
def calcular_todo():
    df, flujos = cargar_excel()
    precios = cargar_api()

    T0 = date.today()
    RESULTADOS = []

    for ticker, lista in flujos.items():

        if ticker not in precios:
            continue

        precio = precios[ticker]["precio"]
        pct = precios[ticker]["pct_change"]

        # Flujos FUTUROS (solo desde HOY hacia adelante)
        fut = [c for c in lista if c["fecha"] > T0]
        if not fut:
            continue

        # Estructura XIRR
        cfs = [-precio] + [c["flujo"] for c in fut]
        dates = [T0] + [c["fecha"] for c in fut]

        # Cálculos
        tir = xirr(cfs, dates)
        d_mac = duration_macaulay(tir, [c["flujo"] for c in fut],
                                         [c["fecha"] for c in fut], T0)
        d_mod = duration_modificada(d_mac, tir)

        valor_nominal = fut[-1]["flujo"] if fut else None
        paridad = (precio / valor_nominal * 100) if valor_nominal else None

        tipo = df[df["ticker"] == ticker]["type"].iloc[0]

        # Clasificación curva
        if ticker.startswith("AL"):
            curva = "AL"
        elif ticker.startswith("GD"):
            curva = "GD"
        else:
            curva = None

        RESULTADOS.append({
            "ticker": safe(ticker),
            "type": safe(tipo),
            "curva": safe(curva),
            "precio": safe(precio),
            "pct_change": safe(pct),
            "tir_pct": safe(tir * 100 if tir is not None else None),
            "paridad": safe(paridad),
            "duration_mod": safe(d_mod)
        })

    return RESULTADOS


# ============================================================
# 5) CURVAS AL / GD
# ============================================================
def curva_AL():
    data = calcular_todo()
    return [x for x in data if x["curva"] == "AL"]


def curva_GD():
    data = calcular_todo()
    return [x for x in data if x["curva"] == "GD"]
