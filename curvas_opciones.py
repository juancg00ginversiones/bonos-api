import warnings
warnings.filterwarnings("ignore")

import math
import datetime as dt
from typing import List, Dict, Any, Tuple, Optional

import requests
import pandas as pd
import yfinance as yf

# ============================================================
# CONFIGURACIÓN
# ============================================================

DERIBIT_BASE = "https://www.deribit.com/api/v2"
MESES_HORIZONTE = 6

# Tickers permitidos
LISTA_TICKERS = [
    "SPY", "QQQ", "IWM", "DIA",
    "VIX", "VXN",
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA",
    "BTC", "ETH",
    "TLT", "IEF"
]


# ============================================================
# HELPERS
# ============================================================

def clean_iv(iv):
    if iv is None:
        return None
    try:
        val = float(iv)
    except:
        return None

    if val > 3.0:
        val = val / 100.0
    if val < 0.01 or val > 3.0:
        return None
    return val


def pick_monthly_expiries(expiries, n=6):
    expiries = sorted(set(expiries))
    today = dt.date.today()
    monthly = {}

    for exp in expiries:
        if exp <= today:
            continue
        key = exp.strftime("%Y-%m")
        if key not in monthly:
            monthly[key] = exp
        if len(monthly) >= n:
            break

    return list(monthly.values())


# ============================================================
# DERIBIT (BTC)
# ============================================================

def fetch_deribit_btc():
    url = f"{DERIBIT_BASE}/public/get_book_summary_by_currency"
    r = requests.get(url, params={"currency": "BTC", "kind": "option"})
    r.raise_for_status()
    data = r.json()["result"]

    rows = []
    for opt in data:
        try:
            name = opt["instrument_name"]
            parts = name.split("-")
            expiry = dt.datetime.strptime(parts[1], "%d%b%y").date()
            strike = float(parts[2])
        except:
            continue

        rows.append({
            "expiry": expiry,
            "strike": strike,
            "iv": clean_iv(opt.get("mark_iv")),
            "spot": opt.get("underlying_price")
        })

    return pd.DataFrame(rows)


# ============================================================
# YFINANCE
# ============================================================

def yfin_get_raw_chains(underlying):
    ticker = yf.Ticker(underlying)

    try:
        y_expiries = ticker.options
    except:
        return None, None, [], None

    hist = ticker.history(period="1d")
    if hist.empty:
        return None, None, [], None

    spot = float(hist["Close"].iloc[0])

    all_expiries = []
    for e in y_expiries:
        try:
            d = dt.datetime.strptime(e, "%Y-%m-%d").date()
            all_expiries.append(d)
        except:
            continue

    expiries = pick_monthly_expiries(all_expiries, MESES_HORIZONTE)

    calls_rows = []
    puts_rows = []

    for exp in expiries:
        exp_str = exp.strftime("%Y-%m-%d")
        try:
            chain = ticker.option_chain(exp_str)
        except:
            continue

        for _, row in chain.calls.iterrows():
            calls_rows.append({
                "expiry": exp,
                "strike": row["strike"],
                "iv_call": clean_iv(row["impliedVolatility"]),
                "bid_call": row["bid"],
                "ask_call": row["ask"],
            })

        for _, row in chain.puts.iterrows():
            puts_rows.append({
                "expiry": exp,
                "strike": row["strike"],
                "iv_put": clean_iv(row["impliedVolatility"]),
                "bid_put": row["bid"],
                "ask_put": row["ask"],
            })

    return pd.DataFrame(calls_rows), pd.DataFrame(puts_rows), expiries, spot


# ============================================================
# FUNCION DEBUG PARA LA API
# ============================================================

def analyze_ticker_for_api(ticker: str):
    """
    Versión DEBUG:
    - NO hace curva ni análisis.
    - NO levanta excepciones.
    - Devuelve lo que se pudo obtener de YFinance/Deribit.
    """

    t = ticker.upper().strip()

    result = {
        "ticker": t,
        "status": "ok"
    }

    try:
        # ===================== BTC (Deribit) =====================
        if t == "BTC":
            df = fetch_deribit_btc()

            if df is None or df.empty:
                return {
                    "ticker": t,
                    "status": "error",
                    "source": "deribit",
                    "error": "No se obtuvieron datos"
                }

            expiries = sorted(df["expiry"].unique())

            return {
                "ticker": t,
                "source": "deribit",
                "rows": len(df),
                "unique_expiries": [e.strftime("%Y-%m-%d") for e in expiries],
                "spot_avg": float(df["spot"].dropna().mean()) if not df["spot"].dropna().empty else None
            }

        # ===================== OTROS (YFinance) =====================
        calls_df, puts_df, expiries, spot = yfin_get_raw_chains(t)

        out = {
            "ticker": t,
            "source": "yfinance",
            "spot": float(spot) if spot is not None else None,
        }

        if calls_df is None or puts_df is None:
            out["status"] = "error"
            out["error"] = "YFinance no devolvió datos de opciones"
            return out

        out["n_calls"] = len(calls_df)
        out["n_puts"] = len(puts_df)
        out["expiries"] = [e.strftime("%Y-%m-%d") for e in expiries] if expiries else []

        if not calls_df.empty:
            out["calls_columns"] = list(calls_df.columns)
        if not puts_df.empty:
            out["puts_columns"] = list(puts_df.columns)

        return out

    except Exception as e:
        return {
            "ticker": t,
            "status": "error",
            "error": str(e),
            "error_type": type(e).__name__
        }
