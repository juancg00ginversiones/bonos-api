import warnings
warnings.filterwarnings("ignore")

import math
import datetime as dt
from typing import List, Tuple, Optional, Dict, Any

import requests
import pandas as pd
import yfinance as yf

# ============================================================
# CONFIG
# ============================================================

DERIBIT_BASE = "https://www.deribit.com/api/v2"
MESES_HORIZONTE = 6

LISTA_TICKERS = [
    "SPY","QQQ","IWM","DIA",
    "VIX","VXN",
    "AAPL","MSFT","GOOGL","AMZN","META","NVDA","TSLA",
    "BTC","ETH",
    "TLT","IEF"
]


# ============================================================
# HELPERS
# ============================================================

def clean_iv(iv: Any) -> Optional[float]:
    """Limpia IV por seguridad."""
    if iv is None:
        return None
    try:
        v = float(iv)
    except:
        return None

    if v > 3:
        v = v / 100.0
    if v < 0.01 or v > 3.0:
        return None

    return v


def pick_monthly_expiries(expiries, n=MESES_HORIZONTE):
    """Toma 1 expiración por mes."""
    expiries = sorted(expiries)
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
# BTC – DERIBIT
# ============================================================

def fetch_deribit_btc() -> pd.DataFrame:
    url = f"{DERIBIT_BASE}/public/get_book_summary_by_currency"
    r = requests.get(url, params={"currency": "BTC", "kind": "option"})
    r.raise_for_status()
    data = r.json()["result"]

    rows = []
    for opt in data:
        name = opt["instrument_name"]  # BTC-29NOV24-65000-C
        parts = name.split("-")
        expiry = dt.datetime.strptime(parts[1], "%d%b%y").date()
        strike = float(parts[2])
        iv = clean_iv(opt.get("mark_iv"))
        spot = opt.get("underlying_price")

        rows.append({
            "expiry": expiry,
            "strike": strike,
            "iv": iv,
            "spot": spot
        })

    return pd.DataFrame(rows)


def summarize_deribit(df: pd.DataFrame):
    expiries = pick_monthly_expiries(df["expiry"].unique())
    df2 = df[df["expiry"].isin(expiries)].copy()
    spot = df2["spot"].dropna().mean()

    rows = []
    for exp in expiries:
        sub = df2[df2["expiry"] == exp].dropna(subset=["iv"])
        if sub.empty:
            continue

        central_row = sub.loc[sub["iv"].idxmin()]
        rows.append({
            "expiry": exp,
            "spot": spot,
            "central_strike": central_row["strike"]
        })

    return df2, pd.DataFrame(rows)


# ============================================================
# YFINANCE
# ============================================================

def yfin_get_raw_chains(ticker: str):
    yf_ticker = yf.Ticker(ticker)

    try:
        expiries_raw = yf_ticker.options
    except:
        return None, None, [], None

    hist = yf_ticker.history(period="1d")
    if hist.empty:
        return None, None, [], None

    spot = float(hist["Close"].iloc[0])

    expiries = []
    for e in expiries_raw:
        try:
            expiries.append(dt.datetime.strptime(e, "%Y-%m-%d").date())
        except:
            continue

    expiries = pick_monthly_expiries(expiries)

    calls = []
    puts = []

    for exp in expiries:
        try:
            chain = yf_ticker.option_chain(exp.strftime("%Y-%m-%d"))
        except:
            continue

        for _, row in chain.calls.iterrows():
            calls.append({
                "expiry": exp,
                "strike": row["strike"],
                "iv_call": clean_iv(row["impliedVolatility"]),
                "bid_call": row["bid"],
                "ask_call": row["ask"]
            })

        for _, row in chain.puts.iterrows():
            puts.append({
                "expiry": exp,
                "strike": row["strike"],
                "iv_put": clean_iv(row["impliedVolatility"]),
                "bid_put": row["bid"],
                "ask_put": row["ask"]
            })

    return pd.DataFrame(calls), pd.DataFrame(puts), expiries, spot


# ============================================================
# MERGE CALL + PUT
# ============================================================

def fuse_calls_puts(calls_df, puts_df, spot, expiries):
    merged = pd.merge(calls_df, puts_df, on=["expiry", "strike"], how="outer")
    rows = []

    for _, row in merged.iterrows():
        iv_c = clean_iv(row.get("iv_call"))
        iv_p = clean_iv(row.get("iv_put"))

        if iv_c is None and iv_p is None:
            iv_f = None
        elif iv_c is None:
            iv_f = iv_p
        elif iv_p is None:
            iv_f = iv_c
        else:
            # Ponderación por spread
            bid_c, ask_c = row.get("bid_call"), row.get("ask_call")
            bid_p, ask_p = row.get("bid_put"), row.get("ask_put")

            spread_c = (ask_c - bid_c) if (ask_c and bid_c and ask_c > bid_c) else 1
            spread_p = (ask_p - bid_p) if (ask_p and bid_p and ask_p > bid_p) else 1

            w_c = 1 / spread_c
            w_p = 1 / spread_p

            iv_f = (iv_c * w_c + iv_p * w_p) / (w_c + w_p)

        rows.append({
            "expiry": row["expiry"],
            "strike": row["strike"],
            "iv": iv_f,
            "spot": spot
        })

    df = pd.DataFrame(rows)
    return df[df["expiry"].isin(expiries)]


def summarize_fused(df, expiries, spot):
    rows = []
    for exp in expiries:
        sub = df[df["expiry"] == exp].dropna(subset=["iv"])
        if sub.empty:
            continue
        central_row = sub.loc[sub["iv"].idxmin()]
        rows.append({
            "expiry": exp,
            "spot": spot,
            "central_strike": central_row["strike"]
        })
    return pd.DataFrame(rows)


# ============================================================
# FORWARD CURVE
# ============================================================

def build_forward_table(df, summary_df):
    rows = []
    today = dt.date.today()

    for _, row in summary_df.iterrows():
        exp = row["expiry"]
        spot = float(row["spot"])
        central = float(row["central_strike"])

        sub = df[df["expiry"] == exp].dropna(subset=["iv"])
        if sub.empty:
            continue

        dte = (exp - today).days
        if dte <= 0:
            continue

        # atm iv
        sub = sub.copy()
        sub["dist"] = (sub["strike"] - central).abs()
        atm_iv = sub.sort_values("dist").head(10)["iv"].median()

        if pd.isna(atm_iv):
            em = None
        else:
            em = central * atm_iv * math.sqrt(dte / 365)

        rows.append({
            "expiry": exp.strftime("%Y-%m-%d"),
            "central": central,
            "em_up": (central + em) if em else central,
            "em_down": (central - em) if em else central,
            "expected_move": em if em else 0.0
        })

    df_fwd = pd.DataFrame(rows)
    return df_fwd.sort_values("expiry")


# ============================================================
# ANALYSIS
# ============================================================

def analyze_forward(forward_df):
    if forward_df.empty or len(forward_df) < 2:
        return "NEUTRAL", 0.0, "DESCONOCIDA"

    first = forward_df.iloc[0]
    last = forward_df.iloc[-1]

    total_change = (last["central"] / first["central"] - 1) * 100

    if total_change > 3:
        trend = "ALCISTA"
    elif total_change < -3:
        trend = "BAJISTA"
    else:
        trend = "NEUTRAL"

    em_rel = (
        forward_df["expected_move"] / forward_df["central"]
    ).replace([None, float("nan")], 0)

    avg_em = em_rel.mean() * 100

    if avg_em < 1:
        vol = "BAJA"
    elif avg_em < 5:
        vol = "MEDIA"
    else:
        vol = "ALTA"

    return trend, total_change, vol


# ============================================================
# MAIN API FUNCTION
# ============================================================

def analyze_ticker_for_api(ticker: str):
    ticker = ticker.upper()

    if ticker not in LISTA_TICKERS:
        raise ValueError("Ticker no permitido.")

    if ticker == "BTC":
        df = fetch_deribit_btc()
        df_chain, summary = summarize_deribit(df)
    else:
        calls, puts, expiries, spot = yfin_get_raw_chains(ticker)
        if calls is None:
            raise ValueError("No hay datos de opciones disponibles.")
        df_chain = fuse_calls_puts(calls, puts, spot, expiries)
        summary = summarize_fused(df_chain, expiries, spot)

    # Construir tabla forward
    forward_df = build_forward_table(df_chain, summary)

    # Spot consistente
    try:
        spot_out = float(df_chain["spot"].iloc[0])
    except:
        spot_out = float(summary["spot"].iloc[0])

    # Analizar tendencia / volatilidad
    trend, total_change, vol = analyze_forward(forward_df)

    return {
        "ticker": ticker,
        "spot": spot_out,
        "forward_curve": forward_df.to_dict(orient="records"),
        "analysis": {
            "trend": trend,
            "total_change_pct": total_change,
            "volatility": vol
        }
    }
