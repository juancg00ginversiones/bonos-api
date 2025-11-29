import warnings
warnings.filterwarnings("ignore")

import math
import datetime as dt
from typing import List, Dict, Any, Tuple
import requests
import pandas as pd
import yfinance as yf

# ============================================================
# CONFIGURACIÓN
# ============================================================

DERIBIT_BASE = "https://www.deribit.com/api/v2"
MESES_HORIZONTE = 6

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
        v = float(iv)
    except:
        return None
    if v > 3:
        v /= 100
    if v < 0.01 or v > 3:
        return None
    return v


def pick_monthly_expiries(expiries, n=6):
    expiries = sorted(set(expiries))
    today = dt.date.today()
    monthly = {}
    for e in expiries:
        if e <= today:
            continue
        key = e.strftime("%Y-%m")
        if key not in monthly:
            monthly[key] = e
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


def summarize_deribit(df):
    expiries = pick_monthly_expiries(df["expiry"].unique(), MESES_HORIZONTE)
    df2 = df[df["expiry"].isin(expiries)]
    spot = df2["spot"].dropna().mean()

    rows = []
    for exp in expiries:
        sub = df2[df2["expiry"] == exp].dropna(subset=["iv"])
        if sub.empty:
            continue
        c = sub.loc[sub["iv"].idxmin()]
        rows.append({
            "expiry": exp,
            "spot": float(spot),
            "central_strike": float(c["strike"]),
        })

    return df2, pd.DataFrame(rows)


# ============================================================
# YFINANCE
# ============================================================

def yfin_get_raw_chains(t):
    tk = yf.Ticker(t)
    try:
        y_exp = tk.options
    except:
        return None, None, [], None

    hist = tk.history(period="1d")
    if hist.empty:
        return None, None, [], None
    spot = float(hist["Close"].iloc[0])

    expiries = []
    for e in y_exp:
        try:
            expiries.append(dt.datetime.strptime(e, "%Y-%m-%d").date())
        except:
            continue

    expiries = pick_monthly_expiries(expiries, MESES_HORIZONTE)

    calls = []
    puts = []

    for exp in expiries:
        try:
            chain = tk.option_chain(exp.strftime("%Y-%m-%d"))
        except:
            continue

        for _, r in chain.calls.iterrows():
            calls.append({
                "expiry": exp,
                "strike": float(r["strike"]),
                "iv_call": clean_iv(r["impliedVolatility"]),
                "bid_call": r["bid"],
                "ask_call": r["ask"],
            })
        for _, r in chain.puts.iterrows():
            puts.append({
                "expiry": exp,
                "strike": float(r["strike"]),
                "iv_put": clean_iv(r["impliedVolatility"]),
                "bid_put": r["bid"],
                "ask_put": r["ask"],
            })

    return pd.DataFrame(calls), pd.DataFrame(puts), expiries, spot


# ============================================================
# FUSIÓN
# ============================================================

def fuse_calls_puts(ca, pu, spot, expiries):
    merged = pd.merge(ca, pu, on=["expiry", "strike"], how="outer")
    rows = []

    for _, r in merged.iterrows():
        ivc = clean_iv(r.get("iv_call"))
        ivp = clean_iv(r.get("iv_put"))

        if ivc is None and ivp is None:
            iv = None
        elif ivc is None:
            iv = ivp
        elif ivp is None:
            iv = ivc
        else:
            bc, ac = r.get("bid_call"), r.get("ask_call")
            bp, ap = r.get("bid_put"), r.get("ask_put")
            sc = ac - bc if (ac and bc and ac > bc) else 1
            sp = ap - bp if (ap and bp and ap > bp) else 1
            w1, w2 = 1 / sc, 1 / sp
            iv = (ivc * w1 + ivp * w2) / (w1 + w2)

        rows.append({"expiry": r["expiry"], "strike": r["strike"], "iv": iv, "spot": spot})

    df = pd.DataFrame(rows)
    return df[df["expiry"].isin(expiries)]


# ============================================================
# SUMMARY YFIN (CORRECTO)
# ============================================================

def summarize_yfin_fused(df, expiries, spot):
    rows = []
    for exp in expiries:
        sub = df[df["expiry"] == exp].dropna(subset=["iv"])
        if sub.empty:
            continue
        c = sub.loc[sub["iv"].idxmin()]
        rows.append({
            "expiry": exp,
            "spot": spot,
            "central_strike": float(c["strike"]),
        })

    return pd.DataFrame(rows)


# ============================================================
# FORWARD CURVE
# ============================================================

def build_forward(df, summary):
    today = dt.date.today()
    rows = []

    for _, s in summary.iterrows():
        exp = s["expiry"]
        spot = float(s["spot"])
        central = float(s["central_strike"])

        sub = df[df["expiry"] == exp].dropna(subset=["iv"])
        if sub.empty:
            continue

        sub = sub.copy()
        sub["dist"] = (sub["strike"] - central).abs()
        atm_iv = sub.sort_values("dist").head(10)["iv"].median()

        days = (exp - today).days
        if days <= 0:
            continue

        em = central * atm_iv * math.sqrt(days / 365) if atm_iv else None
        em_up = central + em if em else None
        em_down = central - em if em else None

        rows.append({
            "expiry": exp.strftime("%Y-%m-%d"),
            "central": central,
            "pct_vs_spot": (central / spot - 1) * 100,
            "atm_iv": atm_iv,
            "days_to_expiry": days,
            "expected_move": em,
            "em_up": em_up,
            "em_down": em_down
        })

    return rows


# ============================================================
# ANÁLISIS
# ============================================================

def analyze_forward(forward):
    if len(forward) < 2:
        return {
            "trend": "NEUTRAL",
            "total_change_pct": 0.0,
            "volatility": "DESCONOCIDA",
        }

    first = forward[0]["central"]
    last = forward[-1]["central"]
    total_change = (last / first - 1) * 100

    if total_change > 3:
        trend = "ALCISTA"
    elif total_change < -3:
        trend = "BAJISTA"
        trend = "BAJISTA"
    else:
        trend = "NEUTRAL"

    em_rel = []
    for r in forward:
        if r["expected_move"] and r["central"]:
            em_rel.append(r["expected_move"] / r["central"])

    if not em_rel:
        vol = "DESCONOCIDA"
    else:
        avg = sum(em_rel) / len(em_rel)
        if avg < 0.02:
            vol = "BAJA"
        elif avg < 0.05:
            vol = "MEDIA"
        else:
            vol = "ALTA"

    return {
        "trend": trend,
        "total_change_pct": total_change,
        "volatility": vol
    }


# ============================================================
# API FINAL
# ============================================================

def analyze_ticker_for_api(t):
    t = t.upper()

    # ========== BTC ==========
    if t == "BTC":
        df = fetch_deribit_btc()
        chain, summary = summarize_deribit(df)

    # ========== YFINANCE ==========
    else:
        calls, puts, expiries, spot = yfin_get_raw_chains(t)

        if calls is None:
            return {"error": f"No se pudieron obtener opciones de {t}"}

        chain = fuse_calls_puts(calls, puts, spot, expiries)
        summary = summarize_yfin_fused(chain, expiries, spot)

    # Forward curve
    forward = build_forward(chain, summary)

    # Análisis
    analysis = analyze_forward(forward)

    return {
        "ticker": t,
        "spot": summary.iloc[0]["spot"] if not summary.empty else None,
        "forward": forward,
        "analysis": analysis
    }
