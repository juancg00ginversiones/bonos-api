import warnings
warnings.filterwarnings("ignore")

import math
import datetime as dt
from typing import List, Any, Tuple, Optional

import requests
import pandas as pd
import yfinance as yf

# ============================================================
# TICKERS PERMITIDOS
# ============================================================

LISTA_TICKERS: List[str] = [
    "SPY", "QQQ", "IWM", "DIA",
    "VIX", "VXN",
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA",
    "BTC",
    "TLT", "IEF",
]

DERIBIT_BASE = "https://www.deribit.com/api/v2"
MESES_HORIZONTE = 6


# ============================================================
# HELPERS
# ============================================================

def clean_iv(iv: Any) -> Optional[float]:
    if iv is None:
        return None
    try:
        val = float(iv)
    except:
        return None
    if val > 3:
        val /= 100.0
    if val < 0.005 or val > 3.0:
        return None
    return val


def pick_monthly_expiries(expiries, n=MESES_HORIZONTE):
    expiries = sorted(set(expiries))
    today = dt.date.today()
    monthly = {}
    for exp in expiries:
        if exp <= today:
            continue
        key = exp.strftime("%Y-%m")
        if key not in monthly:
            monthly[key] = exp
        if len(monthly) == n:
            break
    return list(monthly.values())


# ============================================================
# DERIBIT (BTC)
# ============================================================

def fetch_deribit_btc() -> pd.DataFrame:
    url = f"{DERIBIT_BASE}/public/get_book_summary_by_currency"
    r = requests.get(url, params={"currency": "BTC", "kind": "option"})
    r.raise_for_status()

    rows = []
    for opt in r.json()["result"]:
        try:
            name = opt["instrument_name"]           # BTC-29NOV24-65000-C
            parts = name.split("-")
            expiry = dt.datetime.strptime(parts[1], "%d%b%y").date()
            strike = float(parts[2])
            iv = clean_iv(opt.get("mark_iv"))
            spot = opt.get("underlying_price")
            rows.append({
                "expiry": expiry,
                "strike": strike,
                "iv": iv,
                "spot": spot,
            })
        except:
            continue

    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError("Sin datos BTC en Deribit")
    return df


def summarize_deribit(df: pd.DataFrame):
    expiries = pick_monthly_expiries(df["expiry"].unique())
    df2 = df[df["expiry"].isin(expiries)].copy()

    spot_vals = df2["spot"].dropna()
    spot = spot_vals.mean() if not spot_vals.empty else None

    rows = []
    for exp in expiries:
        sub = df2[df2["expiry"] == exp].dropna(subset=["iv"])
        if sub.empty:
            continue

        # *** ORIGINAL: STRIKE CON MÍNIMA IV ***
        central_row = sub.loc[sub["iv"].idxmin()]

        rows.append({
            "expiry": exp,
            "spot": float(spot),
            "central_strike": float(central_row["strike"])
        })

    return df2, pd.DataFrame(rows)


# ============================================================
# YFINANCE
# ============================================================

def yfin_get_raw_chains(underlying: str):
    ticker = yf.Ticker(underlying)

    try:
        y_expiries = ticker.options
    except:
        return None, None, [], None

    hist = ticker.history(period="1d")
    if hist.empty:
        return None, None, [], None

    spot = float(hist["Close"].iloc[0])

    all_exps = []
    for e in y_expiries:
        try:
            all_exps.append(dt.datetime.strptime(e, "%Y-%m-%d").date())
        except:
            continue

    expiries = pick_monthly_expiries(all_exps)
    if not expiries:
        return None, None, [], spot

    calls_rows, puts_rows = [], []

    for exp in expiries:
        try:
            chain = ticker.option_chain(exp.strftime("%Y-%m-%d"))
        except:
            continue

        # Calls
        for _, row in chain.calls.iterrows():
            calls_rows.append({
                "expiry": exp,
                "strike": row["strike"],
                "iv_call": clean_iv(row["impliedVolatility"]),
                "bid_call": row["bid"],
                "ask_call": row["ask"],
            })

        # Puts
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
# FUSIÓN CALL + PUT
# ============================================================

def fuse_calls_puts(calls_df, puts_df, spot, expiries):
    merged = pd.merge(calls_df, puts_df, on=["expiry", "strike"], how="outer")

    rows = []
    for _, r in merged.iterrows():
        iv_c = clean_iv(r.get("iv_call"))
        iv_p = clean_iv(r.get("iv_put"))

        if iv_c is None and iv_p is None:
            iv_f = None
        elif iv_c is None:
            iv_f = iv_p
        elif iv_p is None:
            iv_f = iv_c
        else:
            spread_c = (r.get("ask_call") or 1) - (r.get("bid_call") or 0)
            spread_p = (r.get("ask_put") or 1) - (r.get("bid_put") or 0)

            spread_c = spread_c if spread_c > 0 else 1
            spread_p = spread_p if spread_p > 0 else 1

            w_c = 1/spread_c
            w_p = 1/spread_p
            iv_f = (iv_c*w_c + iv_p*w_p) / (w_c+w_p)

        rows.append({
            "expiry": r["expiry"],
            "strike": r["strike"],
            "iv": iv_f,
            "spot": spot,
        })

    df = pd.DataFrame(rows)
    return df[df["expiry"].isin(expiries)]


# ============================================================
# SUMMARY YFINANCE
# ============================================================

def summarize_yfin(df, expiries, spot):
    rows = []
    for exp in expiries:
        sub = df[df["expiry"] == exp].dropna(subset=["iv"])
        if sub.empty:
            continue

        central_row = sub.loc[sub["iv"].idxmin()]

        rows.append({
            "expiry": exp,
            "spot": float(spot),
            "central_strike": float(central_row["strike"])
        })

    return pd.DataFrame(rows)


# ============================================================
# ANALYSIS (TENDENCIA, VOLATILIDAD)
# ============================================================

def build_forward_table(df, summary):
    rows = []
    today = dt.date.today()

    for _, r in summary.iterrows():
        exp = r["expiry"]
        spot = float(r["spot"])
        central = float(r["central_strike"])

        sub = df[df["expiry"] == exp].dropna(subset=["iv"])
        if sub.empty:
            continue

        dte = (exp - today).days
        if dte <= 0:
            continue

        sub = sub.copy()
        sub["dist"] = (sub["strike"] - central).abs()
        atm_slice = sub.sort_values("dist").head(10)
        atm_iv = atm_slice["iv"].median()

        if pd.isna(atm_iv):
            em = None
        else:
            em = central * atm_iv * math.sqrt(dte/365)

        rows.append({
            "expiry": exp,
            "central_strike": central,
            "central": central,               # 👈 ALIAS PARA HORIZONS
            "pct_vs_spot": (central/spot - 1)*100,
            "days_to_expiry": dte,
            "atm_iv": atm_iv,
            "expected_move": em,
            "em_up": central + em if em else None,
            "em_down": central - em if em else None,
        })

    return pd.DataFrame(rows).sort_values("expiry")


def analyze_forward(forward_df):
    if forward_df.empty or len(forward_df) < 2:
        return {
            "trend": "NEUTRAL",
            "total_change_pct": 0,
            "volatility": "N/A"
        }

    first = forward_df.iloc[0]["central"]
    last = forward_df.iloc[-1]["central"]
    pct_change = (last / first - 1) * 100

    # Tendencia
    if pct_change > 3:
        trend = "ALCISTA"
    elif pct_change < -3:
        trend = "BAJISTA"
    else:
        trend = "NEUTRAL"

    # Volatilidad (promedio EM relativo)
    em_rel = (forward_df["expected_move"] / forward_df["central"]).dropna()
    if em_rel.empty:
        vol = "N/A"
    else:
        avg_em = em_rel.mean() * 100
        if avg_em < 2:
            vol = "BAJA"
        elif avg_em < 5:
            vol = "MEDIA"
            vol = "ALTA"

    return {
        "trend": trend,
        "total_change_pct": pct_change,
        "volatility": vol
    }


# ============================================================
# FINAL API
# ============================================================

def analyze_ticker_for_api(ticker: str):
    ticker = ticker.upper()

    try:
        if ticker == "BTC":
            df = fetch_deribit_btc()
            df_chain, summary = summarize_deribit(df)
        else:
            calls_df, puts_df, expiries, spot = yfin_get_raw_chains(ticker)
            if calls_df is None or puts_df is None:
                return {"error": "Sin datos para ese ticker"}

            df_chain = fuse_calls_puts(calls_df, puts_df, spot, expiries)
            summary = summarize_yfin(df_chain, expiries, spot)

        forward_df = build_forward_table(df_chain, summary)
        analysis = analyze_forward(forward_df)

        spot_vals = df_chain["spot"].dropna()
        spot_final = float(spot_vals.iloc[0]) if not spot_vals.empty else None

        return {
            "ticker": ticker,
            "spot": spot_final,
            "forward_curve": forward_df.to_dict(orient="records"),
            "analysis": analysis
        }

    except Exception as e:
        return {"error": str(e)}


