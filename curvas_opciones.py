import warnings
warnings.filterwarnings("ignore")

import math
import datetime as dt
from typing import List, Dict, Any, Tuple, Optional

import requests
import pandas as pd
import yfinance as yf

# ============================================================
# TICKERS PERMITIDOS
# ============================================================

LISTA_TICKERS: List[str] = [
    # Índices
    "SPY", "QQQ", "IWM", "DIA",
    # Volatilidad
    "VIX", "VXN",
    # Magnificent 7
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA",
    # Crypto
    "BTC", "ETH",
    # Bonos / tasas
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
    if val < 0.01 or val > 3.0:
        return None
    return val


def pick_monthly_expiries(expiries: List[dt.date], n: int = MESES_HORIZONTE) -> List[dt.date]:
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
# BTC (Deribit)
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


def summarize_deribit(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    expiries = pick_monthly_expiries(df["expiry"].unique(), MESES_HORIZONTE)
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

    expiries = []
    for e in y_expiries:
        try:
            expiries.append(dt.datetime.strptime(e, "%Y-%m-%d").date())
        except:
            continue

    expiries = pick_monthly_expiries(expiries, MESES_HORIZONTE)

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
                "ask_call": row["ask"]
            })

        # Puts
        for _, row in chain.puts.iterrows():
            puts_rows.append({
                "expiry": exp,
                "strike": row["strike"],
                "iv_put": clean_iv(row["impliedVolatility"]),
                "bid_put": row["bid"],
                "ask_put": row["ask"]
            })

    return pd.DataFrame(calls_rows), pd.DataFrame(puts_rows), expiries, spot


# ============================================================
# FUSIÓN CALL+PUT
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
            spread_c = row.get("ask_call", 1) - row.get("bid_call", 0)
            spread_p = row.get("ask_put", 1) - row.get("bid_put", 0)

            spread_c = spread_c if spread_c > 0 else 1
            spread_p = spread_p if spread_p > 0 else 1

            w_c = 1.0 / spread_c
            w_p = 1.0 / spread_p
            iv_f = (iv_c * w_c + iv_p * w_p) / (w_c + w_p)

        rows.append({
            "expiry": row["expiry"],
            "strike": row["strike"],
            "iv": iv_f,
            "spot": spot
        })

    df_fused = pd.DataFrame(rows)
    return df_fused[df_fused["expiry"].isin(expiries)]


# ============================================================
# FORWARD CURVE
# ============================================================

def build_forward_table(df, summary):
    rows = []
    today = dt.date.today()

    for _, row in summary.iterrows():
        exp = row["expiry"]
        spot = float(row["spot"])
        central = float(row["central_strike"])

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
            em = central * atm_iv * math.sqrt(dte / 365)

        rows.append({
            "expiry": exp,
            "central": central,
            "pct_vs_spot": (central / spot - 1) * 100,
            "days_to_expiry": dte,
            "atm_iv": atm_iv,
            "expected_move": em,
            "em_up": central + em if em else None,
            "em_down": central - em if em else None
        })

    df_fwd = pd.DataFrame(rows)
    return df_fwd.sort_values("expiry")


# ============================================================
# FUNCIÓN FINAL
# ============================================================

def analyze_ticker_for_api(ticker: str):
    ticker = ticker.upper()

    if ticker == "BTC":
        df = fetch_deribit_btc()
        df_chain, summary = summarize_deribit(df)
    else:
        calls_df, puts_df, expiries, spot = yfin_get_raw_chains(ticker)

        if calls_df is None:
            raise ValueError(f"No hay datos para {ticker}.")

        df_chain = fuse_calls_puts(calls_df, puts_df, spot, expiries)
        summary = pd.DataFrame([
            {
                "expiry": exp,
                "spot": spot,
                "central_strike": df_chain[df_chain["expiry"] == exp].loc[df_chain["iv"].idxmin()]["strike"]
            }
            for exp in expiries
            if not df_chain[df_chain["expiry"] == exp].dropna(subset=["iv"]).empty
        ])

    forward = build_forward_table(df_chain, summary)

    return {
        "ticker": ticker,
        "spot": float(df_chain["spot"].iloc[0]),
        "forward_curve": forward.to_dict(orient="records")
    }


