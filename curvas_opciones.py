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
    # Índices
    "SPY", "QQQ", "IWM", "DIA",
    # Volatilidad
    "VIX", "VXN",
    # Magnificent 7
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA",
    # Crypto (Deribit)
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
# CRYPTO (BTC & ETH → DERIBIT)
# ============================================================

def fetch_deribit_currency(curr: str) -> pd.DataFrame:
    url = f"{DERIBIT_BASE}/public/get_book_summary_by_currency"
    r = requests.get(url, params={"currency": curr, "kind": "option"})
    r.raise_for_status()
    data = r.json()["result"]

    rows = []
    for opt in data:
        try:
            name = opt["instrument_name"]
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
        except:
            continue

    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError(f"No hay datos de opciones para {curr} (Deribit).")

    return df


def summarize_deribit(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    expiries = pick_monthly_expiries(df["expiry"].unique(), MESES_HORIZONTE)
    df2 = df[df["expiry"].isin(expiries)].copy()

    spot_vals = df2["spot"].dropna()
    spot = spot_vals.mean() if not spot_vals.empty else None

    rows = []
    for exp in expiries:
        sub = df2[df2["expiry"] == exp].dropna(subset=["iv", "strike"])
        if sub.empty or spot is None:
            continue

        # ATM strike (más cercano al spot)
        sub = sub.copy()
        sub["dist"] = (sub["strike"] - spot).abs()
        central_row = sub.loc[sub["dist"].idxmin()]

        rows.append({
            "expiry": exp,
            "spot": float(spot),
            "central_strike": float(central_row["strike"])
        })

    summary = pd.DataFrame(rows)
    if summary.empty:
        raise ValueError("No se pudo construir el resumen para Deribit.")

    return df2, summary


# ============================================================
# YFINANCE
# ============================================================

def yfin_get_raw_chains(underlying: str):
    ticker = yf.Ticker(underlying)

    try:
        y_exp = ticker.options
        if not y_exp:
            return None, None, [], None
    except:
        return None, None, [], None

    hist = ticker.history(period="1d")
    if hist.empty:
        return None, None, [], None

    spot = float(hist["Close"].iloc[0])

    # Filtrar expiraciones
    expiries = []
    for e in y_exp:
        try:
            expiries.append(dt.datetime.strptime(e, "%Y-%m-%d").date())
        except:
            continue

    expiries = pick_monthly_expiries(expiries, MESES_HORIZONTE)
    if not expiries:
        return None, None, [], spot

    calls_rows, puts_rows = [], []

    for exp in expiries:
        try:
            chain = ticker.option_chain(exp.strftime("%Y-%m-%d"))
        except:
            continue

        for _, row in chain.calls.iterrows():
            calls_rows.append({
                "expiry": exp,
                "strike": row["strike"],
                "iv_call": clean_iv(row["impliedVolatility"]),
                "bid_call": row["bid"],
                "ask_call": row["ask"]
            })

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
# FUSIÓN CALLS + PUTS
# ============================================================

def fuse_calls_puts(calls_df, puts_df, spot, expiries):
    if calls_df.empty and puts_df.empty:
        raise ValueError("No hay opciones en yfinance para este ticker.")

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
            spread_c = max(r.get("ask_call", 1) - r.get("bid_call", 0), 0.01)
            spread_p = max(r.get("ask_put", 1) - r.get("bid_put", 0), 0.01)
            w_c = 1 / spread_c
            w_p = 1 / spread_p
            iv_f = (iv_c * w_c + iv_p * w_p) / (w_c + w_p)

        rows.append({
            "expiry": r["expiry"],
            "strike": r["strike"],
            "iv": iv_f,
            "spot": spot
        })

    df = pd.DataFrame(rows)
    df = df[df["expiry"].isin(expiries)]
    if df.empty:
        raise ValueError("No se pudieron fusionar opciones para este ticker.")

    return df


# ============================================================
# FORWARD CURVE
# ============================================================

def build_forward_table(df, summary):
    rows = []
    today = dt.date.today()

    for _, row in summary.iterrows():
        exp = row["expiry"]
        spot = row["spot"]
        central = float(row["central_strike"])

        sub = df[df["expiry"] == exp].dropna(subset=["iv"])
        if sub.empty or spot is None:
            continue

        dte = (exp - today).days
        if dte <= 0:
            continue

        sub = sub.copy()
        sub["dist"] = (sub["strike"] - central).abs()
        atm_iv = sub.sort_values("dist")["iv"].head(10).median()

        em = None if atm_iv is None else central * atm_iv * math.sqrt(dte / 365)

        rows.append({
            "expiry": exp,
            "days_to_expiry": dte,
            "central": central,
            "pct_vs_spot": (central / spot - 1) * 100 if spot else None,
            "atm_iv": atm_iv,
            "expected_move": em,
            "em_up": central + em if em else None,
            "em_down": central - em if em else None
        })

    return pd.DataFrame(rows).sort_values("expiry")


# ============================================================
# FUNCIÓN FINAL
# ============================================================

def analyze_ticker_for_api(ticker: str):
    ticker = ticker.upper()

    try:
        # --- CRYPTO ---
        if ticker in ["BTC", "ETH"]:
            df = fetch_deribit_currency(ticker)
            df_chain, summary = summarize_deribit(df)

        # --- YFINANCE ---
        else:
            calls_df, puts_df, expiries, spot = yfin_get_raw_chains(ticker)
            if calls_df is None or puts_df is None or not expiries:
                return {"error": f"No hay datos para {ticker}."}

            df_chain = fuse_calls_puts(calls_df, puts_df, spot, expiries)

            # Build summary (ATM for each expiry)
            rows = []
            for exp in expiries:
                sub = df_chain[df_chain["expiry"] == exp].dropna(subset=["iv", "strike"])
                if sub.empty:
                    continue

                sub = sub.copy()
                sub["dist"] = (sub["strike"] - spot).abs()
                central_row = sub.loc[sub["dist"].idxmin()]

                rows.append({
                    "expiry": exp,
                    "spot": float(spot),
                    "central_strike": float(central_row["strike"])
                })

            summary = pd.DataFrame(rows)
            if summary.empty:
                return {"error": f"No se pudo generar resumen para {ticker}."}

        # Forward curve
        forward = build_forward_table(df_chain, summary)

        # Final spot robusto
        spot_vals = df_chain["spot"].dropna()
        spot_final = float(spot_vals.iloc[0]) if not spot_vals.empty else None

        return {
            "ticker": ticker,
            "spot": spot_final,
            "forward_curve": forward.to_dict(orient="records")
        }

    except ValueError as e:
        return {"error": str(e)}
    except Exception as e:
        return {"error": f"Error interno procesando {ticker}: {str(e)}"}



