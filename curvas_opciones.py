import warnings
warnings.filterwarnings("ignore")

import math
import datetime as dt
import pandas as pd
import requests
import yfinance as yf

DERIBIT_BASE = "https://www.deribit.com/api/v2"
MESES_HORIZONTE = 6

# =============== UTILIDADES ORIGINALES ===============

def clean_iv(iv):
    if iv is None or pd.isna(iv):
        return None
    if iv > 3:
        iv = iv / 100.0
    if iv < 0.01 or iv > 3.0:
        return None
    return float(iv)

def pick_monthly_expiries(expiries, n=6):
    expiries = sorted(expiries)
    today = dt.date.today()
    monthly = {}
    for exp in expiries:
        if exp < today:
            continue
        key = exp.strftime("%Y-%m")
        if key not in monthly:
            monthly[key] = exp
        if len(monthly) == n:
            break
    return list(monthly.values())

# =============== BTC – DERIBIT (sin tocar) ===============

def fetch_deribit_btc():
    r = requests.get(
        f"{DERIBIT_BASE}/public/get_book_summary_by_currency",
        params={"currency": "BTC", "kind": "option"}
    )
    r.raise_for_status()
    data = r.json()["result"]

    rows = []
    for opt in data:
        name = opt["instrument_name"]
        parts = name.split("-")
        expiry = dt.datetime.strptime(parts[1], "%d%b%y").date()
        strike = float(parts[2])
        rows.append({
            "expiry": expiry,
            "strike": strike,
            "iv": clean_iv(opt.get("mark_iv")),
            "spot": opt.get("underlying_price"),
        })

    return pd.DataFrame(rows)

def summarize_deribit(df):
    expiries = pick_monthly_expiries(df["expiry"].unique(), MESES_HORIZONTE)
    df2 = df[df["expiry"].isin(expiries)].copy()
    spot = df2["spot"].dropna().mean()

    rows = []
    for exp in expiries:
        sub = df2[df2["expiry"] == exp].dropna(subset=["iv"])
        if sub.empty:
            continue
        central = sub.loc[sub["iv"].idxmin()]
        rows.append({
            "expiry": exp,
            "spot": float(spot),
            "central_strike": float(central["strike"]),
        })
    return df2, pd.DataFrame(rows)

# =============== YFINANCE – CÓDIGO ORIGINAL (sin tocar) ===============

def yfin_get_raw_chains(underlying):
    ticker = yf.Ticker(underlying)

    try:
        y_expiries = ticker.options
    except Exception:
        return None, None, [], None

    hist = ticker.history(period="1d")
    if hist.empty:
        return None, None, [], None

    spot = float(hist["Close"].iloc[0])

    all_exp = [dt.datetime.strptime(e, "%Y-%m-%d").date() for e in y_expiries]
    expiries = pick_monthly_expiries(all_exp, MESES_HORIZONTE)

    calls_rows = []
    puts_rows = []

    for exp in expiries:
        try:
            chain = ticker.option_chain(exp.strftime("%Y-%m-%d"))
        except Exception:
            continue

        for _, row in chain.calls.iterrows():
            calls_rows.append({
                "expiry": exp,
                "strike": float(row["strike"]),
                "iv_call": clean_iv(row["impliedVolatility"]),
                "bid_call": row["bid"],
                "ask_call": row["ask"],
            })

        for _, row in chain.puts.iterrows():
            puts_rows.append({
                "expiry": exp,
                "strike": float(row["strike"]),
                "iv_put": clean_iv(row["impliedVolatility"]),
                "bid_put": row["bid"],
                "ask_put": row["ask"],
            })

    return pd.DataFrame(calls_rows), pd.DataFrame(puts_rows), expiries, spot

def fuse_calls_puts(calls_df, puts_df, spot, expiries):
    merged = pd.merge(calls_df, puts_df, on=["expiry", "strike"], how="outer")

    rows = []
    for _, row in merged.iterrows():
        exp = row["expiry"]
        strike = float(row["strike"])
        iv_c = clean_iv(row.get("iv_call"))
        iv_p = clean_iv(row.get("iv_put"))

        if iv_c is None and iv_p is None:
            iv_f = None
        elif iv_c is None:
            iv_f = iv_p
        elif iv_p is None:
            iv_f = iv_c
        else:
            bid_c, ask_c = row.get("bid_call"), row.get("ask_call")
            bid_p, ask_p = row.get("bid_put"), row.get("ask_put")

            sc = ask_c - bid_c if (ask_c and bid_c and ask_c > bid_c) else 1
            sp = ask_p - bid_p if (ask_p and bid_p and ask_p > bid_p) else 1

            w_c = 1.0/sc
            w_p = 1.0/sp

            iv_f = (iv_c*w_c + iv_p*w_p)/(w_c + w_p)

        rows.append({
            "expiry": exp,
            "strike": strike,
            "iv": iv_f,
            "spot": spot,
        })

    df = pd.DataFrame(rows)
    return df[df["expiry"].isin(expiries)]

def summarize_yfin_fused(df, expiries, spot):
    rows = []
    for exp in expiries:
        sub = df[df["expiry"] == exp].dropna(subset=["iv"])
        if sub.empty: continue
        central = sub.loc[sub["iv"].idxmin()]
        rows.append({
            "expiry": exp,
            "spot": float(spot),
            "central_strike": float(central["strike"])
        })
    return pd.DataFrame(rows)

# =============== FORWARD / ANÁLISIS (SIN TOCAR) ===============

def build_forward_table(df, summary):
    rows = []
    today = dt.date.today()
    for _, r in summary.iterrows():
        exp = r["expiry"]
        spot = float(r["spot"])
        central = float(r["central_strike"])
        sub = df[df["expiry"] == exp].dropna(subset=["iv"])
        if sub.empty: continue

        dte = (exp - today).days
        if dte <= 0: continue

        sub2 = sub.copy()
        sub2["dist"] = (sub2["strike"] - central).abs()
        atm_iv = sub2.sort_values("dist").head(10)["iv"].median()

        em = central * atm_iv * math.sqrt(dte/365) if atm_iv else None

        rows.append({
            "expiry": exp,
            "spot": spot,
            "central_strike": central,
            "pct_vs_spot": (central/spot - 1)*100,
            "atm_iv": atm_iv,
            "days_to_expiry": dte,
            "expected_move": em,
            "em_up": central + em if em else None,
            "em_down": central - em if em else None,
        })

    return pd.DataFrame(rows)

def analyze_forward(forward_df):
    if forward_df.empty or len(forward_df) < 2:
        return "No hay suficientes puntos.", "NEUTRAL", 0, "DESCONOCIDA"

    first = forward_df.iloc[0]
    last = forward_df.iloc[-1]

    total = (last["central_strike"]/first["central_strike"] - 1)*100

    trend = "ALCISTA" if total > 3 else "BAJISTA" if total < -3 else "NEUTRAL"

    em_rel = (forward_df["expected_move"]/forward_df["central_strike"]).dropna()
    if em_rel.empty:
        vol = "DESCONOCIDA"
    else:
        p = em_rel.mean()*100
        vol = "BAJA" if p < 2 else "MEDIA" if p < 5 else "ALTA"

    return "OK", trend, total, vol

# =============== API WRAPPER ===============

def analyze_ticker_for_api(ticker: str):
    t = ticker.upper().strip()

    try:
        # BTC (Deribit)
        if t == "BTC":
            raw = fetch_deribit_btc()
            chain, summary = summarize_deribit(raw)
            fwd = build_forward_table(chain, summary)
            txt, tr, ch, vol = analyze_forward(fwd)

            return {
                "ticker": t,
                "source": "deribit",
                "spot": float(summary["spot"].iloc[0]),
                "forward": fwd.to_dict(orient="records"),
                "analysis": {"trend": tr, "change_pct": ch, "vol": vol},
            }

        # YFinance (tu código original)
        calls, puts, expiries, spot = yfin_get_raw_chains(t)
        if calls is None or puts is None:
            raise ValueError("No hay opciones en YFinance.")

        fused = fuse_calls_puts(calls, puts, spot, expiries)
        summary = summarize_yfin_fused(fused, expiries, spot)
        fwd = build_forward_table(fused, summary)
        txt, tr, ch, vol = analyze_forward(fwd)

        return {
            "ticker": t,
            "source": "yfinance",
            "spot": float(spot),
            "forward": fwd.to_dict(orient="records"),
            "analysis": {"trend": tr, "change_pct": ch, "vol": vol},
        }

    except Exception as e:
        return {"error": str(e), "ticker": t}
