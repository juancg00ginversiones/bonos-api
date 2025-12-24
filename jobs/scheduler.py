import asyncio
import datetime as dt
from typing import Dict, Any, List

from services.cache import cache_set, cache_get, cache_is_fresh, CACHE_KEYS
from services.data912 import fetch_data912
from services.classify import classify_instrument
from services.docta_auth import get_access_token
from services.docta_bonds import (
    docta_get_cashflow,
    docta_get_yields_intraday,
    docta_get_yields_historical,
    docta_post_pricer
)

# ============================
# FRECUENCIAS (TTL)
# ============================
TTL_MARKET = 120          # 2 min
TTL_YIELDS = 600          # 10 min
TTL_DAILY = 86400         # 24 hs

# ============================
# CONTROL DE CONCURRENCIA
# ============================
DOCTA_MAX_CONCURRENCY = 8  # conservador (y suficiente)
_sema = asyncio.Semaphore(DOCTA_MAX_CONCURRENCY)

_task: asyncio.Task | None = None
_stop_event = asyncio.Event()

async def start_scheduler():
    global _task
    _stop_event.clear()
    _task = asyncio.create_task(_run_loop())

async def stop_scheduler():
    _stop_event.set()
    global _task
    if _task:
        _task.cancel()
        _task = None

async def _run_loop():
    """
    Loop principal:
    - Market (Data912) cada 2m
    - Yields cada 10m
    - Daily pack (cashflow/historical/pricer) cada 24h
    """
    # al iniciar, hacemos warmup inmediato
    await _refresh_market()
    await _refresh_yields()
    await _refresh_daily_pack()

    while not _stop_event.is_set():
        try:
            await asyncio.sleep(5)

            # Market
            if not cache_is_fresh(CACHE_KEYS.MARKET_SUMMARY):
                await _refresh_market()

            # Yields
            if not cache_is_fresh(CACHE_KEYS.DOCTA_YIELDS):
                await _refresh_yields()

            # Daily
            if (not cache_is_fresh(CACHE_KEYS.DOCTA_CASHFLOWS)
                or not cache_is_fresh(CACHE_KEYS.DOCTA_HISTORICAL)
                or not cache_is_fresh(CACHE_KEYS.DOCTA_PRICER)):
                await _refresh_daily_pack()

        except asyncio.CancelledError:
            break
        except Exception as e:
            # si algo falla, no se cae el server
            print("Scheduler loop error:", str(e))

async def _refresh_market():
    try:
        notes = await fetch_data912("arg_notes")
        corp = await fetch_data912("arg_corp")
        bonds = await fetch_data912("arg_bonds")

        def normalize(group: str, rows: List[Dict[str, Any]]):
            out = []
            for r in rows:
                symbol = (r.get("symbol") or "").upper().strip()
                if not symbol:
                    continue

                cls = classify_instrument(group, symbol)
                out.append({
                    "symbol": symbol,
                    "c": r.get("c"),
                    "v": r.get("v"),
                    "q_bid": r.get("q_bid"),
                    "px_bid": r.get("px_bid"),
                    "px_ask": r.get("px_ask"),
                    "q_ask": r.get("q_ask"),
                    "q_op": r.get("q_op"),
                    "pct_change": r.get("pct_change"),
                    **cls
                })
            return out

        payload = {
            "timestamp_utc": dt.datetime.utcnow().isoformat(),
            "notes": normalize("notes", notes),
            "corp": normalize("corp", corp),
            "bonds": normalize("bonds", bonds),
            "counts": {"notes": len(notes), "corp": len(corp), "bonds": len(bonds)}
        }

        cache_set(CACHE_KEYS.MARKET_SUMMARY, payload, TTL_MARKET)
        print("✅ Market refreshed:", payload["counts"])
    except Exception as e:
        print("❌ refresh_market error:", str(e))

def _get_docta_config():
    cfg = cache_get(CACHE_KEYS.DOCTA_CONFIG) or {}
    return cfg.get("client_id"), cfg.get("client_secret"), cfg.get("scope")

async def _get_token():
    client_id, client_secret, scope = _get_docta_config()
    if not client_id or not client_secret:
        raise RuntimeError("Missing Docta credentials in cache.")
    return await get_access_token(client_id, client_secret, scope)

def _extract_all_symbols_from_market() -> List[str]:
    m = cache_get(CACHE_KEYS.MARKET_SUMMARY) or {}
    symbols = set()

    for group in ["notes", "corp", "bonds"]:
        for r in (m.get(group) or []):
            s = r.get("symbol")
            if s:
                symbols.add(s.upper().strip())

    # orden estable
    return sorted(symbols)

async def _refresh_yields():
    try:
        token = await _get_token()
        symbols = _extract_all_symbols_from_market()

        results: Dict[str, Any] = {
            "timestamp_utc": dt.datetime.utcnow().isoformat(),
            "data": {},
            "errors": {}
        }

        async def worker(sym: str):
            async with _sema:
                try:
                    y = await docta_get_yields_intraday(token, sym)
                    if y is None:
                        return
                    results["data"][sym] = y
                except Exception as e:
                    results["errors"][sym] = str(e)

        await asyncio.gather(*(worker(s) for s in symbols))
        cache_set(CACHE_KEYS.DOCTA_YIELDS, results, TTL_YIELDS)
        print(f"✅ Yields refreshed: {len(results['data'])} tickers (errors {len(results['errors'])})")
    except Exception as e:
        print("❌ refresh_yields error:", str(e))

async def _refresh_daily_pack():
    """
    Cashflows + historical + pricer_scenarios
    Todo 1 vez por día.
    """
    try:
        token = await _get_token()
        symbols = _extract_all_symbols_from_market()

        today = dt.date.today()
        # rango histórico fijo (como pediste)
        from_date = "2020-01-01"
        to_date = today.strftime("%Y-%m-%d")

        cashflows: Dict[str, Any] = {"timestamp_utc": dt.datetime.utcnow().isoformat(), "data": {}, "errors": {}}
        historical: Dict[str, Any] = {"timestamp_utc": dt.datetime.utcnow().isoformat(), "from_date": from_date, "to_date": to_date, "data": {}, "errors": {}}
        pricer: Dict[str, Any] = {"timestamp_utc": dt.datetime.utcnow().isoformat(), "data": {}, "errors": {}}

        market = cache_get(CACHE_KEYS.MARKET_SUMMARY) or {}

        # helper para obtener precio “c” si existe
        def find_price(sym: str) -> float | None:
            for group in ["notes", "corp", "bonds"]:
                for r in (market.get(group) or []):
                    if (r.get("symbol") or "").upper() == sym.upper():
                        c = r.get("c")
                        try:
                            return float(c) if c is not None else None
                        except:
                            return None
            return None

        operation_date = today.strftime("%Y-%m-%d")
        settlement_entry = "24hs"

        # Escenarios prefijados (consistentes)
        pct_scenarios = [-0.10, -0.05, -0.02, 0.02, 0.05, 0.10]

        async def cashflow_worker(sym: str):
            async with _sema:
                try:
                    cf = await docta_get_cashflow(token, sym, nominal_units=100.0)
                    if cf is None:
                        return
                    cashflows["data"][sym] = cf
                except Exception as e:
                    cashflows["errors"][sym] = str(e)

        async def hist_worker(sym: str):
            async with _sema:
                try:
                    h = await docta_get_yields_historical(token, sym, from_date=from_date, to_date=to_date)
                    if h is None:
                        return
                    historical["data"][sym] = h
                except Exception as e:
                    historical["errors"][sym] = str(e)

        async def pricer_worker(sym: str):
            async with _sema:
                try:
                    px = find_price(sym)
                    if px is None:
                        return

                    # pricer suele usarse más con tickers “D”, pero no lo forzamos
                    scenarios = []
                    for p in pct_scenarios:
                        val = px * (1.0 + p)
                        res = await docta_post_pricer(
                            token=token,
                            ticker=sym,
                            target="price",
                            value=float(val),
                            settlement_entry=settlement_entry,
                            operation_date=operation_date
                        )
                        scenarios.append({
                            "pct": p,
                            "input_dirty_price": float(val),
                            "result": res
                        })

                    pricer["data"][sym] = {
                        "base_price": px,
                        "operation_date": operation_date,
                        "settlement_entry": settlement_entry,
                        "scenarios": scenarios
                    }
                except Exception as e:
                    pricer["errors"][sym] = str(e)

        # Ejecutamos en tandas para estabilidad
        await asyncio.gather(*(cashflow_worker(s) for s in symbols))
        await asyncio.gather(*(hist_worker(s) for s in symbols))
        await asyncio.gather(*(pricer_worker(s) for s in symbols))

        cache_set(CACHE_KEYS.DOCTA_CASHFLOWS, cashflows, TTL_DAILY)
        cache_set(CACHE_KEYS.DOCTA_HISTORICAL, historical, TTL_DAILY)
        cache_set(CACHE_KEYS.DOCTA_PRICER, pricer, TTL_DAILY)

        print(f"✅ Daily pack refreshed: cashflows {len(cashflows['data'])}, historical {len(historical['data'])}, pricer {len(pricer['data'])}")
    except Exception as e:
        print("❌ refresh_daily_pack error:", str(e))
