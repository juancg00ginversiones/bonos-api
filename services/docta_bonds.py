import datetime as dt
import httpx
from typing import Dict, Any, List, Optional

from services.docta_auth import get_access_token

DOCTA_BASE = "https://api.doctacapital.com.ar/api/v1"

async def docta_get_cashflow(token: str, symbol: str, nominal_units: float = 100.0, timeout: float = 20.0) -> Optional[Dict[str, Any]]:
    url = f"{DOCTA_BASE}/bonds/analytics/{symbol.upper()}/cashflow/"
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.get(url, params={"nominal_units": nominal_units}, headers={"Authorization": f"Bearer {token}"})
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

async def docta_get_yields_intraday(token: str, symbol: str, timeout: float = 20.0) -> Optional[Dict[str, Any]]:
    url = f"{DOCTA_BASE}/bonds/yields/{symbol.upper()}/intraday"
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.get(url, headers={"Authorization": f"Bearer {token}"})
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

async def docta_get_yields_historical(
    token: str,
    symbol: str,
    from_date: str,
    to_date: str,
    timeout: float = 30.0
) -> Optional[Dict[str, Any]]:
    url = f"{DOCTA_BASE}/bonds/yields/{symbol.upper()}/historical/"
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.get(url, params={"from_date": from_date, "to_date": to_date}, headers={"Authorization": f"Bearer {token}"})
        if r.status_code == 404:
            return None
        if r.status_code == 422:
            # cuando falta o está mal un parámetro
            return {"error": "validation_error", "detail": r.text}
        r.raise_for_status()
        return r.json()

async def docta_post_pricer(
    token: str,
    ticker: str,
    target: str,
    value: float,
    settlement_entry: str,
    operation_date: str,
    timeout: float = 30.0
) -> Optional[Dict[str, Any]]:
    url = f"{DOCTA_BASE}/analytics/bonds/pricer"
    payload = {
        "ticker": ticker.upper(),
        "target": target,
        "value": value,
        "settlement_entry": settlement_entry,
        "operation_date": operation_date,
    }

    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(url, json=payload, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        if r.status_code == 404:
            return None
        if r.status_code == 422:
            return {"error": "validation_error", "detail": r.text, "request": payload}
        r.raise_for_status()
        return r.json()
