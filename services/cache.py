import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

# Cache en memoria: { key: {"value":..., "expires_at":...} }
_CACHE: Dict[str, Dict[str, Any]] = {}

@dataclass(frozen=True)
class CACHE_KEYS:
    DOCTA_CONFIG = "docta_config"

    MARKET_SUMMARY = "market_summary"

    DOCTA_YIELDS = "docta_yields"
    DOCTA_CASHFLOWS = "docta_cashflows"
    DOCTA_HISTORICAL = "docta_historical"
    DOCTA_PRICER = "docta_pricer"

def cache_set(key: str, value: Any, ttl_seconds: int) -> None:
    _CACHE[key] = {"value": value, "expires_at": time.time() + ttl_seconds}

def cache_get(key: str) -> Optional[Any]:
    item = _CACHE.get(key)
    if not item:
        return None
    if time.time() > item["expires_at"]:
        return None
    return item["value"]

def cache_is_fresh(key: str) -> bool:
    item = _CACHE.get(key)
    return bool(item) and time.time() <= item["expires_at"]
