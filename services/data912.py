import httpx
from typing import Dict, Any, List

DATA912_BASE = "https://data912.com/live"

async def fetch_data912(endpoint: str, timeout: float = 20.0) -> List[Dict[str, Any]]:
    url = f"{DATA912_BASE}/{endpoint}"
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.get(url)
        r.raise_for_status()
        data = r.json()
        # Data912 suele devolver lista
        if isinstance(data, list):
            return data
        # fallback
        return data.get("data", []) if isinstance(data, dict) else []
