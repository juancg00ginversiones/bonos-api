from typing import Dict, Any

def classify_instrument(group: str, symbol: str) -> Dict[str, Any]:
    """
    group: notes | corp | bonds
    """
    s = symbol.upper().strip()

    if group == "notes":
        return {"asset_type": "LECAP/LETRA", "currency": "ARS", "group": "notes"}

    if group == "corp":
        # ONs: si termina en D -> USD (según tu criterio)
        if s.endswith("D"):
            return {"asset_type": "ON", "currency": "USD", "group": "corp"}
        return {"asset_type": "ON", "currency": "ARS", "group": "corp"}

    if group == "bonds":
        # Heurística:
        # - termina en D -> especie USD
        # - contiene C -> CER (ej AE38C)
        # - sino -> ARS “común”
        if "C" in s and not s.endswith("D"):
            return {"asset_type": "BONO_CER", "currency": "ARS", "group": "bonds"}
        if "C" in s and s.endswith("D"):
            return {"asset_type": "BONO_CER", "currency": "USD", "group": "bonds"}

        if s.endswith("D"):
            return {"asset_type": "BONO_USD", "currency": "USD", "group": "bonds"}

        return {"asset_type": "BONO_ARS", "currency": "ARS", "group": "bonds"}

    return {"asset_type": "UNKNOWN", "currency": "UNKNOWN", "group": group}
