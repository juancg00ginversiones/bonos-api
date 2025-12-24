from docta_client import docta_get, docta_post, get_docta_token
from config import DEFAULT_NOMINAL_UNITS

def get_cashflow(ticker: str):
    token = get_docta_token()
    return docta_get(
        f"/api/v1/bonds/analytics/{ticker}/cashflow/",
        token,
        params={"nominal_units": DEFAULT_NOMINAL_UNITS}
    ).json()

def get_yields_intraday(ticker: str):
    token = get_docta_token()
    return docta_get(
        f"/api/v1/bonds/yields/{ticker}/intraday",
        token
    ).json()

def get_yields_historical(ticker: str, from_date="2020-01-01"):
    token = get_docta_token()
    return docta_get(
        f"/api/v1/bonds/yields/{ticker}/historical",
        token,
        params={"from_date": from_date}
    ).json()

def run_pricer(ticker: str, target="price", value=65):
    token = get_docta_token()
    payload = {
        "ticker": ticker,
        "target": target,
        "value": value
    }
    return docta_post(
        "/api/v1/analytics/bonds/pricer",
        token,
        payload
    ).json()
