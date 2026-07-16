import requests

from energy_pipeline.extract.http import build_retry_session

BASE_URL = "https://apidatos.ree.es/es/datos"


def fetch_energy_balance(
    start_date: str,
    end_date: str,
    session: requests.Session | None = None,
) -> dict:
    if session is not None:
        return _fetch_energy_balance(start_date, end_date, session)

    with build_retry_session() as client:
        return _fetch_energy_balance(start_date, end_date, client)


def _fetch_energy_balance(
    start_date: str,
    end_date: str,
    client: requests.Session,
) -> dict:

    url = f"{BASE_URL}/balance/balance-electrico"

    params = {
        "start_date": f"{start_date}T00:00",
        "end_date": f"{end_date}T23:59",
        "time_trunc": "day",
    }

    response = client.get(url, params=params, timeout=10)

    response.raise_for_status()

    payload = response.json()

    if "data" not in payload or "included" not in payload:
        raise ValueError("Unexpected REE response structure")

    return payload
