import requests

from energy_pipeline.extract.dates import validate_date_range
from energy_pipeline.extract.errors import ExtractionError
from energy_pipeline.extract.http import build_retry_session

BASE_URL = "https://apidatos.ree.es/es/datos"


def fetch_energy_balance(
    start_date: str,
    end_date: str,
    session: requests.Session | None = None,
) -> dict:
    validate_date_range(start_date, end_date)

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

    try:
        response = client.get(url, params=params, timeout=10)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as error:
        raise ExtractionError("Failed to fetch REE energy balance") from error
    except ValueError as error:
        raise ExtractionError("REE response is not valid JSON") from error

    if "data" not in payload or "included" not in payload:
        raise ExtractionError("Unexpected REE response structure")

    return payload
