import requests

from energy_pipeline.extract.http import build_retry_session

BASE_URL = "https://opendata.aemet.es/opendata/api"


def fetch_daily_climatology(
    start_date: str,
    end_date: str,
    api_key: str,
    session: requests.Session | None = None,
) -> list[dict]:
    if session is not None:
        return _fetch_daily_climatology(start_date, end_date, api_key, session)

    with build_retry_session() as client:
        return _fetch_daily_climatology(start_date, end_date, api_key, client)


def _fetch_daily_climatology(
    start_date: str,
    end_date: str,
    api_key: str,
    client: requests.Session,
) -> list[dict]:
    metadata_url = (
        f"{BASE_URL}/valores/climatologicos/diarios/datos/"
        f"fechaini/{start_date}T00:00:00UTC/"
        f"fechafin/{end_date}T23:59:59UTC/"
        "todasestaciones"
    )

    metadata_response = client.get(
        metadata_url,
        headers={"api_key": api_key},
        timeout=10,
    )
    metadata_response.raise_for_status()
    metadata = metadata_response.json()

    data_url = metadata.get("datos")
    if not data_url:
        raise ValueError("Unexpected AEMET metadata response structure")

    data_response = client.get(data_url, timeout=10)
    data_response.raise_for_status()
    payload = data_response.json()

    if not isinstance(payload, list):
        raise ValueError("Unexpected AEMET data response structure")

    return payload
