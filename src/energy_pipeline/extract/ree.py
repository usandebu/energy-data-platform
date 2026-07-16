import requests

BASE_URL = "https://apidatos.ree.es/es/datos"


def fetch_energy_balance(
    start_date: str,
    end_date: str,
    session: requests.Session | None = None,
) -> dict:
    if session is not None:
        return _fetch_energy_balance(start_date, end_date, session)

    with requests.Session() as client:
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


if __name__ == "__main__":
    result = fetch_energy_balance(
        start_date="2024-01-01",
        end_date="2024-01-02",
    )

    print(result.keys())
