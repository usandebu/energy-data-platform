from pathlib import Path

import requests

from energy_pipeline.extract.ree import fetch_energy_balance
from energy_pipeline.storage.raw import save_raw_json


def ingest_energy_balance(
    start_date: str,
    end_date: str,
    raw_root: Path = Path("data/raw"),
    session: requests.Session | None = None,
) -> Path:
    payload = fetch_energy_balance(
        start_date=start_date,
        end_date=end_date,
        session=session,
    )

    destination = (
        raw_root
        / "ree"
        / "balance-electrico"
        / f"{start_date}_{end_date}.json"
    )

    save_raw_json(payload, destination)

    return destination
