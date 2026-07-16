import argparse
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest REE energy balance raw data.",
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--raw-root",
        default=Path("data/raw"),
        type=Path,
        help="Root directory where raw files will be stored.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    destination = ingest_energy_balance(
        start_date=args.start_date,
        end_date=args.end_date,
        raw_root=args.raw_root,
    )

    print(f"Raw REE energy balance saved to {destination}")


if __name__ == "__main__":
    main()
