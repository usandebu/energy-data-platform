import argparse
import logging
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

from energy_pipeline.extract.dates import parse_iso_date, validate_date_range
from energy_pipeline.extract.errors import ExtractionError
from energy_pipeline.extract.ree import fetch_energy_balance
from energy_pipeline.ingest.config import parse_raw_root
from energy_pipeline.storage.raw import RawObjectKey, save_raw_object

logger = logging.getLogger(__name__)


def ingest_energy_balance(
    start_date: str,
    end_date: str,
    raw_root: Path = Path("data/raw"),
    session: requests.Session | None = None,
) -> Path:
    requested_day = _parse_single_day(start_date, end_date)
    payload = fetch_energy_balance(
        start_date=start_date,
        end_date=end_date,
        session=session,
    )

    return save_raw_object(
        payload=payload,
        key=RawObjectKey(
            source="ree",
            dataset="balance-electrico",
            date=requested_day,
        ),
        raw_root=raw_root,
    )


def _parse_single_day(start_date: str, end_date: str):
    validate_date_range(start_date, end_date)

    if start_date != end_date:
        raise ValueError("Raw REE ingestion expects a single day; use backfill for ranges")

    return parse_iso_date(start_date, "start_date")


def parse_args() -> argparse.Namespace:
    load_dotenv()

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
        default=parse_raw_root(os.getenv("RAW_ROOT", "data/raw")),
        type=parse_raw_root,
        help="Root directory where raw files will be stored.",
    )

    return parser.parse_args()


def run() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    try:
        args = parse_args()
        destination = ingest_energy_balance(
            start_date=args.start_date,
            end_date=args.end_date,
            raw_root=args.raw_root,
        )
    except (ExtractionError, ValueError) as error:
        logger.error("%s", error)
        return 1

    logger.info("Raw REE energy balance saved to %s", destination)
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
