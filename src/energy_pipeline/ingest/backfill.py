import argparse
import logging
import os
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

from energy_pipeline.extract.dates import parse_iso_date, validate_date_range
from energy_pipeline.extract.errors import ExtractionError
from energy_pipeline.ingest.aemet import ingest_daily_climatology
from energy_pipeline.ingest.config import (
    parse_raw_bucket,
    parse_raw_root,
    parse_raw_storage_backend,
)
from energy_pipeline.ingest.ree import ingest_energy_balance
from energy_pipeline.storage.base import RawStorage
from energy_pipeline.storage.factory import build_raw_storage
from energy_pipeline.storage.local import LocalRawStorage
from energy_pipeline.storage.raw import RawObjectKey, raw_data_path

logger = logging.getLogger(__name__)

Source = str
IngestFunction = Callable[[str, str], str]


@dataclass(frozen=True)
class BackfillResult:
    source: Source
    downloaded: int = 0
    skipped: int = 0
    failed: int = 0


def iter_days(start_date: str, end_date: str) -> Iterator[str]:
    validate_date_range(start_date, end_date)

    current = parse_iso_date(start_date, "start_date")
    final = parse_iso_date(end_date, "end_date")

    while current <= final:
        yield current.isoformat()
        current += timedelta(days=1)


def raw_destination(source: Source, day: str, raw_root: Path) -> Path:
    return raw_data_path(raw_root, raw_object_key(source, day))


def raw_object_key(source: Source, day: str) -> RawObjectKey:
    requested_day = parse_iso_date(day, "day")

    if source == "ree":
        return RawObjectKey(
            source="ree",
            dataset="balance-electrico",
            date=requested_day,
        )
    if source == "aemet":
        return RawObjectKey(
            source="aemet",
            dataset="climatologia-diaria",
            date=requested_day,
        )

    raise ValueError(f"Unsupported source: {source}")


def resolve_sources(source: Source) -> list[Source]:
    if source == "all":
        return ["ree", "aemet"]
    if source in {"ree", "aemet"}:
        return [source]

    raise ValueError(f"Unsupported source: {source}")


def build_ingest_function(
    source: Source,
    raw_root: Path,
    api_key: str | None,
    storage: RawStorage | None = None,
) -> IngestFunction:
    storage = storage or LocalRawStorage(raw_root)
    if source == "ree":
        return lambda start_date, end_date: ingest_energy_balance(
            start_date=start_date,
            end_date=end_date,
            raw_root=raw_root,
            storage=storage,
        )

    if source == "aemet":
        if not api_key:
            raise ValueError("AEMET_API_KEY environment variable is required")

        return lambda start_date, end_date: ingest_daily_climatology(
            start_date=start_date,
            end_date=end_date,
            api_key=api_key,
            raw_root=raw_root,
            storage=storage,
        )

    raise ValueError(f"Unsupported source: {source}")


def backfill_source(
    source: Source,
    start_date: str,
    end_date: str,
    raw_root: Path,
    ingest: IngestFunction,
    storage: RawStorage | None = None,
    skip_existing: bool = True,
    continue_on_error: bool = True,
    sleep_seconds: float = 0.0,
) -> BackfillResult:
    storage = storage or LocalRawStorage(raw_root)
    downloaded = 0
    skipped = 0
    failed = 0

    for day in iter_days(start_date, end_date):
        key = raw_object_key(source, day)

        if skip_existing and storage.exists(key):
            skipped += 1
            logger.info(
                "[%s] Skipping existing raw file for %s: %s",
                source,
                day,
                storage.data_uri(key),
            )
            continue

        try:
            saved_path = ingest(day, day)
        except (ExtractionError, ValueError) as error:
            failed += 1
            logger.error("[%s] Failed to ingest %s: %s", source, day, error)
            if not continue_on_error:
                raise
        else:
            downloaded += 1
            logger.info("[%s] Saved raw file for %s: %s", source, day, saved_path)

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    return BackfillResult(
        source=source,
        downloaded=downloaded,
        skipped=skipped,
        failed=failed,
    )


def parse_args() -> argparse.Namespace:
    load_dotenv()

    yesterday = date.today() - timedelta(days=1)

    parser = argparse.ArgumentParser(
        description="Backfill REE and AEMET raw daily data.",
    )
    parser.add_argument(
        "--source",
        choices=["ree", "aemet", "all"],
        default="all",
        help="Source to ingest.",
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--end-date",
        default=yesterday.isoformat(),
        help="End date in YYYY-MM-DD format. Defaults to yesterday.",
    )
    parser.add_argument(
        "--raw-root",
        default=parse_raw_root(os.getenv("RAW_ROOT", "data/raw")),
        type=parse_raw_root,
        help="Root directory where raw files will be stored.",
    )
    parser.add_argument(
        "--raw-storage-backend",
        default=parse_raw_storage_backend(os.getenv("RAW_STORAGE_BACKEND", "local")),
        type=parse_raw_storage_backend,
        choices=["local", "s3"],
        help="Raw storage backend.",
    )
    parser.add_argument(
        "--raw-bucket",
        default=os.getenv("RAW_BUCKET"),
        help="S3 bucket used when --raw-storage-backend=s3.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Download files even when the destination already exists.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop the backfill on the first failed day.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.0,
        help="Seconds to wait between API calls.",
    )

    args = parser.parse_args()
    args.raw_bucket = parse_raw_bucket(args.raw_bucket, args.raw_storage_backend)

    return args


def run() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    try:
        args = parse_args()
        validate_date_range(args.start_date, args.end_date)
        storage = build_raw_storage(
            backend=args.raw_storage_backend,
            raw_root=args.raw_root,
            raw_bucket=args.raw_bucket,
        )

        results = []
        for source in resolve_sources(args.source):
            ingest = build_ingest_function(
                source=source,
                raw_root=args.raw_root,
                api_key=os.getenv("AEMET_API_KEY"),
                storage=storage,
            )
            results.append(
                backfill_source(
                    source=source,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    raw_root=args.raw_root,
                    ingest=ingest,
                    storage=storage,
                    skip_existing=not args.overwrite,
                    continue_on_error=not args.fail_fast,
                    sleep_seconds=args.sleep_seconds,
                )
            )
    except (ExtractionError, ValueError) as error:
        logger.error("%s", error)
        return 1

    total_failed = 0
    for result in results:
        total_failed += result.failed
        logger.info(
            "[%s] Backfill finished: downloaded=%s skipped=%s failed=%s",
            result.source,
            result.downloaded,
            result.skipped,
            result.failed,
        )

    return 1 if total_failed else 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
