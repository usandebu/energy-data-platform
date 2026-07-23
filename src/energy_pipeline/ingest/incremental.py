import argparse
import logging
import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

from energy_pipeline.extract.dates import parse_iso_date
from energy_pipeline.extract.errors import ExtractionError
from energy_pipeline.ingest.backfill import (
    BackfillResult,
    backfill_source,
    build_ingest_function,
    resolve_sources,
)
from energy_pipeline.ingest.config import (
    parse_raw_bucket,
    parse_raw_root,
    parse_raw_storage_backend,
)
from energy_pipeline.storage.base import RawStorage
from energy_pipeline.storage.factory import build_raw_storage

logger = logging.getLogger(__name__)

Source = str

DEFAULT_SOURCE_LAG_DAYS: dict[Source, int] = {
    "ree": 1,
    "aemet": 3,
}

DATASET_BY_SOURCE: dict[Source, str] = {
    "ree": "balance-electrico",
    "aemet": "climatologia-diaria",
}


@dataclass(frozen=True)
class IncrementalPlan:
    source: Source
    latest_raw_date: date | None
    target_end_date: date
    start_date: date | None
    end_date: date | None

    @property
    def has_work(self) -> bool:
        return self.start_date is not None and self.end_date is not None


def build_incremental_plan(
    source: Source,
    storage: RawStorage,
    today: date,
    source_lag_days: dict[Source, int] | None = None,
    bootstrap_start_date: date | None = None,
) -> IncrementalPlan:
    source_lag_days = source_lag_days or DEFAULT_SOURCE_LAG_DAYS
    dataset = DATASET_BY_SOURCE[source]
    latest_raw_date = storage.latest_date(source, dataset)
    target_end_date = today - timedelta(days=source_lag_days[source])

    if latest_raw_date is None:
        if bootstrap_start_date is None:
            raise ValueError(
                f"[{source}] No raw files found. "
                "Provide --bootstrap-start-date for the first incremental run."
            )
        start_date = bootstrap_start_date
    else:
        start_date = latest_raw_date + timedelta(days=1)

    if start_date > target_end_date:
        return IncrementalPlan(
            source=source,
            latest_raw_date=latest_raw_date,
            target_end_date=target_end_date,
            start_date=None,
            end_date=None,
        )

    return IncrementalPlan(
        source=source,
        latest_raw_date=latest_raw_date,
        target_end_date=target_end_date,
        start_date=start_date,
        end_date=target_end_date,
    )


def run_incremental_source(
    source: Source,
    raw_root: Path,
    storage: RawStorage,
    api_key: str | None,
    today: date,
    source_lag_days: dict[Source, int] | None = None,
    bootstrap_start_date: date | None = None,
    fail_fast: bool = False,
    sleep_seconds: float = 0.0,
) -> BackfillResult:
    plan = build_incremental_plan(
        source=source,
        storage=storage,
        today=today,
        source_lag_days=source_lag_days,
        bootstrap_start_date=bootstrap_start_date,
    )

    logger.info(
        "[%s] latest_raw_date=%s target_end_date=%s",
        source,
        plan.latest_raw_date,
        plan.target_end_date,
    )

    if not plan.has_work:
        logger.info("[%s] No pending raw dates to ingest", source)
        return BackfillResult(source=source)

    logger.info(
        "[%s] Ingesting pending raw dates from %s to %s",
        source,
        plan.start_date,
        plan.end_date,
    )

    ingest = build_ingest_function(
        source=source,
        raw_root=raw_root,
        api_key=api_key,
        storage=storage,
    )
    return backfill_source(
        source=source,
        start_date=plan.start_date.isoformat(),
        end_date=plan.end_date.isoformat(),
        raw_root=raw_root,
        ingest=ingest,
        storage=storage,
        skip_existing=True,
        continue_on_error=not fail_fast,
        sleep_seconds=sleep_seconds,
    )


def parse_args() -> argparse.Namespace:
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Ingest pending REE and AEMET raw daily data.",
    )
    parser.add_argument(
        "--source",
        choices=["ree", "aemet", "all"],
        default="all",
        help="Source to ingest.",
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
        "--bootstrap-start-date",
        type=lambda value: parse_iso_date(value, "bootstrap_start_date"),
        help="First date to ingest when no raw files exist yet.",
    )
    parser.add_argument(
        "--ree-lag-days",
        type=int,
        default=DEFAULT_SOURCE_LAG_DAYS["ree"],
        help="Days to wait before considering REE data available.",
    )
    parser.add_argument(
        "--aemet-lag-days",
        type=int,
        default=DEFAULT_SOURCE_LAG_DAYS["aemet"],
        help="Days to wait before considering AEMET data available.",
    )
    parser.add_argument(
        "--today",
        type=lambda value: parse_iso_date(value, "today"),
        default=date.today(),
        help="Override today's date in YYYY-MM-DD format, mostly for testing.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop the incremental ingestion on the first failed day.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.0,
        help="Seconds to wait between API calls.",
    )

    args = parser.parse_args()
    args.raw_bucket = parse_raw_bucket(args.raw_bucket, args.raw_storage_backend)

    if args.ree_lag_days < 0 or args.aemet_lag_days < 0:
        raise ValueError("Lag days must be greater than or equal to zero")

    return args


def run() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    try:
        args = parse_args()
        storage = build_raw_storage(
            backend=args.raw_storage_backend,
            raw_root=args.raw_root,
            raw_bucket=args.raw_bucket,
        )
        source_lag_days = {
            "ree": args.ree_lag_days,
            "aemet": args.aemet_lag_days,
        }

        results = []
        for source in resolve_sources(args.source):
            results.append(
                run_incremental_source(
                    source=source,
                    raw_root=args.raw_root,
                    storage=storage,
                    api_key=os.getenv("AEMET_API_KEY"),
                    today=args.today,
                    source_lag_days=source_lag_days,
                    bootstrap_start_date=args.bootstrap_start_date,
                    fail_fast=args.fail_fast,
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
            "[%s] Incremental finished: downloaded=%s skipped=%s failed=%s",
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
