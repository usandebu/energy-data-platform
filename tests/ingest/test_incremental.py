from datetime import date
from pathlib import Path
from unittest.mock import Mock

import pytest

from energy_pipeline.ingest import incremental
from energy_pipeline.ingest.backfill import BackfillResult


def test_build_incremental_plan_uses_latest_raw_date_and_source_lag():
    storage = Mock()
    storage.latest_date.return_value = date(2026, 7, 20)

    plan = incremental.build_incremental_plan(
        source="ree",
        storage=storage,
        today=date(2026, 7, 23),
    )

    assert plan.latest_raw_date == date(2026, 7, 20)
    assert plan.target_end_date == date(2026, 7, 22)
    assert plan.start_date == date(2026, 7, 21)
    assert plan.end_date == date(2026, 7, 22)
    assert plan.has_work is True
    storage.latest_date.assert_called_once_with("ree", "balance-electrico")


def test_build_incremental_plan_respects_aemet_default_lag():
    storage = Mock()
    storage.latest_date.return_value = date(2026, 7, 19)

    plan = incremental.build_incremental_plan(
        source="aemet",
        storage=storage,
        today=date(2026, 7, 23),
    )

    assert plan.target_end_date == date(2026, 7, 20)
    assert plan.start_date == date(2026, 7, 20)
    assert plan.end_date == date(2026, 7, 20)


def test_build_incremental_plan_has_no_work_when_source_is_up_to_date():
    storage = Mock()
    storage.latest_date.return_value = date(2026, 7, 22)

    plan = incremental.build_incremental_plan(
        source="ree",
        storage=storage,
        today=date(2026, 7, 23),
    )

    assert plan.has_work is False
    assert plan.start_date is None
    assert plan.end_date is None


def test_build_incremental_plan_requires_bootstrap_start_date_when_empty():
    storage = Mock()
    storage.latest_date.return_value = None

    with pytest.raises(ValueError, match="bootstrap-start-date"):
        incremental.build_incremental_plan(
            source="ree",
            storage=storage,
            today=date(2026, 7, 23),
        )


def test_build_incremental_plan_uses_bootstrap_start_date_when_empty():
    storage = Mock()
    storage.latest_date.return_value = None

    plan = incremental.build_incremental_plan(
        source="ree",
        storage=storage,
        today=date(2026, 7, 23),
        bootstrap_start_date=date(2026, 7, 1),
    )

    assert plan.start_date == date(2026, 7, 1)
    assert plan.end_date == date(2026, 7, 22)


def test_run_incremental_source_skips_backfill_when_no_work(monkeypatch):
    storage = Mock()
    storage.latest_date.return_value = date(2026, 7, 22)
    backfill = Mock()
    monkeypatch.setattr(incremental, "backfill_source", backfill)

    result = incremental.run_incremental_source(
        source="ree",
        raw_root=Path("data/raw"),
        storage=storage,
        api_key=None,
        today=date(2026, 7, 23),
    )

    assert result == BackfillResult(source="ree")
    backfill.assert_not_called()


def test_run_incremental_source_calls_backfill_for_pending_dates(monkeypatch):
    storage = Mock()
    storage.latest_date.return_value = date(2026, 7, 20)
    ingest = Mock()
    build_ingest = Mock(return_value=ingest)
    backfill = Mock(return_value=BackfillResult(source="ree", downloaded=2))
    monkeypatch.setattr(incremental, "build_ingest_function", build_ingest)
    monkeypatch.setattr(incremental, "backfill_source", backfill)

    result = incremental.run_incremental_source(
        source="ree",
        raw_root=Path("data/raw"),
        storage=storage,
        api_key=None,
        today=date(2026, 7, 23),
    )

    assert result == BackfillResult(source="ree", downloaded=2)
    build_ingest.assert_called_once_with(
        source="ree",
        raw_root=Path("data/raw"),
        api_key=None,
        storage=storage,
    )
    backfill.assert_called_once_with(
        source="ree",
        start_date="2026-07-21",
        end_date="2026-07-22",
        raw_root=Path("data/raw"),
        ingest=ingest,
        storage=storage,
        skip_existing=True,
        continue_on_error=True,
        sleep_seconds=0.0,
    )
