import logging
from unittest.mock import Mock

from energy_pipeline.ingest import backfill
from energy_pipeline.ingest.backfill import BackfillResult


def test_iter_days_returns_inclusive_date_range():
    assert list(backfill.iter_days("2024-01-01", "2024-01-03")) == [
        "2024-01-01",
        "2024-01-02",
        "2024-01-03",
    ]


def test_raw_destination_returns_ree_path(tmp_path):
    assert backfill.raw_destination("ree", "2024-01-01", tmp_path) == (
        tmp_path
        / "ree"
        / "balance-electrico"
        / "year=2024"
        / "month=01"
        / "day=01"
        / "data.json"
    )


def test_raw_destination_returns_aemet_path(tmp_path):
    assert backfill.raw_destination("aemet", "2024-01-01", tmp_path) == (
        tmp_path
        / "aemet"
        / "climatologia-diaria"
        / "year=2024"
        / "month=01"
        / "day=01"
        / "data.json"
    )


def test_backfill_source_downloads_each_missing_day(tmp_path):
    calls = []

    def fake_ingest(start_date, end_date):
        calls.append((start_date, end_date))
        destination = backfill.raw_destination("ree", start_date, tmp_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text("{}", encoding="utf-8")
        return str(destination)

    result = backfill.backfill_source(
        source="ree",
        start_date="2024-01-01",
        end_date="2024-01-02",
        raw_root=tmp_path,
        ingest=fake_ingest,
    )

    assert calls == [
        ("2024-01-01", "2024-01-01"),
        ("2024-01-02", "2024-01-02"),
    ]
    assert result == BackfillResult(source="ree", downloaded=2, skipped=0, failed=0)


def test_backfill_source_skips_existing_files(tmp_path):
    existing = backfill.raw_destination("ree", "2024-01-01", tmp_path)
    existing.parent.mkdir(parents=True)
    existing.write_text("{}", encoding="utf-8")

    calls = []

    def fake_ingest(start_date, end_date):
        calls.append((start_date, end_date))
        return "unused"

    result = backfill.backfill_source(
        source="ree",
        start_date="2024-01-01",
        end_date="2024-01-01",
        raw_root=tmp_path,
        ingest=fake_ingest,
    )

    assert calls == []
    assert result == BackfillResult(source="ree", downloaded=0, skipped=1, failed=0)


def test_backfill_source_continues_after_error_by_default(tmp_path, caplog):
    calls = []

    def fake_ingest(start_date, end_date):
        calls.append((start_date, end_date))
        if start_date == "2024-01-01":
            raise ValueError("temporary source error")

        destination = backfill.raw_destination("ree", start_date, tmp_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text("{}", encoding="utf-8")
        return str(destination)

    with caplog.at_level(logging.ERROR, logger=backfill.logger.name):
        result = backfill.backfill_source(
            source="ree",
            start_date="2024-01-01",
            end_date="2024-01-02",
            raw_root=tmp_path,
            ingest=fake_ingest,
        )

    assert calls == [
        ("2024-01-01", "2024-01-01"),
        ("2024-01-02", "2024-01-02"),
    ]
    assert result == BackfillResult(source="ree", downloaded=1, skipped=0, failed=1)
    assert "[ree] Failed to ingest 2024-01-01" in caplog.text


def test_build_ingest_function_requires_aemet_api_key(tmp_path):
    try:
        backfill.build_ingest_function("aemet", tmp_path, api_key=None)
    except ValueError as error:
        assert str(error) == "AEMET_API_KEY environment variable is required"
    else:
        raise AssertionError("Expected ValueError")


def test_backfill_source_uses_storage_to_skip_existing_objects(tmp_path):
    storage = Mock()
    storage.exists.return_value = True
    calls = []

    def fake_ingest(start_date, end_date):
        calls.append((start_date, end_date))
        return "unused"

    result = backfill.backfill_source(
        source="ree",
        start_date="2024-01-01",
        end_date="2024-01-01",
        raw_root=tmp_path,
        ingest=fake_ingest,
        storage=storage,
    )

    assert calls == []
    assert result == BackfillResult(source="ree", downloaded=0, skipped=1, failed=0)
    storage.exists.assert_called_once_with(
        backfill.raw_object_key("ree", "2024-01-01")
    )


def test_main_runs_backfill_for_selected_source(monkeypatch, tmp_path):
    calls = []

    def fake_build_ingest_function(source, raw_root, api_key):
        calls.append(("build", source, raw_root, api_key))
        return lambda start_date, end_date: backfill.raw_destination(
            source,
            start_date,
            tmp_path,
        ).as_posix()

    def fake_backfill_source(**kwargs):
        calls.append(("backfill", kwargs))
        return BackfillResult(source=kwargs["source"], downloaded=1, skipped=0, failed=0)

    monkeypatch.setattr(backfill, "build_ingest_function", fake_build_ingest_function)
    monkeypatch.setattr(backfill, "backfill_source", fake_backfill_source)
    monkeypatch.setenv("AEMET_API_KEY", "fake-api-key")
    monkeypatch.setattr(
        "sys.argv",
        [
            "backfill",
            "--source",
            "aemet",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-01",
            "--raw-root",
            str(tmp_path),
        ],
    )

    exit_code = backfill.run()

    assert exit_code == 0
    assert calls[0] == ("build", "aemet", tmp_path, "fake-api-key")
    assert calls[1][0] == "backfill"
    assert calls[1][1]["source"] == "aemet"
    assert calls[1][1]["start_date"] == "2024-01-01"
    assert calls[1][1]["end_date"] == "2024-01-01"
    assert calls[1][1]["skip_existing"] is True
