import json
import logging
from pathlib import Path
from unittest.mock import Mock

from energy_pipeline.ingest import aemet
from energy_pipeline.ingest.aemet import ingest_daily_climatology


def test_ingest_daily_climatology_fetches_and_saves_raw_payload(tmp_path):
    payload = [
        {
            "fecha": "2024-01-01",
            "indicativo": "3195",
            "nombre": "MADRID, RETIRO",
            "tmed": "8,4",
        }
    ]

    metadata_response = Mock()
    metadata_response.json.return_value = {
        "datos": "https://opendata.aemet.es/opendata/sh/example",
    }

    data_response = Mock()
    data_response.json.return_value = payload

    session = Mock()
    session.get.side_effect = [metadata_response, data_response]

    destination = ingest_daily_climatology(
        start_date="2024-01-01",
        end_date="2024-01-01",
        api_key="fake-api-key",
        raw_root=tmp_path,
        session=session,
    )

    expected_destination = (
        tmp_path
        / "aemet"
        / "climatologia-diaria"
        / "year=2024"
        / "month=01"
        / "day=01"
        / "data.json"
    )
    expected_metadata = expected_destination.with_name("metadata.json")

    assert destination == str(expected_destination)
    assert json.loads(expected_destination.read_text(encoding="utf-8")) == {"data": payload}
    assert json.loads(expected_metadata.read_text(encoding="utf-8"))["requested_date"] == (
        "2024-01-01"
    )
    metadata_response.raise_for_status.assert_called_once_with()
    data_response.raise_for_status.assert_called_once_with()


def test_ingest_daily_climatology_rejects_ranges(tmp_path):
    session = Mock()

    try:
        ingest_daily_climatology(
            start_date="2024-01-01",
            end_date="2024-01-02",
            api_key="fake-api-key",
            raw_root=tmp_path,
            session=session,
        )
    except ValueError as error:
        assert str(error) == "Raw AEMET ingestion expects a single day; use backfill for ranges"
    else:
        raise AssertionError("Expected ValueError")

    session.get.assert_not_called()


def test_main_ingests_daily_climatology_from_cli_args(monkeypatch, tmp_path, caplog):
    destination = (
        tmp_path
        / "aemet"
        / "climatologia-diaria"
        / "year=2024"
        / "month=01"
        / "day=01"
        / "data.json"
    )
    calls = []

    def fake_ingest_daily_climatology(start_date, end_date, api_key, raw_root, **kwargs):
        calls.append(
            {
                "start_date": start_date,
                "end_date": end_date,
                "api_key": api_key,
                "raw_root": raw_root,
            }
        )
        return destination

    monkeypatch.setattr(aemet, "ingest_daily_climatology", fake_ingest_daily_climatology)
    monkeypatch.setenv("AEMET_API_KEY", "fake-api-key")
    monkeypatch.setattr(
        "sys.argv",
        [
            "aemet",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-02",
            "--raw-root",
            str(tmp_path),
        ],
    )

    with caplog.at_level(logging.INFO, logger=aemet.logger.name):
        exit_code = aemet.run()

    assert exit_code == 0
    assert calls == [
        {
            "start_date": "2024-01-01",
            "end_date": "2024-01-02",
            "api_key": "fake-api-key",
            "raw_root": tmp_path,
        }
    ]
    assert f"Raw AEMET daily climatology saved to {destination}" in caplog.text


def test_run_requires_aemet_api_key(monkeypatch, caplog):
    monkeypatch.setenv("AEMET_API_KEY", "")
    monkeypatch.setattr(
        "sys.argv",
        [
            "aemet",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-02",
        ],
    )

    with caplog.at_level(logging.ERROR, logger=aemet.logger.name):
        exit_code = aemet.run()

    assert exit_code == 1
    assert "AEMET_API_KEY environment variable is required" in caplog.text


def test_main_uses_raw_root_from_environment(monkeypatch, tmp_path):
    calls = []

    def fake_ingest_daily_climatology(start_date, end_date, api_key, raw_root, **kwargs):
        calls.append(raw_root)
        return (
            tmp_path
            / "aemet"
            / "climatologia-diaria"
            / "year=2024"
            / "month=01"
            / "day=01"
            / "data.json"
        )

    env_raw_root = tmp_path / "env-raw"

    monkeypatch.setattr(aemet, "ingest_daily_climatology", fake_ingest_daily_climatology)
    monkeypatch.setenv("AEMET_API_KEY", "fake-api-key")
    monkeypatch.setenv("RAW_ROOT", str(env_raw_root))
    monkeypatch.setattr(
        "sys.argv",
        [
            "aemet",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-02",
        ],
    )

    exit_code = aemet.run()

    assert exit_code == 0
    assert calls == [env_raw_root]


def test_main_uses_default_raw_root_without_cli_arg_or_environment(monkeypatch):
    calls = []

    def fake_ingest_daily_climatology(start_date, end_date, api_key, raw_root, **kwargs):
        calls.append(raw_root)
        return Path(
            "data/raw/aemet/climatologia-diaria/year=2024/month=01/day=01/data.json"
        )

    monkeypatch.setattr(aemet, "ingest_daily_climatology", fake_ingest_daily_climatology)
    monkeypatch.setenv("AEMET_API_KEY", "fake-api-key")
    monkeypatch.delenv("RAW_ROOT", raising=False)
    monkeypatch.setattr(
        "sys.argv",
        [
            "aemet",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-02",
        ],
    )

    exit_code = aemet.run()

    assert exit_code == 0
    assert calls == [Path("data/raw")]


def test_run_logs_value_error_and_returns_failure(monkeypatch, caplog):
    def fake_ingest_daily_climatology(start_date, end_date, api_key, raw_root, **kwargs):
        raise ValueError("start_date must be before or equal to end_date")

    monkeypatch.setattr(aemet, "ingest_daily_climatology", fake_ingest_daily_climatology)
    monkeypatch.setenv("AEMET_API_KEY", "fake-api-key")
    monkeypatch.setattr(
        "sys.argv",
        [
            "aemet",
            "--start-date",
            "2024-01-02",
            "--end-date",
            "2024-01-01",
        ],
    )

    with caplog.at_level(logging.ERROR, logger=aemet.logger.name):
        exit_code = aemet.run()

    assert exit_code == 1
    assert "start_date must be before or equal to end_date" in caplog.text


def test_run_rejects_empty_raw_root(monkeypatch, caplog):
    monkeypatch.setenv("RAW_ROOT", "")
    monkeypatch.setenv("AEMET_API_KEY", "fake-api-key")
    monkeypatch.setattr(
        "sys.argv",
        [
            "aemet",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-02",
        ],
    )

    with caplog.at_level(logging.ERROR, logger=aemet.logger.name):
        exit_code = aemet.run()

    assert exit_code == 1
    assert "RAW_ROOT cannot be empty" in caplog.text
