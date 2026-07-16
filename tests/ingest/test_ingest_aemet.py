import json
import logging
from pathlib import Path
from unittest.mock import Mock

import pytest

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
        end_date="2024-01-02",
        api_key="fake-api-key",
        raw_root=tmp_path,
        session=session,
    )

    expected_destination = (
        tmp_path
        / "aemet"
        / "daily-climatology"
        / "2024-01-01_2024-01-02.json"
    )

    assert destination == expected_destination
    assert json.loads(destination.read_text(encoding="utf-8")) == {"data": payload}
    metadata_response.raise_for_status.assert_called_once_with()
    data_response.raise_for_status.assert_called_once_with()


def test_main_ingests_daily_climatology_from_cli_args(monkeypatch, tmp_path, caplog):
    destination = tmp_path / "aemet" / "daily-climatology" / "2024-01-01_2024-01-02.json"
    calls = []

    def fake_ingest_daily_climatology(start_date, end_date, api_key, raw_root):
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
        aemet.main()

    assert calls == [
        {
            "start_date": "2024-01-01",
            "end_date": "2024-01-02",
            "api_key": "fake-api-key",
            "raw_root": tmp_path,
        }
    ]
    assert f"Raw AEMET daily climatology saved to {destination}" in caplog.text


def test_main_requires_aemet_api_key(monkeypatch):
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

    with pytest.raises(
        ValueError,
        match="AEMET_API_KEY environment variable is required",
    ):
        aemet.main()


def test_main_uses_raw_root_from_environment(monkeypatch, tmp_path):
    calls = []

    def fake_ingest_daily_climatology(start_date, end_date, api_key, raw_root):
        calls.append(raw_root)
        return tmp_path / "aemet" / "daily-climatology" / "2024-01-01_2024-01-02.json"

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

    aemet.main()

    assert calls == [env_raw_root]


def test_main_uses_default_raw_root_without_cli_arg_or_environment(monkeypatch):
    calls = []

    def fake_ingest_daily_climatology(start_date, end_date, api_key, raw_root):
        calls.append(raw_root)
        return Path("data/raw/aemet/daily-climatology/2024-01-01_2024-01-02.json")

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

    aemet.main()

    assert calls == [Path("data/raw")]
