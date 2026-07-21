import json
from datetime import UTC, date, datetime
from unittest.mock import patch

import pytest

from energy_pipeline.storage.local import LocalRawStorage
from energy_pipeline.storage.raw import (
    RawObjectKey,
    raw_data_path,
    raw_metadata_path,
    raw_object_directory,
    save_raw_json,
    save_raw_object,
)


def test_raw_object_directory_returns_partitioned_path(tmp_path):
    key = RawObjectKey(
        source="ree",
        dataset="balance-electrico",
        date=date(2024, 1, 2),
    )

    assert raw_object_directory(tmp_path, key) == (
        tmp_path
        / "ree"
        / "balance-electrico"
        / "year=2024"
        / "month=01"
        / "day=02"
    )


def test_raw_data_and_metadata_paths_use_standard_file_names(tmp_path):
    key = RawObjectKey(
        source="aemet",
        dataset="climatologia-diaria",
        date=date(2024, 1, 2),
    )

    assert raw_data_path(tmp_path, key).name == "data.json"
    assert raw_metadata_path(tmp_path, key).name == "metadata.json"


def test_save_raw_json_creates_directories_and_file(tmp_path):
    payload = {
        "data": {"type": "Balance de energía eléctrica"},
        "included": [],
    }
    destination = tmp_path / "data" / "raw" / "ree" / "balance.json"

    save_raw_json(payload, destination)

    assert destination.exists()
    assert json.loads(destination.read_text(encoding="utf-8")) == payload
    assert not destination.with_suffix(".json.tmp").exists()


def test_save_raw_json_replaces_existing_file(tmp_path):
    destination = tmp_path / "balance.json"
    destination.write_text('{"old": true}', encoding="utf-8")
    new_payload = {"new": True}

    save_raw_json(new_payload, destination)

    assert json.loads(destination.read_text(encoding="utf-8")) == new_payload


def test_save_raw_json_removes_temporary_file_if_write_fails(tmp_path):
    destination = tmp_path / "balance.json"
    temporary_path = destination.with_suffix(".json.tmp")

    with (
        patch("energy_pipeline.storage.raw.json.dump", side_effect=OSError("disk error")),
        pytest.raises(OSError, match="disk error"),
    ):
        save_raw_json({"data": {}}, destination)

    assert not destination.exists()
    assert not temporary_path.exists()


def test_save_raw_object_writes_data_and_metadata(tmp_path):
    payload = {
        "data": {"type": "Balance de energía eléctrica"},
        "included": [],
    }
    key = RawObjectKey(
        source="ree",
        dataset="balance-electrico",
        date=date(2024, 1, 1),
    )

    destination = save_raw_object(
        payload=payload,
        key=key,
        raw_root=tmp_path,
        downloaded_at=datetime(2026, 7, 20, 18, 30, 12, tzinfo=UTC),
    )

    expected_data_path = (
        tmp_path
        / "ree"
        / "balance-electrico"
        / "year=2024"
        / "month=01"
        / "day=01"
        / "data.json"
    )
    expected_metadata_path = expected_data_path.with_name("metadata.json")

    assert destination == expected_data_path
    assert json.loads(expected_data_path.read_text(encoding="utf-8")) == payload
    assert json.loads(expected_metadata_path.read_text(encoding="utf-8")) == {
        "source": "ree",
        "dataset": "balance-electrico",
        "requested_date": "2024-01-01",
        "downloaded_at": "2026-07-20T18:30:12Z",
        "content_length": len(
            json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        ),
    }


def test_local_raw_storage_saves_object_and_checks_existence(tmp_path):
    payload = {"data": {"type": "Balance de energía eléctrica"}, "included": []}
    key = RawObjectKey(
        source="ree",
        dataset="balance-electrico",
        date=date(2024, 1, 1),
    )
    storage = LocalRawStorage(tmp_path)

    assert storage.exists(key) is False

    destination = storage.save_object(payload, key)

    assert destination == str(raw_data_path(tmp_path, key))
    assert storage.exists(key) is True
    assert json.loads(raw_data_path(tmp_path, key).read_text(encoding="utf-8")) == payload
