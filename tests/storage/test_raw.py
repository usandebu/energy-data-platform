import json
from unittest.mock import patch

import pytest

from energy_pipeline.storage.raw import save_raw_json


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
