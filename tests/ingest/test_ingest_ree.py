import json
from unittest.mock import Mock

from energy_pipeline.ingest.ree import ingest_energy_balance


def test_ingest_energy_balance_fetches_and_saves_raw_payload(tmp_path):
    payload = {
        "data": {"type": "Balance de energía eléctrica"},
        "included": [],
    }

    response = Mock()
    response.json.return_value = payload

    session = Mock()
    session.get.return_value = response

    destination = ingest_energy_balance(
        start_date="2024-01-01",
        end_date="2024-01-02",
        raw_root=tmp_path,
        session=session,
    )

    expected_destination = (
        tmp_path
        / "ree"
        / "balance-electrico"
        / "2024-01-01_2024-01-02.json"
    )

    assert destination == expected_destination
    assert json.loads(destination.read_text(encoding="utf-8")) == payload
    response.raise_for_status.assert_called_once_with()
