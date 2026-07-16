import json
from unittest.mock import Mock

from energy_pipeline.ingest import ree
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


def test_main_ingests_energy_balance_from_cli_args(monkeypatch, tmp_path, capsys):
    destination = tmp_path / "ree" / "balance-electrico" / "2024-01-01_2024-01-02.json"
    calls = []

    def fake_ingest_energy_balance(start_date, end_date, raw_root):
        calls.append(
            {
                "start_date": start_date,
                "end_date": end_date,
                "raw_root": raw_root,
            }
        )
        return destination

    monkeypatch.setattr(ree, "ingest_energy_balance", fake_ingest_energy_balance)
    monkeypatch.setattr(
        "sys.argv",
        [
            "ree",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-02",
            "--raw-root",
            str(tmp_path),
        ],
    )

    ree.main()

    assert calls == [
        {
            "start_date": "2024-01-01",
            "end_date": "2024-01-02",
            "raw_root": tmp_path,
        }
    ]
    assert capsys.readouterr().out == (
        f"Raw REE energy balance saved to {destination}\n"
    )
