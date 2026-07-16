from unittest.mock import Mock

import pytest

from energy_pipeline.extract.ree import BASE_URL, fetch_energy_balance


def test_fetch_energy_balance_returns_payload():
    expected_payload = {
        "data": {"type": "Balance de energía eléctrica"},
        "included": [],
    }

    response = Mock()
    response.json.return_value = expected_payload

    session = Mock()
    session.get.return_value = response

    result = fetch_energy_balance(
        start_date="2024-01-01",
        end_date="2024-01-02",
        session=session,
    )

    assert result == expected_payload

    session.get.assert_called_once_with(
        f"{BASE_URL}/balance/balance-electrico",
        params={
            "start_date": "2024-01-01T00:00",
            "end_date": "2024-01-02T23:59",
            "time_trunc": "day",
        },
        timeout=10,
    )

    response.raise_for_status.assert_called_once_with()
    response.json.assert_called_once_with()


def test_fetch_energy_balance_rejects_unexpected_structure():
    response = Mock()
    response.json.return_value = {"message": "Unexpected response"}

    session = Mock()
    session.get.return_value = response

    with pytest.raises(
        ValueError,
        match="Unexpected REE response structure",
    ):
        fetch_energy_balance(
            start_date="2024-01-01",
            end_date="2024-01-02",
            session=session,
        )

    response.raise_for_status.assert_called_once_with()


@pytest.mark.parametrize(
    ("start_date", "end_date", "expected_message"),
    [
        ("2024/01/01", "2024-01-02", "start_date must use YYYY-MM-DD format"),
        ("2024-01-01", "2024-02-30", "end_date must use YYYY-MM-DD format"),
        (
            "2024-01-02",
            "2024-01-01",
            "start_date must be before or equal to end_date",
        ),
    ],
)
def test_fetch_energy_balance_rejects_invalid_date_range(
    start_date,
    end_date,
    expected_message,
):
    session = Mock()

    with pytest.raises(ValueError, match=expected_message):
        fetch_energy_balance(
            start_date=start_date,
            end_date=end_date,
            session=session,
        )

    session.get.assert_not_called()
