from unittest.mock import Mock

import pytest
import requests

from energy_pipeline.extract.aemet import BASE_URL, fetch_daily_climatology
from energy_pipeline.extract.errors import ExtractionError


def test_fetch_daily_climatology_returns_payload():
    expected_payload = [
        {
            "fecha": "2024-01-01",
            "indicativo": "3195",
            "nombre": "MADRID, RETIRO",
            "tmed": "8,4",
        }
    ]

    metadata_response = Mock()
    metadata_response.json.return_value = {
        "descripcion": "exito",
        "estado": 200,
        "datos": "https://opendata.aemet.es/opendata/sh/example",
    }

    data_response = Mock()
    data_response.json.return_value = expected_payload

    session = Mock()
    session.get.side_effect = [metadata_response, data_response]

    result = fetch_daily_climatology(
        start_date="2024-01-01",
        end_date="2024-01-02",
        api_key="fake-api-key",
        session=session,
    )

    assert result == expected_payload
    assert session.get.call_args_list[0].args == (
        f"{BASE_URL}/valores/climatologicos/diarios/datos/"
        "fechaini/2024-01-01T00:00:00UTC/"
        "fechafin/2024-01-02T23:59:59UTC/"
        "todasestaciones",
    )
    assert session.get.call_args_list[0].kwargs == {
        "headers": {"api_key": "fake-api-key"},
        "timeout": 10,
    }
    assert session.get.call_args_list[1].args == (
        "https://opendata.aemet.es/opendata/sh/example",
    )
    assert session.get.call_args_list[1].kwargs == {"timeout": 10}
    metadata_response.raise_for_status.assert_called_once_with()
    data_response.raise_for_status.assert_called_once_with()


def test_fetch_daily_climatology_rejects_metadata_without_data_url():
    metadata_response = Mock()
    metadata_response.json.return_value = {"descripcion": "Sin datos"}

    session = Mock()
    session.get.return_value = metadata_response

    with pytest.raises(
        ExtractionError,
        match="Unexpected AEMET metadata response structure",
    ):
        fetch_daily_climatology(
            start_date="2024-01-01",
            end_date="2024-01-02",
            api_key="fake-api-key",
            session=session,
        )

    metadata_response.raise_for_status.assert_called_once_with()


def test_fetch_daily_climatology_rejects_non_list_payload():
    metadata_response = Mock()
    metadata_response.json.return_value = {
        "datos": "https://opendata.aemet.es/opendata/sh/example",
    }

    data_response = Mock()
    data_response.json.return_value = {"message": "Unexpected response"}

    session = Mock()
    session.get.side_effect = [metadata_response, data_response]

    with pytest.raises(
        ExtractionError,
        match="Unexpected AEMET data response structure",
    ):
        fetch_daily_climatology(
            start_date="2024-01-01",
            end_date="2024-01-02",
            api_key="fake-api-key",
            session=session,
        )

    metadata_response.raise_for_status.assert_called_once_with()
    data_response.raise_for_status.assert_called_once_with()


def test_fetch_daily_climatology_wraps_metadata_http_errors():
    metadata_response = Mock()
    metadata_response.raise_for_status.side_effect = requests.HTTPError(
        "500 Server Error"
    )

    session = Mock()
    session.get.return_value = metadata_response

    with pytest.raises(ExtractionError, match="Failed to fetch AEMET metadata"):
        fetch_daily_climatology(
            start_date="2024-01-01",
            end_date="2024-01-02",
            api_key="fake-api-key",
            session=session,
        )


def test_fetch_daily_climatology_wraps_metadata_invalid_json():
    metadata_response = Mock()
    metadata_response.json.side_effect = ValueError("invalid json")

    session = Mock()
    session.get.return_value = metadata_response

    with pytest.raises(
        ExtractionError,
        match="AEMET metadata response is not valid JSON",
    ):
        fetch_daily_climatology(
            start_date="2024-01-01",
            end_date="2024-01-02",
            api_key="fake-api-key",
            session=session,
        )


def test_fetch_daily_climatology_wraps_data_http_errors():
    metadata_response = Mock()
    metadata_response.json.return_value = {
        "datos": "https://opendata.aemet.es/opendata/sh/example",
    }

    data_response = Mock()
    data_response.raise_for_status.side_effect = requests.HTTPError("500 Server Error")

    session = Mock()
    session.get.side_effect = [metadata_response, data_response]

    with pytest.raises(ExtractionError, match="Failed to fetch AEMET data"):
        fetch_daily_climatology(
            start_date="2024-01-01",
            end_date="2024-01-02",
            api_key="fake-api-key",
            session=session,
        )


def test_fetch_daily_climatology_wraps_data_invalid_json():
    metadata_response = Mock()
    metadata_response.json.return_value = {
        "datos": "https://opendata.aemet.es/opendata/sh/example",
    }

    data_response = Mock()
    data_response.json.side_effect = ValueError("invalid json")

    session = Mock()
    session.get.side_effect = [metadata_response, data_response]

    with pytest.raises(
        ExtractionError,
        match="AEMET data response is not valid JSON",
    ):
        fetch_daily_climatology(
            start_date="2024-01-01",
            end_date="2024-01-02",
            api_key="fake-api-key",
            session=session,
        )


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
def test_fetch_daily_climatology_rejects_invalid_date_range(
    start_date,
    end_date,
    expected_message,
):
    session = Mock()

    with pytest.raises(ValueError, match=expected_message):
        fetch_daily_climatology(
            start_date=start_date,
            end_date=end_date,
            api_key="fake-api-key",
            session=session,
        )

    session.get.assert_not_called()
