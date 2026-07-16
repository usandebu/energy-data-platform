import pytest

from energy_pipeline.extract.dates import parse_iso_date, validate_date_range


def test_parse_iso_date_accepts_valid_date():
    result = parse_iso_date("2024-01-31", "start_date")

    assert result.isoformat() == "2024-01-31"


@pytest.mark.parametrize(
    ("value", "field_name", "expected_message"),
    [
        ("2024/01/31", "start_date", "start_date must use YYYY-MM-DD format"),
        ("2024-02-30", "end_date", "end_date must use YYYY-MM-DD format"),
    ],
)
def test_parse_iso_date_rejects_invalid_date(value, field_name, expected_message):
    with pytest.raises(ValueError, match=expected_message):
        parse_iso_date(value, field_name)


def test_validate_date_range_accepts_same_day():
    validate_date_range("2024-01-01", "2024-01-01")


def test_validate_date_range_accepts_ordered_range():
    validate_date_range("2024-01-01", "2024-01-02")


def test_validate_date_range_rejects_start_date_after_end_date():
    with pytest.raises(
        ValueError,
        match="start_date must be before or equal to end_date",
    ):
        validate_date_range("2024-01-02", "2024-01-01")
