from datetime import date


def parse_iso_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{field_name} must use YYYY-MM-DD format") from error


def validate_date_range(start_date: str, end_date: str) -> None:
    parsed_start = parse_iso_date(start_date, "start_date")
    parsed_end = parse_iso_date(end_date, "end_date")

    if parsed_start > parsed_end:
        raise ValueError("start_date must be before or equal to end_date")
