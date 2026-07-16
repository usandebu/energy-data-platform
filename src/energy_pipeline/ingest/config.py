from pathlib import Path


def parse_raw_root(value: str) -> Path:
    if not value.strip():
        raise ValueError("RAW_ROOT cannot be empty")

    return Path(value)
