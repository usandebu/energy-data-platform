from pathlib import Path


def parse_raw_storage_backend(value: str) -> str:
    normalized = value.strip().lower()

    if normalized not in {"local", "s3"}:
        raise ValueError("RAW_STORAGE_BACKEND must be local or s3")

    return normalized


def parse_raw_root(value: str) -> Path:
    if not value.strip():
        raise ValueError("RAW_ROOT cannot be empty")

    return Path(value)


def parse_raw_bucket(value: str | None, backend: str) -> str | None:
    if backend != "s3":
        return None

    if value is None or not value.strip():
        raise ValueError("RAW_BUCKET is required when RAW_STORAGE_BACKEND=s3")

    return value.strip()
