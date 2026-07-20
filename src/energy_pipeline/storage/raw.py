import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path


@dataclass(frozen=True)
class RawObjectKey:
    source: str
    dataset: str
    date: date


def raw_object_directory(raw_root: Path, key: RawObjectKey) -> Path:
    return (
        raw_root
        / key.source
        / key.dataset
        / f"year={key.date:%Y}"
        / f"month={key.date:%m}"
        / f"day={key.date:%d}"
    )


def raw_data_path(raw_root: Path, key: RawObjectKey) -> Path:
    return raw_object_directory(raw_root, key) / "data.json"


def raw_metadata_path(raw_root: Path, key: RawObjectKey) -> Path:
    return raw_object_directory(raw_root, key) / "metadata.json"


def save_raw_json(payload: dict, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)

    temporary_path = destination.with_suffix(f"{destination.suffix}.tmp")

    try:
        with temporary_path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)

        temporary_path.replace(destination)
    finally:
        temporary_path.unlink(missing_ok=True)


def save_raw_object(
    payload: dict,
    key: RawObjectKey,
    raw_root: Path,
    downloaded_at: datetime | None = None,
) -> Path:
    data_path = raw_data_path(raw_root, key)
    metadata_path = raw_metadata_path(raw_root, key)
    downloaded_at = downloaded_at or datetime.now(UTC)

    save_raw_json(payload, data_path)
    save_raw_json(
        {
            "source": key.source,
            "dataset": key.dataset,
            "requested_date": key.date.isoformat(),
            "downloaded_at": downloaded_at.isoformat().replace("+00:00", "Z"),
            "content_length": _json_content_length(payload),
        },
        metadata_path,
    )

    return data_path


def _json_content_length(payload: dict) -> int:
    return len(
        json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    )
