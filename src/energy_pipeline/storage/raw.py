import json
from pathlib import Path


def save_raw_json(payload: dict, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)

    temporary_path = destination.with_suffix(f"{destination.suffix}.tmp")

    try:
        with temporary_path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)

        temporary_path.replace(destination)
    finally:
        temporary_path.unlink(missing_ok=True)
