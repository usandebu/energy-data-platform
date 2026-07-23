from datetime import date
from pathlib import Path

from energy_pipeline.storage.raw import RawObjectKey, raw_data_path, save_raw_object


class LocalRawStorage:
    def __init__(self, raw_root: Path = Path("data/raw")) -> None:
        self.raw_root = raw_root

    def exists(self, key: RawObjectKey) -> bool:
        return raw_data_path(self.raw_root, key).exists()

    def save_object(self, payload: dict, key: RawObjectKey) -> str:
        return str(
            save_raw_object(
                payload=payload,
                key=key,
                raw_root=self.raw_root,
            )
        )

    def data_uri(self, key: RawObjectKey) -> str:
        return str(raw_data_path(self.raw_root, key))

    def latest_date(self, source: str, dataset: str) -> date | None:
        latest: date | None = None

        for path in (self.raw_root / source / dataset).glob(
            "year=*/month=*/day=*/data.json"
        ):
            raw_date = _date_from_partitioned_path(path)
            if raw_date is None:
                continue
            if latest is None or raw_date > latest:
                latest = raw_date

        return latest


def _date_from_partitioned_path(path: Path) -> date | None:
    try:
        year = int(path.parents[2].name.removeprefix("year="))
        month = int(path.parents[1].name.removeprefix("month="))
        day = int(path.parent.name.removeprefix("day="))
        return date(year, month, day)
    except (IndexError, ValueError):
        return None
