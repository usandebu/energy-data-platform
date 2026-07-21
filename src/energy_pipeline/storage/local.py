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
