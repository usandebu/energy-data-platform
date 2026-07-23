from datetime import date
from typing import Protocol

from energy_pipeline.storage.raw import RawObjectKey


class RawStorage(Protocol):
    def exists(self, key: RawObjectKey) -> bool:
        ...

    def save_object(self, payload: dict, key: RawObjectKey) -> str:
        ...

    def data_uri(self, key: RawObjectKey) -> str:
        ...

    def latest_date(self, source: str, dataset: str) -> date | None:
        ...
