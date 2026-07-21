from pathlib import Path

from energy_pipeline.storage.base import RawStorage
from energy_pipeline.storage.local import LocalRawStorage
from energy_pipeline.storage.s3 import S3RawStorage


def build_raw_storage(
    backend: str,
    raw_root: Path,
    raw_bucket: str | None = None,
) -> RawStorage:
    if backend == "local":
        return LocalRawStorage(raw_root)

    if backend == "s3":
        if not raw_bucket:
            raise ValueError("RAW_BUCKET is required when RAW_STORAGE_BACKEND=s3")

        return S3RawStorage(bucket=raw_bucket)

    raise ValueError("RAW_STORAGE_BACKEND must be local or s3")
